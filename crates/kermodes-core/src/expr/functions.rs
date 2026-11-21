//! Scalar functions: string lengths, `regexp_replace`, and parts and truncation of dates and
//! timestamps, each with DuckDB's semantics (the oracle the SQL front end is checked against).

use std::fmt::Write as _;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, Date32Array, Float64Array, Int64Array, StringBuilder,
    TimestampNanosecondArray,
};
use arrow::compute::cast;
use arrow::compute::kernels::temporal::{DatePart, date_part};
use arrow::datatypes::{DataType, Date32Type, Float64Type, TimeUnit, TimestampNanosecondType};
use arrow::error::ArrowError;
use regex::{CaptureLocations, Regex, RegexBuilder};
use sqlparser::ast::{
    DateTimeField, Expr as Ast, Function, FunctionArg, FunctionArgExpr, FunctionArguments,
    ObjectNamePart, Spanned, Value as Literal, ValueWithSpan,
};

use super::{Binder, Node, edit_distance, quoted};
use crate::{Error, type_name};

const FUNCTIONS: [&str; 5] = [
    "date_part",
    "date_trunc",
    "length",
    "regexp_replace",
    "strlen",
];
const FIELDS: &str = "year, month, day, hour, minute, second, dow, doy, epoch";
const UNITS: &str = "second, minute, hour, day, week, month, quarter, year";
const NANOS_PER_SECOND: i64 = 1_000_000_000;
const NANOS_PER_DAY: i64 = 86_400 * NANOS_PER_SECOND;

impl Binder<'_> {
    pub(super) fn function(
        &self,
        ast: &Ast,
        function: &Function,
    ) -> Result<(Node, DataType), Error> {
        let name = match function.name.0.as_slice() {
            [ObjectNamePart::Identifier(ident)] => ident.value.to_ascii_lowercase(),
            _ => String::new(),
        };
        if !FUNCTIONS.contains(&name.as_str()) {
            let close: Vec<&str> = FUNCTIONS
                .iter()
                .copied()
                .filter(|f| edit_distance(f, &name) <= 2)
                .take(3)
                .collect();
            let hint = if close.is_empty() {
                format!("functions are: {}", FUNCTIONS.join(", "))
            } else {
                format!("did you mean {}?", quoted(&close))
            };
            return Err(self.error(
                ast.span(),
                format!("unknown function `{}`; {hint}", function.name),
            ));
        }
        let args = self.arguments(ast, function, &name)?;
        match (name.as_str(), args.as_slice()) {
            ("length", [text]) => self.length(ast, text, Count::Chars, &name),
            ("strlen", [text]) => self.length(ast, text, Count::Bytes, &name),
            ("regexp_replace", [text, pattern, replacement]) => {
                self.regexp_replace(ast, text, pattern, replacement, None)
            }
            ("regexp_replace", [text, pattern, replacement, options]) => {
                self.regexp_replace(ast, text, pattern, replacement, Some(*options))
            }
            ("date_part", [field, value]) => {
                let part = self.string_literal(field, &name, "part")?;
                self.date_part(ast, field, part, value, &name)
            }
            ("date_trunc", [unit, value]) => self.date_trunc(ast, unit, value, &name),
            _ => {
                let signature = match name.as_str() {
                    "length" => "length(string)",
                    "strlen" => "strlen(string)",
                    "regexp_replace" => "regexp_replace(string, pattern, replacement[, 'g'])",
                    "date_part" => "date_part('part', date or timestamp)",
                    _ => "date_trunc('unit', date or timestamp)",
                };
                Err(self.error(
                    ast.span(),
                    format!(
                        "`{name}` is called as `{signature}`, but got {} arguments",
                        args.len()
                    ),
                ))
            }
        }
    }

    /// `extract(field FROM value)`, which is `date_part('field', value)`.
    pub(super) fn extract(
        &self,
        ast: &Ast,
        field: &DateTimeField,
        value: &Ast,
    ) -> Result<(Node, DataType), Error> {
        let part = match field {
            DateTimeField::Custom(ident) => ident.value.clone(),
            other => other.to_string().to_ascii_lowercase(),
        };
        self.date_part(ast, ast, &part, value, "extract")
    }

    /// The positional arguments of a plain call: no `DISTINCT`, `FILTER`, `OVER`, or named
    /// arguments, which no scalar function takes.
    fn arguments<'f>(
        &self,
        ast: &Ast,
        function: &'f Function,
        name: &str,
    ) -> Result<Vec<&'f Ast>, Error> {
        let plain = matches!(function.parameters, FunctionArguments::None)
            && function.within_group.is_empty()
            && function.filter.is_none()
            && function.null_treatment.is_none()
            && function.over.is_none()
            && !function.uses_odbc_syntax;
        let list = match &function.args {
            FunctionArguments::List(list)
                if plain && list.duplicate_treatment.is_none() && list.clauses.is_empty() =>
            {
                list
            }
            _ => {
                return Err(self.error(
                    ast.span(),
                    format!("`{name}` takes plain arguments: `{ast}` is not supported"),
                ));
            }
        };
        list.args
            .iter()
            .map(|arg| match arg {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Ok(expr),
                other => Err(self.error(
                    other.span(),
                    format!("argument `{other}` of `{name}` is not supported"),
                )),
            })
            .collect()
    }

    /// An argument that must be a string literal, because it is read when binding.
    fn string_literal<'a>(
        &self,
        ast: &'a Ast,
        function: &str,
        what: &str,
    ) -> Result<&'a str, Error> {
        if let Ast::Value(ValueWithSpan {
            value: Literal::SingleQuotedString(text),
            ..
        }) = ast
        {
            return Ok(text.as_str());
        }
        Err(self.error(
            ast.span(),
            format!("the {what} of `{function}` must be a string literal, but is `{ast}`"),
        ))
    }

    fn length(
        &self,
        ast: &Ast,
        text: &Ast,
        count: Count,
        name: &str,
    ) -> Result<(Node, DataType), Error> {
        let text = self.string(text, name)?;
        let node = Node::Length(Box::new(text), count);
        Ok((self.fold(ast, node)?, DataType::Int64))
    }

    fn regexp_replace(
        &self,
        ast: &Ast,
        text: &Ast,
        pattern: &Ast,
        replacement: &Ast,
        options: Option<&Ast>,
    ) -> Result<(Node, DataType), Error> {
        const NAME: &str = "regexp_replace";
        let text = self.string(text, NAME)?;
        let pattern_text = self.string_literal(pattern, NAME, "pattern")?;
        let replacement_text = self.string_literal(replacement, NAME, "replacement")?;
        let mut global = false;
        if let Some(options) = options {
            for option in self.string_literal(options, NAME, "options")?.chars() {
                if option != 'g' {
                    return Err(self.error(
                        options.span(),
                        format!(
                            "`{NAME}` option `{option}` is not supported; the one option is \
                             'g' (replace every match, not only the first)"
                        ),
                    ));
                }
                global = true;
            }
        }
        // RE2's defaults, as DuckDB uses them: `.` does not match a newline, so a pattern
        // ending `.*$` leaves a string with a newline after its match unreplaced.
        let regex = RegexBuilder::new(pattern_text).build().map_err(|e| {
            self.error(
                pattern.span(),
                format!("invalid pattern `{pattern_text}` for `{NAME}`: {e}"),
            )
        })?;
        let pieces = rewrite(replacement_text, regex.captures_len() - 1)
            .map_err(|message| self.error(replacement.span(), format!("`{NAME}`: {message}")))?;
        let replace = Replace {
            regex,
            pieces,
            global,
        };
        let node = Node::RegexpReplace(Box::new(text), Box::new(replace));
        Ok((self.fold(ast, node)?, DataType::Utf8))
    }

    fn date_part(
        &self,
        ast: &Ast,
        part_ast: &Ast,
        part: &str,
        value: &Ast,
        name: &str,
    ) -> Result<(Node, DataType), Error> {
        let field = canonical(part).and_then(Field::named).ok_or_else(|| {
            self.error(
                part_ast.span(),
                format!("unknown date part `{part}` for `{name}`; parts are: {FIELDS}"),
            )
        })?;
        let (node, data_type) = self.temporal(value, name)?;
        if data_type == DataType::Date32
            && matches!(field, Field::Hour | Field::Minute | Field::Second)
        {
            return Err(self.error(
                part_ast.span(),
                format!("a date32 has no `{part}`, and `{value}` is a date32"),
            ));
        }
        let data_type = match field {
            Field::Epoch => DataType::Float64,
            _ => DataType::Int64,
        };
        Ok((
            self.fold(ast, Node::DatePart(Box::new(node), field))?,
            data_type,
        ))
    }

    fn date_trunc(
        &self,
        ast: &Ast,
        unit_ast: &Ast,
        value: &Ast,
        name: &str,
    ) -> Result<(Node, DataType), Error> {
        let unit_text = self.string_literal(unit_ast, name, "unit")?;
        let unit = canonical(unit_text).and_then(Unit::named).ok_or_else(|| {
            self.error(
                unit_ast.span(),
                format!("unknown unit `{unit_text}` for `{name}`; units are: {UNITS}"),
            )
        })?;
        let (node, data_type) = self.temporal(value, name)?;
        Ok((
            self.fold(ast, Node::DateTrunc(Box::new(node), unit))?,
            data_type,
        ))
    }

    /// A date or timestamp argument; NULL is read as a timestamp.
    fn temporal(&self, ast: &Ast, name: &str) -> Result<(Node, DataType), Error> {
        let (node, data_type) = self.bind(ast)?;
        let utc = matches!(&data_type, DataType::Timestamp(TimeUnit::Nanosecond, Some(zone)) if zone.as_ref() == "UTC");
        if utc || data_type == DataType::Date32 {
            return Ok((node, data_type));
        }
        if data_type == DataType::Null {
            let to = DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()));
            return Ok((self.cast(ast, node, &to)?, to));
        }
        Err(self.error(
            ast.span(),
            format!(
                "`{name}` needs a date32 or timestamp, but `{ast}` is {}",
                type_name(&data_type)
            ),
        ))
    }
}

/// DuckDB's spellings of a date part or unit, folded to one name.
fn canonical(name: &str) -> Option<&'static str> {
    Some(match name.to_ascii_lowercase().as_str() {
        "year" | "years" | "y" | "yr" | "yrs" => "year",
        "quarter" | "quarters" => "quarter",
        "month" | "months" | "mon" | "mons" => "month",
        "week" | "weeks" | "w" | "weekofyear" => "week",
        "day" | "days" | "d" | "dayofmonth" => "day",
        "hour" | "hours" | "h" | "hr" | "hrs" => "hour",
        "minute" | "minutes" | "m" | "min" | "mins" => "minute",
        "second" | "seconds" | "s" | "sec" | "secs" => "second",
        "dow" | "dayofweek" | "weekday" => "dow",
        "doy" | "dayofyear" => "doy",
        "epoch" => "epoch",
        _ => return None,
    })
}

/// What `length` counts.
#[derive(Debug, Clone, Copy)]
pub(super) enum Count {
    /// Unicode code points, as DuckDB's `length`.
    Chars,
    /// UTF-8 bytes, as DuckDB's `strlen`.
    Bytes,
}

/// The length of each string, as int64.
pub(super) fn length(array: &dyn Array, count: Count) -> Result<ArrayRef, ArrowError> {
    let strings = array.as_string::<i32>();
    let offsets = strings.value_offsets();
    let lengths: Vec<i64> = match count {
        Count::Bytes => offsets.windows(2).map(|w| i64::from(w[1] - w[0])).collect(),
        Count::Chars => {
            let data = strings.value_data();
            // Every byte starts a code point except UTF-8's continuation bytes, 0b10xx_xxxx.
            offsets
                .windows(2)
                .map(|w| {
                    let bytes = &data[w[0] as usize..w[1] as usize];
                    bytes.iter().filter(|&&b| b & 0xC0 != 0x80).count() as i64
                })
                .collect()
        }
    };
    Ok(Arc::new(Int64Array::new(
        lengths.into(),
        strings.nulls().cloned(),
    )))
}

/// A compiled `regexp_replace`.
#[derive(Debug, Clone)]
pub(super) struct Replace {
    regex: Regex,
    pieces: Vec<Piece>,
    /// Every match, not only the first.
    global: bool,
}

/// A part of a replacement: text as written, or what a capture group matched.
#[derive(Debug, Clone)]
enum Piece {
    Text(String),
    Group(usize),
}

/// Reads a replacement in RE2's syntax, which DuckDB uses: `\0` to `\9` are capture groups and
/// `\\` is a backslash. RE2 leaves the string unchanged when a replacement is malformed; here it is
/// an error when binding, which says why.
fn rewrite(replacement: &str, groups: usize) -> Result<Vec<Piece>, String> {
    let mut pieces = Vec::new();
    let mut text = String::new();
    let mut chars = replacement.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            text.push(c);
            continue;
        }
        match chars.next() {
            Some('\\') => text.push('\\'),
            Some(digit @ '0'..='9') => {
                let group = digit as usize - '0' as usize;
                if group > groups {
                    return Err(format!(
                        "the replacement refers to group {group}, but the pattern has {groups} groups"
                    ));
                }
                if !text.is_empty() {
                    pieces.push(Piece::Text(std::mem::take(&mut text)));
                }
                pieces.push(Piece::Group(group));
            }
            other => {
                let after = other.map_or(String::new(), String::from);
                return Err(format!(
                    "`\\{after}` in the replacement is neither a group (`\\1`) nor a backslash \
                     (`\\\\`)"
                ));
            }
        }
    }
    if !text.is_empty() {
        pieces.push(Piece::Text(text));
    }
    Ok(pieces)
}

impl Replace {
    pub(super) fn apply(&self, array: &dyn Array) -> Result<ArrayRef, ArrowError> {
        let strings = array.as_string::<i32>();
        let mut out = StringBuilder::with_capacity(strings.len(), strings.value_data().len());
        let mut locations = self.regex.capture_locations();
        for text in strings.iter() {
            match text {
                Some(text) => {
                    self.replace(text, &mut locations, &mut out);
                    out.append_value("");
                }
                None => out.append_null(),
            }
        }
        Ok(Arc::new(out.finish()))
    }

    /// Writes `text` with its first (or every) match replaced into the value `out` is building.
    /// Matches are found as RE2's `GlobalReplace` finds them: an empty match where the previous
    /// match ended is skipped, one character further on.
    fn replace(&self, text: &str, locations: &mut CaptureLocations, out: &mut StringBuilder) {
        let mut copied = 0;
        let mut at = 0;
        let mut previous_end = None;
        while at <= text.len() {
            let Some(found) = self.regex.captures_read_at(locations, text, at) else {
                break;
            };
            if found.is_empty() && previous_end == Some(found.end()) {
                let next = text[found.end()..].chars().next();
                at = found.end() + next.map_or(1, char::len_utf8);
                continue;
            }
            put(out, &text[copied..found.start()]);
            for piece in &self.pieces {
                match piece {
                    Piece::Text(literal) => put(out, literal),
                    Piece::Group(group) => {
                        if let Some((start, end)) = locations.get(*group) {
                            put(out, &text[start..end]);
                        }
                    }
                }
            }
            copied = found.end();
            previous_end = Some(copied);
            if !self.global {
                break;
            }
            at = copied;
        }
        put(out, &text[copied..]);
    }
}

/// Appends to the value `out` is building; writing to a `StringBuilder` cannot fail.
fn put(out: &mut StringBuilder, text: &str) {
    let _ = out.write_str(text);
}

/// A part `date_part` and `extract` read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Field {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    /// Whole seconds.
    Second,
    /// Day of the week, 0 for Sunday.
    Dow,
    /// Day of the year, from 1.
    Doy,
    /// Seconds since 1970-01-01 UTC, as float64.
    Epoch,
}

impl Field {
    fn named(name: &str) -> Option<Self> {
        Some(match name {
            "year" => Self::Year,
            "month" => Self::Month,
            "day" => Self::Day,
            "hour" => Self::Hour,
            "minute" => Self::Minute,
            "second" => Self::Second,
            "dow" => Self::Dow,
            "doy" => Self::Doy,
            "epoch" => Self::Epoch,
            _ => return None,
        })
    }
}

/// A part of each date or timestamp: int64, or float64 for `epoch`.
pub(super) fn extract(array: &dyn Array, field: Field) -> Result<ArrayRef, ArrowError> {
    let part = match field {
        Field::Year => DatePart::Year,
        Field::Month => DatePart::Month,
        Field::Day => DatePart::Day,
        Field::Hour => DatePart::Hour,
        Field::Minute => DatePart::Minute,
        Field::Second => DatePart::Second,
        Field::Dow => DatePart::DayOfWeekSunday0,
        Field::Doy => DatePart::DayOfYear,
        Field::Epoch => return Ok(epoch(array)),
    };
    let parts = match array.data_type() {
        // The engine's timestamps are UTC, whose wall time is the instant itself: without the
        // zone, arrow reads times of day with integer arithmetic instead of converting each row
        // through the time zone database.
        DataType::Timestamp(_, _) => date_part(&utc_as_naive(array), part)?,
        _ => date_part(array, part)?,
    };
    cast(&parts, &DataType::Int64)
}

fn utc_as_naive(array: &dyn Array) -> TimestampNanosecondArray {
    array
        .as_primitive::<TimestampNanosecondType>()
        .clone()
        .with_timezone_opt(None::<&str>)
}

fn epoch(array: &dyn Array) -> ArrayRef {
    let seconds: Float64Array = match array.data_type() {
        DataType::Date32 => array
            .as_primitive::<Date32Type>()
            .unary::<_, Float64Type>(|days| f64::from(days) * 86_400.0),
        // DuckDB keeps microseconds and divides them as a double: truncating to microseconds
        // first gives its result to the bit.
        _ => array
            .as_primitive::<TimestampNanosecondType>()
            .unary::<_, Float64Type>(|nanos| nanos.div_euclid(1_000) as f64 / 1e6),
    };
    Arc::new(seconds)
}

/// A unit `date_trunc` rounds down to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Unit {
    Second,
    Minute,
    Hour,
    Day,
    /// From Monday, as ISO 8601 weeks start.
    Week,
    Month,
    Quarter,
    Year,
}

impl Unit {
    fn named(name: &str) -> Option<Self> {
        Some(match name {
            "second" => Self::Second,
            "minute" => Self::Minute,
            "hour" => Self::Hour,
            "day" => Self::Day,
            "week" => Self::Week,
            "month" => Self::Month,
            "quarter" => Self::Quarter,
            "year" => Self::Year,
            _ => return None,
        })
    }

    /// The first day of the unit holding `days` (days since 1970-01-01). A date is its own day,
    /// hour, minute, and second.
    fn floor_days(self, days: i64) -> i64 {
        match self {
            Unit::Second | Unit::Minute | Unit::Hour | Unit::Day => days,
            // 1970-01-01 was a Thursday, three days after a Monday.
            Unit::Week => days - (days + 3).rem_euclid(7),
            Unit::Month => {
                let (_, _, day) = civil_from_days(days);
                days - i64::from(day - 1)
            }
            Unit::Quarter => {
                let (year, month, _) = civil_from_days(days);
                days_from_civil(year, (month - 1) / 3 * 3 + 1, 1)
            }
            Unit::Year => days_from_civil(civil_from_days(days).0, 1, 1),
        }
    }

    /// The start of the unit holding `nanos`, or `None` where it falls before the timestamp range.
    fn floor_nanos(self, nanos: i64) -> Option<i64> {
        let step = match self {
            Unit::Second => NANOS_PER_SECOND,
            Unit::Minute => 60 * NANOS_PER_SECOND,
            Unit::Hour => 3_600 * NANOS_PER_SECOND,
            _ => {
                let days = self.floor_days(nanos.div_euclid(NANOS_PER_DAY));
                return days.checked_mul(NANOS_PER_DAY);
            }
        };
        nanos.checked_sub(nanos.rem_euclid(step))
    }
}

/// Each date or timestamp rounded down to the start of its `unit`, keeping its type.
pub(super) fn truncate(array: &dyn Array, unit: Unit) -> Result<ArrayRef, ArrowError> {
    let out_of_range = || {
        ArrowError::ComputeError(format!(
            "date_trunc to {unit:?} falls outside the range of {}",
            type_name(array.data_type())
        ))
    };
    if let DataType::Date32 = array.data_type() {
        let days: Date32Array = array.as_primitive::<Date32Type>().try_unary(|days| {
            i32::try_from(unit.floor_days(i64::from(days))).map_err(|_| out_of_range())
        })?;
        return Ok(Arc::new(days));
    }
    let stamps = array.as_primitive::<TimestampNanosecondType>();
    let floored: TimestampNanosecondArray =
        stamps.try_unary(|nanos| unit.floor_nanos(nanos).ok_or_else(out_of_range))?;
    Ok(Arc::new(floored.with_timezone_opt(stamps.timezone())))
}

/// The (year, month, day) of a count of days since 1970-01-01, by Howard Hinnant's
/// `civil_from_days` (proleptic Gregorian calendar, any day an i64 holds).
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let day_of_era = z.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let shifted_month = (5 * day_of_year + 2) / 153;
    let day = (day_of_year - (153 * shifted_month + 2) / 5 + 1) as u32;
    let month = (if shifted_month < 10 {
        shifted_month + 3
    } else {
        shifted_month - 9
    }) as u32;
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

/// Days since 1970-01-01 of a (year, month, day), the inverse of `civil_from_days`.
fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let year = year - i64::from(month <= 2);
    let era = year.div_euclid(400);
    let year_of_era = year.rem_euclid(400);
    let month = i64::from(month);
    let shifted_month = if month > 2 { month - 3 } else { month + 9 };
    let day_of_year = (153 * shifted_month + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}
