//! Names parsed once where they enter: table names (`namespace.table`), ingest source IDs, and
//! dates.

use std::fmt;

use crate::Error;

/// A table's name: `namespace.table`, both lowercase identifiers (`[a-z][a-z0-9_]*`, at most 63
/// characters). The `sys` namespace is the platform's: anyone reads it, and only the platform
/// (the controller's `sys.deployments`) writes it, which `parse_user` holds user code to.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TableName {
    namespace: String,
    table: String,
}

impl TableName {
    pub fn parse(text: &str) -> Result<Self, Error> {
        let invalid = |why: &str| Error::Name(format!("`{text}` is not a table name: {why}"));
        let Some((namespace, table)) = text.split_once('.') else {
            return Err(invalid("expected `namespace.table`"));
        };
        for part in [namespace, table] {
            if !is_identifier(part) {
                return Err(invalid(
                    "each part is lowercase letters, digits, and `_`, starting with a letter",
                ));
            }
        }
        Ok(Self {
            namespace: namespace.to_owned(),
            table: table.to_owned(),
        })
    }

    /// A table user code may create or write: any but the platform's `sys` namespace.
    pub fn parse_user(text: &str) -> Result<Self, Error> {
        let name = Self::parse(text)?;
        if name.namespace == "sys" {
            return Err(Error::Name(format!(
                "`{text}` is the platform's: the `sys` namespace is reserved"
            )));
        }
        Ok(name)
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn table(&self) -> &str {
        &self.table
    }
}

impl fmt::Display for TableName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.namespace, self.table)
    }
}

/// An ingest source within a table (one writer each): lowercase letters, digits, `_`, and `-`,
/// starting with a letter or digit, at most 63 characters. It names a directory.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SourceId(String);

impl SourceId {
    pub fn parse(text: &str) -> Result<Self, Error> {
        let valid = (1..=63).contains(&text.len())
            && text.as_bytes()[0].is_ascii_alphanumeric()
            && text
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-');
        if !valid {
            return Err(Error::Name(format!(
                "`{text}` is not a source id: lowercase letters, digits, `_`, and `-`, starting \
                 with a letter or digit"
            )));
        }
        Ok(Self(text.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SourceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A UTC calendar date, the unit weir's partitions and history (§8) are kept by: days since
/// 1970-01-01, written `YYYY-MM-DD`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Date(i32);

impl Date {
    /// The first and last dates `YYYY-MM-DD` writes: 0001-01-01 and 9999-12-31.
    pub const MIN: Date = Date(-719_162);
    pub const MAX: Date = Date(2_932_896);

    /// The date of a UTC instant in nanoseconds since the epoch.
    pub fn of_ns(ns: i64) -> Self {
        Self(ns.div_euclid(86_400_000_000_000) as i32)
    }

    pub fn from_days(days: i32) -> Self {
        Self(days)
    }

    /// Days since 1970-01-01.
    pub fn days(self) -> i32 {
        self.0
    }

    /// `YYYY-MM-DD`, a real calendar date.
    pub fn parse(text: &str) -> Result<Self, Error> {
        let bad = || Error::Name(format!("{text:?} is not a date (YYYY-MM-DD)"));
        let bytes = text.as_bytes();
        if bytes.len() != 10 || bytes[4] != b'-' || bytes[7] != b'-' {
            return Err(bad());
        }
        let number = |at: std::ops::Range<usize>| {
            text.get(at)
                .filter(|digits| digits.bytes().all(|b| b.is_ascii_digit()))
                .and_then(|digits| digits.parse::<i64>().ok())
                .ok_or_else(bad)
        };
        let (year, month, day) = (number(0..4)?, number(5..7)?, number(8..10)?);
        let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
        let days_in_month = match month {
            2 if leap => 29,
            2 => 28,
            4 | 6 | 9 | 11 => 30,
            1..=12 => 31,
            _ => return Err(bad()),
        };
        if !(1..=days_in_month).contains(&day) {
            return Err(bad());
        }
        // Howard Hinnant's `days_from_civil`.
        let year = if month <= 2 { year - 1 } else { year };
        let era = year.div_euclid(400);
        let yoe = year - era * 400;
        let doy = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
        let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        Ok(Self((era * 146_097 + doe - 719_468) as i32))
    }
}

impl fmt::Display for Date {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Howard Hinnant's `civil_from_days`.
        let z = i64::from(self.0) + 719_468;
        let era = z.div_euclid(146_097);
        let doe = z - era * 146_097;
        let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let day = doy - (153 * mp + 2) / 5 + 1;
        let month = if mp < 10 { mp + 3 } else { mp - 9 };
        let year = yoe + era * 400 + i64::from(month <= 2);
        write!(f, "{year:04}-{month:02}-{day:02}")
    }
}

fn is_identifier(text: &str) -> bool {
    (1..=63).contains(&text.len())
        && text.as_bytes()[0].is_ascii_lowercase()
        && text
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

#[cfg(test)]
mod tests {
    use super::Date;

    #[test]
    fn dates_follow_the_calendar() {
        let day = 86_400_000_000_000_i64;
        let at = |ns: i64| Date::of_ns(ns).to_string();
        assert_eq!(at(0), "1970-01-01");
        assert_eq!(at(-1), "1969-12-31");
        assert_eq!(at(1_767_225_600 * 1_000_000_000), "2026-01-01");
        // 2024 is a leap year; 2100 is not.
        assert_eq!(at(19_782 * day), "2024-02-29");
        assert_eq!(at(47_541 * day), "2100-03-01");
    }

    #[test]
    fn dates_parse_back_and_reject_what_is_not_one() {
        for days in [-1, 0, 19_782, 20_404, 47_541] {
            let date = Date::from_days(days);
            assert_eq!(Date::parse(&date.to_string()).unwrap(), date);
        }
        for bad in [
            "2026-02-29",
            "2100-02-29",
            "2026-13-01",
            "2026-00-10",
            "2026-04-31",
            "2026-1-01",
            "20261101",
            "2026-11-0x",
            "+026-11-01",
        ] {
            assert!(Date::parse(bad).is_err(), "{bad}");
        }
        assert_eq!(Date::parse("2024-02-29").unwrap().days(), 19_782);
        assert_eq!(Date::parse("0001-01-01").unwrap(), Date::MIN);
        assert_eq!(Date::parse("9999-12-31").unwrap(), Date::MAX);
    }
}
