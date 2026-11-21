//! A chart's server side (architecture.md §15.4): the points of one or more `y` columns against
//! an `x` column of any table, sent whole while they fit the chart's budget and downsampled when
//! they do not, so a chart of millions of rows sends what its pixels can show.
//!
//! Downsampling cuts x into buckets and keeps, in each, the rows with its lowest and highest x
//! and each y's lowest and highest value: the line's shape, its spikes included, from at most
//! `points` rows. A bucket's width is a power of two in x's own units (nanoseconds for a time),
//! the least that fits the budget, and buckets are aligned to its multiples from zero: as rows
//! arrive, only the last bucket's points change, and when the range outgrows the width it
//! doubles, merging neighbours, whose points are then a subset of those already drawn. A chart
//! that is zoomed asks for its x range (`set_range`), and only rows within it are charted.
//!
//! Points are always in x order, and every value sent is the table's own (rows are picked, never
//! averaged). A text x (a bar per category) is sent whole, and is not ranged. Each frame reads
//! the table's current rows (a snapshot, which views an append table's chunks without copying),
//! so a series follows any table: appended, upserted, or deleted from.

use arrow::array::{Array, ArrayRef, AsArray, RecordBatch};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{
    DataType, Date32Type, Float64Type, Int16Type, Int32Type, Int64Type, TimeUnit,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType,
};
use arrow::ipc::writer::StreamWriter;

use crate::session::{Table, WeakTable};
use crate::storage::interleave_rows;
use crate::viewer::{Frame, Status};
use crate::{Error, type_name};

/// The fewest and most points a chart may ask for.
pub const MIN_POINTS: usize = 16;
pub const MAX_POINTS: usize = 100_000;

pub struct Series {
    table: WeakTable,
    /// `table` filtered by the chart's `where`; `None` without one.
    shown: Option<Table>,
    /// The x column's position, then each y's.
    columns: Vec<usize>,
    text: bool,
    points: usize,
    /// The x range charted, both ends included, in x's own units; `None` for all of it.
    range: Option<(f64, f64)>,
    refreshing: bool,
}

impl Series {
    /// The series of `ys` against `x` in `table`, of the rows `filter` keeps (an expression, as
    /// `where` takes), in at most `points` rows (clamped to `MIN_POINTS..=MAX_POINTS`).
    pub fn new(
        table: &Table,
        x: &str,
        ys: &[String],
        filter: Option<&str>,
        points: usize,
    ) -> Result<Self, Error> {
        let schema = table.schema();
        let position = |name: &str| {
            schema.index_of(name).map_err(|_| {
                Error::Schema(format!(
                    "table `{}` has no column `{name}`",
                    table.description()
                ))
            })
        };
        let mut columns = vec![position(x)?];
        let text = match schema.field(columns[0]).data_type() {
            DataType::Utf8 => true,
            DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float64
            | DataType::Date32
            | DataType::Timestamp(_, _) => false,
            other => {
                return Err(Error::Unsupported(format!(
                    "`{x}` in table `{}` is {}: a chart's x is a number, a date, a time, or text",
                    table.description(),
                    type_name(other)
                )));
            }
        };
        if ys.is_empty() {
            return Err(Error::Unsupported("a chart needs a y column".to_owned()));
        }
        for y in ys {
            let at = position(y)?;
            let data_type = schema.field(at).data_type();
            if !matches!(
                data_type,
                DataType::Int16 | DataType::Int32 | DataType::Int64 | DataType::Float64
            ) {
                return Err(Error::Unsupported(format!(
                    "`{y}` in table `{}` is {}: a chart's y is a number",
                    table.description(),
                    type_name(data_type)
                )));
            }
            columns.push(at);
        }
        let shown = match filter.map(str::trim) {
            Some(predicate) if !predicate.is_empty() => Some(table.filter(predicate)?),
            _ => None,
        };
        Ok(Self {
            table: table.downgrade(),
            shown,
            columns,
            text,
            points: points.clamp(MIN_POINTS, MAX_POINTS),
            range: None,
            refreshing: table.is_refreshing(),
        })
    }

    /// Charts only rows whose x is within `range`, both ends included, in x's own units (days
    /// for a date, the column's unit since the epoch for a time); `None` for every row.
    pub fn set_range(&mut self, range: Option<(f64, f64)>) -> Result<(), Error> {
        if let Some((low, high)) = range
            && !(low.is_finite() && high.is_finite() && low <= high)
        {
            return Err(Error::Unsupported(format!(
                "a chart's x range is two finite numbers, the lower first: not {low} to {high}"
            )));
        }
        self.range = range;
        Ok(())
    }

    /// The points as they are now: x then each y, as an Arrow IPC stream, with the number of
    /// rows they stand for; `None` once the table is gone.
    pub fn frame(&self) -> Result<Option<Frame>, Error> {
        let Some(table) = self.table.upgrade() else {
            return Ok(None);
        };
        let shown = self.shown.as_ref().unwrap_or(&table);
        let failed = |message: String| Error::Compute {
            table: shown.description(),
            message,
        };
        let batches = shown
            .snapshot()?
            .iter()
            .map(|batch| batch.project(&self.columns))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| failed(e.to_string()))?;
        let total = batches.iter().map(RecordBatch::num_rows).sum();
        let picked = if self.text {
            let picked = text_rows(&batches);
            if picked.len() > MAX_POINTS {
                return Err(Error::Unsupported(format!(
                    "{} rows of table `{}` to chart by text: at most {MAX_POINTS}; filter it, or \
                     chart by a number or time",
                    picked.len(),
                    shown.description()
                )));
            }
            picked
        } else {
            pick(&batches, self.points, self.range)
        };
        let schema = shown
            .schema()
            .project(&self.columns)
            .map_err(|e| failed(e.to_string()))?;
        let points = if picked.is_empty() {
            RecordBatch::new_empty(schema.into())
        } else {
            let batches: Vec<&RecordBatch> = batches.iter().collect();
            interleave_rows(&batches, &picked).map_err(|e| failed(e.to_string()))?
        };
        let mut rows = Vec::new();
        (|| {
            let mut writer = StreamWriter::try_new(&mut rows, &points.schema())?;
            writer.write(&points)?;
            writer.finish()
        })()
        .map_err(|e| failed(e.to_string()))?;
        Ok(Some(Frame {
            fetched: None,
            start: 0,
            total,
            cycle: shown.changed(),
            rows,
            keys: Vec::new(),
        }))
    }

    /// The cycle that last changed the charted rows; `None` once the table is gone.
    pub fn changed(&self) -> Option<u64> {
        let table = self.table.upgrade()?;
        Some(self.shown.as_ref().unwrap_or(&table).changed())
    }

    /// As `Viewer::status`.
    pub fn status(&self) -> Status {
        let loading = self.table.upgrade().is_some_and(|table| table.is_loading());
        Status::new(loading, self.refreshing)
    }
}

/// A value of a chart's numeric x or y column, as the position it is drawn at.
trait Coord: Copy {
    fn at(self) -> f64;
}
impl Coord for i16 {
    fn at(self) -> f64 {
        f64::from(self)
    }
}
impl Coord for i32 {
    fn at(self) -> f64 {
        f64::from(self)
    }
}
impl Coord for i64 {
    fn at(self) -> f64 {
        self as f64
    }
}
impl Coord for f64 {
    fn at(self) -> f64 {
        self
    }
}

/// A numeric column's values, by their native type (dates are days, times their unit's count).
enum Values<'a> {
    I16(&'a [i16]),
    I32(&'a [i32]),
    I64(&'a [i64]),
    F64(&'a [f64]),
}

fn values(array: &ArrayRef) -> Values<'_> {
    match array.data_type() {
        DataType::Int16 => Values::I16(array.as_primitive::<Int16Type>().values()),
        DataType::Int32 => Values::I32(array.as_primitive::<Int32Type>().values()),
        DataType::Date32 => Values::I32(array.as_primitive::<Date32Type>().values()),
        DataType::Int64 => Values::I64(array.as_primitive::<Int64Type>().values()),
        DataType::Float64 => Values::F64(array.as_primitive::<Float64Type>().values()),
        DataType::Timestamp(TimeUnit::Second, _) => {
            Values::I64(array.as_primitive::<TimestampSecondType>().values())
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            Values::I64(array.as_primitive::<TimestampMillisecondType>().values())
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            Values::I64(array.as_primitive::<TimestampMicrosecondType>().values())
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            Values::I64(array.as_primitive::<TimestampNanosecondType>().values())
        }
        other => unreachable!("Series::new admits no {other} column"),
    }
}

/// Calls `f(row, value)` for each row with a value (neither NULL nor NaN).
fn each<X: Coord>(values: &[X], nulls: Option<&NullBuffer>, mut f: impl FnMut(usize, f64)) {
    for (row, value) in values.iter().enumerate() {
        let value = value.at();
        if nulls.is_none_or(|n| n.is_valid(row)) && !value.is_nan() {
            f(row, value);
        }
    }
}

/// Calls `f(row, x, y)` for each row with both values.
fn each_pair<X: Coord, Y: Coord>(
    x: &[X],
    x_nulls: Option<&NullBuffer>,
    y: &[Y],
    y_nulls: Option<&NullBuffer>,
    mut f: impl FnMut(usize, f64, f64),
) {
    each(x, x_nulls, |row, x| {
        let value = y[row].at();
        if y_nulls.is_none_or(|n| n.is_valid(row)) && !value.is_nan() {
            f(row, x, value);
        }
    });
}

fn each_x(batch: &RecordBatch, f: impl FnMut(usize, f64)) {
    let x = batch.column(0);
    match values(x) {
        Values::I16(v) => each(v, x.nulls(), f),
        Values::I32(v) => each(v, x.nulls(), f),
        Values::I64(v) => each(v, x.nulls(), f),
        Values::F64(v) => each(v, x.nulls(), f),
    }
}

fn each_xy(batch: &RecordBatch, y: usize, f: impl FnMut(usize, f64, f64)) {
    let (xs, ys) = (batch.column(0), batch.column(y));
    let (xn, yn) = (xs.nulls(), ys.nulls());
    match (values(xs), values(ys)) {
        (Values::I16(x), Values::I16(y)) => each_pair(x, xn, y, yn, f),
        (Values::I16(x), Values::I32(y)) => each_pair(x, xn, y, yn, f),
        (Values::I16(x), Values::I64(y)) => each_pair(x, xn, y, yn, f),
        (Values::I16(x), Values::F64(y)) => each_pair(x, xn, y, yn, f),
        (Values::I32(x), Values::I16(y)) => each_pair(x, xn, y, yn, f),
        (Values::I32(x), Values::I32(y)) => each_pair(x, xn, y, yn, f),
        (Values::I32(x), Values::I64(y)) => each_pair(x, xn, y, yn, f),
        (Values::I32(x), Values::F64(y)) => each_pair(x, xn, y, yn, f),
        (Values::I64(x), Values::I16(y)) => each_pair(x, xn, y, yn, f),
        (Values::I64(x), Values::I32(y)) => each_pair(x, xn, y, yn, f),
        (Values::I64(x), Values::I64(y)) => each_pair(x, xn, y, yn, f),
        (Values::I64(x), Values::F64(y)) => each_pair(x, xn, y, yn, f),
        (Values::F64(x), Values::I16(y)) => each_pair(x, xn, y, yn, f),
        (Values::F64(x), Values::I32(y)) => each_pair(x, xn, y, yn, f),
        (Values::F64(x), Values::I64(y)) => each_pair(x, xn, y, yn, f),
        (Values::F64(x), Values::F64(y)) => each_pair(x, xn, y, yn, f),
    }
}

/// A row's value and where it is: (batch, row).
type Point = (f64, (usize, usize));

fn lower(slot: &mut Option<Point>, value: f64, at: (usize, usize)) {
    if slot.is_none_or(|(v, _)| value < v) {
        *slot = Some((value, at));
    }
}

fn higher(slot: &mut Option<Point>, value: f64, at: (usize, usize)) {
    if slot.is_none_or(|(v, _)| value > v) {
        *slot = Some((value, at));
    }
}

/// The bucket width for x from `low` to `high` in at most `buckets` buckets: the least power of
/// two whose multiples from zero cut that span into no more. Powers of two halve and double
/// exactly, so a wider width's buckets are pairs of a narrower one's, and its multiples divide
/// any x exactly as a float.
fn bucket_width(low: f64, high: f64, buckets: usize) -> f64 {
    let count = |width: f64| (high / width).floor() - (low / width).floor() + 1.0;
    let mut width = if high > low {
        ((high - low) / buckets as f64).log2().ceil().exp2()
    } else {
        1.0
    };
    while count(width) > buckets as f64 {
        width *= 2.0;
    }
    width
}

/// The rows to send of `batches` (x first, then the ys), as (batch, row) in x order: every row
/// with an x in `range` if they fit `points`, else each bucket's extremes (see the module's
/// docs).
fn pick(batches: &[RecordBatch], points: usize, range: Option<(f64, f64)>) -> Vec<(usize, usize)> {
    let inside = |x: f64| range.is_none_or(|(from, to)| from <= x && x <= to);
    let (mut count, mut low, mut high) = (0, f64::INFINITY, f64::NEG_INFINITY);
    for batch in batches {
        each_x(batch, |_, x| {
            if inside(x) {
                count += 1;
                low = low.min(x);
                high = high.max(x);
            }
        });
    }
    // Points compare by x, then by place: ties keep the table's order.
    let order = |a: &Point, b: &Point| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1));
    let mut picked: Vec<Point> = Vec::with_capacity(count.min(points));
    if count <= points {
        for (b, batch) in batches.iter().enumerate() {
            each_x(batch, |row, x| {
                if inside(x) {
                    picked.push((x, (b, row)));
                }
            });
        }
        // An x-ordered table (a time series) is sent as it is.
        if !picked.is_sorted_by(|a, b| a.0 <= b.0) {
            picked.sort_by(order);
        }
        return picked.into_iter().map(|(_, at)| at).collect();
    }
    let ys = batches.first().map_or(0, |b| b.num_columns() - 1);
    // Each bucket sends at most its lowest and highest x and each y's lowest and highest.
    let width = bucket_width(low, high, (points / (2 + 2 * ys)).max(1));
    let first = (low / width).floor();
    let buckets = ((high / width).floor() - first) as usize + 1;
    let bucket = |x: f64| ((x / width).floor() - first) as usize;
    let mut x_ends = vec![(None, None); buckets];
    for (b, batch) in batches.iter().enumerate() {
        each_x(batch, |row, x| {
            if inside(x) {
                let (least, most) = &mut x_ends[bucket(x)];
                lower(least, x, (b, row));
                higher(most, x, (b, row));
            }
        });
    }
    for (least, most) in &x_ends {
        picked.extend(least.iter().chain(most));
    }
    let mut y_ends = vec![(None, None); buckets];
    for y in 1..=ys {
        y_ends.fill((None, None));
        for (b, batch) in batches.iter().enumerate() {
            each_xy(batch, y, |row, x, value| {
                if inside(x) {
                    let (least, most) = &mut y_ends[bucket(x)];
                    lower(least, value, (b, row));
                    higher(most, value, (b, row));
                }
            });
        }
        // Kept by their y; sent in order of their x
        for (least, most) in &y_ends {
            for (_, at) in least.iter().chain(most) {
                picked.push((x_at(batches, *at), *at));
            }
        }
    }
    // A row picked twice (a bucket's first x and its highest y) sorts beside itself.
    picked.sort_by(order);
    picked.dedup_by_key(|(_, at)| *at);
    picked.into_iter().map(|(_, at)| at).collect()
}

/// The x of the row at `at`, which has one.
fn x_at(batches: &[RecordBatch], (b, row): (usize, usize)) -> f64 {
    match values(batches[b].column(0)) {
        Values::I16(v) => v[row].at(),
        Values::I32(v) => v[row].at(),
        Values::I64(v) => v[row].at(),
        Values::F64(v) => v[row].at(),
    }
}

/// Every row with an x, in the table's order: a text x's categories.
fn text_rows(batches: &[RecordBatch]) -> Vec<(usize, usize)> {
    batches
        .iter()
        .enumerate()
        .flat_map(|(b, batch)| {
            let x = batch.column(0);
            (0..x.len())
                .filter(move |row| x.is_valid(*row))
                .map(move |row| (b, row))
        })
        .collect()
}
