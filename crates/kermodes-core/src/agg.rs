//! `agg_by`: aggregations per group, in two phases.
//!
//! Added rows are cut into fixed 256k-row pieces. Each piece is aggregated on its own into
//! partial states, reading its rows sequentially and hashing each key once. Groups are sharded by
//! key hash; each shard then merges the partials of its groups in piece order. Removed and
//! modified rows are routed to their group's shard and the groups they touch are recomputed from
//! their rows.
//!
//! Counts, distinct counts, integer sums and means, min/max, and first/last are exact: integer
//! sums accumulate in 128 bits, so they never wrap and an integer mean divides the exact sum,
//! rounding once. Float sums, means, and standard deviations depend on where piece boundaries
//! fall, so a ticking table and a static recomputation can differ in the last bits; piece
//! boundaries never depend on the number of cores, so results do not either.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, AsArray, Float64Array, Int64Array, PrimitiveArray,
    UInt32Array,
};
use arrow::compute::take;
use arrow::datatypes::{
    DataType, Date32Type, Field, Float64Type, Int16Type, Int32Type, Int64Type, Schema, SchemaRef,
    TimeUnit, TimestampNanosecondType,
};
use arrow::error::ArrowError;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use arrow::row::{OwnedRow, RowConverter, Rows as EncodedRows, SortField};
use hashbrown::HashTable;

use crate::groups::{HASHER, KeyKind, LocalEntry, LocalGroups, Membership, shard_of, split};
use crate::ops::{pick_column, push_removed};
use crate::parallel::{PARALLEL_ROWS, offsets, run, threads};
use crate::session::Reader;
use crate::storage::{Delta, Modified, RowKey, Rows, Storage};
use crate::{Error, type_name};

/// Rows per piece. Fixed, so results never depend on the number of cores.
const PIECE_ROWS: usize = 1 << 18;

/// One aggregation's state for every group, one vector per field, indexed by group id.
pub(crate) enum Accumulator {
    /// `count(*)` when `column` is `None`, otherwise non-null values.
    Count { column: Option<usize>, n: Vec<i64> },
    /// `sum` or `mean` of an integer column, summed exactly in 128 bits. `name` is the column's,
    /// for the error when a sum does not fit the int64 output.
    SumInt {
        column: usize,
        name: String,
        mean: bool,
        sum: Vec<i128>,
        n: Vec<i64>,
    },
    /// `sum` or `mean` of a float column.
    SumFloat {
        column: usize,
        state: Vec<FloatSum>,
        mean: bool,
    },
    /// The count, sum, and sum of squares, in double-double.
    Std { column: usize, state: Vec<Moments> },
    /// `min` or `max`: the winning value (comparable encoding) and its row key; ties go to the
    /// smaller key. `column` is read when folding; `source` is the input column the value is
    /// copied from.
    Extreme {
        column: usize,
        source: usize,
        max: bool,
        data_type: DataType,
        converter: RowConverter,
        best: Vec<Option<(OwnedRow, RowKey)>>,
    },
    /// `first` or `last`: the smallest or largest row key, nulls included.
    Edge {
        column: usize,
        source: usize,
        last: bool,
        key: Vec<Option<RowKey>>,
    },
    /// `count_distinct`: the distinct non-null values of `column` per group.
    CountDistinct { column: usize, values: Distinct },
}

/// One group's float sum and count, kept together so a fold touches one cache line per row.
#[derive(Clone, Copy, Default)]
pub(crate) struct FloatSum {
    sum: f64,
    n: i64,
}

/// One group's count, sum, and sum of squares, each sum a double-double so adding and removing
/// the same values cancels exactly whenever the sums fit in its ~106 bits, and otherwise leaves a
/// residue near 2^-106 of the sums. Static and ticking tables share this code, so they round
/// alike.
#[derive(Clone, Copy, Default)]
pub(crate) struct Moments {
    n: i64,
    sum: Dd,
    squares: Dd,
}

impl Moments {
    fn insert(&mut self, x: f64) {
        self.n += 1;
        self.sum = self.sum.plus(x);
        self.squares = self.squares.add(two_product(x, x));
    }

    fn remove(&mut self, x: f64) {
        self.n -= 1;
        if self.n == 0 {
            // An emptied group restarts exactly.
            *self = Moments::default();
            return;
        }
        self.sum = self.sum.plus(-x);
        self.squares = self.squares.add(two_product(-x, x));
    }

    fn merge(&mut self, other: &Moments) {
        self.n += other.n;
        self.sum = self.sum.add(other.sum);
        self.squares = self.squares.add(other.squares);
    }

    /// The sample standard deviation, from `(n * squares - sum^2) / (n * (n - 1))`: the
    /// numerator in double-double (it cancels to exactly 0 when every value is equal and the
    /// sums are exact), clamped at 0 against rounding, then one division and one square root.
    fn std(&self) -> Option<f64> {
        (self.n > 1).then(|| {
            let n = self.n as f64;
            let n_squares = self.squares.mul(Dd { hi: n, lo: 0.0 });
            let spread = n_squares.add(self.sum.mul(self.sum).neg());
            // `< 0.0` rather than `max`, which would turn a NaN spread into 0.
            let spread = if spread.hi < 0.0 { 0.0 } else { spread.hi };
            (spread / (n * (n - 1.0))).sqrt()
        })
    }
}

/// A double-double: the unevaluated sum `hi + lo`, with `lo` below half an ulp of `hi`.
#[derive(Clone, Copy, Default)]
struct Dd {
    hi: f64,
    lo: f64,
}

/// `a + b` rounded, and the exact error of that rounding (Knuth's TwoSum).
fn two_sum(a: f64, b: f64) -> (f64, f64) {
    let s = a + b;
    let v = s - a;
    (s, (a - (s - v)) + (b - v))
}

/// `a * b` exactly: the rounded product and its error, which a fused multiply-add computes.
fn two_product(a: f64, b: f64) -> Dd {
    let hi = a * b;
    Dd {
        hi,
        lo: a.mul_add(b, -hi),
    }
}

impl Dd {
    fn neg(self) -> Dd {
        Dd {
            hi: -self.hi,
            lo: -self.lo,
        }
    }

    /// `self + x` for a double `x`.
    fn plus(self, x: f64) -> Dd {
        let (s, e) = two_sum(self.hi, x);
        let (hi, lo) = two_sum(s, e + self.lo);
        Dd { hi, lo }
    }

    /// `self + other`, carrying both error terms (the accurate double-double sum).
    fn add(self, other: Dd) -> Dd {
        let (s, e) = two_sum(self.hi, other.hi);
        let (t, f) = two_sum(self.lo, other.lo);
        let (s, e) = two_sum(s, e + t);
        let (hi, lo) = two_sum(s, e + f);
        Dd { hi, lo }
    }

    fn mul(self, other: Dd) -> Dd {
        let p = two_product(self.hi, other.hi);
        let cross = self.hi * other.lo + self.lo * other.hi;
        let (hi, lo) = two_sum(p.hi, p.lo + cross);
        Dd { hi, lo }
    }
}

/// An aggregation call `function(column)`, checked against a schema: the function is known
/// (aliases resolved), `*` only appears in `count(*)`, and the column exists and is a number for
/// the functions that need one.
pub(crate) struct Call {
    /// One of count, count_distinct, sum, mean, std, min, max, first, last.
    pub function: &'static str,
    /// `None` for `count(*)`.
    pub column: Option<usize>,
}

impl Call {
    pub fn parse(spec: &str, schema: &Schema) -> Result<Self, Error> {
        let error = |message: String| call_error(spec, message);
        let text = spec.trim();
        let (name, rest) = text
            .split_once('(')
            .ok_or_else(|| error("expected `function(column)`".into()))?;
        let argument = rest
            .strip_suffix(')')
            .ok_or_else(|| error("expected `function(column)`".into()))?
            .trim();
        let function = match name.trim().to_ascii_lowercase().as_str() {
            "count" => "count",
            "count_distinct" => "count_distinct",
            "sum" => "sum",
            "mean" | "avg" => "mean",
            "std" | "stddev" => "std",
            "min" => "min",
            "max" => "max",
            "first" => "first",
            "last" => "last",
            other => {
                return Err(error(format!(
                    "unknown aggregation `{other}`; expected count, count_distinct, sum, mean, \
                     std, min, max, first, or last"
                )));
            }
        };
        if argument == "*" {
            if function != "count" {
                return Err(error("only count accepts `*`".into()));
            }
            return Ok(Self {
                function,
                column: None,
            });
        }
        let column = schema.index_of(argument).map_err(|_| {
            let known: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
            error(format!(
                "unknown column `{argument}`; columns are: {}",
                known.join(", ")
            ))
        })?;
        let input_type = schema.field(column).data_type();
        let numeric = matches!(
            input_type,
            DataType::Int16 | DataType::Int32 | DataType::Int64 | DataType::Float64
        );
        if matches!(function, "sum" | "mean" | "std") && !numeric {
            return Err(error(format!(
                "`{}` needs a number, but `{argument}` is {}",
                name.trim(),
                type_name(input_type)
            )));
        }
        let comparable = matches!(
            input_type,
            DataType::Null
                | DataType::Boolean
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float64
                | DataType::Utf8
                | DataType::Date32
                | DataType::Timestamp(TimeUnit::Nanosecond, _)
        );
        if function == "count_distinct" && !comparable {
            return Err(error(format!(
                "`count_distinct` cannot compare values of `{argument}`, which is {}",
                type_name(input_type)
            )));
        }
        Ok(Self {
            function,
            column: Some(column),
        })
    }
}

/// An error in the aggregation `spec`.
pub(crate) fn call_error(spec: &str, message: String) -> Error {
    Error::Expression {
        text: spec.to_owned(),
        column: None,
        message,
    }
}

impl Accumulator {
    /// Parses `function(column)`, returning the accumulator and its output type.
    pub fn parse(spec: &str, schema: &Schema) -> Result<(Self, DataType), Error> {
        let call = Call::parse(spec, schema)?;
        let Some(column) = call.column else {
            let count = Accumulator::Count {
                column: None,
                n: Vec::new(),
            };
            return Ok((count, DataType::Int64));
        };
        let input_type = schema.field(column).data_type().clone();
        let float_input = input_type == DataType::Float64;
        Ok(match call.function {
            "count" => (
                Accumulator::Count {
                    column: Some(column),
                    n: Vec::new(),
                },
                DataType::Int64,
            ),
            "count_distinct" => (
                Accumulator::CountDistinct {
                    column,
                    values: Distinct::new(input_type == DataType::Utf8),
                },
                DataType::Int64,
            ),
            "sum" | "mean" if !float_input => {
                let mean = call.function == "mean";
                let sum = Accumulator::SumInt {
                    column,
                    name: schema.field(column).name().clone(),
                    mean,
                    sum: Vec::new(),
                    n: Vec::new(),
                };
                (
                    sum,
                    if mean {
                        DataType::Float64
                    } else {
                        DataType::Int64
                    },
                )
            }
            "sum" | "mean" => (
                Accumulator::SumFloat {
                    column,
                    state: Vec::new(),
                    mean: call.function == "mean",
                },
                DataType::Float64,
            ),
            "std" => (
                Accumulator::Std {
                    column,
                    state: Vec::new(),
                },
                DataType::Float64,
            ),
            "min" | "max" => (
                Accumulator::Extreme {
                    column,
                    source: column,
                    max: call.function == "max",
                    converter: RowConverter::new(vec![SortField::new(input_type.clone())])
                        .map_err(|e| call_error(spec, e.to_string()))?,
                    data_type: input_type.clone(),
                    best: Vec::new(),
                },
                input_type,
            ),
            _ => (
                Accumulator::Edge {
                    column,
                    source: column,
                    last: call.function == "last",
                    key: Vec::new(),
                },
                input_type,
            ),
        })
    }

    /// An accumulator for the same aggregation, holding no groups.
    fn fresh(&self) -> Result<Self, ArrowError> {
        Ok(match self {
            Accumulator::Count { column, .. } => Accumulator::Count {
                column: *column,
                n: Vec::new(),
            },
            Accumulator::SumInt {
                column, name, mean, ..
            } => Accumulator::SumInt {
                column: *column,
                name: name.clone(),
                mean: *mean,
                sum: Vec::new(),
                n: Vec::new(),
            },
            Accumulator::SumFloat { column, mean, .. } => Accumulator::SumFloat {
                column: *column,
                state: Vec::new(),
                mean: *mean,
            },
            Accumulator::Std { column, .. } => Accumulator::Std {
                column: *column,
                state: Vec::new(),
            },
            Accumulator::Extreme {
                column,
                source,
                max,
                data_type,
                ..
            } => Accumulator::Extreme {
                column: *column,
                source: *source,
                max: *max,
                converter: RowConverter::new(vec![SortField::new(data_type.clone())])?,
                data_type: data_type.clone(),
                best: Vec::new(),
            },
            Accumulator::Edge {
                column,
                source,
                last,
                ..
            } => Accumulator::Edge {
                column: *column,
                source: *source,
                last: *last,
                key: Vec::new(),
            },
            Accumulator::CountDistinct { column, values } => Accumulator::CountDistinct {
                column: *column,
                values: Distinct::new(values.strings.is_some()),
            },
        })
    }

    /// The input column folded, if any.
    fn column(&self) -> Option<usize> {
        match self {
            Accumulator::Count { column, .. } => *column,
            Accumulator::SumInt { column, .. }
            | Accumulator::SumFloat { column, .. }
            | Accumulator::Std { column, .. }
            | Accumulator::Extreme { column, .. }
            | Accumulator::Edge { column, .. }
            | Accumulator::CountDistinct { column, .. } => Some(*column),
        }
    }

    /// Points the folded column at its position within the projected batches.
    fn project(&mut self, projection: &[usize]) {
        let at = |c: &mut usize| *c = projection.binary_search(c).expect("projected column");
        match self {
            Accumulator::Count { column, .. } => {
                if let Some(c) = column {
                    at(c);
                }
            }
            Accumulator::SumInt { column, .. }
            | Accumulator::SumFloat { column, .. }
            | Accumulator::Std { column, .. }
            | Accumulator::Extreme { column, .. }
            | Accumulator::Edge { column, .. }
            | Accumulator::CountDistinct { column, .. } => at(column),
        }
    }

    /// Grows every per-group vector to `len` groups.
    fn resize(&mut self, len: usize) {
        match self {
            Accumulator::Count { n, .. } => n.resize(len, 0),
            Accumulator::SumInt { sum, n, .. } => {
                sum.resize(len, 0);
                n.resize(len, 0);
            }
            Accumulator::SumFloat { state, .. } => state.resize(len, FloatSum::default()),
            Accumulator::Std { state, .. } => state.resize(len, Moments::default()),
            Accumulator::Extreme { best, .. } => best.resize(len, None),
            Accumulator::Edge { key, .. } => key.resize(len, None),
            Accumulator::CountDistinct { values, .. } => values.resize(len),
        }
    }

    fn reset(&mut self, group: usize) {
        match self {
            Accumulator::Count { n, .. } => n[group] = 0,
            Accumulator::SumInt { sum, n, .. } => (sum[group], n[group]) = (0, 0),
            Accumulator::SumFloat { state, .. } => state[group] = FloatSum::default(),
            Accumulator::Std { state, .. } => state[group] = Moments::default(),
            Accumulator::Extreme { best, .. } => best[group] = None,
            Accumulator::Edge { key, .. } => key[group] = None,
            Accumulator::CountDistinct { values, .. } => values.reset(group),
        }
    }

    /// Readies a piece's partial states for merging, once all its rows are folded.
    fn seal(&mut self) {
        if let Accumulator::CountDistinct { values, .. } = self {
            values.seal();
        }
    }

    /// Folds rows of `batch` into the groups in `ids`, in order: every row, or the selected
    /// `rows` (`ids[i]` is the group of `rows[i]`).
    fn fold(
        &mut self,
        batch: &RecordBatch,
        keys: &[RowKey],
        ids: &[u32],
        rows: Option<&[u32]>,
    ) -> Result<(), ArrowError> {
        let row_of = |i: usize| rows.map_or(i, |r| r[i] as usize);
        match self {
            Accumulator::Count { column: None, n } => {
                for &group in ids {
                    n[group as usize] += 1;
                }
            }
            Accumulator::Count {
                column: Some(column),
                n,
            } => {
                let array = batch.column(*column);
                for (i, &group) in ids.iter().enumerate() {
                    if array.is_valid(row_of(i)) {
                        n[group as usize] += 1;
                    }
                }
            }
            Accumulator::SumInt { column, sum, n, .. } => {
                // An i128 holds the sum of 2^64 int64 values, more rows than a table can hold.
                each_i64(batch.column(*column).as_ref(), ids, rows, |group, value| {
                    sum[group] += i128::from(value);
                    n[group] += 1;
                });
            }
            Accumulator::SumFloat { column, state, .. } => {
                each_f64(batch.column(*column).as_ref(), ids, rows, |group, value| {
                    let s = &mut state[group];
                    s.sum += value;
                    s.n += 1;
                });
            }
            Accumulator::Std { column, state } => {
                each_f64(batch.column(*column).as_ref(), ids, rows, |group, x| {
                    state[group].insert(x);
                });
            }
            Accumulator::Extreme {
                column,
                max,
                converter,
                best,
                ..
            } => {
                let array = match rows {
                    None => batch.column(*column).clone(),
                    Some(rows) => take(
                        batch.column(*column),
                        &UInt32Array::from(rows.to_vec()),
                        None,
                    )?,
                };
                let encoded = converter.convert_columns(std::slice::from_ref(&array))?;
                for (i, &group) in ids.iter().enumerate() {
                    if array.is_null(i) {
                        continue;
                    }
                    let candidate = encoded.row(i);
                    let key = keys[row_of(i)];
                    let current = &mut best[group as usize];
                    let better = match current {
                        None => true,
                        Some((value, held)) => beats(*max, candidate.cmp(&value.row()), key, *held),
                    };
                    if better {
                        *current = Some((candidate.owned(), key));
                    }
                }
            }
            Accumulator::Edge { last, key, .. } => {
                for (i, &group) in ids.iter().enumerate() {
                    let candidate = keys[row_of(i)];
                    let current = &mut key[group as usize];
                    let better = match current {
                        None => true,
                        Some(held) if *last => candidate > *held,
                        Some(held) => candidate < *held,
                    };
                    if better {
                        *current = Some(candidate);
                    }
                }
            }
            Accumulator::CountDistinct { column, values } => {
                values.fold(batch.column(*column).as_ref(), ids, rows);
            }
        }
        Ok(())
    }

    /// Takes the selected `rows` of `batch` (`ids[i]` is the group of `rows[i]`) out of their
    /// groups. Counts and sums subtract; `count_distinct` drops a value with its group's last row
    /// holding it; `std` subtracts its sums. `min`/`max`/`first`/`last` cannot be undone,
    /// so a group losing its winning row is marked in `stale` for recomputation; losing any other
    /// row changes nothing.
    fn unfold(
        &mut self,
        batch: &RecordBatch,
        keys: &[RowKey],
        ids: &[u32],
        rows: &[u32],
        stale: &mut [bool],
    ) -> Result<(), ArrowError> {
        match self {
            Accumulator::Count { column: None, n } => {
                for &group in ids {
                    n[group as usize] -= 1;
                }
            }
            Accumulator::Count {
                column: Some(column),
                n,
            } => {
                let array = batch.column(*column);
                for (&group, &row) in ids.iter().zip(rows) {
                    if array.is_valid(row as usize) {
                        n[group as usize] -= 1;
                    }
                }
            }
            Accumulator::SumInt { column, sum, n, .. } => {
                each_i64(
                    batch.column(*column).as_ref(),
                    ids,
                    Some(rows),
                    |group, value| {
                        sum[group] -= i128::from(value);
                        n[group] -= 1;
                    },
                );
            }
            Accumulator::SumFloat { column, state, .. } => {
                each_f64(
                    batch.column(*column).as_ref(),
                    ids,
                    Some(rows),
                    |group, value| {
                        let s = &mut state[group];
                        s.n -= 1;
                        // An emptied group restarts exactly, shedding accumulated rounding.
                        s.sum = if s.n == 0 { 0.0 } else { s.sum - value };
                    },
                );
            }
            Accumulator::Std { column, state } => {
                each_f64(
                    batch.column(*column).as_ref(),
                    ids,
                    Some(rows),
                    |group, x| state[group].remove(x),
                );
            }
            Accumulator::Extreme { best, .. } => {
                for (&group, &row) in ids.iter().zip(rows) {
                    let group = group as usize;
                    if best[group]
                        .as_ref()
                        .is_some_and(|(_, key)| *key == keys[row as usize])
                    {
                        stale[group] = true;
                    }
                }
            }
            Accumulator::Edge { key, .. } => {
                for (&group, &row) in ids.iter().zip(rows) {
                    let group = group as usize;
                    if key[group] == Some(keys[row as usize]) {
                        stale[group] = true;
                    }
                }
            }
            Accumulator::CountDistinct { column, values } => {
                values.unfold(batch.column(*column).as_ref(), ids, rows)?;
            }
        }
        Ok(())
    }

    /// Merges group `from` of `other` (the same aggregation) into group `into` of this one.
    fn merge(&mut self, into: usize, other: &Accumulator, from: usize) -> Result<(), ArrowError> {
        match (self, other) {
            (Accumulator::Count { n, .. }, Accumulator::Count { n: m, .. }) => n[into] += m[from],
            (Accumulator::SumInt { sum, n, .. }, Accumulator::SumInt { sum: s, n: m, .. }) => {
                sum[into] += s[from];
                n[into] += m[from];
            }
            (Accumulator::SumFloat { state, .. }, Accumulator::SumFloat { state: theirs, .. }) => {
                let (s, t) = (&mut state[into], theirs[from]);
                s.sum += t.sum;
                s.n += t.n;
            }
            (Accumulator::Std { state, .. }, Accumulator::Std { state: theirs, .. }) => {
                state[into].merge(&theirs[from]);
            }
            (Accumulator::Extreme { max, best, .. }, Accumulator::Extreme { best: theirs, .. }) => {
                if let Some((value, key)) = &theirs[from] {
                    let better = match &best[into] {
                        None => true,
                        Some((held, held_key)) => {
                            beats(*max, value.row().cmp(&held.row()), *key, *held_key)
                        }
                    };
                    if better {
                        best[into] = Some((value.clone(), *key));
                    }
                }
            }
            (Accumulator::Edge { last, key, .. }, Accumulator::Edge { key: theirs, .. }) => {
                if let Some(candidate) = theirs[from] {
                    let better = match key[into] {
                        None => true,
                        Some(held) if *last => candidate > held,
                        Some(held) => candidate < held,
                    };
                    if better {
                        key[into] = Some(candidate);
                    }
                }
            }
            (
                Accumulator::CountDistinct { values, .. },
                Accumulator::CountDistinct { values: theirs, .. },
            ) => values.merge(into, theirs, from),
            _ => unreachable!("partials come from the same aggregation"),
        }
        Ok(())
    }

    /// The output column for `groups`, in that order.
    fn output(&self, groups: &[u32], input: Reader<'_>) -> Result<ArrayRef, ArrowError> {
        let at = groups.iter().map(|g| *g as usize);
        Ok(match self {
            Accumulator::Count { n, .. } => {
                Arc::new(Int64Array::from_iter_values(at.map(|g| n[g])))
            }
            Accumulator::SumInt {
                mean: true, sum, n, ..
            } => Arc::new(Float64Array::from_iter(
                at.map(|g| (n[g] > 0).then(|| exact_mean(sum[g], n[g]))),
            )),
            Accumulator::SumInt { name, sum, n, .. } => Arc::new(
                at.map(|g| {
                    if n[g] == 0 {
                        return Ok(None);
                    }
                    i64::try_from(sum[g]).map(Some).map_err(|_| {
                        ArrowError::ComputeError(format!(
                            "sum of `{name}` is {}, which does not fit in int64",
                            sum[g]
                        ))
                    })
                })
                .collect::<Result<Int64Array, ArrowError>>()?,
            ),
            Accumulator::SumFloat { state, mean, .. } => {
                Arc::new(Float64Array::from_iter(at.map(|g| {
                    let s = state[g];
                    (s.n > 0).then(|| if *mean { s.sum / s.n as f64 } else { s.sum })
                })))
            }
            Accumulator::Std { state, .. } => {
                Arc::new(Float64Array::from_iter(at.map(|g| state[g].std())))
            }
            Accumulator::Extreme { source, best, .. } => {
                let keys: Vec<_> = at.map(|g| best[g].as_ref().map(|(_, key)| *key)).collect();
                pick_column(input, *source, &keys)?
            }
            Accumulator::Edge { source, key, .. } => {
                let keys: Vec<_> = at.map(|g| key[g]).collect();
                pick_column(input, *source, &keys)?
            }
            Accumulator::CountDistinct { values, .. } => Arc::new(Int64Array::from_iter_values(
                at.map(|g| values.counts.distinct[g]),
            )),
        })
    }
}

/// `sum / n` rounded once to the nearest float64, as if divided exactly. The magnitude is scaled
/// up until the integer quotient has at least 55 bits: the 53 a float64 keeps, the rounding bit,
/// and below it a sticky bit set when the division left a remainder. That quotient rounds as the
/// true one does; scaling back down by a power of two is exact.
fn exact_mean(sum: i128, n: i64) -> f64 {
    let (magnitude, n) = (sum.unsigned_abs(), u128::from(n.unsigned_abs()));
    let bits = |x: u128| 128 - x.leading_zeros();
    let shift = (bits(n) + 55).saturating_sub(bits(magnitude));
    let scaled = magnitude << shift;
    // The quotient with a sticky bit for any remainder, so the one rounding to f64 is exact.
    let quotient = (scaled / n) | u128::from(scaled % n != 0);
    let value = quotient as f64 * f64::from_bits(u64::from(1023 - shift) << 52);
    if sum < 0 { -value } else { value }
}

/// `count_distinct`'s state for every group: how many rows hold each distinct non-null value, so
/// removing a row is exact. One table holds every group's values, keyed by group and value, so a
/// group with few values costs no table of its own and memory follows the distinct values
/// present. Fixed-width values are stored as 64 bits (`each_bits`); strings are interned once,
/// their bytes in one buffer, and stored by id.
pub(crate) struct Distinct {
    counts: Counts,
    strings: Option<Strings>,
    /// A sealed piece's entries, by group: group `g`'s are `sealed[starts[g]..starts[g + 1]]`.
    starts: Vec<usize>,
    sealed: Vec<(u64, u64)>,
}

struct Counts {
    entries: HashTable<Entry>,
    /// Per group: its distinct values.
    distinct: Vec<i64>,
    /// Per group: the generation its live entries carry. Resetting a group starts a new
    /// generation rather than searching the table for its entries; the old entries are dead, and
    /// are swept out once they outnumber the live ones.
    generation: Vec<u32>,
    dead: usize,
}

/// One distinct value of one group, and how many of the group's rows hold it.
#[derive(Clone, Copy)]
struct Entry {
    value: u64,
    count: u64,
    group: u32,
    generation: u32,
}

fn entry_hash(group: u32, generation: u32, value: u64) -> u64 {
    HASHER.hash_one((group, generation, value))
}

impl Counts {
    /// Adds `count` rows holding `value` to `group`; true if the value is new to the group.
    fn add(&mut self, group: usize, value: u64, count: u64) -> bool {
        let (id, generation) = (group as u32, self.generation[group]);
        let hash = entry_hash(id, generation, value);
        let same = |e: &Entry| e.value == value && e.group == id && e.generation == generation;
        if let Some(entry) = self.entries.find_mut(hash, same) {
            entry.count += count;
            return false;
        }
        let entry = Entry {
            value,
            count,
            group: id,
            generation,
        };
        self.entries
            .insert_unique(hash, entry, |e| entry_hash(e.group, e.generation, e.value));
        self.distinct[group] += 1;
        true
    }

    /// Takes one row holding `value` out of `group`: whether it was the value's last row, or
    /// `None` if the group holds no such value.
    fn remove(&mut self, group: usize, value: u64) -> Option<bool> {
        let (id, generation) = (group as u32, self.generation[group]);
        let hash = entry_hash(id, generation, value);
        let same = |e: &Entry| e.value == value && e.group == id && e.generation == generation;
        let mut entry = self.entries.find_entry(hash, same).ok()?;
        entry.get_mut().count -= 1;
        if entry.get().count > 0 {
            return Some(false);
        }
        entry.remove();
        self.distinct[group] -= 1;
        Some(true)
    }
}

impl Distinct {
    fn new(strings: bool) -> Self {
        Self {
            counts: Counts {
                entries: HashTable::new(),
                distinct: Vec::new(),
                generation: Vec::new(),
                dead: 0,
            },
            strings: strings.then(Strings::default),
            starts: Vec::new(),
            sealed: Vec::new(),
        }
    }

    fn resize(&mut self, len: usize) {
        self.counts.distinct.resize(len, 0);
        self.counts.generation.resize(len, 0);
    }

    fn reset(&mut self, group: usize) {
        let counts = &mut self.counts;
        let live = std::mem::take(&mut counts.distinct[group]);
        if live == 0 {
            return;
        }
        // Sweeping once the dead outnumber the live keeps a generation from wrapping around onto
        // entries still in the table.
        counts.generation[group] = counts.generation[group].wrapping_add(1);
        counts.dead += live as usize;
        if counts.dead * 2 > counts.entries.len() {
            let Counts {
                entries,
                generation,
                dead,
                ..
            } = counts;
            let strings = &mut self.strings;
            entries.retain(|e| {
                let live = e.generation == generation[e.group as usize];
                if !live && let Some(strings) = strings.as_mut() {
                    strings.release(e.value as u32);
                }
                live
            });
            *dead = 0;
        }
    }

    fn fold(&mut self, array: &dyn Array, ids: &[u32], rows: Option<&[u32]>) {
        let counts = &mut self.counts;
        match &mut self.strings {
            None => each_bits(array, ids, rows, |group, value| {
                counts.add(group, value, 1);
            }),
            Some(strings) => each_str(array, ids, rows, |group, text| {
                let id = strings.intern(text, HASHER.hash_one(text));
                if counts.add(group, u64::from(id), 1) {
                    strings.refs[id as usize] += 1;
                }
            }),
        }
    }

    fn unfold(&mut self, array: &dyn Array, ids: &[u32], rows: &[u32]) -> Result<(), ArrowError> {
        let counts = &mut self.counts;
        let mut missing = false;
        match &mut self.strings {
            None => each_bits(array, ids, Some(rows), |group, value| {
                missing |= counts.remove(group, value).is_none();
            }),
            Some(strings) => each_str(array, ids, Some(rows), |group, text| {
                let removed = strings
                    .find(text, HASHER.hash_one(text))
                    .and_then(|id| Some((id, counts.remove(group, u64::from(id))?)));
                match removed {
                    None => missing = true,
                    Some((id, true)) => strings.release(id),
                    Some((_, false)) => {}
                }
            }),
        }
        if missing {
            return Err(ArrowError::ComputeError(
                "count_distinct: a removed row's value was not in its group".into(),
            ));
        }
        Ok(())
    }

    /// Indexes a piece's entries by group for merging, and drops its table.
    fn seal(&mut self) {
        let counts = &mut self.counts;
        let mut starts = Vec::with_capacity(counts.distinct.len() + 1);
        let mut total = 0;
        starts.push(0);
        for &distinct in &counts.distinct {
            total += distinct as usize;
            starts.push(total);
        }
        let mut next = starts.clone();
        let mut sealed = vec![(0, 0); total];
        for e in counts.entries.iter() {
            let at = &mut next[e.group as usize];
            sealed[*at] = (e.value, e.count);
            *at += 1;
        }
        counts.entries = HashTable::new();
        self.starts = starts;
        self.sealed = sealed;
    }

    /// Merges group `from` of a sealed piece into group `into`.
    fn merge(&mut self, into: usize, theirs: &Distinct, from: usize) {
        let entries = &theirs.sealed[theirs.starts[from]..theirs.starts[from + 1]];
        let counts = &mut self.counts;
        match (&mut self.strings, &theirs.strings) {
            (Some(strings), Some(their_strings)) => {
                for &(value, count) in entries {
                    let their_id = value as usize;
                    let text = their_strings.get(their_id);
                    let id = strings.intern(text, their_strings.hashes[their_id]);
                    if counts.add(into, u64::from(id), count) {
                        strings.refs[id as usize] += 1;
                    }
                }
            }
            _ => {
                for &(value, count) in entries {
                    counts.add(into, value, count);
                }
            }
        }
    }
}

/// Interned strings, by id: each one's bytes once, in one buffer.
#[derive(Default)]
struct Strings {
    bytes: Vec<u8>,
    /// Per id: where its bytes start in `bytes`, and how many there are.
    spans: Vec<(usize, usize)>,
    /// Per id: the engine-wide hash of its bytes.
    hashes: Vec<u64>,
    /// Per id: the entries holding it. An id no entry holds is freed for reuse.
    refs: Vec<u32>,
    free: Vec<u32>,
    table: HashTable<u32>,
    /// Bytes of freed strings still in `bytes`.
    garbage: usize,
}

impl Strings {
    fn get(&self, id: usize) -> &[u8] {
        let (start, len) = self.spans[id];
        &self.bytes[start..start + len]
    }

    fn find(&self, text: &[u8], hash: u64) -> Option<u32> {
        self.table
            .find(hash, |&id| self.get(id as usize) == text)
            .copied()
    }

    /// The id of `text`, whose engine-wide hash is `hash`, interning it if new. A new id is held
    /// by no entry yet: the caller counts the entry it interned it for.
    fn intern(&mut self, text: &[u8], hash: u64) -> u32 {
        if let Some(id) = self.find(text, hash) {
            return id;
        }
        let span = (self.bytes.len(), text.len());
        self.bytes.extend_from_slice(text);
        let id = match self.free.pop() {
            Some(id) => {
                self.spans[id as usize] = span;
                self.hashes[id as usize] = hash;
                id
            }
            None => {
                self.spans.push(span);
                self.hashes.push(hash);
                self.refs.push(0);
                (self.spans.len() - 1) as u32
            }
        };
        let hashes = &self.hashes;
        self.table
            .insert_unique(hash, id, |&id| hashes[id as usize]);
        id
    }

    /// Drops one entry's hold on `id`, freeing the string with the last.
    fn release(&mut self, id: u32) {
        let index = id as usize;
        self.refs[index] -= 1;
        if self.refs[index] > 0 {
            return;
        }
        if let Ok(entry) = self
            .table
            .find_entry(self.hashes[index], |other| *other == id)
        {
            entry.remove();
        }
        self.garbage += self.spans[index].1;
        self.free.push(id);
        // Compacting only once freed bytes are half the buffer keeps the copying proportional to
        // the bytes freed.
        if self.garbage > self.bytes.len() / 2 {
            let mut bytes = Vec::with_capacity(self.bytes.len() - self.garbage);
            for (span, &refs) in self.spans.iter_mut().zip(&self.refs) {
                if refs > 0 {
                    let (start, len) = *span;
                    *span = (bytes.len(), len);
                    bytes.extend_from_slice(&self.bytes[start..start + len]);
                }
            }
            self.bytes = bytes;
            self.garbage = 0;
        }
    }
}

/// Whether a candidate beats the held value: by value, then the smaller row key on ties.
fn beats(max: bool, order: std::cmp::Ordering, candidate: RowKey, held: RowKey) -> bool {
    let order = if max { order } else { order.reverse() };
    order.is_gt() || (order.is_eq() && candidate < held)
}

/// Calls `f(group, value)` for each row with a value, in order: every row of `array`, or the
/// selected `rows`.
fn each<T: ArrowPrimitiveType>(
    array: &PrimitiveArray<T>,
    ids: &[u32],
    rows: Option<&[u32]>,
    mut f: impl FnMut(usize, T::Native),
) {
    let values = array.values();
    let nulls = array.null_count() > 0;
    match (rows, nulls) {
        (None, false) => {
            for (&group, &value) in ids.iter().zip(values.iter()) {
                f(group as usize, value);
            }
        }
        (None, true) => {
            for (row, (&group, &value)) in ids.iter().zip(values.iter()).enumerate() {
                if array.is_valid(row) {
                    f(group as usize, value);
                }
            }
        }
        (Some(rows), _) => {
            for (&group, &row) in ids.iter().zip(rows) {
                if !nulls || array.is_valid(row as usize) {
                    f(group as usize, values[row as usize]);
                }
            }
        }
    }
}

fn each_f64(array: &dyn Array, ids: &[u32], rows: Option<&[u32]>, mut f: impl FnMut(usize, f64)) {
    match array.data_type() {
        DataType::Int16 => each(array.as_primitive::<Int16Type>(), ids, rows, |g, v| {
            f(g, f64::from(v));
        }),
        DataType::Int32 => each(array.as_primitive::<Int32Type>(), ids, rows, |g, v| {
            f(g, f64::from(v));
        }),
        DataType::Int64 => each(array.as_primitive::<Int64Type>(), ids, rows, |g, v| {
            f(g, v as f64);
        }),
        _ => each(array.as_primitive::<Float64Type>(), ids, rows, f),
    }
}

fn each_i64(array: &dyn Array, ids: &[u32], rows: Option<&[u32]>, mut f: impl FnMut(usize, i64)) {
    match array.data_type() {
        DataType::Int16 => each(array.as_primitive::<Int16Type>(), ids, rows, |g, v| {
            f(g, i64::from(v));
        }),
        DataType::Int32 => each(array.as_primitive::<Int32Type>(), ids, rows, |g, v| {
            f(g, i64::from(v));
        }),
        _ => each(array.as_primitive::<Int64Type>(), ids, rows, f),
    }
}

/// Calls `f(group, bits)` for each row with a value, in order (as `each`), with the value as 64
/// bits: integers, dates, and timestamps sign-extended, booleans as 0 or 1, and floats by bit
/// pattern with every NaN made one NaN and -0.0 made 0.0, so values SQL's DISTINCT treats as
/// equal have equal bits. A column of type null has no values.
fn each_bits(array: &dyn Array, ids: &[u32], rows: Option<&[u32]>, mut f: impl FnMut(usize, u64)) {
    match array.data_type() {
        DataType::Int16 => each(array.as_primitive::<Int16Type>(), ids, rows, |g, v| {
            f(g, i64::from(v) as u64);
        }),
        DataType::Int32 => each(array.as_primitive::<Int32Type>(), ids, rows, |g, v| {
            f(g, i64::from(v) as u64);
        }),
        DataType::Date32 => each(array.as_primitive::<Date32Type>(), ids, rows, |g, v| {
            f(g, i64::from(v) as u64);
        }),
        DataType::Int64 => each(array.as_primitive::<Int64Type>(), ids, rows, |g, v| {
            f(g, v as u64);
        }),
        DataType::Timestamp(..) => each(
            array.as_primitive::<TimestampNanosecondType>(),
            ids,
            rows,
            |g, v| f(g, v as u64),
        ),
        DataType::Float64 => each(array.as_primitive::<Float64Type>(), ids, rows, |g, v| {
            let bits = if v.is_nan() {
                f64::NAN.to_bits()
            } else if v == 0.0 {
                0
            } else {
                v.to_bits()
            };
            f(g, bits);
        }),
        DataType::Boolean => {
            let array = array.as_boolean();
            for (i, &group) in ids.iter().enumerate() {
                let row = rows.map_or(i, |r| r[i] as usize);
                if array.is_valid(row) {
                    f(group as usize, u64::from(array.value(row)));
                }
            }
        }
        _ => {}
    }
}

/// Calls `f(group, bytes)` for each row of a string column with a value, in order (as `each`).
fn each_str(array: &dyn Array, ids: &[u32], rows: Option<&[u32]>, mut f: impl FnMut(usize, &[u8])) {
    let array = array.as_string::<i32>();
    let nulls = array.null_count() > 0;
    for (i, &group) in ids.iter().enumerate() {
        let row = rows.map_or(i, |r| r[i] as usize);
        if !nulls || array.is_valid(row) {
            f(group as usize, array.value(row).as_bytes());
        }
    }
}

/// What every shard needs to know about the operator.
struct Context {
    /// Input columns the operator reads, sorted: the group columns and aggregated columns.
    projection: Vec<usize>,
    /// Group columns in the input schema, for output rows.
    group_columns: Vec<usize>,
    /// How the group key is read from projected batches.
    keys: KeyKind,
    output: SchemaRef,
    add_only: bool,
}

/// Aggregations per group: partials per piece, merged per shard.
pub(crate) struct AggBy {
    context: Context,
    /// Empty accumulators to copy for each piece's partials.
    templates: Vec<Accumulator>,
    shards: Vec<Shard>,
}

impl AggBy {
    pub fn new(
        input: &Schema,
        group_columns: Vec<usize>,
        specs: &[&str],
        add_only: bool,
        output: SchemaRef,
    ) -> Result<Self, Error> {
        let schema_error = |e: ArrowError| Error::Schema(e.to_string());
        let mut templates = specs
            .iter()
            .map(|spec| Accumulator::parse(spec, input).map(|(a, _)| a))
            .collect::<Result<Vec<_>, _>>()?;
        let mut projection = group_columns.clone();
        projection.extend(templates.iter().filter_map(Accumulator::column));
        projection.sort_unstable();
        projection.dedup();
        for template in &mut templates {
            template.project(&projection);
        }
        let projected = input.project(&projection).map_err(schema_error)?;
        let local_groups: Vec<usize> = group_columns
            .iter()
            .map(|c| {
                projection
                    .binary_search(c)
                    .expect("group columns are projected")
            })
            .collect();

        let count = threads().min(64);
        let mut shards = Vec::with_capacity(count);
        for index in 0..count {
            let groups = Membership::new(
                &projected,
                local_groups.clone(),
                add_only,
                index as RowKey,
                count as RowKey,
            )
            .map_err(schema_error)?;
            let accumulators = templates
                .iter()
                .map(Accumulator::fresh)
                .collect::<Result<_, _>>()
                .map_err(schema_error)?;
            shards.push(Shard {
                index,
                groups,
                accumulators,
                published: Vec::new(),
                stale: Vec::new(),
            });
        }
        Ok(Self {
            context: Context {
                keys: KeyKind::new(&projected, local_groups).map_err(schema_error)?,
                projection,
                group_columns,
                output,
                add_only,
            },
            templates,
            shards,
        })
    }

    pub fn process(
        &mut self,
        input: Reader<'_>,
        delta: &Delta,
        own: &Storage,
    ) -> Result<Delta, ArrowError> {
        let view = View::new(delta, &self.context.projection)?;
        let threads = if view.rows() >= PARALLEL_ROWS {
            self.shards.len()
        } else {
            1
        };
        let context = &self.context;
        let shard_count = self.shards.len();

        // Removed and modified rows: route each to the shard owning its group.
        let routed = view.routed();
        let mut pieces = Vec::new();
        for (segment, batch) in routed.iter().enumerate() {
            for (offset, len) in offsets(batch.num_rows(), PIECE_ROWS) {
                pieces.push((segment, offset, len));
            }
        }
        let routes = run(threads, pieces, |(), (segment, offset, len)| {
            let batch = routed[segment].slice(offset, len);
            let shards = split(&context.keys, &batch, offset as u32, shard_count)?;
            Ok::<_, ArrowError>(Route { segment, shards })
        })
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

        // Added rows: aggregate each piece into partial states.
        let mut pieces = Vec::new();
        for (keys, batch) in &view.added {
            for (offset, len) in offsets(batch.num_rows(), PIECE_ROWS) {
                pieces.push((&keys[offset..offset + len], batch.slice(offset, len)));
            }
        }
        let templates = &self.templates;
        let partials = run(threads, pieces, |table, (keys, batch)| {
            Partial::build(context, templates, table, keys, batch, shard_count)
        })
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

        let shards: Vec<&mut Shard> = self.shards.iter_mut().collect();
        let outputs = run(threads, shards, |(), shard| {
            let part = view.part(&routes, shard.index);
            shard.process(context, input, &part, &partials, own)
        });
        let mut out = Delta::default();
        for part in outputs {
            let part = part?;
            out.added.extend(part.added);
            out.removed.extend(part.removed);
            out.modified.extend(part.modified);
        }
        Ok(out)
    }
}

/// One piece of added rows, aggregated on its own.
struct Partial {
    batch: RecordBatch,
    encoded: Option<EncodedRows>,
    groups: LocalGroups,
    accumulators: Vec<Accumulator>,
    /// Per local group: its largest row key.
    last: Vec<RowKey>,
    /// Per local group, every row key, when the input is not append-only.
    keys: Option<Vec<Vec<RowKey>>>,
    /// Local groups per shard, in first-appearance order.
    by_shard: Vec<Vec<u32>>,
}

impl Partial {
    fn build(
        context: &Context,
        templates: &[Accumulator],
        table: &mut HashTable<LocalEntry>,
        row_keys: &[RowKey],
        batch: RecordBatch,
        shards: usize,
    ) -> Result<Self, ArrowError> {
        let encoded = context.keys.encode(&batch)?;
        let keys = context.keys.keys(&batch, encoded.as_ref());
        let groups = LocalGroups::build(&keys, batch.num_rows(), table);
        let count = groups.len();
        let mut accumulators = templates
            .iter()
            .map(Accumulator::fresh)
            .collect::<Result<Vec<_>, _>>()?;
        for accumulator in &mut accumulators {
            accumulator.resize(count);
            accumulator.fold(&batch, row_keys, &groups.ids, None)?;
            accumulator.seal();
        }
        let mut last = vec![0; count];
        let mut all = (!context.add_only).then(|| vec![Vec::new(); count]);
        for (&id, &key) in groups.ids.iter().zip(row_keys) {
            let group = id as usize;
            last[group] = last[group].max(key);
            if let Some(all) = &mut all {
                all[group].push(key);
            }
        }
        let mut by_shard = vec![Vec::new(); shards];
        for (group, hash) in groups.hash.iter().enumerate() {
            by_shard[shard_of(*hash, shards)].push(group as u32);
        }
        Ok(Self {
            batch,
            encoded,
            groups,
            accumulators,
            last,
            keys: all,
            by_shard,
        })
    }
}

/// A delta with batches projected to the columns the operator reads.
struct View<'a> {
    removed: Vec<(&'a [RowKey], RecordBatch)>,
    modified: Vec<(&'a [RowKey], RecordBatch, RecordBatch)>,
    added: Vec<(&'a [RowKey], RecordBatch)>,
}

/// The rows of one routed batch piece in each shard.
struct Route {
    segment: usize,
    shards: Vec<Vec<u32>>,
}

/// Rows of one batch that belong to a shard, by index, in order.
struct Selected<'a> {
    keys: &'a [RowKey],
    batch: &'a RecordBatch,
    rows: Vec<u32>,
}

/// Modified rows that stay within a shard.
struct SelectedModified<'a> {
    keys: &'a [RowKey],
    previous: &'a RecordBatch,
    current: &'a RecordBatch,
    rows: Vec<u32>,
}

/// One shard's removed and modified rows. A modified row that changes shard is removed from its
/// old shard and added to its new one.
#[derive(Default)]
struct Part<'a> {
    removed: Vec<Selected<'a>>,
    modified: Vec<SelectedModified<'a>>,
    added: Vec<Selected<'a>>,
}

impl<'a> View<'a> {
    fn new(delta: &'a Delta, projection: &[usize]) -> Result<Self, ArrowError> {
        Ok(Self {
            removed: delta
                .removed
                .iter()
                .map(|r| Ok((r.keys.as_slice(), r.batch.project(projection)?)))
                .collect::<Result<_, ArrowError>>()?,
            modified: delta
                .modified
                .iter()
                .map(|m| {
                    Ok((
                        m.keys.as_slice(),
                        m.previous.project(projection)?,
                        m.current.project(projection)?,
                    ))
                })
                .collect::<Result<_, ArrowError>>()?,
            added: delta
                .added
                .iter()
                .map(|r| Ok((r.keys.as_slice(), r.batch.project(projection)?)))
                .collect::<Result<_, ArrowError>>()?,
        })
    }

    fn rows(&self) -> usize {
        self.removed.iter().map(|(k, _)| k.len()).sum::<usize>()
            + self
                .modified
                .iter()
                .map(|(k, _, _)| 2 * k.len())
                .sum::<usize>()
            + self.added.iter().map(|(k, _)| k.len()).sum::<usize>()
    }

    /// The batches routed by key: removed, modified previous, modified current.
    fn routed(&self) -> Vec<&RecordBatch> {
        self.removed
            .iter()
            .map(|(_, b)| b)
            .chain(self.modified.iter().map(|(_, previous, _)| previous))
            .chain(self.modified.iter().map(|(_, _, current)| current))
            .collect()
    }

    /// The routed rows of `shard`, given every route in segment and row order.
    fn part(&self, routes: &[Route], shard: usize) -> Part<'_> {
        let segments = self.removed.len() + 2 * self.modified.len();
        let mut rows: Vec<Vec<u32>> = vec![Vec::new(); segments];
        for route in routes {
            rows[route.segment].extend_from_slice(&route.shards[shard]);
        }
        let mut rows = rows.into_iter();
        let mut part = Part::default();
        let selected = |keys, batch, rows: Vec<u32>| {
            (!rows.is_empty()).then_some(Selected { keys, batch, rows })
        };
        for (keys, batch) in &self.removed {
            let mine = rows.next().expect("one row list per segment");
            part.removed.extend(selected(*keys, batch, mine));
        }
        let previous: Vec<Vec<u32>> = rows.by_ref().take(self.modified.len()).collect();
        let current: Vec<Vec<u32>> = rows.collect();
        for (((keys, before, after), old), new) in self.modified.iter().zip(previous).zip(current) {
            let (stay, leave, enter) = compare(&old, &new);
            if !stay.is_empty() {
                part.modified.push(SelectedModified {
                    keys,
                    previous: before,
                    current: after,
                    rows: stay,
                });
            }
            part.removed.extend(selected(*keys, before, leave));
            part.added.extend(selected(*keys, after, enter));
        }
        part
    }
}

/// Splits two ascending row lists into rows in both, only the first, and only the second.
fn compare(old: &[u32], new: &[u32]) -> (Vec<u32>, Vec<u32>, Vec<u32>) {
    let (mut both, mut only_old, mut only_new) = (Vec::new(), Vec::new(), Vec::new());
    let (mut i, mut j) = (0, 0);
    while i < old.len() || j < new.len() {
        match (old.get(i), new.get(j)) {
            (Some(a), Some(b)) if a == b => {
                both.push(*a);
                i += 1;
                j += 1;
            }
            (Some(a), Some(b)) if a < b => {
                only_old.push(*a);
                i += 1;
            }
            (Some(a), None) => {
                only_old.push(*a);
                i += 1;
            }
            (_, Some(b)) => {
                only_new.push(*b);
                j += 1;
            }
            (None, None) => unreachable!("loop condition"),
        }
    }
    (both, only_old, only_new)
}

/// The groups whose key hashes to one shard.
struct Shard {
    index: usize,
    groups: Membership,
    accumulators: Vec<Accumulator>,
    published: Vec<bool>,
    /// Recompute from members this cycle, because a row left the group.
    stale: Vec<bool>,
}

impl Shard {
    /// Grows per-group vectors to the membership's ids and resets newly created groups.
    fn sync(&mut self) {
        let len = self.groups.capacity();
        self.published.resize(len, false);
        self.stale.resize(len, false);
        for accumulator in &mut self.accumulators {
            accumulator.resize(len);
        }
        for id in self.groups.take_created() {
            let group = id as usize;
            self.published[group] = false;
            self.stale[group] = false;
            for accumulator in &mut self.accumulators {
                accumulator.reset(group);
            }
        }
    }

    fn assign(&mut self, batch: &RecordBatch, rows: &[u32]) -> Result<Vec<u32>, ArrowError> {
        let ids = self.groups.assign(batch, Some(rows))?;
        self.sync();
        Ok(ids)
    }

    fn process(
        &mut self,
        context: &Context,
        input: Reader<'_>,
        part: &Part<'_>,
        partials: &[Partial],
        own: &Storage,
    ) -> Result<Delta, ArrowError> {
        for selected in &part.removed {
            let ids = self.assign(selected.batch, &selected.rows)?;
            for (&id, &row) in ids.iter().zip(&selected.rows) {
                self.groups.members[id as usize].remove(selected.keys[row as usize])?;
            }
            for accumulator in &mut self.accumulators {
                accumulator.unfold(
                    selected.batch,
                    selected.keys,
                    &ids,
                    &selected.rows,
                    &mut self.stale,
                )?;
            }
        }
        for modified in &part.modified {
            let before = self.assign(modified.previous, &modified.rows)?;
            let after = self.assign(modified.current, &modified.rows)?;
            for ((&old, &new), &row) in before.iter().zip(&after).zip(&modified.rows) {
                let key = modified.keys[row as usize];
                self.groups.members[old as usize].remove(key)?;
                self.groups.members[new as usize].insert(key);
            }
            for accumulator in &mut self.accumulators {
                accumulator.unfold(
                    modified.previous,
                    modified.keys,
                    &before,
                    &modified.rows,
                    &mut self.stale,
                )?;
                accumulator.fold(
                    modified.current,
                    modified.keys,
                    &after,
                    Some(&modified.rows),
                )?;
            }
        }
        for selected in &part.added {
            let ids = self.assign(selected.batch, &selected.rows)?;
            for (&id, &row) in ids.iter().zip(&selected.rows) {
                self.groups.members[id as usize].insert(selected.keys[row as usize]);
            }
            for accumulator in &mut self.accumulators {
                accumulator.fold(selected.batch, selected.keys, &ids, Some(&selected.rows))?;
            }
        }
        for partial in partials {
            self.merge(context, partial)?;
        }

        let touched = self.groups.take_touched();
        self.recompute_stale(context, input, &touched)?;

        let (mut added, mut changed, mut removed) = (Vec::new(), Vec::new(), Vec::new());
        for id in touched {
            let group = id as usize;
            if self.groups.members[group].is_empty() {
                if self.published[group] {
                    removed.push(self.groups.slot[group]);
                }
                self.published[group] = false;
                self.groups.release(id);
            } else if self.published[group] {
                changed.push(id);
            } else {
                self.published[group] = true;
                added.push(id);
            }
        }

        let mut out = Delta::default();
        let (slots, batch) = self.output_rows(context, input, &added)?;
        out.added.push(Rows { keys: slots, batch });
        let (slots, current) = self.output_rows(context, input, &changed)?;
        out.modified.push(Modified {
            previous: own.gather(&slots)?,
            current,
            keys: slots,
        });
        push_removed(&mut out, own, removed)?;
        Ok(out)
    }

    /// Merges one piece's partial states for this shard's groups.
    fn merge(&mut self, context: &Context, partial: &Partial) -> Result<(), ArrowError> {
        let keys = context.keys.keys(&partial.batch, partial.encoded.as_ref());
        let mine = &partial.by_shard[self.index];
        let ids: Vec<u32> = mine
            .iter()
            .map(|&local| {
                let row = partial.groups.first_row[local as usize] as usize;
                self.groups.group(
                    partial.groups.hash[local as usize],
                    |key| keys.matches(row, key),
                    || keys.owned(row),
                )
            })
            .collect();
        self.sync();
        for (&local, &id) in mine.iter().zip(&ids) {
            let (local, group) = (local as usize, id as usize);
            let members = &mut self.groups.members[group];
            match &partial.keys {
                Some(all) => all[local].iter().for_each(|key| members.insert(*key)),
                None => members.insert(partial.last[local]),
            }
            if self.stale[group] {
                continue;
            }
            for (accumulator, theirs) in self.accumulators.iter_mut().zip(&partial.accumulators) {
                accumulator.merge(group, theirs, local)?;
            }
        }
        Ok(())
    }

    /// Recomputes every stale, non-empty touched group from its rows, in key order.
    fn recompute_stale(
        &mut self,
        context: &Context,
        input: Reader<'_>,
        touched: &[u32],
    ) -> Result<(), ArrowError> {
        let (mut keys, mut ids) = (Vec::new(), Vec::new());
        for &id in touched {
            let group = id as usize;
            if !std::mem::take(&mut self.stale[group]) || self.groups.members[group].is_empty() {
                continue;
            }
            for accumulator in &mut self.accumulators {
                accumulator.reset(group);
            }
            for key in self.groups.members[group].keys() {
                keys.push(key);
                ids.push(id);
            }
        }
        if keys.is_empty() {
            return Ok(());
        }
        let batch = input.gather(&keys)?.project(&context.projection)?;
        for accumulator in &mut self.accumulators {
            accumulator.fold(&batch, &keys, &ids, None)?;
        }
        Ok(())
    }

    /// The output rows for some groups: group columns, then one column per aggregation.
    fn output_rows(
        &self,
        context: &Context,
        input: Reader<'_>,
        ids: &[u32],
    ) -> Result<(Vec<RowKey>, RecordBatch), ArrowError> {
        let representatives: Vec<RowKey> = ids
            .iter()
            .map(|id| {
                self.groups.members[*id as usize]
                    .last()
                    .expect("published groups have rows")
            })
            .collect();
        let rows = input.gather(&representatives)?;
        let mut columns: Vec<ArrayRef> = context
            .group_columns
            .iter()
            .map(|c| rows.column(*c).clone())
            .collect();
        for accumulator in &self.accumulators {
            columns.push(accumulator.output(ids, input)?);
        }
        let slots = ids
            .iter()
            .map(|id| self.groups.slot[*id as usize])
            .collect();
        let options = RecordBatchOptions::new().with_row_count(Some(ids.len()));
        let batch = RecordBatch::try_new_with_options(context.output.clone(), columns, &options)?;
        Ok((slots, batch))
    }
}

/// Output schema for `agg_by`: the group columns, then the aggregations.
pub(crate) fn agg_schema(
    input: &Schema,
    group_columns: &[usize],
    outputs: &[(String, DataType)],
) -> Result<SchemaRef, Error> {
    let mut fields: Vec<Field> = group_columns
        .iter()
        .map(|i| input.field(*i).clone().with_nullable(true))
        .collect();
    for (name, data_type) in outputs {
        if fields.iter().any(|f| f.name() == name) {
            return Err(Error::Schema(format!("duplicate column `{name}`")));
        }
        fields.push(Field::new(name, data_type.clone(), true));
    }
    Ok(Arc::new(Schema::new(fields)))
}
