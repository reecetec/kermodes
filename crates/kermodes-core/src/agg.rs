//! `agg_by`: aggregations per group, in two phases.
//!
//! Added rows are cut into fixed 256k-row pieces. Each piece is aggregated on its own into
//! partial states, reading its rows sequentially and hashing each key once. Groups are sharded by
//! key hash; each shard then merges the partials of its groups in piece order. Removed and
//! modified rows are routed to their group's shard and the groups they touch are recomputed from
//! their rows.
//!
//! Every aggregation is exact in any order, so results never depend on piece boundaries, parts,
//! or the number of cores: counts, distinct counts, min/max, and first/last trivially; integer
//! sums in 128 bits, so they never wrap; float sums, means, and standard deviations over exact
//! sums (`exact`) that round to float64 once, when read.
//!
//! The static executor (`exec`) aggregates without engine storage (`Fold`): each part of its
//! input folds into partial states of its own, and parts merge in their order, so the same
//! partial states serve both paths and give the same results.
//!
//! Groups come out in the order their keys first appear in the input. A cycle's new groups follow
//! every earlier one, in the order the cycle's modified rows (their new values), then its added
//! rows, first hold their keys.

use std::sync::{Arc, mpsc};
use std::thread;

use arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, AsArray, Float64Array, Int64Array, PrimitiveArray,
    UInt32Array, new_null_array,
};
use arrow::compute::kernels::aggregate;
use arrow::compute::{concat_batches, interleave, take, take_record_batch};
use arrow::datatypes::{
    ArrowNativeTypeOp, DataType, Date32Type, Field, Float64Type, Int16Type, Int32Type, Int64Type,
    Schema, SchemaRef, TimeUnit, TimestampNanosecondType,
};
use arrow::error::ArrowError;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use arrow::row::{RowConverter, Rows as EncodedRows, SortField};
use hashbrown::HashTable;

use crate::exact::{Exact, Scratch, Term, Wides, int_mean, sample_std};
use crate::groups::{
    HASHER, KeyKind, KeyTable, LocalEntry, LocalGroups, Membership, shard_of, split,
};
use crate::ops::push_removed;
use crate::parallel::{self, PARALLEL_ROWS, offsets, run, threads, threads_for};
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
    /// `sum` or `mean` of a float column, summed exactly.
    SumFloat {
        column: usize,
        state: Vec<FloatSum>,
        wides: Wides,
        mean: bool,
    },
    /// The count, exact sum, and exact sum of squares.
    Std {
        column: usize,
        state: Vec<Moments>,
        wides: Wides,
    },
    /// `min` or `max`: the winning value and its row key; ties go to the smaller key.
    Extreme {
        column: usize,
        max: bool,
        kept: Kept,
    },
    /// `first` or `last`: the value of the smallest or largest row key, nulls included.
    Edge {
        column: usize,
        last: bool,
        kept: Kept,
    },
    /// `count_distinct`: the distinct non-null values of `column` per group.
    CountDistinct {
        column: usize,
        values: Box<Distinct>,
    },
}

/// One group's exact float sum and count, kept together so a fold touches one cache line per row.
#[derive(Clone, Copy, Default)]
pub(crate) struct FloatSum {
    sum: Exact<2>,
    n: i64,
}

impl FloatSum {
    #[inline]
    fn add(&mut self, wides: &mut Wides, x: f64, remove: bool) {
        if remove {
            self.n -= 1;
            if self.n == 0 {
                self.clear(wides);
                return;
            }
        } else {
            self.n += 1;
        }
        self.sum.add(wides, Term::of_f64(x), remove);
    }

    /// Empties the group, freeing any wide sum it held.
    fn clear(&mut self, wides: &mut Wides) {
        self.sum.clear(wides);
        self.n = 0;
    }
}

/// One group's count, exact sum, and exact sum of squares, so adding, removing, and merging
/// values in any order leave the same state.
#[derive(Clone, Copy, Default)]
pub(crate) struct Moments {
    n: i64,
    sum: Exact<2>,
    squares: Exact<4>,
}

impl Moments {
    #[inline]
    fn add(&mut self, wides: &mut Wides, x: Term, remove: bool) {
        if remove {
            self.n -= 1;
            if self.n == 0 {
                self.clear(wides);
                return;
            }
        } else {
            self.n += 1;
        }
        self.sum.add(wides, x, remove);
        self.squares.add(wides, x.square(), remove);
    }

    fn merge(&mut self, wides: &mut Wides, other: &Moments, theirs: &Wides) {
        self.n += other.n;
        self.sum.merge(wides, &other.sum, theirs);
        self.squares.merge(wides, &other.squares, theirs);
    }

    fn clear(&mut self, wides: &mut Wides) {
        self.sum.clear(wides);
        self.squares.clear(wides);
        self.n = 0;
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

/// `function(argument)`'s function and argument, as written; `None` if it is not that shape.
fn split_call(spec: &str) -> Option<(&str, &str)> {
    let (name, rest) = spec.trim().split_once('(')?;
    Some((name.trim(), rest.strip_suffix(')')?.trim()))
}

/// The column the aggregation `spec` reads, `None` for `count(*)` or a spec that is not
/// `function(column)`.
pub(crate) fn reads(spec: &str) -> Option<&str> {
    split_call(spec)
        .map(|(_, argument)| argument)
        .filter(|argument| *argument != "*")
}

impl Call {
    pub fn parse(spec: &str, schema: &Schema) -> Result<Self, Error> {
        let error = |message: String| call_error(spec, message);
        let (name, argument) =
            split_call(spec).ok_or_else(|| error("expected `function(column)`".into()))?;
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
                    values: Box::new(Distinct::new(input_type == DataType::Utf8)),
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
                    wides: Wides::default(),
                    mean: call.function == "mean",
                },
                DataType::Float64,
            ),
            "std" => (
                Accumulator::Std {
                    column,
                    state: Vec::new(),
                    wides: Wides::default(),
                },
                DataType::Float64,
            ),
            "min" | "max" => (
                Accumulator::Extreme {
                    column,
                    max: call.function == "max",
                    kept: Kept::new(input_type.clone())
                        .map_err(|e| call_error(spec, e.to_string()))?,
                },
                input_type,
            ),
            _ => (
                Accumulator::Edge {
                    column,
                    last: call.function == "last",
                    kept: Kept::new(input_type.clone())
                        .map_err(|e| call_error(spec, e.to_string()))?,
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
                wides: Wides::default(),
                mean: *mean,
            },
            Accumulator::Std { column, .. } => Accumulator::Std {
                column: *column,
                state: Vec::new(),
                wides: Wides::default(),
            },
            Accumulator::Extreme { column, max, kept } => Accumulator::Extreme {
                column: *column,
                max: *max,
                kept: Kept::new(kept.data_type.clone())?,
            },
            Accumulator::Edge { column, last, kept } => Accumulator::Edge {
                column: *column,
                last: *last,
                kept: Kept::new(kept.data_type.clone())?,
            },
            Accumulator::CountDistinct { column, values } => Accumulator::CountDistinct {
                column: *column,
                values: Box::new(Distinct::new(values.strings.is_some())),
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
            Accumulator::Extreme { kept, .. } | Accumulator::Edge { kept, .. } => {
                kept.best.resize(len, None)
            }
            Accumulator::CountDistinct { values, .. } => values.resize(len),
        }
    }

    fn reset(&mut self, group: usize) {
        match self {
            Accumulator::Count { n, .. } => n[group] = 0,
            Accumulator::SumInt { sum, n, .. } => (sum[group], n[group]) = (0, 0),
            Accumulator::SumFloat { state, wides, .. } => state[group].clear(wides),
            Accumulator::Std { state, wides, .. } => state[group].clear(wides),
            Accumulator::Extreme { kept, .. } | Accumulator::Edge { kept, .. } => {
                kept.best[group] = None
            }
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
            Accumulator::Count { column, n } if rows.is_none() && n.len() == 1 => {
                // One group holds every row: count the batch at once.
                let nulls = column.map_or(0, |c| batch.column(c).null_count());
                n[0] += (ids.len() - nulls) as i64;
            }
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
            Accumulator::SumFloat {
                column,
                state,
                wides,
                ..
            } => {
                let values = batch.column(*column).as_primitive::<Float64Type>();
                each(values, ids, rows, |group, x| {
                    state[group].add(wides, x, false)
                });
            }
            Accumulator::Std {
                column,
                state,
                wides,
            } => {
                each_term(batch.column(*column).as_ref(), ids, rows, |group, x| {
                    state[group].add(wides, x, false);
                });
            }
            Accumulator::Extreme { column, max, kept }
                if rows.is_none() && kept.best.len() == 1 =>
            {
                // One group holds every row: only the batch's own winner can win, so only it is
                // encoded.
                let array = batch.column(*column);
                if let Some(row) = kept.extreme_row(array, keys, *max)? {
                    let encoded = kept.encode_row(array, row)?;
                    kept.offer_extreme(0, *max, encoded.row(0).data(), keys[row]);
                }
            }
            Accumulator::Extreme { column, max, kept } => {
                let (array, encoded) = kept.encode(batch.column(*column), rows)?;
                for (i, &group) in ids.iter().enumerate() {
                    if array.is_valid(i) {
                        let key = keys[row_of(i)];
                        kept.offer_extreme(group as usize, *max, encoded.row(i).data(), key);
                    }
                }
            }
            Accumulator::Edge { column, last, kept } if rows.is_none() && kept.best.len() == 1 => {
                // One group holds every row: only the batch's first (or last) row can win, so
                // only it is encoded.
                let by_key = |row: &usize| keys[*row];
                let rows = 0..batch.num_rows();
                let row = if *last {
                    rows.max_by_key(by_key)
                } else {
                    rows.min_by_key(by_key)
                };
                if let Some(row) = row {
                    let encoded = kept.encode_row(batch.column(*column), row)?;
                    kept.offer_edge(0, *last, encoded.row(0).data(), keys[row]);
                }
            }
            Accumulator::Edge { column, last, kept } => {
                let (_, encoded) = kept.encode(batch.column(*column), rows)?;
                for (i, &group) in ids.iter().enumerate() {
                    let key = keys[row_of(i)];
                    kept.offer_edge(group as usize, *last, encoded.row(i).data(), key);
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
            Accumulator::SumFloat {
                column,
                state,
                wides,
                ..
            } => {
                let values = batch.column(*column).as_primitive::<Float64Type>();
                each(values, ids, Some(rows), |group, x| {
                    state[group].add(wides, x, true);
                });
            }
            Accumulator::Std {
                column,
                state,
                wides,
            } => {
                each_term(
                    batch.column(*column).as_ref(),
                    ids,
                    Some(rows),
                    |group, x| state[group].add(wides, x, true),
                );
            }
            Accumulator::Extreme { kept, .. } | Accumulator::Edge { kept, .. } => {
                for (&group, &row) in ids.iter().zip(rows) {
                    let group = group as usize;
                    if kept.best[group]
                        .as_ref()
                        .is_some_and(|(_, key)| *key == keys[row as usize])
                    {
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
            (
                Accumulator::SumFloat { state, wides, .. },
                Accumulator::SumFloat {
                    state: theirs,
                    wides: their_wides,
                    ..
                },
            ) => {
                let (s, t) = (&mut state[into], &theirs[from]);
                s.sum.merge(wides, &t.sum, their_wides);
                s.n += t.n;
            }
            (
                Accumulator::Std { state, wides, .. },
                Accumulator::Std {
                    state: theirs,
                    wides: their_wides,
                    ..
                },
            ) => {
                state[into].merge(wides, &theirs[from], their_wides);
            }
            (Accumulator::Extreme { max, kept, .. }, Accumulator::Extreme { kept: theirs, .. }) => {
                if let Some((value, key)) = &theirs.best[from] {
                    kept.offer_extreme(into, *max, value, *key);
                }
            }
            (Accumulator::Edge { last, kept, .. }, Accumulator::Edge { kept: theirs, .. }) => {
                if let Some((value, key)) = &theirs.best[from] {
                    kept.offer_edge(into, *last, value, *key);
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
    fn output(&self, groups: &[u32]) -> Result<ArrayRef, ArrowError> {
        let at = groups.iter().map(|g| *g as usize);
        Ok(match self {
            Accumulator::Count { n, .. } => {
                Arc::new(Int64Array::from_iter_values(at.map(|g| n[g])))
            }
            Accumulator::SumInt {
                mean: true, sum, n, ..
            } => Arc::new(Float64Array::from_iter(
                at.map(|g| (n[g] > 0).then(|| int_mean(sum[g], n[g]))),
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
            Accumulator::SumFloat {
                state, wides, mean, ..
            } => Arc::new(Float64Array::from_iter(at.map(|g| {
                let s = &state[g];
                (s.n > 0).then(|| {
                    if *mean {
                        s.sum.mean(s.n, wides)
                    } else {
                        s.sum.value(wides)
                    }
                })
            }))),
            Accumulator::Std { state, wides, .. } => {
                let mut scratch = Scratch::default();
                Arc::new(Float64Array::from_iter(at.map(|g| {
                    let m = &state[g];
                    (m.n > 1).then(|| sample_std(m.n, &m.sum, &m.squares, wides, &mut scratch))
                })))
            }
            Accumulator::Extreme { kept, .. } | Accumulator::Edge { kept, .. } => {
                kept.output(at)?
            }
            Accumulator::CountDistinct { values, .. } => Arc::new(Int64Array::from_iter_values(
                at.map(|g| values.counts.distinct[g]),
            )),
        })
    }
}

/// Per group, the input value a `min`, `max`, `first`, or `last` keeps, with the row key it came
/// from (`None` before any row): in Arrow's row encoding, so a value of any type compares and
/// copies as bytes, into a buffer reused as the value changes, and the output needs no input
/// rows.
pub(crate) struct Kept {
    data_type: DataType,
    converter: RowConverter,
    best: Vec<Option<(Vec<u8>, RowKey)>>,
}

impl Kept {
    fn new(data_type: DataType) -> Result<Self, ArrowError> {
        Ok(Self {
            converter: RowConverter::new(vec![SortField::new(data_type.clone())])?,
            data_type,
            best: Vec::new(),
        })
    }

    /// The values a fold reads, every row of `column` or the selected `rows`, and their
    /// encoding.
    fn encode(
        &self,
        column: &ArrayRef,
        rows: Option<&[u32]>,
    ) -> Result<(ArrayRef, EncodedRows), ArrowError> {
        let array = match rows {
            None => column.clone(),
            Some(rows) => take(column, &UInt32Array::from(rows.to_vec()), None)?,
        };
        let encoded = self
            .converter
            .convert_columns(std::slice::from_ref(&array))?;
        Ok((array, encoded))
    }

    /// The encoding of `array`'s row `row` alone.
    fn encode_row(&self, array: &ArrayRef, row: usize) -> Result<EncodedRows, ArrowError> {
        self.converter.convert_columns(&[array.slice(row, 1)])
    }

    /// The row of `array` with the smallest value (the largest if `max`), ties to the smallest
    /// key; `None` without a value. For the engine's types, Arrow's min/max kernels find the
    /// value and one pass over the values finds its rows, so no value is encoded.
    fn extreme_row(
        &self,
        array: &ArrayRef,
        keys: &[RowKey],
        max: bool,
    ) -> Result<Option<usize>, ArrowError> {
        let valid = |row: &usize| array.is_valid(*row);
        Ok(match array.data_type() {
            DataType::Int16 => primitive_extreme(array.as_primitive::<Int16Type>(), keys, max),
            DataType::Int32 => primitive_extreme(array.as_primitive::<Int32Type>(), keys, max),
            DataType::Int64 => primitive_extreme(array.as_primitive::<Int64Type>(), keys, max),
            DataType::Float64 => primitive_extreme(array.as_primitive::<Float64Type>(), keys, max),
            DataType::Date32 => primitive_extreme(array.as_primitive::<Date32Type>(), keys, max),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                primitive_extreme(array.as_primitive::<TimestampNanosecondType>(), keys, max)
            }
            DataType::Utf8 => {
                let strings = array.as_string::<i32>();
                let best = if max {
                    aggregate::max_string(strings)
                } else {
                    aggregate::min_string(strings)
                };
                best.and_then(|best| {
                    smallest_key(
                        keys,
                        (0..array.len())
                            .filter(valid)
                            .filter(|r| strings.value(*r) == best),
                    )
                })
            }
            DataType::Boolean => {
                let bools = array.as_boolean();
                let best = if max {
                    aggregate::max_boolean(bools)
                } else {
                    aggregate::min_boolean(bools)
                };
                best.and_then(|best| {
                    smallest_key(
                        keys,
                        (0..array.len())
                            .filter(valid)
                            .filter(|r| bools.value(*r) == best),
                    )
                })
            }
            // Other types (a computed column's NULL type): compare encoded values.
            _ => {
                let encoded = self
                    .converter
                    .convert_columns(std::slice::from_ref(array))?;
                (0..array.len()).filter(valid).reduce(|best, row| {
                    let order = encoded.row(row).cmp(&encoded.row(best));
                    if beats(max, order, keys[row], keys[best]) {
                        row
                    } else {
                        best
                    }
                })
            }
        })
    }

    fn set(&mut self, group: usize, value: &[u8], key: RowKey) {
        match &mut self.best[group] {
            Some((held, at)) => {
                held.clear();
                held.extend_from_slice(value);
                *at = key;
            }
            none => *none = Some((value.to_vec(), key)),
        }
    }

    /// Keeps `value`, of the row keyed `key`, for `group` if it beats the value held: as `min`
    /// (`max` if `max`) orders values, ties to the smaller key.
    fn offer_extreme(&mut self, group: usize, max: bool, value: &[u8], key: RowKey) {
        let better = match &self.best[group] {
            None => true,
            Some((held, at)) => beats(max, value.cmp(held.as_slice()), key, *at),
        };
        if better {
            self.set(group, value, key);
        }
    }

    /// Keeps `value`, of the row keyed `key`, for `group` if its key is smaller than the held
    /// one's (larger if `last`).
    fn offer_edge(&mut self, group: usize, last: bool, value: &[u8], key: RowKey) {
        let better = match &self.best[group] {
            None => true,
            Some((_, held)) if last => key > *held,
            Some((_, held)) => key < *held,
        };
        if better {
            self.set(group, value, key);
        }
    }

    fn output(&self, groups: impl Iterator<Item = usize>) -> Result<ArrayRef, ArrowError> {
        let null = self
            .converter
            .convert_columns(&[new_null_array(&self.data_type, 1)])?;
        let parser = self.converter.parser();
        let rows = groups.map(|g| match &self.best[g] {
            Some((value, _)) => parser.parse(value),
            None => null.row(0),
        });
        Ok(self.converter.convert_rows(rows)?.remove(0))
    }
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

/// The row of `array` with the smallest value (the largest if `max`), ties to the smallest key;
/// `None` without a value. Arrow's kernel orders floats as the row encoding does: by IEEE 754
/// total order.
fn primitive_extreme<T: ArrowPrimitiveType>(
    array: &PrimitiveArray<T>,
    keys: &[RowKey],
    max: bool,
) -> Option<usize> {
    let best = if max {
        aggregate::max(array)
    } else {
        aggregate::min(array)
    }?;
    let rows = array.values().iter().enumerate();
    smallest_key(
        keys,
        rows.filter(|(row, value)| value.is_eq(best) && array.is_valid(*row))
            .map(|(row, _)| row),
    )
}

/// Of `rows`, the one with the smallest key.
fn smallest_key(keys: &[RowKey], rows: impl Iterator<Item = usize>) -> Option<usize> {
    rows.min_by_key(|row| keys[*row])
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

/// Calls `f(group, term)` for each row of a number column with a value, in order (as `each`),
/// integers exactly rather than through float64.
fn each_term(array: &dyn Array, ids: &[u32], rows: Option<&[u32]>, mut f: impl FnMut(usize, Term)) {
    match array.data_type() {
        DataType::Float64 => each(array.as_primitive::<Float64Type>(), ids, rows, |g, v| {
            f(g, Term::of_f64(v));
        }),
        _ => each_i64(array, ids, rows, |g, v| f(g, Term::of_i64(v))),
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
    /// The output slot of the next group published, so slots follow first appearance whatever
    /// shard holds the group.
    next_slot: RowKey,
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
            let groups = Membership::new(&projected, local_groups.clone(), add_only)
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
                place: Vec::new(),
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
            next_slot: 0,
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
        let mut partials = run(threads, pieces, |table, (keys, batch)| {
            Partial::build(context, templates, table, keys, batch, shard_count)
        })
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
        // Where each row that may create a group stands among them: modified rows' new values,
        // then each piece's groups, in first-appearance order within it.
        let mut places: u64 = view
            .modified
            .iter()
            .map(|(keys, ..)| keys.len() as u64)
            .sum();
        for partial in &mut partials {
            partial.place = places;
            places += partial.groups.len() as u64;
        }

        let shards: Vec<&mut Shard> = self.shards.iter_mut().collect();
        let outputs = run(threads, shards, |(), shard| {
            let part = view.part(&routes, shard.index);
            shard.process(context, input, &part, &partials, own)
        })
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

        // New groups take the next slots in the order their keys first appear.
        let lists: Vec<&[u64]> = outputs.iter().map(|o| o.places.as_slice()).collect();
        let order = by_place(places as usize, &lists);
        for (slot, &(shard, index)) in (self.next_slot..).zip(&order) {
            self.shards[shard].groups.slot[outputs[shard].ids[index] as usize] = slot;
        }
        let sources: Vec<&[ArrayRef]> = outputs.iter().map(|o| o.added.columns()).collect();
        let mut out = Delta::default();
        for batch in pick(&sources, &order, &context.output)? {
            let next = self.next_slot + batch.num_rows() as RowKey;
            let keys = (self.next_slot..next).collect();
            self.next_slot = next;
            out.added.push(Rows { keys, batch });
        }
        for published in outputs {
            out.removed.extend(published.changes.removed);
            out.modified.extend(published.changes.modified);
        }
        Ok(out)
    }
}

/// A shard's output for one cycle: its groups' modifies and removes, and the groups it published
/// for the first time, with their places among the cycle's rows and their rows.
struct Published {
    changes: Delta,
    ids: Vec<u32>,
    places: Vec<u64>,
    added: RecordBatch,
}

/// The items of `lists` as `(list, index)`, in the order of their places: `lists[list][index]`
/// is the item's place, distinct among all items and below `count`. One bit per place, so linear
/// in `count`, without a sort.
fn by_place(count: usize, lists: &[&[u64]]) -> Vec<(usize, usize)> {
    let mut seen = vec![0u64; count.div_ceil(64)];
    for list in lists {
        for &place in *list {
            seen[place as usize / 64] |= 1 << (place % 64);
        }
    }
    let mut before = Vec::with_capacity(seen.len());
    let mut total = 0;
    for word in &seen {
        before.push(total);
        total += word.count_ones() as usize;
    }
    let mut at = vec![(0, 0); total];
    for (l, list) in lists.iter().enumerate() {
        for (index, &place) in list.iter().enumerate() {
            let (word, bit) = (place as usize / 64, place % 64);
            let rank = (seen[word] & ((1 << bit) - 1)).count_ones() as usize;
            at[before[word] + rank] = (l, index);
        }
    }
    at
}

/// The rows `at` picks, as `(source, row)`, from sources holding `output`'s columns, in pieces
/// built on every core.
fn pick(
    sources: &[&[ArrayRef]],
    at: &[(usize, usize)],
    output: &SchemaRef,
) -> Result<Vec<RecordBatch>, ArrowError> {
    let pieces: Vec<&[(usize, usize)]> = at.chunks(parallel::PIECE_ROWS).collect();
    run(threads_for(at.len()), pieces, |(), piece| {
        let picked = (0..output.fields().len())
            .map(|column| {
                let arrays: Vec<&dyn Array> = sources.iter().map(|c| c[column].as_ref()).collect();
                interleave(&arrays, piece)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let options = RecordBatchOptions::new().with_row_count(Some(piece.len()));
        RecordBatch::try_new_with_options(output.clone(), picked, &options)
    })
    .into_iter()
    .collect()
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
    /// The place of local group 0 among the cycle's rows that may create groups.
    place: u64,
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
            place: 0,
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

/// Rows of one batch that belong to a shard, by index, in order. `place`: where the batch's
/// first row stands among the cycle's rows that may create groups (modified rows' new values),
/// for the groups its rows create.
struct Selected<'a> {
    keys: &'a [RowKey],
    batch: &'a RecordBatch,
    rows: Vec<u32>,
    place: u64,
}

/// Modified rows that stay within a shard, with their `place` as `Selected`'s.
struct SelectedModified<'a> {
    keys: &'a [RowKey],
    previous: &'a RecordBatch,
    current: &'a RecordBatch,
    rows: Vec<u32>,
    place: u64,
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
        let selected = |keys, batch, rows: Vec<u32>, place| {
            (!rows.is_empty()).then_some(Selected {
                keys,
                batch,
                rows,
                place,
            })
        };
        for (keys, batch) in &self.removed {
            let mine = rows.next().expect("one row list per segment");
            part.removed.extend(selected(*keys, batch, mine, 0));
        }
        let previous: Vec<Vec<u32>> = rows.by_ref().take(self.modified.len()).collect();
        let current: Vec<Vec<u32>> = rows.collect();
        let mut place = 0;
        for (((keys, before, after), old), new) in self.modified.iter().zip(previous).zip(current) {
            let (stay, leave, enter) = compare(&old, &new);
            if !stay.is_empty() {
                part.modified.push(SelectedModified {
                    keys,
                    previous: before,
                    current: after,
                    rows: stay,
                    place,
                });
            }
            part.removed.extend(selected(*keys, before, leave, 0));
            part.added.extend(selected(*keys, after, enter, place));
            place += keys.len() as u64;
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
    /// For a group created this cycle, the earliest place among the cycle's rows that hold its
    /// key, which orders it among the cycle's new groups.
    place: Vec<u64>,
}

impl Shard {
    /// Grows per-group vectors to the membership's ids and resets newly created groups.
    fn sync(&mut self) {
        let len = self.groups.capacity();
        self.published.resize(len, false);
        self.stale.resize(len, false);
        self.place.resize(len, u64::MAX);
        for accumulator in &mut self.accumulators {
            accumulator.resize(len);
        }
        for id in self.groups.take_created() {
            let group = id as usize;
            self.published[group] = false;
            self.stale[group] = false;
            self.place[group] = u64::MAX;
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

    /// Notes `place + rows[i]` as a place of group `ids[i]`.
    fn note_places(&mut self, ids: &[u32], rows: &[u32], place: u64) {
        for (&id, &row) in ids.iter().zip(rows) {
            let at = &mut self.place[id as usize];
            *at = (*at).min(place + u64::from(row));
        }
    }

    fn process(
        &mut self,
        context: &Context,
        input: Reader<'_>,
        part: &Part<'_>,
        partials: &[Partial],
        own: &Storage,
    ) -> Result<Published, ArrowError> {
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
            self.note_places(&after, &modified.rows, modified.place);
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
            self.note_places(&ids, &selected.rows, selected.place);
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

        let places = added.iter().map(|id| self.place[*id as usize]).collect();
        let added_rows = self.output_rows(context, input, &added)?;
        let mut changes = Delta::default();
        let slots: Vec<RowKey> = changed
            .iter()
            .map(|id| self.groups.slot[*id as usize])
            .collect();
        changes.modified.push(Modified {
            previous: own.gather(&slots)?,
            current: self.output_rows(context, input, &changed)?,
            keys: slots,
        });
        push_removed(&mut changes, own, removed)?;
        Ok(Published {
            changes,
            ids: added,
            places,
            added: added_rows,
        })
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
            let at = &mut self.place[group];
            *at = (*at).min(partial.place + local as u64);
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
    ) -> Result<RecordBatch, ArrowError> {
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
            columns.push(accumulator.output(ids)?);
        }
        let options = RecordBatchOptions::new().with_row_count(Some(ids.len()));
        RecordBatch::try_new_with_options(context.output.clone(), columns, &options)
    }
}

/// `agg_by` over rows that only arrive, without engine storage (the static executor): a part
/// folds its rows into partial states of its own (`add`, then `seal`), and parts merge in their
/// order into `Merged`.
pub(crate) struct Fold {
    input: SchemaRef,
    /// How group keys are read from input batches, and from `keys` (the group columns alone).
    input_keys: KeyKind,
    group_keys: KeyKind,
    group_columns: Vec<usize>,
    key_schema: SchemaRef,
    accumulators: Vec<Accumulator>,
    /// Whether an aggregation reads row keys: min, max, first, and last order rows by them.
    positions: bool,
    groups: KeyTable,
    /// The group columns of each group, by id, in pieces; one batch once sealed.
    keys: Vec<RecordBatch>,
    /// Once sealed: `keys`' row encoding, for keys that need one, and each shard's groups.
    encoded: Option<EncodedRows>,
    by_shard: Vec<Vec<u32>>,
    /// Scratch, kept for its capacity.
    local: HashTable<LocalEntry>,
    row_keys: Vec<RowKey>,
    /// Rows folded so far, and whether the part has stopped deduplicating across batches
    /// (`DEDUP_CHECK_ROWS`).
    folded: usize,
    passing: bool,
}

/// After this many rows, a part whose groups number more than half its rows stops looking each
/// batch's groups up in its own table and appends them as they are: the merge looks every group
/// up again anyway, and with keys this close to unique (order IDs, a user and a minute) the
/// part's lookups would almost all add a group, costing a second hash table for nothing.
const DEDUP_CHECK_ROWS: usize = 1 << 16;

impl Fold {
    pub fn new(
        input: &SchemaRef,
        group_columns: Vec<usize>,
        specs: &[&str],
    ) -> Result<Self, Error> {
        let accumulators = specs
            .iter()
            .map(|spec| Accumulator::parse(spec, input).map(|(a, _)| a))
            .collect::<Result<Vec<_>, _>>()?;
        Self::with(input.clone(), group_columns, accumulators)
            .map_err(|e| Error::Schema(e.to_string()))
    }

    fn with(
        input: SchemaRef,
        group_columns: Vec<usize>,
        accumulators: Vec<Accumulator>,
    ) -> Result<Self, ArrowError> {
        let key_schema = Arc::new(input.project(&group_columns)?);
        Ok(Self {
            input_keys: KeyKind::new(&input, group_columns.clone())?,
            group_keys: KeyKind::new(&key_schema, (0..group_columns.len()).collect())?,
            positions: accumulators
                .iter()
                .any(|a| matches!(a, Accumulator::Extreme { .. } | Accumulator::Edge { .. })),
            input,
            group_columns,
            key_schema,
            accumulators,
            groups: KeyTable::default(),
            keys: Vec::new(),
            encoded: None,
            by_shard: Vec::new(),
            local: HashTable::new(),
            row_keys: Vec::new(),
            folded: 0,
            passing: false,
        })
    }

    /// The same aggregation, holding no groups: a part's partial states.
    pub fn fresh(&self) -> Result<Self, ArrowError> {
        let accumulators = self
            .accumulators
            .iter()
            .map(Accumulator::fresh)
            .collect::<Result<_, _>>()?;
        Self::with(self.input.clone(), self.group_columns.clone(), accumulators)
    }

    /// Folds `batch`, whose rows' keys run from `first`.
    pub fn add(&mut self, batch: &RecordBatch, first: RowKey) -> Result<(), ArrowError> {
        let rows = batch.num_rows();
        if rows == 0 {
            return Ok(());
        }
        let encoded = self.input_keys.encode(batch)?;
        let keys = self.input_keys.keys(batch, encoded.as_ref());
        let local = LocalGroups::build(&keys, rows, &mut self.local);
        let mut new = Vec::new();
        let ids: Vec<u32> = if self.passing {
            new.extend_from_slice(&local.first_row);
            local
                .hash
                .iter()
                .map(|&hash| self.groups.push(hash))
                .collect()
        } else {
            (0..local.len())
                .map(|g| {
                    let row = local.first_row[g] as usize;
                    let (id, created) = self.groups.id(&keys, row, local.hash[g]);
                    if created {
                        new.push(row as u32);
                    }
                    id
                })
                .collect()
        };
        if !new.is_empty() && !self.group_columns.is_empty() {
            let indices = UInt32Array::from(new);
            let columns = self
                .group_columns
                .iter()
                .map(|c| take(batch.column(*c), &indices, None))
                .collect::<Result<Vec<_>, _>>()?;
            self.keys
                .push(RecordBatch::try_new(self.key_schema.clone(), columns)?);
        }
        // When the batch's groups are numbered as the part's (its first batch, or every batch
        // of an aggregation without group columns), its ids serve as they are.
        let ids = if ids.iter().enumerate().all(|(g, id)| *id as usize == g) {
            local.ids
        } else {
            local.ids.iter().map(|g| ids[*g as usize]).collect()
        };
        self.row_keys.clear();
        if self.positions {
            self.row_keys.extend(first..first + rows as RowKey);
        }
        for accumulator in &mut self.accumulators {
            accumulator.resize(self.groups.len());
            accumulator.fold(batch, &self.row_keys, &ids, None)?;
        }
        self.folded += rows;
        if !self.passing && self.folded >= DEDUP_CHECK_ROWS && 2 * self.groups.len() > self.folded {
            self.passing = true;
        }
        Ok(())
    }

    /// Readies a part's states for merging into `shards` shards, once all its rows are folded.
    pub fn seal(&mut self, shards: usize) -> Result<(), ArrowError> {
        let keys = if self.group_columns.is_empty() {
            let options = RecordBatchOptions::new().with_row_count(Some(self.groups.len()));
            RecordBatch::try_new_with_options(self.key_schema.clone(), Vec::new(), &options)?
        } else {
            concat_batches(&self.key_schema, &self.keys)?
        };
        self.encoded = self.group_keys.encode(&keys)?;
        self.keys = vec![keys];
        self.by_shard = vec![Vec::new(); shards];
        for group in 0..self.groups.len() {
            self.by_shard[shard_of(self.groups.hash(group), shards)].push(group as u32);
        }
        for accumulator in &mut self.accumulators {
            accumulator.seal();
        }
        Ok(())
    }

    /// Merged states of the same aggregation, holding no groups, in `shards` shards.
    pub fn merged(&self, shards: usize) -> Result<Merged, ArrowError> {
        let shards = (0..shards)
            .map(|_| {
                Ok(Merging {
                    groups: KeyTable::default(),
                    keys: Vec::new(),
                    accumulators: self
                        .accumulators
                        .iter()
                        .map(Accumulator::fresh)
                        .collect::<Result<_, _>>()?,
                    first: Vec::new(),
                })
            })
            .collect::<Result<_, ArrowError>>()?;
        Ok(Merged {
            key_schema: self.key_schema.clone(),
            shards,
            parts: 0,
            places: 0,
        })
    }
}

/// Every part's groups merged, sharded by key hash so shards merge on their own threads. Each
/// shard merges the parts in their order, so a group's states merge as they would in one table,
/// whatever the number of shards; the output puts the groups back in the order their keys first
/// appear in the input, whose parts' groups are numbered in that order.
pub(crate) struct Merged {
    key_schema: SchemaRef,
    shards: Vec<Merging>,
    /// Parts merged so far, and their groups: the next part's groups are placed from there.
    parts: usize,
    places: u64,
}

/// One shard of `Merged`: the groups whose key hashes to it.
struct Merging {
    groups: KeyTable,
    /// The group columns of each group, by id, in pieces.
    keys: Vec<RecordBatch>,
    accumulators: Vec<Accumulator>,
    /// Per group, where its key first appears: the place of its first part's groups, plus its
    /// id within that part.
    first: Vec<u64>,
}

impl Merged {
    /// Merges the sealed parts `feed` hands to the function it is given, in part order, as they
    /// come. Each shard merges on a thread of its own for the whole run, fed through a short
    /// queue, so it merges one part while later parts fold and no shard waits for the others.
    /// The function returns false once a shard has failed; `feed` should then stop, and the
    /// shard's error, as `failed` makes it, is returned (after any of `feed`'s own).
    pub fn merge_from<E>(
        &mut self,
        feed: impl FnOnce(&mut dyn FnMut(Fold) -> bool) -> Result<(), E>,
        failed: impl Fn(ArrowError) -> E,
    ) -> Result<(), E> {
        let Self {
            shards,
            parts,
            places,
            ..
        } = self;
        thread::scope(|scope| {
            let mut senders = Vec::with_capacity(shards.len());
            let mut mergers = Vec::with_capacity(shards.len());
            for (shard, merging) in shards.iter_mut().enumerate() {
                // Two parts queued: enough to keep a shard busy, few enough that unmerged
                // parts do not pile up when merging is the slower side.
                let (sender, queued) = mpsc::sync_channel::<(u64, Arc<Fold>)>(2);
                senders.push(sender);
                mergers.push(scope.spawn(move || {
                    queued
                        .into_iter()
                        .try_for_each(|(start, part)| merging.merge(shard, start, &part))
                }));
            }
            let mut give = |part: Fold| {
                let start = *places;
                *parts += 1;
                *places += part.groups.len() as u64;
                let part = Arc::new(part);
                senders
                    .iter()
                    .all(|sender| sender.send((start, part.clone())).is_ok())
            };
            let fed = feed(&mut give);
            drop(senders);
            let mut merged = Ok(());
            for merger in mergers {
                let result = merger
                    .join()
                    .unwrap_or_else(|payload| std::panic::resume_unwind(payload));
                if merged.is_ok() {
                    merged = result;
                }
            }
            fed?;
            merged.map_err(failed)
        })
    }

    /// One row per group, in the order their keys first appear, with `output`'s columns (the
    /// group columns, then each aggregation), in pieces built on every core.
    pub fn finish(&self, output: &SchemaRef) -> Result<Vec<RecordBatch>, ArrowError> {
        let firsts: Vec<&[u64]> = self.shards.iter().map(|m| m.first.as_slice()).collect();
        let at = by_place(self.places as usize, &firsts);
        let shards: Vec<&Merging> = self.shards.iter().collect();
        let columns = run(shards.len(), shards, |(), merging| {
            merging.columns(&self.key_schema)
        })
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
        let sources: Vec<&[ArrayRef]> = columns.iter().map(Vec::as_slice).collect();
        pick(&sources, &at, output)
    }
}

impl Merging {
    /// This shard's output columns, by group id.
    fn columns(&self, key_schema: &SchemaRef) -> Result<Vec<ArrayRef>, ArrowError> {
        let mut columns = concat_batches(key_schema, &self.keys)?.columns().to_vec();
        let ids: Vec<u32> = (0..self.groups.len() as u32).collect();
        for accumulator in &self.accumulators {
            columns.push(accumulator.output(&ids)?);
        }
        Ok(columns)
    }

    /// Merges this shard's groups of the sealed part `part`, whose groups are placed from `start`.
    fn merge(&mut self, shard: usize, start: u64, part: &Fold) -> Result<(), ArrowError> {
        let [theirs] = part.keys.as_slice() else {
            unreachable!("a merged part is sealed");
        };
        let keys = part.group_keys.keys(theirs, part.encoded.as_ref());
        let mine = &part.by_shard[shard];
        let mut new = Vec::new();
        let ids: Vec<u32> = mine
            .iter()
            .map(|&group| {
                let (id, created) =
                    self.groups
                        .id(&keys, group as usize, part.groups.hash(group as usize));
                if created {
                    new.push(group);
                    self.first.push(start + u64::from(group));
                }
                id
            })
            .collect();
        if !new.is_empty() && theirs.num_columns() > 0 {
            self.keys
                .push(take_record_batch(theirs, &UInt32Array::from(new))?);
        }
        for (accumulator, theirs) in self.accumulators.iter_mut().zip(&part.accumulators) {
            accumulator.resize(self.groups.len());
            for (&from, &into) in mine.iter().zip(&ids) {
                accumulator.merge(into as usize, theirs, from as usize)?;
            }
        }
        Ok(())
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
