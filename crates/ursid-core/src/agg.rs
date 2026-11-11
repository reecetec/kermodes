//! `agg_by`: aggregations per group, in two phases.
//!
//! Added rows are cut into fixed 256k-row pieces. Each piece is aggregated on its own into
//! partial states, reading its rows sequentially and hashing each key once. Groups are sharded by
//! key hash; each shard then merges the partials of its groups in piece order. Removed and
//! modified rows are routed to their group's shard and the groups they touch are recomputed from
//! their rows.
//!
//! Counts, integer sums, min/max, and first/last are exact. Float sums, means, and standard
//! deviations depend on where piece boundaries fall, so a ticking table and a static
//! recomputation can differ in the last bits; piece boundaries never depend on the number of
//! cores, so results do not either.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, AsArray, Float64Array, Int64Array, PrimitiveArray,
    UInt32Array,
};
use arrow::compute::take;
use arrow::datatypes::{DataType, Field, Float64Type, Int32Type, Int64Type, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use arrow::row::{OwnedRow, RowConverter, Rows as EncodedRows, SortField};
use hashbrown::HashTable;

use crate::groups::{KeyKind, LocalEntry, LocalGroups, Membership, shard_of, split};
use crate::ops::{pick_column, push_removed};
use crate::parallel::{PARALLEL_ROWS, offsets, run, threads};
use crate::storage::{Delta, Modified, RowKey, Rows, Storage};
use crate::{Error, type_name};

/// Rows per piece. Fixed, so results never depend on the number of cores.
const PIECE_ROWS: usize = 1 << 18;

/// One aggregation's state for every group, one vector per field, indexed by group id.
pub(crate) enum Accumulator {
    /// `count(*)` when `column` is `None`, otherwise non-null values.
    Count { column: Option<usize>, n: Vec<i64> },
    SumInt {
        column: usize,
        sum: Vec<i64>,
        n: Vec<i64>,
    },
    /// `sum` of a float column, or `mean` of any numeric column.
    SumFloat {
        column: usize,
        state: Vec<FloatSum>,
        mean: bool,
    },
    /// Welford's running mean and sum of squared deviations.
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
}

/// One group's float sum and count, kept together so a fold touches one cache line per row.
#[derive(Clone, Copy, Default)]
pub(crate) struct FloatSum {
    sum: f64,
    n: i64,
}

/// One group's Welford state, kept together for the same reason.
#[derive(Clone, Copy, Default)]
pub(crate) struct Moments {
    n: i64,
    mean: f64,
    m2: f64,
}

impl Accumulator {
    /// Parses `function(column)`, returning the accumulator and its output type.
    pub fn parse(spec: &str, schema: &Schema) -> Result<(Self, DataType), Error> {
        let error = |message: String| Error::Expression {
            text: spec.to_owned(),
            column: None,
            message,
        };
        let text = spec.trim();
        let (name, rest) = text
            .split_once('(')
            .ok_or_else(|| error("expected `function(column)`".into()))?;
        let argument = rest
            .strip_suffix(')')
            .ok_or_else(|| error("expected `function(column)`".into()))?
            .trim();
        let function = name.trim().to_ascii_lowercase();
        let known = [
            "count", "sum", "mean", "avg", "std", "stddev", "min", "max", "first", "last",
        ];
        if !known.contains(&function.as_str()) {
            return Err(error(format!(
                "unknown aggregation `{function}`; expected count, sum, mean, std, min, max, first, or last"
            )));
        }
        if argument == "*" {
            if function != "count" {
                return Err(error("only count accepts `*`".into()));
            }
            let count = Accumulator::Count {
                column: None,
                n: Vec::new(),
            };
            return Ok((count, DataType::Int64));
        }
        let column = schema.index_of(argument).map_err(|_| {
            let known: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
            error(format!(
                "unknown column `{argument}`; columns are: {}",
                known.join(", ")
            ))
        })?;
        let input_type = schema.field(column).data_type().clone();
        let numeric = matches!(
            input_type,
            DataType::Int32 | DataType::Int64 | DataType::Float64
        );
        if matches!(function.as_str(), "sum" | "mean" | "avg" | "std" | "stddev") && !numeric {
            return Err(error(format!(
                "`{name}` needs a number, but `{argument}` is {}",
                type_name(&input_type)
            )));
        }
        let float_input = input_type == DataType::Float64;
        Ok(match function.as_str() {
            "count" => (
                Accumulator::Count {
                    column: Some(column),
                    n: Vec::new(),
                },
                DataType::Int64,
            ),
            "sum" if !float_input => (
                Accumulator::SumInt {
                    column,
                    sum: Vec::new(),
                    n: Vec::new(),
                },
                DataType::Int64,
            ),
            "sum" | "mean" | "avg" => (
                Accumulator::SumFloat {
                    column,
                    state: Vec::new(),
                    mean: function != "sum",
                },
                DataType::Float64,
            ),
            "std" | "stddev" => (
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
                    max: function == "max",
                    converter: RowConverter::new(vec![SortField::new(input_type.clone())])
                        .map_err(|e| error(e.to_string()))?,
                    data_type: input_type.clone(),
                    best: Vec::new(),
                },
                input_type,
            ),
            _ => (
                Accumulator::Edge {
                    column,
                    source: column,
                    last: function == "last",
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
            Accumulator::SumInt { column, .. } => Accumulator::SumInt {
                column: *column,
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
            | Accumulator::Edge { column, .. } => Some(*column),
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
            | Accumulator::Edge { column, .. } => at(column),
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
            Accumulator::SumInt { column, sum, n } => {
                let mut overflow = false;
                each_i64(batch.column(*column).as_ref(), ids, rows, |group, value| {
                    match sum[group].checked_add(value) {
                        Some(total) => sum[group] = total,
                        None => overflow = true,
                    }
                    n[group] += 1;
                });
                if overflow {
                    return Err(overflow_error());
                }
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
                    let s = &mut state[group];
                    s.n += 1;
                    let delta = x - s.mean;
                    s.mean += delta / s.n as f64;
                    s.m2 += delta * (x - s.mean);
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
        }
        Ok(())
    }

    /// Takes the selected `rows` of `batch` (`ids[i]` is the group of `rows[i]`) out of their
    /// groups. Counts and sums subtract; `std` reverses Welford's update. `min`/`max`/`first`/
    /// `last` cannot be undone, so a group losing its winning row is marked in `stale` for
    /// recomputation; losing any other row changes nothing.
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
            Accumulator::SumInt { column, sum, n } => {
                let mut overflow = false;
                each_i64(
                    batch.column(*column).as_ref(),
                    ids,
                    Some(rows),
                    |group, value| {
                        match sum[group].checked_sub(value) {
                            Some(total) => sum[group] = total,
                            None => overflow = true,
                        }
                        n[group] -= 1;
                    },
                );
                if overflow {
                    return Err(overflow_error());
                }
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
                    |group, x| {
                        let s = &mut state[group];
                        s.n -= 1;
                        if s.n == 0 {
                            *s = Moments::default();
                            return;
                        }
                        let mean = s.mean - (x - s.mean) / s.n as f64;
                        // Rounding can push a sum of squares just below zero; one value has none.
                        s.m2 = if s.n == 1 {
                            0.0
                        } else {
                            (s.m2 - (x - mean) * (x - s.mean)).max(0.0)
                        };
                        s.mean = mean;
                    },
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
        }
        Ok(())
    }

    /// Merges group `from` of `other` (the same aggregation) into group `into` of this one.
    fn merge(&mut self, into: usize, other: &Accumulator, from: usize) -> Result<(), ArrowError> {
        match (self, other) {
            (Accumulator::Count { n, .. }, Accumulator::Count { n: m, .. }) => n[into] += m[from],
            (Accumulator::SumInt { sum, n, .. }, Accumulator::SumInt { sum: s, n: m, .. }) => {
                sum[into] = sum[into].checked_add(s[from]).ok_or_else(overflow_error)?;
                n[into] += m[from];
            }
            (Accumulator::SumFloat { state, .. }, Accumulator::SumFloat { state: theirs, .. }) => {
                let (s, t) = (&mut state[into], theirs[from]);
                s.sum += t.sum;
                s.n += t.n;
            }
            (Accumulator::Std { state, .. }, Accumulator::Std { state: theirs, .. }) => {
                // Chan, Golub, and LeVeque's update for combining two partial variances.
                let (s, t) = (&mut state[into], theirs[from]);
                if t.n == 0 {
                    return Ok(());
                }
                if s.n == 0 {
                    *s = t;
                    return Ok(());
                }
                let (a, b) = (s.n as f64, t.n as f64);
                let delta = t.mean - s.mean;
                s.mean += delta * (b / (a + b));
                s.m2 += t.m2 + delta * delta * (a * b / (a + b));
                s.n += t.n;
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
            _ => unreachable!("partials come from the same aggregation"),
        }
        Ok(())
    }

    /// The output column for `groups`, in that order.
    fn output(&self, groups: &[u32], input: &Storage) -> Result<ArrayRef, ArrowError> {
        let at = groups.iter().map(|g| *g as usize);
        Ok(match self {
            Accumulator::Count { n, .. } => {
                Arc::new(Int64Array::from_iter_values(at.map(|g| n[g])))
            }
            Accumulator::SumInt { sum, n, .. } => Arc::new(Int64Array::from_iter(
                at.map(|g| (n[g] > 0).then_some(sum[g])),
            )),
            Accumulator::SumFloat { state, mean, .. } => {
                Arc::new(Float64Array::from_iter(at.map(|g| {
                    let s = state[g];
                    (s.n > 0).then(|| if *mean { s.sum / s.n as f64 } else { s.sum })
                })))
            }
            Accumulator::Std { state, .. } => Arc::new(Float64Array::from_iter(at.map(|g| {
                let s = state[g];
                (s.n > 1).then(|| (s.m2 / (s.n - 1) as f64).sqrt())
            }))),
            Accumulator::Extreme { source, best, .. } => {
                let keys: Vec<_> = at.map(|g| best[g].as_ref().map(|(_, key)| *key)).collect();
                pick_column(input, *source, &keys)?
            }
            Accumulator::Edge { source, key, .. } => {
                let keys: Vec<_> = at.map(|g| key[g]).collect();
                pick_column(input, *source, &keys)?
            }
        })
    }
}

fn overflow_error() -> ArrowError {
    ArrowError::ComputeError("Overflow happened in sum".into())
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
        DataType::Int32 => each(array.as_primitive::<Int32Type>(), ids, rows, |g, v| {
            f(g, i64::from(v));
        }),
        _ => each(array.as_primitive::<Int64Type>(), ids, rows, f),
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
        input: &Storage,
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
        input: &Storage,
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
        input: &Storage,
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
        input: &Storage,
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
