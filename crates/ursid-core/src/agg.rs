//! `agg_by`: aggregations per group. Groups are split into shards by a hash of their key; each
//! shard folds its rows in input-key order, so shards can run on separate cores and every
//! result is bit-identical to folding the same rows one by one.

use std::num::NonZero;
use std::sync::Arc;
use std::thread;

use arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, AsArray, Float64Array, Int64Array, PrimitiveArray,
    UInt32Array,
};
use arrow::compute::take;
use arrow::datatypes::{DataType, Field, Float64Type, Int32Type, Int64Type, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use arrow::row::{OwnedRow, RowConverter, SortField};

use crate::groups::{Membership, Sharder};
use crate::ops::{pick_column, push_removed};
use crate::storage::{Delta, Modified, RowKey, Rows, Storage};
use crate::{Error, type_name};

/// Below this many rows in a cycle's delta, shards run one after another on the calling thread.
const PARALLEL_ROWS: usize = 1 << 16;

/// Rows per piece when hashing a delta's rows into shards; pieces spread that work over threads.
const PIECE_ROWS: usize = 1 << 16;

/// One aggregation's state for every group in a shard, one vector per field, indexed by group
/// id. Rows are always folded in input-key order.
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
        sum: Vec<f64>,
        n: Vec<i64>,
        mean: bool,
    },
    /// Welford's running mean and sum of squared deviations.
    Std {
        column: usize,
        n: Vec<i64>,
        mean: Vec<f64>,
        m2: Vec<f64>,
    },
    /// `min` or `max`: the winning value (comparable encoding) and its row key. `column` is
    /// read when folding; `source` is the input column the value is copied from.
    Extreme {
        column: usize,
        source: usize,
        max: bool,
        converter: RowConverter,
        best: Vec<Option<(OwnedRow, RowKey)>>,
    },
    /// `first` or `last`: the row key, nulls included.
    Edge {
        column: usize,
        source: usize,
        last: bool,
        key: Vec<Option<RowKey>>,
    },
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
                    sum: Vec::new(),
                    n: Vec::new(),
                    mean: function != "sum",
                },
                DataType::Float64,
            ),
            "std" | "stddev" => (
                Accumulator::Std {
                    column,
                    n: Vec::new(),
                    mean: Vec::new(),
                    m2: Vec::new(),
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

    /// Points the folded column at its position within the projected batches shards see.
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
            Accumulator::SumFloat { sum, n, .. } => {
                sum.resize(len, 0.0);
                n.resize(len, 0);
            }
            Accumulator::Std { n, mean, m2, .. } => {
                n.resize(len, 0);
                mean.resize(len, 0.0);
                m2.resize(len, 0.0);
            }
            Accumulator::Extreme { best, .. } => best.resize(len, None),
            Accumulator::Edge { key, .. } => key.resize(len, None),
        }
    }

    fn reset(&mut self, group: usize) {
        match self {
            Accumulator::Count { n, .. } => n[group] = 0,
            Accumulator::SumInt { sum, n, .. } => (sum[group], n[group]) = (0, 0),
            Accumulator::SumFloat { sum, n, .. } => (sum[group], n[group]) = (0.0, 0),
            Accumulator::Std { n, mean, m2, .. } => {
                (n[group], mean[group], m2[group]) = (0, 0.0, 0.0);
            }
            Accumulator::Extreme { best, .. } => best[group] = None,
            Accumulator::Edge { key, .. } => key[group] = None,
        }
    }

    /// Folds rows of `batch` into the groups in `ids`, in order: every row, or the selected
    /// `rows` (`ids[i]` is the group of `rows[i]`). Groups marked in `skip` are left alone.
    fn fold(
        &mut self,
        batch: &RecordBatch,
        keys: &[RowKey],
        ids: &[u32],
        rows: Option<&[u32]>,
        skip: Option<&[bool]>,
    ) -> Result<(), ArrowError> {
        let skipped = |group: u32| skip.is_some_and(|s| s[group as usize]);
        let row_of = |i: usize| row_at(rows, i);
        match self {
            Accumulator::Count { column: None, n } => {
                for &group in ids {
                    if !skipped(group) {
                        n[group as usize] += 1;
                    }
                }
            }
            Accumulator::Count {
                column: Some(column),
                n,
            } => {
                let array = batch.column(*column);
                for (i, &group) in ids.iter().enumerate() {
                    if !skipped(group) && array.is_valid(row_of(i)) {
                        n[group as usize] += 1;
                    }
                }
            }
            Accumulator::SumInt { column, sum, n } => {
                let mut overflow = false;
                each_i64(
                    batch.column(*column).as_ref(),
                    ids,
                    rows,
                    skip,
                    |group, value| {
                        match sum[group].checked_add(value) {
                            Some(total) => sum[group] = total,
                            None => overflow = true,
                        }
                        n[group] += 1;
                    },
                );
                if overflow {
                    return Err(ArrowError::ComputeError("Overflow happened in sum".into()));
                }
            }
            Accumulator::SumFloat { column, sum, n, .. } => {
                each_f64(
                    batch.column(*column).as_ref(),
                    ids,
                    rows,
                    skip,
                    |group, value| {
                        sum[group] += value;
                        n[group] += 1;
                    },
                );
            }
            Accumulator::Std {
                column,
                n,
                mean,
                m2,
            } => {
                each_f64(
                    batch.column(*column).as_ref(),
                    ids,
                    rows,
                    skip,
                    |group, x| {
                        n[group] += 1;
                        let delta = x - mean[group];
                        mean[group] += delta / n[group] as f64;
                        m2[group] += delta * (x - mean[group]);
                    },
                );
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
                    if skipped(group) || array.is_null(i) {
                        continue;
                    }
                    let candidate = encoded.row(i);
                    let current = &mut best[group as usize];
                    let better = match current {
                        None => true,
                        Some((value, _)) if *max => candidate > value.row(),
                        Some((value, _)) => candidate < value.row(),
                    };
                    if better {
                        *current = Some((candidate.owned(), keys[row_of(i)]));
                    }
                }
            }
            Accumulator::Edge { last, key, .. } => {
                for (i, &group) in ids.iter().enumerate() {
                    if skipped(group) {
                        continue;
                    }
                    let current = &mut key[group as usize];
                    if *last || current.is_none() {
                        *current = Some(keys[row_of(i)]);
                    }
                }
            }
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
            Accumulator::SumFloat { sum, n, mean, .. } => {
                Arc::new(Float64Array::from_iter(at.map(|g| {
                    (n[g] > 0).then(|| if *mean { sum[g] / n[g] as f64 } else { sum[g] })
                })))
            }
            Accumulator::Std { n, m2, .. } => Arc::new(Float64Array::from_iter(
                at.map(|g| (n[g] > 1).then(|| (m2[g] / (n[g] - 1) as f64).sqrt())),
            )),
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

/// Calls `f(group, value)` for each row with a value whose group is not skipped, in order: every
/// row of `array`, or the selected `rows`.
fn each<T: ArrowPrimitiveType>(
    array: &PrimitiveArray<T>,
    ids: &[u32],
    rows: Option<&[u32]>,
    skip: Option<&[bool]>,
    mut f: impl FnMut(usize, T::Native),
) {
    let values = array.values();
    let checked = array.null_count() > 0 || skip.is_some();
    let keep =
        |row: usize, group: u32| !(array.is_null(row) || skip.is_some_and(|s| s[group as usize]));
    match (rows, checked) {
        (None, false) => {
            for (&group, &value) in ids.iter().zip(values.iter()) {
                f(group as usize, value);
            }
        }
        (None, true) => {
            for (row, (&group, &value)) in ids.iter().zip(values.iter()).enumerate() {
                if keep(row, group) {
                    f(group as usize, value);
                }
            }
        }
        (Some(rows), false) => {
            for (&group, &row) in ids.iter().zip(rows) {
                f(group as usize, values[row as usize]);
            }
        }
        (Some(rows), true) => {
            for (&group, &row) in ids.iter().zip(rows) {
                if keep(row as usize, group) {
                    f(group as usize, values[row as usize]);
                }
            }
        }
    }
}

fn each_f64(
    array: &dyn Array,
    ids: &[u32],
    rows: Option<&[u32]>,
    skip: Option<&[bool]>,
    mut f: impl FnMut(usize, f64),
) {
    match array.data_type() {
        DataType::Int32 => each(
            array.as_primitive::<Int32Type>(),
            ids,
            rows,
            skip,
            |g, v| {
                f(g, f64::from(v));
            },
        ),
        DataType::Int64 => each(
            array.as_primitive::<Int64Type>(),
            ids,
            rows,
            skip,
            |g, v| {
                f(g, v as f64);
            },
        ),
        _ => each(array.as_primitive::<Float64Type>(), ids, rows, skip, f),
    }
}

fn each_i64(
    array: &dyn Array,
    ids: &[u32],
    rows: Option<&[u32]>,
    skip: Option<&[bool]>,
    mut f: impl FnMut(usize, i64),
) {
    match array.data_type() {
        DataType::Int32 => each(
            array.as_primitive::<Int32Type>(),
            ids,
            rows,
            skip,
            |g, v| {
                f(g, i64::from(v));
            },
        ),
        _ => each(array.as_primitive::<Int64Type>(), ids, rows, skip, f),
    }
}

/// What every shard needs to know about the operator.
struct Context {
    /// Input columns the shards see, sorted: the group columns and aggregated columns.
    projection: Vec<usize>,
    /// Group columns in the input schema, for output rows.
    group_columns: Vec<usize>,
    output: SchemaRef,
}

/// Aggregations per group, sharded by key hash.
pub(crate) struct AggBy {
    sharder: Sharder,
    shards: Vec<Shard>,
    context: Context,
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
        let parse_all = || -> Result<Vec<Accumulator>, Error> {
            specs
                .iter()
                .map(|spec| Accumulator::parse(spec, input).map(|(a, _)| a))
                .collect()
        };
        let mut projection = group_columns.clone();
        projection.extend(parse_all()?.iter().filter_map(Accumulator::column));
        projection.sort_unstable();
        projection.dedup();
        let projected = input.project(&projection).map_err(schema_error)?;
        let local_groups: Vec<usize> = group_columns
            .iter()
            .map(|c| {
                projection
                    .binary_search(c)
                    .expect("group columns are projected")
            })
            .collect();

        let count = thread::available_parallelism()
            .map_or(1, NonZero::get)
            .min(64);
        let sharder =
            Sharder::new(&projected, local_groups.clone(), count).map_err(schema_error)?;
        let mut shards = Vec::with_capacity(count);
        for shard in 0..count {
            let mut accumulators = parse_all()?;
            for accumulator in &mut accumulators {
                accumulator.project(&projection);
            }
            let groups = Membership::new(
                &projected,
                local_groups.clone(),
                add_only,
                shard as RowKey,
                count as RowKey,
            )
            .map_err(schema_error)?;
            shards.push(Shard {
                groups,
                accumulators,
                published: Vec::new(),
                stale: Vec::new(),
            });
        }
        Ok(Self {
            sharder,
            shards,
            context: Context {
                projection,
                group_columns,
                output,
            },
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
            self.sharder.shards()
        } else {
            1
        };

        // Bucket every batch's rows by shard, piece by piece in parallel. Shards then read their
        // rows in place through these index lists.
        let segments = view.segments();
        let mut pieces = Vec::new();
        for (segment, batch) in segments.iter().enumerate() {
            let mut offset = 0;
            while offset < batch.num_rows() {
                let len = PIECE_ROWS.min(batch.num_rows() - offset);
                pieces.push((segment, offset, len));
                offset += len;
            }
        }
        let sharder = &self.sharder;
        let pieces = run(threads, pieces, |(segment, offset, len)| {
            let shards = sharder.split(&segments[segment].slice(offset, len), offset as u32)?;
            Ok::<_, ArrowError>(Piece { segment, shards })
        })
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

        let context = &self.context;
        let shards: Vec<(usize, &mut Shard)> = self.shards.iter_mut().enumerate().collect();
        let outputs = run(threads, shards, |(index, shard)| {
            let part = view.part(&pieces, index);
            if part.is_empty() {
                return Ok(Delta::default());
            }
            shard.process(context, input, &part, own)
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

/// Runs `f` over `items`, spread across up to `threads` scoped threads, returning results in
/// item order.
fn run<I: Send, T: Send>(threads: usize, items: Vec<I>, f: impl Fn(I) -> T + Sync) -> Vec<T> {
    if threads <= 1 || items.len() <= 1 {
        return items.into_iter().map(f).collect();
    }
    let count = items.len();
    let mut queues: Vec<Vec<(usize, I)>> = (0..threads).map(|_| Vec::new()).collect();
    for (index, item) in items.into_iter().enumerate() {
        queues[index % threads].push((index, item));
    }
    let f = &f;
    let mut results: Vec<Option<T>> = (0..count).map(|_| None).collect();
    thread::scope(|scope| {
        let workers: Vec<_> = queues
            .into_iter()
            .map(|queue| {
                scope.spawn(move || {
                    queue
                        .into_iter()
                        .map(|(index, item)| (index, f(item)))
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        for worker in workers {
            for (index, result) in worker.join().expect("aggregation worker panicked") {
                results[index] = Some(result);
            }
        }
    });
    results
        .into_iter()
        .map(|r| r.expect("every item ran"))
        .collect()
}

/// A delta with batches projected to the columns the shards read.
struct View<'a> {
    removed: Vec<(&'a [RowKey], RecordBatch)>,
    modified: Vec<(&'a [RowKey], RecordBatch, RecordBatch)>,
    added: Vec<(&'a [RowKey], RecordBatch)>,
}

/// One piece of a delta batch: the indices of its rows in each shard.
struct Piece {
    segment: usize,
    shards: Vec<Vec<u32>>,
}

/// Rows of one batch that belong to a shard: every row, or the selected indices, in order.
struct Selected<'a> {
    keys: &'a [RowKey],
    batch: &'a RecordBatch,
    rows: Option<Vec<u32>>,
}

/// Modified rows that stay within a shard.
struct SelectedModified<'a> {
    keys: &'a [RowKey],
    previous: &'a RecordBatch,
    current: &'a RecordBatch,
    rows: Vec<u32>,
}

/// One shard's part of a delta. A modified row that changes shard is removed from its old shard
/// and added to its new one.
#[derive(Default)]
struct Part<'a> {
    removed: Vec<Selected<'a>>,
    modified: Vec<SelectedModified<'a>>,
    added: Vec<Selected<'a>>,
}

impl Part<'_> {
    fn is_empty(&self) -> bool {
        self.removed.is_empty() && self.modified.is_empty() && self.added.is_empty()
    }
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

    /// Every batch, in the order `part` reads them: removed, modified previous, modified
    /// current, added.
    fn segments(&self) -> Vec<&RecordBatch> {
        self.removed
            .iter()
            .map(|(_, b)| b)
            .chain(self.modified.iter().map(|(_, previous, _)| previous))
            .chain(self.modified.iter().map(|(_, _, current)| current))
            .chain(self.added.iter().map(|(_, b)| b))
            .collect()
    }

    /// The rows of `shard`, given every piece in segment and row order.
    fn part<'p>(&'p self, pieces: &[Piece], shard: usize) -> Part<'p> {
        let segments = self.removed.len() + 2 * self.modified.len() + self.added.len();
        let mut rows: Vec<Vec<u32>> = vec![Vec::new(); segments];
        for piece in pieces {
            rows[piece.segment].extend_from_slice(&piece.shards[shard]);
        }
        let mut rows = rows.into_iter();
        let mut part = Part::default();
        let selected = |keys, batch, rows: Vec<u32>| {
            (!rows.is_empty()).then_some(Selected {
                keys,
                batch,
                rows: Some(rows),
            })
        };
        for (keys, batch) in &self.removed {
            let mine = rows.next().expect("one row list per segment");
            part.removed.extend(selected(*keys, batch, mine));
        }
        let previous: Vec<Vec<u32>> = rows.by_ref().take(self.modified.len()).collect();
        let current: Vec<Vec<u32>> = rows.by_ref().take(self.modified.len()).collect();
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
        for ((keys, batch), mine) in self.added.iter().zip(rows) {
            part.added.extend(selected(*keys, batch, mine));
        }
        part
    }
}

/// The batch row of the `i`th selected row: `i` itself when every row is selected.
fn row_at(rows: Option<&[u32]>, i: usize) -> usize {
    rows.map_or(i, |r| r[i] as usize)
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
    groups: Membership,
    accumulators: Vec<Accumulator>,
    published: Vec<bool>,
    /// Recompute from members this cycle: a row left, or arrived out of key order.
    stale: Vec<bool>,
}

impl Shard {
    fn assign(
        &mut self,
        batch: &RecordBatch,
        rows: Option<&[u32]>,
    ) -> Result<Vec<u32>, ArrowError> {
        let ids = self.groups.assign(batch, rows)?;
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
        Ok(ids)
    }

    /// Processes this shard's part of a delta; batches are projected.
    fn process(
        &mut self,
        context: &Context,
        input: &Storage,
        part: &Part<'_>,
        own: &Storage,
    ) -> Result<Delta, ArrowError> {
        for selected in &part.removed {
            let rows = selected.rows.as_deref();
            let ids = self.assign(selected.batch, rows)?;
            for (i, &id) in ids.iter().enumerate() {
                self.groups.members[id as usize].remove(selected.keys[row_at(rows, i)])?;
                self.stale[id as usize] = true;
            }
        }
        for modified in &part.modified {
            let before = self.assign(modified.previous, Some(&modified.rows))?;
            let after = self.assign(modified.current, Some(&modified.rows))?;
            for ((&old, &new), &row) in before.iter().zip(&after).zip(&modified.rows) {
                let key = modified.keys[row as usize];
                self.groups.members[old as usize].remove(key)?;
                self.groups.members[new as usize].insert(key);
                self.stale[old as usize] = true;
                self.stale[new as usize] = true;
            }
        }
        for selected in &part.added {
            let rows = selected.rows.as_deref();
            let ids = self.assign(selected.batch, rows)?;
            let mut any_stale = false;
            for (i, &id) in ids.iter().enumerate() {
                let key = selected.keys[row_at(rows, i)];
                let members = &mut self.groups.members[id as usize];
                if members.last().is_some_and(|last| key < last) {
                    self.stale[id as usize] = true;
                }
                members.insert(key);
                any_stale |= self.stale[id as usize];
            }
            let skip = any_stale.then_some(self.stale.as_slice());
            for accumulator in &mut self.accumulators {
                accumulator.fold(selected.batch, selected.keys, &ids, rows, skip)?;
            }
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
            accumulator.fold(&batch, &keys, &ids, None, None)?;
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
