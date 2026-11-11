//! `rolling`: each row with aggregates over a backward window of its group's rows.
//!
//! The input only gains rows, in key order, so earlier rows never change: each added row joins
//! its group's window, rows that fall out of the window leave it, and the row gets the window's
//! aggregates. Per group, the window's values sit in queues, oldest first; counts and sums add
//! and subtract, `std` keeps sums shifted by a value near the data, and `min`/`max` keep a
//! monotonic queue whose front is the answer. Groups are independent, so their windows are
//! sharded and large batches roll shard by shard on every core.

use std::collections::VecDeque;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Float64Array, Int64Array};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Float64Type, Int64Type, SchemaRef};
use arrow::error::ArrowError;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};

use crate::groups::{KeyKind, LocalGroups, Membership};
use crate::parallel::{PIECE_ROWS, offsets, run, threads, threads_for};
use crate::storage::{Delta, Rows};

/// Which of its group's rows a row's window holds: the row itself and rows before it in key
/// order.
#[derive(Clone, Copy, Debug)]
pub enum Window<'a> {
    /// The row and up to `n - 1` rows before it.
    Rows(usize),
    /// The rows whose `at` lies in `(t - span, t]`, `t` being the row's own; `span` is in
    /// `at`'s units (nanoseconds for timestamps, days for dates).
    Time { at: &'a str, span: i64 },
}

/// A window resolved against the input's columns.
pub(crate) enum Span {
    Rows(usize),
    Time { at: usize, span: i64 },
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Function {
    Count,
    Sum,
    Mean,
    Std,
    Min,
    Max,
}

/// One aggregation over the window.
pub(crate) struct Aggregate {
    pub function: Function,
    /// The input column read, as an index into `Rolling::inputs`; `None` for `count(*)`.
    pub input: Option<usize>,
    /// The output column's type: the input's for `min`/`max`.
    pub output: DataType,
}

/// An input column the aggregations read, as floats (`float64`) or integers (anything else).
pub(crate) struct Input {
    pub column: usize,
    pub float: bool,
}

#[derive(Clone, Copy)]
enum Value {
    Int(i64),
    Float(f64),
}

impl Value {
    fn float(self) -> f64 {
        match self {
            Value::Int(v) => v as f64,
            Value::Float(v) => v,
        }
    }

    /// Ordering for `min`/`max`: integers as integers, floats by IEEE total order.
    fn cmp(self, other: Value) -> std::cmp::Ordering {
        match (self, other) {
            (Value::Int(a), Value::Int(b)) => a.cmp(&b),
            (a, b) => a.float().total_cmp(&b.float()),
        }
    }
}

/// One aggregation's running state for one group's window.
enum State {
    /// `count(*)` reads the window's length instead.
    Rows,
    Count {
        n: i64,
    },
    SumInt {
        sum: i64,
        n: i64,
    },
    SumFloat {
        sum: f64,
        n: i64,
    },
    /// Sums of values and squares, shifted by a value near the data so they stay small:
    /// `s1 = Σ(x - shift)`, `s2 = Σ(x - shift)²`. Errors then scale with the spread, not the
    /// magnitude, and a window of equal values has exactly zero spread.
    Moments {
        n: i64,
        shift: f64,
        s1: f64,
        s2: f64,
    },
    /// Rows by sequence number whose values decrease (`max`) or increase (`min`) from the front.
    Extreme {
        best: VecDeque<(u64, Value)>,
    },
}

/// One group's window.
struct Group {
    /// Sequence number of the window's oldest row; the newest is `next - 1`.
    first: u64,
    next: u64,
    /// The window's times, oldest first (time windows only).
    times: VecDeque<i64>,
    /// Per input, the window's values, oldest first.
    values: Vec<VecDeque<Option<Value>>>,
    states: Vec<State>,
}

pub(crate) struct Rolling {
    groups: Membership,
    /// Reads the group key from batches, for grouping large batches piece by piece.
    keys: KeyKind,
    span: Span,
    inputs: Vec<Input>,
    aggregates: Vec<Aggregate>,
    output: SchemaRef,
    /// Group windows in shards by id: group `id` lives in shard `id % shards.len()` at
    /// `id / shards.len()`, so shards roll on their own threads. Each group still sees its rows
    /// in order, so results never depend on the number of shards.
    shards: Vec<Vec<Group>>,
}

impl Rolling {
    pub fn new(
        groups: Membership,
        keys: KeyKind,
        span: Span,
        inputs: Vec<Input>,
        aggregates: Vec<Aggregate>,
        output: SchemaRef,
    ) -> Self {
        Self {
            groups,
            keys,
            span,
            inputs,
            aggregates,
            output,
            shards: (0..threads()).map(|_| Vec::new()).collect(),
        }
    }

    /// Each row's group. A large batch is grouped in two phases, as in `last_by`: pieces group
    /// their rows in parallel, then the membership takes one entry per piece's group rather than
    /// one per row.
    fn group_ids(&mut self, batch: &RecordBatch) -> Result<Vec<u32>, ArrowError> {
        let rows = batch.num_rows();
        let threads = threads_for(rows);
        let ids = if threads <= 1 {
            self.groups.assign(batch, None)?
        } else {
            let pieces: Vec<(usize, RecordBatch)> = offsets(rows, PIECE_ROWS)
                .map(|(offset, len)| (offset, batch.slice(offset, len)))
                .collect();
            let kind = &self.keys;
            let locals = run(threads, pieces, |table, (offset, piece)| {
                let encoded = kind.encode(&piece)?;
                let groups = LocalGroups::build(
                    &kind.keys(&piece, encoded.as_ref()),
                    piece.num_rows(),
                    table,
                );
                Ok::<_, ArrowError>((offset, piece, encoded, groups))
            });
            let mut ids = vec![0; rows];
            for local in locals {
                let (offset, piece, encoded, groups) = local?;
                let view = self.keys.keys(&piece, encoded.as_ref());
                let global: Vec<u32> = (0..groups.len())
                    .map(|group| {
                        let row = groups.first_row[group] as usize;
                        self.groups.group(
                            groups.hash[group],
                            |key| view.matches(row, key),
                            || view.owned(row),
                        )
                    })
                    .collect();
                for (id, local) in ids[offset..].iter_mut().zip(&groups.ids) {
                    *id = global[*local as usize];
                }
            }
            ids
        };
        self.groups.take_created();
        self.groups.take_touched();
        Ok(ids)
    }

    pub fn process(&mut self, delta: &Delta) -> Result<Delta, ArrowError> {
        if delta.removed.iter().any(|r| !r.keys.is_empty())
            || delta.modified.iter().any(|m| !m.keys.is_empty())
        {
            return Err(ArrowError::ComputeError(
                "rolling: the input removed or modified rows; it must only append them".into(),
            ));
        }
        let mut out = Delta::default();
        for rows in &delta.added {
            out.added.push(Rows {
                keys: rows.keys.clone(),
                batch: self.add(&rows.batch)?,
            });
        }
        Ok(out)
    }

    /// `batch`'s rows, in order, with their windows' aggregates as extra columns.
    fn add(&mut self, batch: &RecordBatch) -> Result<RecordBatch, ArrowError> {
        let ids = self.group_ids(batch)?;
        let count = self.shards.len();
        let capacity = self.groups.capacity();
        for (index, shard) in self.shards.iter_mut().enumerate() {
            while shard.len() < capacity.saturating_sub(index).div_ceil(count) {
                shard.push(Group::new(&self.inputs, &self.aggregates));
            }
        }
        let times = match self.span {
            Span::Time { at, .. } => Some(integers(batch.column(at))?),
            Span::Rows(_) => None,
        };
        let inputs = self
            .inputs
            .iter()
            .map(|input| {
                let column = batch.column(input.column);
                Ok(if input.float {
                    Column::Float(column.as_primitive::<Float64Type>().clone())
                } else {
                    Column::Int(integers(column)?)
                })
            })
            .collect::<Result<Vec<_>, ArrowError>>()?;
        let context = RowContext {
            span: &self.span,
            aggregates: &self.aggregates,
            times: times.as_ref(),
            inputs: &inputs,
        };
        // Each shard's rows, in row order.
        let mut rows: Vec<Vec<u32>> = vec![Vec::new(); count];
        for (row, id) in ids.iter().enumerate() {
            rows[*id as usize % count].push(row as u32);
        }
        let work: Vec<(&mut Vec<Group>, Vec<u32>)> = self
            .shards
            .iter_mut()
            .zip(rows)
            .filter(|(_, rows)| !rows.is_empty())
            .collect();
        let rolled = run(
            threads_for(ids.len()),
            work,
            |scratch: &mut Scratch, (windows, rows)| {
                let mut outputs: Vec<Output> = context
                    .aggregates
                    .iter()
                    .map(|a| Output::new(&a.output, rows.len()))
                    .collect();
                for row in &rows {
                    let window = &mut windows[ids[*row as usize] as usize / count];
                    context.roll(window, *row as usize, scratch, &mut outputs)?;
                }
                Ok::<_, ArrowError>((rows, outputs))
            },
        )
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
        // Back into row order.
        let mut columns = batch.columns().to_vec();
        for (index, aggregate) in self.aggregates.iter().enumerate() {
            let mut column = Output::nulls(&aggregate.output, ids.len());
            for (rows, outputs) in &rolled {
                column.scatter(rows, &outputs[index]);
            }
            columns.push(column.finish(&aggregate.output)?);
        }
        let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
        RecordBatch::try_new_with_options(self.output.clone(), columns, &options)
    }
}

/// Scratch reused row to row: a row's values (one per input) and a leaving row's.
#[derive(Default)]
struct Scratch {
    values: Vec<Option<Value>>,
    leaving: Vec<Option<Value>>,
}

/// What rolling a batch's rows reads.
struct RowContext<'a> {
    span: &'a Span,
    aggregates: &'a [Aggregate],
    times: Option<&'a Int64Array>,
    inputs: &'a [Column],
}

impl RowContext<'_> {
    /// Moves `window` on to include `row`, and appends its aggregates to `outputs`.
    fn roll(
        &self,
        window: &mut Group,
        row: usize,
        scratch: &mut Scratch,
        outputs: &mut [Output],
    ) -> Result<(), ArrowError> {
        match *self.span {
            Span::Rows(n) => {
                while window.next - window.first >= n as u64 {
                    window.evict(self.aggregates, &mut scratch.leaving);
                }
            }
            Span::Time { span, .. } => {
                let times = self.times.expect("time windows read times");
                if times.is_null(row) {
                    // A row without a time is in no window, its own included.
                    for output in outputs {
                        output.push(None);
                    }
                    return Ok(());
                }
                let time = times.value(row);
                if let Some(last) = window.times.back()
                    && time < *last
                {
                    return Err(ArrowError::ComputeError(format!(
                        "rolling: a group's time went backwards, from {last} to {time}; time \
                         windows need each group's rows in time order"
                    )));
                }
                let start = time.saturating_sub(span);
                while window.times.front().is_some_and(|t| *t <= start) {
                    window.evict(self.aggregates, &mut scratch.leaving);
                }
                window.times.push_back(time);
            }
        }
        scratch.values.clear();
        scratch
            .values
            .extend(self.inputs.iter().map(|c| c.get(row)));
        window.push(self.aggregates, &scratch.values)?;
        for ((aggregate, state), output) in self.aggregates.iter().zip(&window.states).zip(outputs)
        {
            output.push(window.result(aggregate.function, state));
        }
        Ok(())
    }
}

impl Group {
    /// An empty window for a new group.
    fn new(inputs: &[Input], aggregates: &[Aggregate]) -> Self {
        Group {
            first: 0,
            next: 0,
            times: VecDeque::new(),
            values: inputs.iter().map(|_| VecDeque::new()).collect(),
            states: aggregates
                .iter()
                .map(|aggregate| match (aggregate.function, aggregate.input) {
                    (Function::Count, None) => State::Rows,
                    (Function::Count, Some(_)) => State::Count { n: 0 },
                    (Function::Sum, Some(input)) if !inputs[input].float => {
                        State::SumInt { sum: 0, n: 0 }
                    }
                    (Function::Sum | Function::Mean, _) => State::SumFloat { sum: 0.0, n: 0 },
                    (Function::Std, _) => State::Moments {
                        n: 0,
                        shift: 0.0,
                        s1: 0.0,
                        s2: 0.0,
                    },
                    (Function::Min | Function::Max, _) => State::Extreme {
                        best: VecDeque::new(),
                    },
                })
                .collect(),
        }
    }

    fn push(
        &mut self,
        aggregates: &[Aggregate],
        values: &[Option<Value>],
    ) -> Result<(), ArrowError> {
        let seq = self.next;
        self.next += 1;
        for (queue, value) in self.values.iter_mut().zip(values) {
            queue.push_back(*value);
        }
        for (aggregate, state) in aggregates.iter().zip(&mut self.states) {
            let Some(value) = aggregate.input.and_then(|input| values[input]) else {
                continue;
            };
            match state {
                State::Rows => {}
                State::Count { n } => *n += 1,
                State::SumInt { sum, n } => {
                    let Value::Int(v) = value else {
                        unreachable!("integer sums read integers")
                    };
                    *sum = sum.checked_add(v).ok_or_else(|| {
                        ArrowError::ComputeError("rolling: an integer sum overflowed".into())
                    })?;
                    *n += 1;
                }
                State::SumFloat { sum, n } => {
                    *sum += value.float();
                    *n += 1;
                }
                State::Moments { n, shift, s1, s2 } => {
                    let x = value.float();
                    if *n == 0 {
                        *shift = x;
                    }
                    let d = x - *shift;
                    (*s1, *s2, *n) = (*s1 + d, *s2 + d * d, *n + 1);
                    // Once the data has moved far from the shift (its mean more than four
                    // spreads away), the sums cancel: rebuild them around the newest value.
                    let offset = *s1 / *n as f64;
                    let spread = (*s2 - *s1 * offset).max(0.0) / *n as f64;
                    if offset * offset > 16.0 * spread {
                        let input = aggregate.input.expect("std reads a column");
                        (*shift, *s1, *s2) = (x, 0.0, 0.0);
                        for v in self.values[input].iter().flatten() {
                            let d = v.float() - x;
                            (*s1, *s2) = (*s1 + d, *s2 + d * d);
                        }
                    }
                }
                State::Extreme { best } => {
                    let max = aggregate.function == Function::Max;
                    while best.back().is_some_and(|(_, kept)| {
                        let order = kept.cmp(value);
                        if max { order.is_le() } else { order.is_ge() }
                    }) {
                        best.pop_back();
                    }
                    best.push_back((seq, value));
                }
            }
        }
        Ok(())
    }

    /// Takes the oldest row out of the window: each aggregation sheds the leaving row's value of
    /// the column it reads.
    fn evict(&mut self, aggregates: &[Aggregate], values: &mut Vec<Option<Value>>) {
        let seq = self.first;
        self.first += 1;
        self.times.pop_front();
        values.clear();
        values.extend(
            self.values
                .iter_mut()
                .map(|queue| queue.pop_front().expect("the window holds the row")),
        );
        for (aggregate, state) in aggregates.iter().zip(&mut self.states) {
            let Some(value) = aggregate.input.and_then(|input| values[input]) else {
                continue;
            };
            match state {
                State::Rows => {}
                State::Count { n } => *n -= 1,
                State::SumInt { sum, n } => {
                    let Value::Int(v) = value else {
                        unreachable!("integer sums read integers")
                    };
                    *sum -= v;
                    *n -= 1;
                }
                State::SumFloat { sum, n } => {
                    *n -= 1;
                    // An empty window's sum is exactly zero, whatever rounding came before.
                    *sum = if *n == 0 { 0.0 } else { *sum - value.float() };
                }
                State::Moments { n, shift, s1, s2 } => {
                    *n -= 1;
                    if *n == 0 {
                        (*s1, *s2) = (0.0, 0.0);
                    } else {
                        let d = value.float() - *shift;
                        (*s1, *s2) = (*s1 - d, *s2 - d * d);
                    }
                }
                State::Extreme { best } => {
                    if best.front().is_some_and(|(kept, _)| *kept == seq) {
                        best.pop_front();
                    }
                }
            }
        }
    }

    fn result(&self, function: Function, state: &State) -> Option<Value> {
        match state {
            State::Rows => Some(Value::Int((self.next - self.first) as i64)),
            State::Count { n } => Some(Value::Int(*n)),
            State::SumInt { sum, n } => (*n > 0).then_some(Value::Int(*sum)),
            State::SumFloat { sum, n } => (*n > 0).then(|| match function {
                Function::Mean => Value::Float(sum / *n as f64),
                _ => Value::Float(*sum),
            }),
            State::Moments { n, s1, s2, .. } => (*n > 1).then(|| {
                let m2 = (s2 - s1 * s1 / *n as f64).max(0.0);
                Value::Float((m2 / (*n - 1) as f64).sqrt())
            }),
            State::Extreme { best } => best.front().map(|(_, value)| *value),
        }
    }
}

/// An input column, read as integers or floats.
enum Column {
    Int(Int64Array),
    Float(Float64Array),
}

impl Column {
    fn get(&self, row: usize) -> Option<Value> {
        match self {
            Column::Int(a) => a.is_valid(row).then(|| Value::Int(a.value(row))),
            Column::Float(a) => a.is_valid(row).then(|| Value::Float(a.value(row))),
        }
    }
}

/// An integer, date, or timestamp column as `int64`.
fn integers(column: &ArrayRef) -> Result<Int64Array, ArrowError> {
    Ok(cast(column, &DataType::Int64)?
        .as_primitive::<Int64Type>()
        .clone())
}

/// One aggregation's values for a batch.
enum Output {
    Int(Vec<Option<i64>>),
    Float(Vec<Option<f64>>),
}

impl Output {
    /// Room for `rows` values of an output of type `data_type`.
    fn new(data_type: &DataType, rows: usize) -> Self {
        match data_type {
            DataType::Float64 => Output::Float(Vec::with_capacity(rows)),
            _ => Output::Int(Vec::with_capacity(rows)),
        }
    }

    /// `rows` NULLs, to be filled by `scatter`.
    fn nulls(data_type: &DataType, rows: usize) -> Self {
        match data_type {
            DataType::Float64 => Output::Float(vec![None; rows]),
            _ => Output::Int(vec![None; rows]),
        }
    }

    fn push(&mut self, value: Option<Value>) {
        match self {
            Output::Int(values) => values.push(value.map(|v| match v {
                Value::Int(v) => v,
                Value::Float(_) => unreachable!("integer outputs hold integers"),
            })),
            Output::Float(values) => values.push(value.map(Value::float)),
        }
    }

    /// Puts `from`'s values (one per entry of `rows`) at those rows.
    fn scatter(&mut self, rows: &[u32], from: &Output) {
        match (self, from) {
            (Output::Int(to), Output::Int(from)) => {
                for (row, value) in rows.iter().zip(from) {
                    to[*row as usize] = *value;
                }
            }
            (Output::Float(to), Output::Float(from)) => {
                for (row, value) in rows.iter().zip(from) {
                    to[*row as usize] = *value;
                }
            }
            _ => unreachable!("an aggregation's outputs share a type"),
        }
    }

    /// The column, in the output type (`min`/`max` of dates and timestamps cast back).
    fn finish(self, data_type: &DataType) -> Result<ArrayRef, ArrowError> {
        let array: ArrayRef = match self {
            Output::Int(values) => Arc::new(Int64Array::from(values)),
            Output::Float(values) => Arc::new(Float64Array::from(values)),
        };
        if array.data_type() == data_type {
            Ok(array)
        } else {
            cast(&array, data_type)
        }
    }
}
