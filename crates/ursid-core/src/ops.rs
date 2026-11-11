//! Derived-table operators. Each turns its input's delta for a cycle into its own delta. A
//! static table is the same code fed every row as one addition.

use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Float64Array, Int64Array, UInt32Array, new_null_array,
};
use arrow::compute::{filter_record_batch, interleave, take_record_batch};
use arrow::datatypes::{DataType, Field, Float64Type, Int32Type, Int64Type, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use arrow::row::{OwnedRow, RowConverter, Rows as EncodedRows, SortField};

use crate::groups::{Arena, Grouper};
use crate::storage::{Delta, Modified, RowKey, Rows, Storage};
use crate::{Error, Expression, type_name};

pub(crate) enum Operator {
    Where(Where),
    Update(Update),
    LastBy(LastBy),
    AggBy(AggBy),
}

impl Operator {
    pub fn process(
        &mut self,
        input: &Storage,
        delta: &Delta,
        own: &Storage,
    ) -> Result<Delta, ArrowError> {
        match self {
            Operator::Where(op) => op.process(delta, own),
            Operator::Update(op) => op.process(delta, own),
            Operator::LastBy(op) => op.process(input, delta, own),
            Operator::AggBy(op) => op.process(input, delta, own),
        }
    }
}

pub(crate) struct Where {
    predicate: Expression,
    /// Over an append-only input the output is append-only too and numbers its own rows;
    /// otherwise it keeps the input's keys so modifies and removes can be followed.
    rekey: bool,
}

impl Where {
    pub fn new(predicate: Expression, input_add_only: bool) -> Self {
        Self {
            predicate,
            rekey: input_add_only,
        }
    }

    fn process(&self, delta: &Delta, own: &Storage) -> Result<Delta, ArrowError> {
        let mut out = Delta::default();
        if self.rekey {
            let mut next = own.next_key();
            for rows in &delta.added {
                let batch = filter_record_batch(&rows.batch, &self.mask(&rows.batch)?)?;
                let count = batch.num_rows() as RowKey;
                out.added.push(Rows {
                    keys: (next..next + count).collect(),
                    batch,
                });
                next += count;
            }
            return Ok(out);
        }
        for rows in &delta.removed {
            let keys = rows
                .keys
                .iter()
                .copied()
                .filter(|k| own.contains(*k))
                .collect();
            push_removed(&mut out, own, keys)?;
        }
        for rows in &delta.added {
            let mask = self.mask(&rows.batch)?;
            let picked: Vec<usize> = (0..mask.len()).filter(|i| mask.value(*i)).collect();
            push_added(&mut out, &rows.keys, &rows.batch, &picked)?;
        }
        for modified in &delta.modified {
            let mask = self.mask(&modified.current)?;
            let (mut stay, mut enter, mut leave) = (Vec::new(), Vec::new(), Vec::new());
            for (i, key) in modified.keys.iter().enumerate() {
                match (mask.value(i), own.contains(*key)) {
                    (true, true) => stay.push(i),
                    (true, false) => enter.push(i),
                    (false, true) => leave.push(*key),
                    (false, false) => {}
                }
            }
            if !stay.is_empty() {
                let keys = pick_keys(&modified.keys, &stay);
                out.modified.push(Modified {
                    previous: own.gather(&keys)?,
                    current: take_rows(&modified.current, &stay)?,
                    keys,
                });
            }
            push_added(&mut out, &modified.keys, &modified.current, &enter)?;
            push_removed(&mut out, own, leave)?;
        }
        Ok(out)
    }

    /// The predicate with NULL read as false.
    fn mask(&self, batch: &RecordBatch) -> Result<BooleanArray, ArrowError> {
        let value = self.predicate.evaluate(batch)?;
        let mask = value.as_boolean();
        if mask.null_count() == 0 {
            return Ok(mask.clone());
        }
        Ok(mask.iter().map(|v| Some(v == Some(true))).collect())
    }
}

pub(crate) struct Assignment {
    /// The schema the expression is bound to: the input's plus earlier assignments.
    pub input: SchemaRef,
    pub expression: Expression,
    /// The column it writes: an existing index to replace, or the next index to append.
    pub position: usize,
}

pub(crate) struct Update {
    pub assignments: Vec<Assignment>,
    pub output: SchemaRef,
}

impl Update {
    fn process(&self, delta: &Delta, own: &Storage) -> Result<Delta, ArrowError> {
        let mut out = Delta::default();
        for rows in &delta.removed {
            push_removed(&mut out, own, rows.keys.clone())?;
        }
        for modified in &delta.modified {
            out.modified.push(Modified {
                keys: modified.keys.clone(),
                previous: own.gather(&modified.keys)?,
                current: self.compute(&modified.current)?,
            });
        }
        for rows in &delta.added {
            out.added.push(Rows {
                keys: rows.keys.clone(),
                batch: self.compute(&rows.batch)?,
            });
        }
        Ok(out)
    }

    fn compute(&self, batch: &RecordBatch) -> Result<RecordBatch, ArrowError> {
        let rows = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
        let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
        for assignment in &self.assignments {
            let input = RecordBatch::try_new_with_options(
                assignment.input.clone(),
                columns.clone(),
                &rows,
            )?;
            let value = assignment.expression.evaluate(&input)?;
            if assignment.position == columns.len() {
                columns.push(value);
            } else {
                columns[assignment.position] = value;
            }
        }
        RecordBatch::try_new_with_options(self.output.clone(), columns, &rows)
    }
}

/// The keys of an input's rows within one group. Append-only inputs only need the newest.
enum Members {
    Newest(Option<RowKey>),
    All(BTreeSet<RowKey>),
}

impl Members {
    fn new(input_add_only: bool) -> Self {
        if input_add_only {
            Members::Newest(None)
        } else {
            Members::All(BTreeSet::new())
        }
    }

    fn insert(&mut self, key: RowKey) {
        match self {
            Members::Newest(newest) => *newest = Some(newest.map_or(key, |n| n.max(key))),
            Members::All(keys) => {
                keys.insert(key);
            }
        }
    }

    fn remove(&mut self, key: RowKey) -> Result<(), ArrowError> {
        match self {
            Members::Newest(_) => Err(ArrowError::InvalidArgumentError(
                "an append-only input removed a row".into(),
            )),
            Members::All(keys) => {
                keys.remove(&key);
                Ok(())
            }
        }
    }

    fn last(&self) -> Option<RowKey> {
        match self {
            Members::Newest(newest) => *newest,
            Members::All(keys) => keys.last().copied(),
        }
    }

    fn is_empty(&self) -> bool {
        self.last().is_none()
    }

    fn keys(&self) -> Vec<RowKey> {
        match self {
            Members::Newest(newest) => newest.iter().copied().collect(),
            Members::All(keys) => keys.iter().copied().collect(),
        }
    }
}

/// The last row of each group, by input key (arrival order).
pub(crate) struct LastBy {
    grouper: Grouper,
    input_add_only: bool,
    groups: Arena<LastGroup>,
    next_slot: RowKey,
}

struct LastGroup {
    slot: RowKey,
    members: Members,
    /// The input key whose values the output row currently shows.
    published: Option<RowKey>,
}

impl LastBy {
    pub fn new(grouper: Grouper, input_add_only: bool) -> Self {
        Self {
            grouper,
            input_add_only,
            groups: Arena::default(),
            next_slot: 0,
        }
    }

    fn touch(&mut self, key: &[u8]) -> usize {
        let (next_slot, add_only) = (&mut self.next_slot, self.input_add_only);
        self.groups.touch(key, || {
            *next_slot += 1;
            LastGroup {
                slot: *next_slot - 1,
                members: Members::new(add_only),
                published: None,
            }
        })
    }

    fn process(
        &mut self,
        input: &Storage,
        delta: &Delta,
        own: &Storage,
    ) -> Result<Delta, ArrowError> {
        let mut modified_rows = HashSet::new();
        for rows in &delta.removed {
            let groups = self.grouper.encode(&rows.batch)?;
            for (row, key) in rows.keys.iter().enumerate() {
                let group = self.touch(groups.get(row));
                self.groups.get(group).members.remove(*key)?;
            }
        }
        for modified in &delta.modified {
            let before = self.grouper.encode(&modified.previous)?;
            let after = self.grouper.encode(&modified.current)?;
            for (row, key) in modified.keys.iter().enumerate() {
                let old = self.touch(before.get(row));
                let new = self.touch(after.get(row));
                if old != new {
                    self.groups.get(old).members.remove(*key)?;
                    self.groups.get(new).members.insert(*key);
                }
                modified_rows.insert(*key);
            }
        }
        for rows in &delta.added {
            let groups = self.grouper.encode(&rows.batch)?;
            for (row, key) in rows.keys.iter().enumerate() {
                let group = self.touch(groups.get(row));
                self.groups.get(group).members.insert(*key);
            }
        }

        let (mut added, mut changed, mut removed) = (Vec::new(), Vec::new(), Vec::new());
        for index in self.groups.take_touched() {
            let group = self.groups.get(index);
            let last = group.members.last();
            match (group.published, last) {
                (None, Some(now)) => added.push((group.slot, now)),
                (Some(_), None) => removed.push(group.slot),
                (Some(was), Some(now)) if was != now || modified_rows.contains(&now) => {
                    changed.push((group.slot, now));
                }
                _ => {}
            }
            group.published = last;
            if last.is_none() {
                self.groups.remove(index);
            }
        }

        let mut out = Delta::default();
        let (slots, keys): (Vec<_>, Vec<_>) = added.into_iter().unzip();
        out.added.push(Rows {
            keys: slots,
            batch: input.gather(&keys)?,
        });
        let (slots, keys): (Vec<_>, Vec<_>) = changed.into_iter().unzip();
        out.modified.push(Modified {
            previous: own.gather(&slots)?,
            current: input.gather(&keys)?,
            keys: slots,
        });
        push_removed(&mut out, own, removed)?;
        Ok(out)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum AggKind {
    Count,
    Sum,
    Mean,
    Std,
    Min,
    Max,
    First,
    Last,
}

/// One aggregation: `count(*)`, or `kind(column)`.
pub(crate) struct Aggregation {
    kind: AggKind,
    column: Option<usize>,
    float_input: bool,
    /// Comparable encoding of the column, for `min` and `max`.
    converter: Option<RowConverter>,
}

impl Aggregation {
    /// Parses `name(column)`, returning the aggregation and its output type.
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
        let kind = match name.trim().to_ascii_lowercase().as_str() {
            "count" => AggKind::Count,
            "sum" => AggKind::Sum,
            "mean" | "avg" => AggKind::Mean,
            "std" | "stddev" => AggKind::Std,
            "min" => AggKind::Min,
            "max" => AggKind::Max,
            "first" => AggKind::First,
            "last" => AggKind::Last,
            other => {
                return Err(error(format!(
                    "unknown aggregation `{other}`; expected count, sum, mean, std, min, max, first, or last"
                )));
            }
        };
        if argument == "*" {
            if kind != AggKind::Count {
                return Err(error("only count accepts `*`".into()));
            }
            let aggregation = Self {
                kind,
                column: None,
                float_input: false,
                converter: None,
            };
            return Ok((aggregation, DataType::Int64));
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
        let output_type = match kind {
            AggKind::Count => DataType::Int64,
            AggKind::Sum | AggKind::Mean | AggKind::Std if !numeric => {
                return Err(error(format!(
                    "`{name}` needs a number, but `{argument}` is {}",
                    type_name(&input_type)
                )));
            }
            AggKind::Sum if input_type == DataType::Float64 => DataType::Float64,
            AggKind::Sum => DataType::Int64,
            AggKind::Mean | AggKind::Std => DataType::Float64,
            AggKind::Min | AggKind::Max | AggKind::First | AggKind::Last => input_type.clone(),
        };
        let converter = match kind {
            AggKind::Min | AggKind::Max => Some(
                RowConverter::new(vec![SortField::new(input_type.clone())])
                    .map_err(|e| error(e.to_string()))?,
            ),
            _ => None,
        };
        let aggregation = Self {
            kind,
            column: Some(column),
            float_input: input_type == DataType::Float64,
            converter,
        };
        Ok((aggregation, output_type))
    }

    fn initial(&self) -> State {
        match self.kind {
            AggKind::Count => State::Count(0),
            AggKind::Sum if self.float_input => State::SumFloat { sum: 0.0, n: 0 },
            AggKind::Sum => State::SumInt { sum: 0, n: 0 },
            AggKind::Mean => State::Mean { sum: 0.0, n: 0 },
            AggKind::Std => State::Std {
                n: 0,
                mean: 0.0,
                m2: 0.0,
            },
            AggKind::Min | AggKind::Max => State::Extreme(None),
            AggKind::First | AggKind::Last => State::Edge(None),
        }
    }
}

/// Accumulated values for one aggregation in one group. Rows are always folded in input-key
/// order, so a group computed incrementally equals the same group computed from scratch.
enum State {
    Count(i64),
    SumInt { sum: i64, n: i64 },
    SumFloat { sum: f64, n: i64 },
    Mean { sum: f64, n: i64 },
    Std { n: i64, mean: f64, m2: f64 },
    Extreme(Option<(OwnedRow, RowKey)>),
    Edge(Option<RowKey>),
}

/// One aggregation's view of a batch, prepared once per batch.
enum Column<'a> {
    None,
    Int32(&'a arrow::array::PrimitiveArray<Int32Type>),
    Int64(&'a arrow::array::PrimitiveArray<Int64Type>),
    Float64(&'a arrow::array::PrimitiveArray<Float64Type>),
    Encoded(&'a dyn Array, EncodedRows),
    Any(&'a dyn Array),
}

impl Column<'_> {
    fn is_null(&self, row: usize) -> bool {
        match self {
            Column::None => false,
            Column::Int32(a) => a.is_null(row),
            Column::Int64(a) => a.is_null(row),
            Column::Float64(a) => a.is_null(row),
            Column::Encoded(a, _) | Column::Any(a) => a.is_null(row),
        }
    }

    fn int(&self, row: usize) -> i64 {
        match self {
            Column::Int32(a) => i64::from(a.value(row)),
            Column::Int64(a) => a.value(row),
            _ => 0,
        }
    }

    fn float(&self, row: usize) -> f64 {
        match self {
            Column::Int32(a) => f64::from(a.value(row)),
            Column::Int64(a) => a.value(row) as f64,
            Column::Float64(a) => a.value(row),
            _ => 0.0,
        }
    }
}

impl State {
    fn fold(
        &mut self,
        kind: AggKind,
        column: &Column<'_>,
        row: usize,
        key: RowKey,
    ) -> Result<(), ArrowError> {
        if !matches!(kind, AggKind::First | AggKind::Last) && column.is_null(row) {
            return Ok(());
        }
        match self {
            State::Count(n) => *n += 1,
            State::SumInt { sum, n } => {
                *sum = sum
                    .checked_add(column.int(row))
                    .ok_or_else(|| ArrowError::ComputeError("Overflow happened in sum".into()))?;
                *n += 1;
            }
            State::SumFloat { sum, n } | State::Mean { sum, n } => {
                *sum += column.float(row);
                *n += 1;
            }
            State::Std { n, mean, m2 } => {
                let x = column.float(row);
                *n += 1;
                let delta = x - *mean;
                *mean += delta / *n as f64;
                *m2 += delta * (x - *mean);
            }
            State::Extreme(best) => {
                let Column::Encoded(_, rows) = column else {
                    return Ok(());
                };
                let candidate = rows.row(row);
                let better = match best {
                    None => true,
                    Some((value, _)) if kind == AggKind::Min => candidate < value.row(),
                    Some((value, _)) => candidate > value.row(),
                };
                if better {
                    *best = Some((candidate.owned(), key));
                }
            }
            State::Edge(edge) => {
                if kind == AggKind::Last || edge.is_none() {
                    *edge = Some(key);
                }
            }
        }
        Ok(())
    }
}

/// Aggregations per group.
pub(crate) struct AggBy {
    grouper: Grouper,
    aggregations: Vec<Aggregation>,
    input_add_only: bool,
    output: SchemaRef,
    groups: Arena<AggGroup>,
    next_slot: RowKey,
}

struct AggGroup {
    slot: RowKey,
    members: Members,
    states: Vec<State>,
    published: bool,
    /// Recompute from members this cycle: a row left, or arrived out of key order.
    stale: bool,
}

impl AggBy {
    pub fn new(
        grouper: Grouper,
        aggregations: Vec<Aggregation>,
        input_add_only: bool,
        output: SchemaRef,
    ) -> Self {
        Self {
            grouper,
            aggregations,
            input_add_only,
            output,
            groups: Arena::default(),
            next_slot: 0,
        }
    }

    fn touch(&mut self, key: &[u8]) -> usize {
        let (next_slot, add_only, aggregations) =
            (&mut self.next_slot, self.input_add_only, &self.aggregations);
        self.groups.touch(key, || {
            *next_slot += 1;
            AggGroup {
                slot: *next_slot - 1,
                members: Members::new(add_only),
                states: aggregations.iter().map(Aggregation::initial).collect(),
                published: false,
                stale: false,
            }
        })
    }

    fn columns<'a>(&self, batch: &'a RecordBatch) -> Result<Vec<Column<'a>>, ArrowError> {
        self.aggregations
            .iter()
            .map(|aggregation| {
                let Some(index) = aggregation.column else {
                    return Ok(Column::None);
                };
                let array = batch.column(index).as_ref();
                if let Some(converter) = &aggregation.converter {
                    let encoded = converter.convert_columns(&[batch.column(index).clone()])?;
                    return Ok(Column::Encoded(array, encoded));
                }
                Ok(match array.data_type() {
                    DataType::Int32 => Column::Int32(array.as_primitive()),
                    DataType::Int64 => Column::Int64(array.as_primitive()),
                    DataType::Float64 => Column::Float64(array.as_primitive()),
                    _ => Column::Any(array),
                })
            })
            .collect()
    }

    fn fold(
        &mut self,
        group: usize,
        columns: &[Column<'_>],
        row: usize,
        key: RowKey,
    ) -> Result<(), ArrowError> {
        let states = &mut self.groups.get(group).states;
        for ((state, aggregation), column) in states.iter_mut().zip(&self.aggregations).zip(columns)
        {
            state.fold(aggregation.kind, column, row, key)?;
        }
        Ok(())
    }

    fn process(
        &mut self,
        input: &Storage,
        delta: &Delta,
        own: &Storage,
    ) -> Result<Delta, ArrowError> {
        for rows in &delta.removed {
            let groups = self.grouper.encode(&rows.batch)?;
            for (row, key) in rows.keys.iter().enumerate() {
                let group = self.touch(groups.get(row));
                let group = self.groups.get(group);
                group.members.remove(*key)?;
                group.stale = true;
            }
        }
        for modified in &delta.modified {
            let before = self.grouper.encode(&modified.previous)?;
            let after = self.grouper.encode(&modified.current)?;
            for (row, key) in modified.keys.iter().enumerate() {
                let old = self.touch(before.get(row));
                self.groups.get(old).members.remove(*key)?;
                self.groups.get(old).stale = true;
                let new = self.touch(after.get(row));
                self.groups.get(new).members.insert(*key);
                self.groups.get(new).stale = true;
            }
        }
        for rows in &delta.added {
            let groups = self.grouper.encode(&rows.batch)?;
            let columns = self.columns(&rows.batch)?;
            for (row, key) in rows.keys.iter().enumerate() {
                let index = self.touch(groups.get(row));
                let group = self.groups.get(index);
                if group.members.last().is_some_and(|last| *key < last) {
                    group.stale = true;
                }
                group.members.insert(*key);
                if !group.stale {
                    self.fold(index, &columns, row, *key)?;
                }
            }
        }

        let touched = self.groups.take_touched();
        for &index in &touched {
            let group = self.groups.get(index);
            if !std::mem::take(&mut group.stale) || group.members.is_empty() {
                continue;
            }
            let members = group.members.keys();
            group.states = self.aggregations.iter().map(Aggregation::initial).collect();
            let batch = input.gather(&members)?;
            let columns = self.columns(&batch)?;
            for (row, key) in members.iter().enumerate() {
                self.fold(index, &columns, row, *key)?;
            }
        }

        let (mut added, mut changed, mut removed) = (Vec::new(), Vec::new(), Vec::new());
        for index in touched {
            let group = self.groups.get(index);
            if group.members.is_empty() {
                if group.published {
                    removed.push(group.slot);
                }
                self.groups.remove(index);
            } else if group.published {
                changed.push(index);
            } else {
                group.published = true;
                added.push(index);
            }
        }

        let mut out = Delta::default();
        let (slots, batch) = self.output_rows(input, &added)?;
        out.added.push(Rows { keys: slots, batch });
        let (slots, current) = self.output_rows(input, &changed)?;
        out.modified.push(Modified {
            previous: own.gather(&slots)?,
            current,
            keys: slots,
        });
        push_removed(&mut out, own, removed)?;
        Ok(out)
    }

    /// The output rows for some groups: group columns, then one column per aggregation.
    fn output_rows(
        &self,
        input: &Storage,
        indices: &[usize],
    ) -> Result<(Vec<RowKey>, RecordBatch), ArrowError> {
        let entries: Vec<_> = indices
            .iter()
            .map(|i| {
                let entry = self.groups.entry_ref(*i);
                (entry.key.clone(), entry.group.slot)
            })
            .collect();
        let encoded: Vec<&[u8]> = entries.iter().map(|(key, _)| key.as_ref()).collect();
        let mut columns = self.grouper.decode(&encoded)?;
        let group_columns = self.grouper.columns().len();
        for (position, aggregation) in self.aggregations.iter().enumerate() {
            let states: Vec<&State> = indices
                .iter()
                .map(|i| &self.groups.entry_ref(*i).group.states[position])
                .collect();
            let field = self.output.field(group_columns + position);
            let column: ArrayRef = match aggregation.kind {
                AggKind::Count => {
                    Arc::new(Int64Array::from_iter_values(states.iter().map(
                        |s| match s {
                            State::Count(n) => *n,
                            _ => 0,
                        },
                    )))
                }
                AggKind::Sum if field.data_type() == &DataType::Int64 => {
                    Arc::new(Int64Array::from_iter(states.iter().map(|s| match s {
                        State::SumInt { sum, n } if *n > 0 => Some(*sum),
                        _ => None,
                    })))
                }
                AggKind::Sum | AggKind::Mean | AggKind::Std => {
                    Arc::new(Float64Array::from_iter(states.iter().map(|s| match s {
                        State::SumFloat { sum, n } if *n > 0 => Some(*sum),
                        State::Mean { sum, n } if *n > 0 => Some(*sum / *n as f64),
                        State::Std { n, m2, .. } if *n > 1 => Some((*m2 / (*n - 1) as f64).sqrt()),
                        _ => None,
                    })))
                }
                AggKind::Min | AggKind::Max | AggKind::First | AggKind::Last => {
                    let keys: Vec<Option<RowKey>> = states
                        .iter()
                        .map(|s| match s {
                            State::Extreme(best) => best.as_ref().map(|(_, key)| *key),
                            State::Edge(edge) => *edge,
                            _ => None,
                        })
                        .collect();
                    let column = aggregation
                        .column
                        .expect("min, max, first, last take a column");
                    pick_column(input, column, &keys)?
                }
            };
            columns.push(column);
        }
        let slots = entries.iter().map(|(_, slot)| *slot).collect();
        let rows = RecordBatchOptions::new().with_row_count(Some(indices.len()));
        let batch = RecordBatch::try_new_with_options(self.output.clone(), columns, &rows)?;
        Ok((slots, batch))
    }
}

/// One input column's values at `keys`, NULL where there is no key.
fn pick_column(
    input: &Storage,
    column: usize,
    keys: &[Option<RowKey>],
) -> Result<ArrayRef, ArrowError> {
    let present: Vec<RowKey> = keys.iter().flatten().copied().collect();
    let values = input.gather(&present)?.column(column).clone();
    let null = new_null_array(values.data_type(), 1);
    let mut next = 0;
    let indices: Vec<(usize, usize)> = keys
        .iter()
        .map(|key| match key {
            Some(_) => {
                next += 1;
                (0, next - 1)
            }
            None => (1, 0),
        })
        .collect();
    interleave(&[values.as_ref(), null.as_ref()], &indices)
}

/// Output schema for `agg_by`: the group columns, then the aggregations.
pub(crate) fn agg_schema(
    input: &Schema,
    grouper: &Grouper,
    outputs: &[(String, DataType)],
) -> Result<SchemaRef, Error> {
    let mut fields: Vec<Field> = grouper
        .columns()
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

fn take_rows(batch: &RecordBatch, rows: &[usize]) -> Result<RecordBatch, ArrowError> {
    let indices = UInt32Array::from_iter_values(rows.iter().map(|r| *r as u32));
    take_record_batch(batch, &indices)
}

fn pick_keys(keys: &[RowKey], rows: &[usize]) -> Vec<RowKey> {
    rows.iter().map(|r| keys[*r]).collect()
}

fn push_added(
    out: &mut Delta,
    keys: &[RowKey],
    batch: &RecordBatch,
    rows: &[usize],
) -> Result<(), ArrowError> {
    if !rows.is_empty() {
        out.added.push(Rows {
            keys: pick_keys(keys, rows),
            batch: take_rows(batch, rows)?,
        });
    }
    Ok(())
}

fn push_removed(out: &mut Delta, own: &Storage, keys: Vec<RowKey>) -> Result<(), ArrowError> {
    if !keys.is_empty() {
        out.removed.push(Rows {
            batch: own.gather(&keys)?,
            keys,
        });
    }
    Ok(())
}
