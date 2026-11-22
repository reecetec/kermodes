//! A plan's scans of stored tables (architecture.md §5.3): all of a table's rows, the lake's
//! files and weir's tail as one, read now (static) or then followed (ticking).

use std::sync::{Arc, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{Array, AsArray, RecordBatch, RecordBatchOptions, UInt64Array};
use arrow::compute::{filter_record_batch, prep_null_mask_filter, take};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::ArrowError;
use kermodes_core::plan::Scan;
use kermodes_weir::{Date, TableEntry, TableName};
use pyo3::prelude::*;

use crate::{Fed, KermodesError, Tables, engine_error, store_error};

/// Where a session's stored tables are: weir, and the lake their history is in.
#[derive(Clone)]
pub(crate) struct Stores {
    pub(crate) tables: Tables,
    /// History's files, opened on first use (`Stores::lake`).
    pub(crate) lake: Arc<OnceLock<Arc<kermodes_lake::Lake>>>,
}

impl Stores {
    /// The lake: at `KERMODES_LAKE` (`s3://bucket/prefix` in the cluster), or in `lake` beside a
    /// store this process owns; otherwise held without its keys, reading each history file
    /// where weir says it is (architecture.md §8.4).
    pub(crate) fn lake(&self) -> PyResult<Arc<kermodes_lake::Lake>> {
        if let Some(lake) = self.lake.get() {
            return Ok(lake.clone());
        }
        let lake = match (std::env::var("KERMODES_LAKE").ok(), &self.tables) {
            (Some(url), _) => kermodes_lake::Lake::open(&url),
            (None, Tables::Local(store)) => {
                kermodes_lake::Lake::open(&store.root().join("lake").display().to_string())
            }
            (None, Tables::Remote(_)) => kermodes_lake::Lake::reader(),
        };
        let lake = Arc::new(lake.map_err(store_error)?);
        Ok(self.lake.get_or_init(|| lake).clone())
    }

    pub(crate) fn entry(&self, name: &str) -> PyResult<TableEntry> {
        let name = TableName::parse(name).map_err(store_error)?;
        self.tables.entry(&name).map_err(store_error)
    }
}

/// The rows of a stored table's ticking `scan`: all of them for which its predicate is true,
/// then every change, with at least its columns.
pub(crate) fn live_rows(
    engine: &kermodes_core::Session,
    stores: &Stores,
    scan: &Scan,
) -> PyResult<kermodes_core::Table> {
    let entry = stores.entry(&scan.name)?;
    if !entry.key.is_empty() {
        // A keyed table's rows are its keys' latest values, and an upsert can move a key out
        // of the predicate, which only a ticking `where` turns into the key's removal.
        let table = fed(engine, &stores.tables, &entry)?;
        return match &scan.predicate {
            Some(predicate) => table.filter(predicate).map_err(engine_error),
            None => Ok(table),
        };
    }
    let last = Date::from_days(Date::of_ns(now_ns()).days() - 1);
    let (schema, history) = history_batches(stores, &entry, scan, last)?;
    let door = Door::new(&entry, schema.clone(), scan.predicate.as_deref())?;
    let mut kept = Vec::new();
    for batch in history {
        if let Some(batch) = door.keep(batch).map_err(arrow_error)? {
            kept.push(batch);
        }
    }
    let today = fed_rows(engine, &stores.tables, &entry, door)?;
    if kept.is_empty() {
        return Ok(today);
    }
    let history = engine.static_table(schema, kept).map_err(engine_error)?;
    kermodes_core::Table::merge(&[&history, &today]).map_err(engine_error)
}

/// What of a stored append table's rows a live table takes: its scan's columns, and the rows its
/// predicate keeps, applied to each batch as it arrives (history's, then weir's), so rows the
/// predicate drops never enter the engine and no `where` keeps a copy of the rows it passes. An
/// append table's rows never change once written, so filtering them on arrival is the same as
/// filtering the table.
pub(crate) struct Door {
    /// The scan's columns, by position in the stored table's
    columns: Vec<usize>,
    schema: SchemaRef,
    /// Bound to `schema`
    predicate: Option<kermodes_core::Expression>,
}

impl Door {
    fn new(entry: &TableEntry, schema: SchemaRef, predicate: Option<&str>) -> PyResult<Self> {
        let predicate = predicate
            .map(|text| kermodes_core::Expression::predicate(text, &schema))
            .transpose()
            .map_err(engine_error)?;
        Ok(Self {
            columns: positions(&entry.schema, &schema)?,
            schema,
            predicate,
        })
    }

    /// A batch weir delivers, in the stored table's columns: the rows the live table takes, or
    /// `None` when it takes none.
    pub(crate) fn admit(&self, batch: &RecordBatch) -> Result<Option<RecordBatch>, ArrowError> {
        self.keep(project(batch, &self.schema, &self.columns)?)
    }

    /// A batch already in the scan's columns: the rows the predicate keeps (NULL as false).
    fn keep(&self, batch: RecordBatch) -> Result<Option<RecordBatch>, ArrowError> {
        let kept = match &self.predicate {
            None => batch,
            Some(predicate) => {
                let mask = predicate.evaluate(&batch)?;
                let mask = mask.as_boolean();
                // NULL counts as false; the kernel that says so wants a mask with NULLs.
                if mask.null_count() > 0 {
                    filter_record_batch(&batch, &prep_null_mask_filter(mask))?
                } else {
                    filter_record_batch(&batch, mask)?
                }
            }
        };
        Ok((kept.num_rows() > 0).then_some(kept))
    }
}

/// A stored table's static `scan` for the static executor: history's parts, each decoded on
/// whichever thread runs it, then today's rows, each part read again by column and row as the
/// executor asks. Today's are read first, from weir, and history up to the day before the day
/// they were stamped on, so a day that turns between the two reads is neither missed nor read
/// twice; both are read once, so a part read again is the same rows. A keyed table is its keys'
/// latest rows. The executor applies the scan's predicate.
pub(crate) fn source(stores: &Stores, scan: &Scan) -> PyResult<kermodes_core::exec::Source> {
    let entry = stores.entry(&scan.name)?;
    if !entry.key.is_empty() {
        let (_, state) = stores.tables.today(&entry, None).map_err(store_error)?;
        return Ok(kermodes_core::exec::Source {
            schema: entry.schema.clone(),
            parts: vec![in_memory(state)],
            reread: None,
            stats: None,
        });
    }
    let (day, today) = stores
        .tables
        .today(&entry, scan.limit)
        .map_err(store_error)?;
    let day = day.unwrap_or_else(|| Date::of_ns(now_ns()));
    let bounds = scan
        .predicate
        .as_deref()
        .map_or_else(Vec::new, kermodes_lake::column_bounds);
    let history = kermodes_lake::history_parts(
        stores.tables.weir(),
        &stores.lake()?,
        &entry,
        &kermodes_lake::Reading {
            last: Some(Date::from_days(day.days() - 1)),
            bounds: &bounds,
            columns: scan.columns.as_deref(),
            limit: scan.limit,
            ..kermodes_lake::Reading::default()
        },
    )
    .map_err(store_error)?;
    // History keeps each row's `Date`; readers get the table's columns.
    let kept: Arc<[usize]> = (0..history.schema.fields().len())
        .filter(|at| history.schema.field(*at).name() != kermodes_lake::DATE)
        .collect();
    let schema = Arc::new(history.schema.project(&kept).map_err(arrow_error)?);
    let columns = positions(&entry.schema, &schema)?;
    let today = today
        .iter()
        .map(|batch| project(batch, &schema, &columns).map_err(arrow_error))
        .collect::<PyResult<Vec<_>>>()?;
    let count = history.len() + usize::from(!today.is_empty());
    let history = Arc::new(history);
    let table: Arc<str> = entry.name.to_string().into();
    let failed = {
        let table = table.clone();
        move |e: &dyn std::fmt::Display| kermodes_core::Error::Source {
            table: table.to_string(),
            message: e.to_string(),
        }
    };
    let reread: kermodes_core::exec::Reread = {
        let (history, schema, kept, failed) =
            (history.clone(), schema.clone(), kept.clone(), failed.clone());
        Arc::new(move |part, columns, rows| {
            let failed = failed.clone();
            if part == history.len() {
                return rows_at(&today, &schema, columns, rows).map_err(|e| failed(&e));
            }
            let columns: Vec<usize> = columns.iter().map(|&at| kept[at]).collect();
            let batches = history.read(part, &columns, rows).map_err(|e| failed(&e))?;
            Ok(Box::new(
                batches.map(move |batch| batch.map_err(|e| failed(&e))),
            ))
        })
    };
    // History's parts know their files' statistics; today's rows have none.
    let stats: kermodes_core::exec::Stats = Arc::new(move |part, columns| {
        if part >= history.len() {
            return Ok(None);
        }
        let columns: Vec<usize> = columns.iter().map(|&at| kept[at]).collect();
        let known = history.stats(part, &columns).map_err(|e| failed(&e))?;
        Ok(known.map(|known| kermodes_core::exec::PartStats {
            rows: known.rows,
            whole: known.whole,
            columns: known
                .columns
                .into_iter()
                .map(|column| {
                    column.map(|c| kermodes_core::exec::ColumnStats {
                        min: c.min,
                        max: c.max,
                        nulls: c.nulls,
                    })
                })
                .collect(),
        }))
    });
    let all: Arc<[usize]> = (0..schema.fields().len()).collect();
    let parts = (0..count)
        .map(|part| {
            let (reread, all) = (reread.clone(), all.clone());
            Box::new(move || reread(part, &all, None)) as kermodes_core::exec::Part
        })
        .collect();
    Ok(kermodes_core::exec::Source {
        schema,
        parts,
        reread: Some(reread),
        stats: Some(stats),
    })
}

/// Batches already read, as a part.
pub(crate) fn in_memory(batches: Vec<RecordBatch>) -> kermodes_core::exec::Part {
    Box::new(move || Ok(Box::new(batches.into_iter().map(Ok)) as kermodes_core::exec::Batches))
}

/// Of batches of `schema` held in memory, the columns at `columns` (ascending), and only the
/// rows at `rows` (ascending, counted across the batches; all where `None`).
fn rows_at(
    batches: &[RecordBatch],
    schema: &Schema,
    columns: &[usize],
    rows: Option<&[u64]>,
) -> Result<kermodes_core::exec::Batches, arrow::error::ArrowError> {
    let schema = Arc::new(schema.project(columns)?);
    let mut read = Vec::with_capacity(batches.len());
    let mut start = 0;
    for batch in batches {
        let end = start + batch.num_rows() as u64;
        let kept = columns.iter().map(|&at| batch.column(at).clone());
        let (kept, count) = match rows {
            None => (kept.collect(), batch.num_rows()),
            Some(rows) => {
                let from = rows.partition_point(|&row| row < start);
                let to = rows.partition_point(|&row| row < end);
                let indices =
                    UInt64Array::from_iter_values(rows[from..to].iter().map(|row| row - start));
                let kept = kept
                    .map(|column| take(&column, &indices, None))
                    .collect::<Result<_, _>>()?;
                (kept, to - from)
            }
        };
        start = end;
        if count > 0 {
            let options = RecordBatchOptions::new().with_row_count(Some(count));
            read.push(Ok(RecordBatch::try_new_with_options(
                schema.clone(),
                kept,
                &options,
            )?));
        }
    }
    Ok(Box::new(read.into_iter()))
}

/// What `rows(scan)` would read, and whether it ticks.
pub(crate) fn describe(stores: &Stores, scan: &Scan) -> PyResult<(String, bool)> {
    let entry = stores.entry(&scan.name)?;
    let then = if scan.live {
        ", then ticking from weir"
    } else {
        ""
    };
    if !entry.key.is_empty() {
        return Ok((format!("its keys' latest rows{then}"), scan.live));
    }
    let last = Date::from_days(Date::of_ns(now_ns()).days() - 1);
    let bounds = scan
        .predicate
        .as_deref()
        .map_or_else(Vec::new, kermodes_lake::column_bounds);
    let lake = stores.lake()?;
    let explain = kermodes_lake::explain_history(
        stores.tables.weir(),
        &lake,
        &entry.name,
        &kermodes_lake::Reading {
            last: Some(last),
            bounds: &bounds,
            columns: scan.columns.as_deref(),
            ..kermodes_lake::Reading::default()
        },
    )
    .map_err(store_error)?;
    Ok((
        format!("history {explain}; today from weir{then}"),
        scan.live,
    ))
}

/// A table shaped as `scan`'s rows, with none: static, or ticking as they would.
pub(crate) fn shadow(
    engine: &kermodes_core::Session,
    stores: &Stores,
    scan: &Scan,
) -> PyResult<kermodes_core::Table> {
    let entry = stores.entry(&scan.name)?;
    let schema = entry.schema.as_ref();
    let key: Vec<&str> = entry.key.iter().map(String::as_str).collect();
    match (scan.live, key.is_empty()) {
        (false, _) => engine.static_table(entry.schema.clone(), Vec::new()),
        (true, true) => engine.input_table(schema),
        (true, false) => engine.keyed_input_table(schema, &key),
    }
    .map_err(engine_error)
}

/// History up to `last`, read now, without `Date`: the start of a ticking scan's rows, in the
/// scan's columns (all the predicate reads among them).
fn history_batches(
    stores: &Stores,
    entry: &TableEntry,
    scan: &Scan,
    last: Date,
) -> PyResult<(SchemaRef, Vec<RecordBatch>)> {
    let bounds = scan
        .predicate
        .as_deref()
        .map_or_else(Vec::new, kermodes_lake::column_bounds);
    let lake = stores.lake()?;
    let history = kermodes_lake::read_history(
        stores.tables.weir(),
        &lake,
        &entry.name,
        &kermodes_lake::Reading {
            last: Some(last),
            bounds: &bounds,
            columns: scan.columns.as_deref(),
            limit: scan.limit,
            ..kermodes_lake::Reading::default()
        },
    )
    .map_err(store_error)?;
    let kept: Vec<usize> = (0..history.schema.fields().len())
        .filter(|at| history.schema.field(*at).name() != kermodes_lake::DATE)
        .collect();
    let schema = Arc::new(history.schema.project(&kept).map_err(arrow_error)?);
    let batches = history
        .batches
        .iter()
        .map(|batch| project(batch, &schema, &kept).map_err(arrow_error))
        .collect::<PyResult<Vec<_>>>()?;
    Ok((schema, batches))
}

/// Where each of `wanted`'s columns is in `schema`, by name.
fn positions(schema: &Schema, wanted: &Schema) -> PyResult<Vec<usize>> {
    wanted
        .fields()
        .iter()
        .map(|f| schema.index_of(f.name()).map_err(arrow_error))
        .collect()
}

/// `batch`'s columns at `columns`, as `schema`, keeping its row count when it keeps no column
/// (`count(*)` reads none).
fn project(
    batch: &RecordBatch,
    schema: &SchemaRef,
    columns: &[usize],
) -> Result<RecordBatch, arrow::error::ArrowError> {
    let kept = columns.iter().map(|at| batch.column(*at).clone()).collect();
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(schema.clone(), kept, &options)
}

/// The keyed stored table, ticking from weir: each key's latest row, then each change as it is
/// written.
fn fed(
    engine: &kermodes_core::Session,
    tables: &Tables,
    entry: &TableEntry,
) -> PyResult<kermodes_core::Table> {
    let key: Vec<&str> = entry.key.iter().map(String::as_str).collect();
    let (table, feed) = engine
        .fed_keyed_table(&entry.schema, &key, &format!("live {}", entry.name))
        .map_err(engine_error)?;
    tables
        .feed(&entry.name, feed, Fed::State { keyed: true })
        .map_err(store_error)?;
    Ok(table)
}

/// The append stored table, ticking from weir: today's rows the door admits, then each batch
/// written, admitted as it arrives.
fn fed_rows(
    engine: &kermodes_core::Session,
    tables: &Tables,
    entry: &TableEntry,
    door: Door,
) -> PyResult<kermodes_core::Table> {
    let (table, feed) = engine
        .fed_table(&door.schema, &format!("live {}", entry.name))
        .map_err(engine_error)?;
    tables
        .feed(&entry.name, feed, Fed::Rows(door))
        .map_err(store_error)?;
    Ok(table)
}

fn arrow_error(error: arrow::error::ArrowError) -> PyErr {
    KermodesError::new_err(error.to_string())
}

pub(crate) fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as i64)
}
