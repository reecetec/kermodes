//! A plan's scans of stored tables (architecture.md §5.3): all of a table's rows, the lake's
//! files and weir's tail as one, read now (static) or then followed (ticking).

use std::sync::{Arc, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{RecordBatch, RecordBatchOptions};
use arrow::datatypes::{Schema, SchemaRef};
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

/// The rows of a stored table's `scan`: all of them for which its predicate is true, with at
/// least its columns, and at least its first `limit` rows where it has one.
pub(crate) fn rows(
    engine: &kermodes_core::Session,
    stores: &Stores,
    scan: &Scan,
) -> PyResult<kermodes_core::Table> {
    let entry = stores.entry(&scan.name)?;
    let table = if !entry.key.is_empty() {
        // A keyed table's rows are its keys' latest values; its history is its log.
        if scan.live {
            fed(engine, &stores.tables, &entry)?
        } else {
            let (_, state) = stores.tables.today(&entry, None).map_err(store_error)?;
            engine
                .static_table(entry.schema.clone(), state)
                .map_err(engine_error)?
        }
    } else if scan.live {
        let last = Date::from_days(Date::of_ns(now_ns()).days() - 1);
        let history = history(engine, stores, &entry, scan, last)?;
        let today = fed(engine, &stores.tables, &entry)?;
        let names: Vec<&str> = history
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        let today = today.select(&names).map_err(engine_error)?;
        kermodes_core::Table::merge(&[&history, &today]).map_err(engine_error)?
    } else {
        let (day, today) = stores
            .tables
            .today(&entry, scan.limit)
            .map_err(store_error)?;
        let day = day.unwrap_or_else(|| Date::of_ns(now_ns()));
        let history = read(stores, &entry, scan, Date::from_days(day.days() - 1))?;
        let mut batches = history.1;
        let enough = scan
            .limit
            .is_some_and(|limit| batches.iter().map(RecordBatch::num_rows).sum::<usize>() >= limit);
        if !enough {
            let columns = positions(&entry.schema, &history.0)?;
            for batch in today {
                batches.push(project(&batch, &history.0, &columns)?);
            }
        }
        engine
            .static_table(history.0, batches)
            .map_err(engine_error)?
    };
    match &scan.predicate {
        Some(predicate) => table.filter(predicate).map_err(engine_error),
        None => Ok(table),
    }
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

/// History up to `last`, read now, as a static table without `Date`.
fn history(
    engine: &kermodes_core::Session,
    stores: &Stores,
    entry: &TableEntry,
    scan: &Scan,
    last: Date,
) -> PyResult<kermodes_core::Table> {
    let (schema, batches) = read(stores, entry, scan, last)?;
    engine.static_table(schema, batches).map_err(engine_error)
}

/// History up to `last`: only the row groups and pages the scan's predicate allows, its
/// columns, and its first rows where it has a limit; without `Date`, which history keeps.
fn read(
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
        .map(|batch| project(batch, &schema, &kept))
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
fn project(batch: &RecordBatch, schema: &SchemaRef, columns: &[usize]) -> PyResult<RecordBatch> {
    let kept = columns.iter().map(|at| batch.column(*at).clone()).collect();
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(schema.clone(), kept, &options).map_err(arrow_error)
}

/// The stored table, ticking from weir: today's rows, then each change as it is written; a
/// keyed table's latest row per key.
fn fed(
    engine: &kermodes_core::Session,
    tables: &Tables,
    entry: &TableEntry,
) -> PyResult<kermodes_core::Table> {
    let description = format!("live {}", entry.name);
    let key: Vec<&str> = entry.key.iter().map(String::as_str).collect();
    let (table, feed) = if key.is_empty() {
        engine.fed_table(&entry.schema, &description)
    } else {
        engine.fed_keyed_table(&entry.schema, &key, &description)
    }
    .map_err(engine_error)?;
    tables
        .feed(
            &entry.name,
            feed,
            Fed::State {
                keyed: !key.is_empty(),
            },
        )
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
