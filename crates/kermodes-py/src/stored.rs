//! `Session.table`: a stored table across history and today (architecture.md §8.4).

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use kermodes_weir::{Date, TableEntry, TableName};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::{Fed, KermodesError, Table, Tables, engine_error, store_error};

/// A stored table across its history and today (`Session.table`): nothing is read until `where`
/// says which dates.
#[pyclass(module = "kermodes", name = "StoredTable", frozen)]
pub struct StoredTable {
    pub(crate) engine: kermodes_core::Session,
    pub(crate) tables: Tables,
    /// The history store, or why there is none: needed only for dates before today.
    pub(crate) lake: Result<Arc<kermodes_lake::Lake>, String>,
    pub(crate) entry: TableEntry,
}

#[pymethods]
impl StoredTable {
    /// Rows for which `predicate` is true. An append table's predicate needs a first date
    /// (`Date >= '2026-11-01'`, `Date = '2026-11-12'`): dates before today are read once from
    /// history, only their files and row groups; today on, if the dates reach it, tick from weir,
    /// each row dated by its arrival. Which day is today is fixed here, so no day is read twice
    /// or missed. A keyed table is its latest state, ticking.
    #[pyo3(name = "where")]
    fn where_(&self, py: Python<'_>, predicate: &str) -> PyResult<Table> {
        let name = &self.entry.name;
        if !self.entry.key.is_empty() {
            return py
                .detach(|| {
                    self.live(name, Fed::State, &self.entry.schema)?
                        .filter(predicate)
                        .map_err(engine_error)
                })
                .map(Table);
        }
        let (first, last) = kermodes_lake::date_bounds(predicate);
        let Some(first) = first else {
            return Err(PyValueError::new_err(format!(
                "{name}'s rows span its whole history: say which dates, as in \
                 where(\"Date >= '2026-11-01' && ...\")"
            )));
        };
        let today = Date::of_ns(now_ns());
        py.detach(|| {
            let mut parts = Vec::new();
            if first < today {
                let yesterday = Date::from_days(today.days() - 1);
                let before = last.map_or(yesterday, |last| last.min(yesterday));
                parts.push(
                    self.history(first, before, predicate)?
                        .filter(predicate)
                        .map_err(engine_error)?,
                );
            }
            if last.is_none_or(|last| last >= today) {
                let schema = kermodes_lake::with_date_column(&self.entry.schema);
                parts.push(
                    self.live(name, Fed::Dated(schema.clone()), &schema)?
                        .filter(predicate)
                        .map_err(engine_error)?,
                );
            }
            match parts.as_slice() {
                [only] => Ok(only.clone()),
                parts => kermodes_core::Table::merge(&parts.iter().collect::<Vec<_>>())
                    .map_err(engine_error),
            }
        })
        .map(Table)
    }

    /// What `where(predicate)` would read of history (files, row groups, and bytes, from the
    /// catalog and the files' footers alone), and whether today follows from weir.
    fn explain(&self, py: Python<'_>, predicate: &str) -> PyResult<String> {
        let (first, last) = kermodes_lake::date_bounds(predicate);
        let today = Date::of_ns(now_ns());
        let name = &self.entry.name;
        let mut lines = Vec::new();
        if let Some(first) = first.filter(|&first| first < today) {
            let yesterday = Date::from_days(today.days() - 1);
            let before = last.map_or(yesterday, |last| last.min(yesterday));
            let explain = py.detach(|| {
                kermodes_lake::explain_history(
                    self.tables.weir(),
                    self.lake()?.as_ref(),
                    name,
                    Some(first),
                    Some(before),
                    &kermodes_lake::column_bounds(predicate),
                )
                .map_err(store_error)
            })?;
            lines.push(format!("{name} {first} to {before}: {explain}"));
        }
        if last.is_none_or(|last| last >= today) {
            lines.push(format!("{name} {today} on: ticking from weir"));
        }
        Ok(lines.join("\n"))
    }
}

impl StoredTable {
    fn lake(&self) -> PyResult<Arc<kermodes_lake::Lake>> {
        self.lake.clone().map_err(KermodesError::new_err)
    }

    /// `first` to `last` of history, read now: only the row groups and pages `predicate`'s
    /// bounds allow.
    fn history(&self, first: Date, last: Date, predicate: &str) -> PyResult<kermodes_core::Table> {
        let lake = self.lake()?;
        let history = kermodes_lake::read_history(
            self.tables.weir(),
            &lake,
            &self.entry.name,
            Some(first),
            Some(last),
            &kermodes_lake::column_bounds(predicate),
        )
        .map_err(store_error)?;
        self.engine
            .static_table(history.schema, history.batches)
            .map_err(engine_error)
    }

    /// Today on, ticking: a fed table of `schema` that takes the stored table's changes as
    /// `taking` says.
    fn live(
        &self,
        name: &TableName,
        taking: Fed,
        schema: &arrow::datatypes::SchemaRef,
    ) -> PyResult<kermodes_core::Table> {
        let description = format!("live {name}");
        let key: Vec<&str> = self.entry.key.iter().map(String::as_str).collect();
        let (table, feed) = if key.is_empty() {
            self.engine.fed_table(schema, &description)
        } else {
            self.engine.fed_keyed_table(schema, &key, &description)
        }
        .map_err(engine_error)?;
        self.tables.feed(name, feed, taking).map_err(store_error)?;
        Ok(table)
    }
}

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as i64)
}
