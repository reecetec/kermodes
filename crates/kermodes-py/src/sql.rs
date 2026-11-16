//! `Session.sql` and `Session.explain` (architecture.md §9.8): the tables a query's names read.

use std::collections::HashMap;

use kermodes_core::sql::{Scan, parse};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::{Session, Table, engine_error};

/// The table a query's name reads.
enum Source<'a> {
    /// A table passed by keyword.
    Passed(&'a kermodes_core::Table),
    /// `live.ns.table`: today, ticking.
    Live(String),
    /// `hist.ns.table`: the merged dates.
    Hist(String),
    /// `ns.table`: the dates its `WHERE` says, across history and today.
    Stored(String),
}

fn source<'a>(
    scan: &Scan,
    passed: &'a HashMap<String, kermodes_core::Table>,
) -> PyResult<Source<'a>> {
    let parts: Vec<&str> = scan.name.split('.').collect();
    Ok(match parts.as_slice() {
        [name] => Source::Passed(passed.get(*name).ok_or_else(|| {
            PyValueError::new_err(format!(
                "no table {name}: name a stored table (ns.table) or pass it, as in \
                 db.sql(query, {name}=table)"
            ))
        })?),
        ["live", name @ ..] => Source::Live(name.join(".")),
        ["hist", name @ ..] => Source::Hist(name.join(".")),
        _ => Source::Stored(scan.name.clone()),
    })
}

/// A stored table, refusing an append table's scan that does not say which dates.
fn stored(session: &Session, name: &str, scan: &Scan) -> PyResult<crate::stored::StoredTable> {
    let table = session.stored_table(name)?;
    let dated = scan
        .predicate
        .as_deref()
        .is_some_and(|p| kermodes_lake::date_bounds(p).0.is_some());
    if table.entry.key.is_empty() && !dated {
        return Err(PyValueError::new_err(format!(
            "{name}'s rows span its whole history: say which dates in WHERE (Date >= \
             '2026-11-01'), or read live.{name} (today) or hist.{name} (the merged dates)"
        )));
    }
    Ok(table)
}

/// The rows of `scan`: its table's rows for which its predicate is true.
fn rows(
    session: &Session,
    scan: &Scan,
    passed: &HashMap<String, kermodes_core::Table>,
) -> PyResult<kermodes_core::Table> {
    let predicate = scan.predicate.as_deref();
    let filtered = |table: kermodes_core::Table| match predicate {
        Some(predicate) => table.filter(predicate).map_err(engine_error),
        None => Ok(table),
    };
    match source(scan, passed)? {
        Source::Passed(table) => filtered(table.clone()),
        Source::Live(name) => filtered(session.live(&name)?),
        Source::Hist(name) => session.historical(&name)?.rows(predicate),
        Source::Stored(name) => stored(session, &name, scan)?.rows(predicate),
    }
}

/// What `rows(scan)` would read, and whether it ticks.
fn describe(
    session: &Session,
    scan: &Scan,
    passed: &HashMap<String, kermodes_core::Table>,
) -> PyResult<(String, bool)> {
    match source(scan, passed)? {
        Source::Passed(table) => Ok(("passed in".to_owned(), table.is_refreshing())),
        Source::Live(name) => {
            // Only to say a table that does not exist does not
            session.stored_table(&name)?;
            Ok((format!("{name} today, ticking from weir"), true))
        }
        Source::Hist(name) => Ok((
            session
                .historical(&name)?
                .describe(scan.predicate.as_deref())?,
            false,
        )),
        Source::Stored(name) => stored(session, &name, scan)?.describe(scan.predicate.as_deref()),
    }
}

/// The tables passed by keyword, by name.
fn passed(tables: Option<&Bound<'_, PyDict>>) -> PyResult<HashMap<String, kermodes_core::Table>> {
    let mut passed = HashMap::new();
    for (name, table) in tables.into_iter().flat_map(|tables| tables.iter()) {
        let table = table.extract::<PyRef<'_, Table>>()?;
        passed.insert(name.extract::<String>()?, table.0.clone());
    }
    Ok(passed)
}

/// The engine's error for a scan that failed; the scan's own `PyErr` is kept aside and raised
/// in its place, so a `ValueError` stays one.
fn set_aside(failed: &mut Option<PyErr>, error: PyErr) -> kermodes_core::Error {
    *failed = Some(error);
    kermodes_core::Error::Unsupported("a table the query reads failed".to_owned())
}

/// `Session.sql`.
pub(crate) fn query(
    session: &Session,
    py: Python<'_>,
    query: &str,
    tables: Option<&Bound<'_, PyDict>>,
) -> PyResult<kermodes_core::Table> {
    let passed = passed(tables)?;
    let parsed = parse(query).map_err(engine_error)?;
    if parsed.explain {
        return Err(PyValueError::new_err(
            "db.explain(query) explains a query; db.sql runs it",
        ));
    }
    py.detach(|| {
        let mut failed = None;
        let built = parsed
            .plan
            .build(&mut |scan| rows(session, scan, &passed).map_err(|e| set_aside(&mut failed, e)));
        match failed {
            Some(error) => Err(error),
            None => built.map_err(engine_error),
        }
    })
}

/// `Session.explain`.
pub(crate) fn explain(
    session: &Session,
    py: Python<'_>,
    query: &str,
    tables: Option<&Bound<'_, PyDict>>,
) -> PyResult<String> {
    let passed = passed(tables)?;
    let parsed = parse(query).map_err(engine_error)?;
    py.detach(|| {
        let mut failed = None;
        let explained = parsed.plan.explain(&mut |scan| {
            describe(session, scan, &passed).map_err(|e| set_aside(&mut failed, e))
        });
        match failed {
            Some(error) => Err(error),
            None => explained.map_err(engine_error),
        }
    })
}
