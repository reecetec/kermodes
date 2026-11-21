//! `Table` (architecture.md §5.3): an engine table, built (static, or ticking with the session's
//! cycles), or a plan over stored tables, built when something needs its rows. Operators on a
//! planned table extend its plan, so its filters and the columns it uses reach the files; the
//! same plan built over empty tables checks each operator when it is called.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::{ArrayIterator, ArrayRef, StructArray};
use arrow::datatypes::Field;
use kermodes_core::SortKey;
use kermodes_core::plan::{Plan, Scan};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyDict};
use pyo3_arrow::ffi::to_stream_pycapsule;
use pyo3_arrow::input::AnyRecordBatch;

use crate::stored::{self, Stores};
use crate::{
    KermodesError, Key, PREVIEW_ROWS, engine_error, parse_period, preview_text, read_batches,
    row_count,
};

/// Rows a notebook shows of a static table that is a plan: its first rows, read for the view.
const DISPLAY_ROWS: usize = 10_000;

/// A table: static, or ticking with the session's cycles. Built in memory (`input_table`,
/// `static_table`, operators on those), or a plan over stored tables (`Session.table`,
/// `Session.sql`) built when something needs its rows: a static one each time, as the data is
/// then; a ticking one once, then kept. It lives while Python refers to it or a live table is
/// computed from it; weak references (the notebook view's) don't count.
///
/// Every method that reaches the session releases the interpreter first. A cycle can free rows
/// that Python owns (inputs are imported without copying), which takes the interpreter while
/// the cycle holds the session; waiting for the session while holding the interpreter would
/// deadlock.
#[pyclass(module = "kermodes", name = "Table", frozen, weakref)]
pub struct Table(Inner);

enum Inner {
    Built(kermodes_core::Table),
    Planned(Box<Planned>),
}

/// Operators over tables, stored or in memory, not built yet.
pub(crate) struct Planned {
    plan: Plan,
    engine: kermodes_core::Session,
    /// Where its stored tables are; `None` when it reads only tables in memory.
    stores: Option<Stores>,
    /// The tables in memory it reads, by the names its scans give them.
    passed: HashMap<String, kermodes_core::Table>,
    /// The plan built over empty tables shaped as its scans' rows: its columns, whether it
    /// ticks, and the engine's checks of every operator, at once and reading nothing.
    shadow: kermodes_core::Table,
    /// A ticking plan's table, built on first use and kept: a live table is one table, however
    /// often it is looked at.
    ticking: Mutex<Option<kermodes_core::Table>>,
}

/// A plan's parts, for joining plans and tables into one.
struct Parts {
    plan: Plan,
    stores: Option<Stores>,
    passed: HashMap<String, kermodes_core::Table>,
    shadow: kermodes_core::Table,
}

impl Table {
    pub(crate) fn built(table: kermodes_core::Table) -> Self {
        Table(Inner::Built(table))
    }

    /// The engine table: this one, or its plan built now (a ticking plan's, once).
    pub(crate) fn table(&self, py: Python<'_>) -> PyResult<kermodes_core::Table> {
        match &self.0 {
            Inner::Built(table) => Ok(table.clone()),
            Inner::Planned(planned) => py.detach(|| planned.table()),
        }
    }

    fn schema(&self) -> &arrow::datatypes::SchemaRef {
        match &self.0 {
            Inner::Built(table) => table.schema(),
            Inner::Planned(planned) => planned.shadow.schema(),
        }
    }

    /// The table `op` makes of this one; a plan's `node` over its plan, checked by `op` over its
    /// shadow.
    fn derive(
        &self,
        py: Python<'_>,
        node: impl FnOnce(Box<Plan>) -> Plan,
        op: impl FnOnce(&kermodes_core::Table) -> Result<kermodes_core::Table, kermodes_core::Error>
        + Send,
    ) -> PyResult<Table> {
        match &self.0 {
            Inner::Built(table) => {
                let table = py.detach(|| op(table)).map_err(engine_error)?;
                Ok(Table::built(table))
            }
            Inner::Planned(planned) => {
                let shadow = py.detach(|| op(&planned.shadow)).map_err(engine_error)?;
                Ok(Table(Inner::Planned(Box::new(Planned {
                    plan: node(Box::new(planned.plan.clone())),
                    engine: planned.engine.clone(),
                    stores: planned.stores.clone(),
                    passed: planned.passed.clone(),
                    shadow,
                    ticking: Mutex::new(None),
                }))))
            }
        }
    }

    /// The table `op` makes of these: built if they all are, else a plan of `node` over theirs.
    fn combine(
        py: Python<'_>,
        tables: &[&Table],
        node: impl FnOnce(Vec<Plan>) -> Plan,
        op: impl FnOnce(&[&kermodes_core::Table]) -> Result<kermodes_core::Table, kermodes_core::Error>
        + Send,
    ) -> PyResult<Table> {
        let built: Option<Vec<&kermodes_core::Table>> = tables
            .iter()
            .map(|table| match &table.0 {
                Inner::Built(table) => Some(table),
                Inner::Planned(_) => None,
            })
            .collect();
        if let Some(built) = built {
            let table = py.detach(|| op(&built)).map_err(engine_error)?;
            return Ok(Table::built(table));
        }
        let engine = tables
            .iter()
            .find_map(|table| match &table.0 {
                Inner::Planned(planned) => Some(planned.engine.clone()),
                Inner::Built(_) => None,
            })
            .expect("one is a plan");
        let parts = py.detach(|| {
            tables
                .iter()
                .map(|table| table.parts(&engine))
                .collect::<PyResult<Vec<_>>>()
        })?;
        let shadows: Vec<&kermodes_core::Table> = parts.iter().map(|p| &p.shadow).collect();
        let shadow = py.detach(|| op(&shadows)).map_err(engine_error)?;
        let (mut plans, mut stores, mut passed) = (Vec::new(), None, HashMap::new());
        for part in parts {
            plans.push(part.plan);
            stores = stores.or(part.stores);
            passed.extend(part.passed);
        }
        Ok(Table(Inner::Planned(Box::new(Planned {
            plan: node(plans),
            engine,
            stores,
            passed,
            shadow,
            ticking: Mutex::new(None),
        }))))
    }

    /// This table as part of a plan: its own, or a scan of it in memory.
    fn parts(&self, engine: &kermodes_core::Session) -> PyResult<Parts> {
        Ok(match &self.0 {
            Inner::Planned(planned) => Parts {
                plan: planned.plan.clone(),
                stores: planned.stores.clone(),
                passed: planned.passed.clone(),
                shadow: planned.shadow.clone(),
            },
            Inner::Built(table) => {
                let name = passed_name();
                Parts {
                    plan: Plan::Scan(scan(&name)),
                    stores: None,
                    shadow: empty_like(engine, table)?,
                    passed: HashMap::from([(name, table.clone())]),
                }
            }
        })
    }
}

#[pymethods]
impl Table {
    /// Column names and type names.
    #[getter]
    fn columns(&self) -> Vec<(String, String)> {
        self.schema()
            .fields()
            .iter()
            .map(|f| (f.name().clone(), kermodes_core::type_name(f.data_type())))
            .collect()
    }

    #[getter]
    fn is_refreshing(&self, py: Python<'_>) -> bool {
        match &self.0 {
            Inner::Built(table) => py.detach(|| table.is_refreshing()),
            Inner::Planned(planned) => py.detach(|| planned.shadow.is_refreshing()),
        }
    }

    /// Queues rows for the next cycle, from any Arrow table or record batch.
    fn append(&self, py: Python<'_>, data: AnyRecordBatch) -> PyResult<()> {
        let (_, batches) = read_batches(data)?;
        let table = self.input()?;
        py.detach(|| table.append(&batches)).map_err(engine_error)
    }

    /// Queues rows that replace the row with the same key, or add one. Keyed tables only.
    fn upsert(&self, py: Python<'_>, data: AnyRecordBatch) -> PyResult<()> {
        let (_, batches) = read_batches(data)?;
        let table = self.input()?;
        py.detach(|| table.upsert(&batches)).map_err(engine_error)
    }

    /// Queues removal of the rows with these keys; `data` holds only the key columns.
    fn delete(&self, py: Python<'_>, data: AnyRecordBatch) -> PyResult<()> {
        let (_, batches) = read_batches(data)?;
        let table = self.input()?;
        py.detach(|| table.delete(&batches)).map_err(engine_error)
    }

    /// Rows for which `predicate` is true; NULL counts as false.
    #[pyo3(name = "where")]
    fn where_(&self, py: Python<'_>, predicate: &str) -> PyResult<Table> {
        self.derive(
            py,
            |input| Plan::Filter {
                input,
                predicate: predicate.to_owned(),
            },
            |table| table.filter(predicate),
        )
    }

    /// Only the listed columns, in that order: each an existing column's name or
    /// `name = expression`, as in `update`.
    #[pyo3(signature = (*columns))]
    fn select(&self, py: Python<'_>, columns: Vec<String>) -> PyResult<Table> {
        let names: Vec<&str> = columns.iter().map(String::as_str).collect();
        self.derive(
            py,
            |input| Plan::Select {
                input,
                columns: columns.clone(),
            },
            |table| table.select(&names),
        )
    }

    /// Adds or replaces columns, each given as `name = expression`.
    #[pyo3(signature = (*assignments))]
    fn update(&self, py: Python<'_>, assignments: Vec<String>) -> PyResult<Table> {
        let texts: Vec<&str> = assignments.iter().map(String::as_str).collect();
        self.derive(
            py,
            |input| Plan::Update {
                input,
                assignments: assignments.clone(),
            },
            |table| table.update(&texts),
        )
    }

    /// The last row, in arrival order, for each value of `columns`.
    #[pyo3(signature = (*columns))]
    fn last_by(&self, py: Python<'_>, columns: Vec<String>) -> PyResult<Table> {
        let names: Vec<&str> = columns.iter().map(String::as_str).collect();
        self.derive(
            py,
            |input| Plan::LastBy {
                input,
                by: columns.clone(),
            },
            |table| table.last_by(&names),
        )
    }

    /// The first `n` rows in key order (arrival order for appended rows), kept current.
    fn head(&self, py: Python<'_>, n: usize) -> PyResult<Table> {
        self.derive(
            py,
            |input| Plan::Head { input, rows: n },
            |table| table.head(n),
        )
    }

    /// The last `n` rows in key order (the newest, for appended rows), kept current.
    fn tail(&self, py: Python<'_>, n: usize) -> PyResult<Table> {
        self.derive(
            py,
            |input| Plan::Tail { input, rows: n },
            |table| table.tail(n),
        )
    }

    /// The rows at positions `start..stop` in reading order (key order, or a ticking sort's), as
    /// pandas `iloc[start:stop]`, kept current as rows come and go anywhere.
    fn slice(&self, py: Python<'_>, start: usize, stop: usize) -> PyResult<Table> {
        self.derive(
            py,
            |input| Plan::Slice {
                input,
                start,
                stop: Some(stop),
            },
            |table| table.slice(start, stop),
        )
    }

    /// Rows ordered by `by`, each `"name"` or `"name desc"`, earlier entries first. NULL sorts as
    /// the largest value; ties keep the input's order. A ticking table's sort keeps its rows in
    /// that order for reading; nothing but `slice` can be built on it.
    #[pyo3(signature = (*by))]
    fn sort(&self, py: Python<'_>, by: Vec<String>) -> PyResult<Table> {
        let keys: Vec<SortKey> = by.iter().map(|entry| SortKey::parse(entry)).collect();
        self.derive(
            py,
            |input| Plan::Sort {
                input,
                by: keys.clone(),
            },
            |table| table.sort_by(&keys),
        )
    }

    /// Each row with columns from the `right` row whose `on` columns hold the same values, or
    /// NULLs; a key with more than one right row fails the table. `on` and `joins` entries are
    /// `"name"` or `"left=right"` (`"new=old"` for `joins`); `joins` defaults to every right
    /// column not in `on`.
    #[pyo3(signature = (right, on, joins=None))]
    fn natural_join(
        &self,
        py: Python<'_>,
        right: &Table,
        on: Key,
        joins: Option<Vec<String>>,
    ) -> PyResult<Table> {
        let on = on.names();
        let (keys, added) = (names(&on), joins.as_deref().map(names));
        Table::combine(
            py,
            &[self, right],
            |mut plans| {
                let right = plans.pop().expect("two");
                let left = plans.pop().expect("two");
                Plan::Join {
                    left: Box::new(left),
                    right: Box::new(right),
                    on: on.clone(),
                    at: None,
                    columns: joins.clone(),
                    bare: Vec::new(),
                }
            },
            |tables| tables[0].natural_join(tables[1], &keys, added.as_deref()),
        )
    }

    /// Each row with columns from the `right` row whose `on` columns match and whose `at` time
    /// is the latest not after this row's (the last to arrive among equal times), or NULLs.
    /// `right` must only gain rows. `at` is `"name"` or `"left=right"`; `joins` defaults to
    /// every right column not in `on` or `at`.
    #[pyo3(signature = (right, on, at, joins=None))]
    fn as_of_join(
        &self,
        py: Python<'_>,
        right: &Table,
        on: Key,
        at: &str,
        joins: Option<Vec<String>>,
    ) -> PyResult<Table> {
        let on = on.names();
        let (keys, added) = (names(&on), joins.as_deref().map(names));
        Table::combine(
            py,
            &[self, right],
            |mut plans| {
                let right = plans.pop().expect("two");
                let left = plans.pop().expect("two");
                Plan::Join {
                    left: Box::new(left),
                    right: Box::new(right),
                    on: on.clone(),
                    at: Some(at.to_owned()),
                    columns: joins.clone(),
                    bare: Vec::new(),
                }
            },
            |tables| tables[0].as_of_join(tables[1], &keys, at, added.as_deref()),
        )
    }

    /// One row per value of `columns`, with each `name="function(column)"` aggregation.
    #[pyo3(signature = (*columns, **aggregations))]
    fn agg_by(
        &self,
        py: Python<'_>,
        columns: Vec<String>,
        aggregations: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Table> {
        let specs = specs(aggregations)?;
        let (by, pairs) = (names(&columns), pairs(&specs));
        self.derive(
            py,
            |input| Plan::Agg {
                input,
                by: columns.clone(),
                aggregations: specs.clone(),
                aliases: Vec::new(),
            },
            |table| table.agg_by(&by, &pairs),
        )
    }

    /// Each row with aggregates over a backward window of its group's rows (`by`; one group
    /// without): the row and the `rows - 1` before it, or the rows whose `at` lies within
    /// `time` before its own (`time` a period such as `"5m"` for timestamps, or a number in
    /// `at`'s units). Each `name="function(column)"`: count, sum, mean, std, min, or max. The
    /// table must only append rows.
    #[pyo3(signature = (*by, rows=None, time=None, at=None, **aggregations))]
    fn rolling(
        &self,
        py: Python<'_>,
        by: Vec<String>,
        rows: Option<usize>,
        time: Option<Span>,
        at: Option<String>,
        aggregations: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Table> {
        let window = match (rows, time, at) {
            (Some(n), None, None) => kermodes_core::Window::Rows(n),
            (None, Some(time), Some(at)) => kermodes_core::Window::Time {
                at,
                span: match time {
                    Span::Period(text) => i64::try_from(parse_period(&text)?.as_nanos())
                        .map_err(|_| PyValueError::new_err("rolling time span is too long"))?,
                    Span::Units(units) => units,
                },
            },
            _ => {
                return Err(PyValueError::new_err(
                    "rolling takes rows=n, or time= with at= naming the time column",
                ));
            }
        };
        let specs = specs(aggregations)?;
        let (columns, pairs) = (names(&by), pairs(&specs));
        self.derive(
            py,
            |input| Plan::Rolling {
                input,
                by: by.clone(),
                window: window.clone(),
                aggregations: specs.clone(),
            },
            |table| table.rolling(&columns, &window, &pairs),
        )
    }

    /// How the table is made: a plan's operators, each static or ticking, and what each stored
    /// table's read would read of its files, from the catalog and the files' footers alone.
    fn explain(&self, py: Python<'_>) -> PyResult<String> {
        match &self.0 {
            Inner::Built(table) => Ok(format!("in memory: {}", py.detach(|| table.description()))),
            Inner::Planned(planned) => py.detach(|| planned.explain()),
        }
    }

    fn __len__(&self, py: Python<'_>) -> PyResult<usize> {
        let table = self.table(py)?;
        py.detach(|| table.num_rows()).map_err(engine_error)
    }

    /// Exports a snapshot of the current rows through the Arrow PyCapsule interface.
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyCapsule>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let table = self.table(py)?;
        let batches = py.detach(|| table.snapshot()).map_err(engine_error)?;
        let schema = table.schema();
        let field = Field::new_struct("", schema.fields().clone(), false)
            .with_metadata(schema.metadata().clone());
        let arrays = batches
            .into_iter()
            .map(|batch| Ok(Arc::new(StructArray::from(batch)) as ArrayRef));
        let reader = Box::new(ArrayIterator::new(arrays, Arc::new(field)));
        Ok(to_stream_pycapsule(py, reader, requested_schema)?)
    }

    /// The cycle that last changed this table; live notebook views poll it.
    #[getter]
    fn _changed(&self, py: Python<'_>) -> u64 {
        let table = match &self.0 {
            Inner::Built(table) => Some(table.clone()),
            Inner::Planned(planned) => planned.kept(),
        };
        table.map_or(0, |table| py.detach(|| table.changed()))
    }

    /// Notebook display: the table viewer, ticking with the table (see `kermodes._live`); a
    /// static plan's first rows, read for the view.
    fn _ipython_display_(slf: &Bound<'_, Self>) -> PyResult<()> {
        let py = slf.py();
        let show = py.import("kermodes._live")?;
        match &slf.get().0 {
            Inner::Planned(planned) if !planned.shadow.is_refreshing() => {
                let first = py.detach(|| planned.first(DISPLAY_ROWS))?;
                show.call_method1("show", (Bound::new(py, Table::built(first))?,))?;
            }
            _ => {
                show.call_method1("show", (slf,))?;
            }
        }
        Ok(())
    }

    /// The table as text, for a terminal and for an agent reading cell outputs: whether it
    /// ticks, its row count (a static plan's: how many of its first rows it shows), how it was
    /// built, its columns with their types, and its first rows; or its error.
    fn __repr__(&self, py: Python<'_>) -> String {
        let table = match &self.0 {
            Inner::Built(table) => Ok(table.clone()),
            Inner::Planned(planned) if !planned.shadow.is_refreshing() => {
                return match py.detach(|| planned.first(PREVIEW_ROWS + 1)) {
                    Ok(first) => {
                        let shown = py.detach(|| first.rows_at(0, PREVIEW_ROWS));
                        match shown {
                            Ok(window) => {
                                let count = if window.total > PREVIEW_ROWS {
                                    format!("first {PREVIEW_ROWS} rows")
                                } else {
                                    row_count(window.total)
                                };
                                let header = format!(
                                    "kermodes.Table · static · {count}\n{}",
                                    planned.outline()
                                );
                                let total = window.total.min(PREVIEW_ROWS);
                                preview_text(&window.rows, total, &header).unwrap_or_else(|e| {
                                    format!("{header}\n(its rows cannot be shown: {e})")
                                })
                            }
                            Err(error) => failed(&planned.outline(), &error.to_string()),
                        }
                    }
                    Err(error) => failed(&planned.outline(), &error.to_string()),
                };
            }
            Inner::Planned(planned) => py.detach(|| planned.table()),
        };
        match table {
            Ok(table) => repr_built(py, &table),
            Err(error) => match &self.0 {
                Inner::Planned(planned) => failed(&planned.outline(), &error.to_string()),
                Inner::Built(table) => failed(&table.description(), &error.to_string()),
            },
        }
    }
}

impl Table {
    /// The table rows are written to: an input table.
    fn input(&self) -> PyResult<kermodes_core::Table> {
        match &self.0 {
            Inner::Built(table) => Ok(table.clone()),
            Inner::Planned(_) => Err(KermodesError::new_err(
                "rows are written to input tables, and to stored tables through \
                 con.writer(name)",
            )),
        }
    }
}

impl Planned {
    /// A plan's table-to-be, its operators checked now by building it over empty tables.
    pub(crate) fn new(
        plan: Plan,
        engine: kermodes_core::Session,
        stores: Option<Stores>,
        passed: HashMap<String, kermodes_core::Table>,
    ) -> PyResult<Self> {
        let mut failed = None;
        let shadow = plan.build(&mut |scan| {
            let empty = match passed.get(&scan.name) {
                Some(table) => empty_like(&engine, table),
                None => stores
                    .as_ref()
                    .ok_or_else(no_stores)
                    .and_then(|stores| stored::shadow(&engine, stores, scan)),
            };
            empty.map_err(|e| set_aside(&mut failed, e))
        });
        let shadow = match failed {
            Some(error) => return Err(error),
            None => shadow.map_err(engine_error)?,
        };
        Ok(Self {
            plan,
            engine,
            stores,
            passed,
            shadow,
            ticking: Mutex::new(None),
        })
    }

    /// Its table: a static plan's built now, as its data is now; a ticking plan's once, then
    /// kept. A ticking plan starts when first used: in a session that runs its own cycles this
    /// waits for the cycle that loads it, so it is never seen empty for want of one; a manual
    /// session's loads at its next `step`.
    fn table(&self) -> PyResult<kermodes_core::Table> {
        if !self.shadow.is_refreshing() {
            return self.build(self.plan.clone());
        }
        let mut kept = self.ticking.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(table) = kept.as_ref() {
            return Ok(table.clone());
        }
        let table = self.build(self.plan.clone())?;
        if self.engine.is_running() {
            let mut cycle = 0;
            while table.is_loading() {
                cycle = self.engine.wait_cycle(cycle, Duration::from_secs(1));
            }
            // A fed table is ready before the rows it held while loading are applied: they are
            // queued for the next cycle.
            if self.engine.has_queued_writes() {
                let now = self.engine.wait_cycle(0, Duration::ZERO);
                self.engine.wait_cycle(now, Duration::from_secs(1));
            }
        }
        *kept = Some(table.clone());
        Ok(table)
    }

    /// A ticking plan's table, if built.
    fn kept(&self) -> Option<kermodes_core::Table> {
        self.ticking
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Its first `rows` rows, built now: what a view shows of a static plan.
    fn first(&self, rows: usize) -> PyResult<kermodes_core::Table> {
        self.build(Plan::Head {
            input: Box::new(self.plan.clone()),
            rows,
        })
    }

    /// The plan's table, built now: a static plan by the static executor, on its own threads and
    /// outside the session's graph, its result registered as a static table, so building it never
    /// holds up a ticking table; a ticking plan by the engine's incremental operators.
    fn build(&self, plan: Plan) -> PyResult<kermodes_core::Table> {
        let plan = plan.pushed_down();
        let mut failed = None;
        if !self.shadow.is_refreshing() {
            let executed = kermodes_core::exec::execute(&plan, &mut |scan| {
                self.source(scan).map_err(|e| set_aside(&mut failed, e))
            });
            return match (failed, executed) {
                (Some(error), _) => Err(error),
                (None, Ok((schema, batches))) => self
                    .engine
                    .computed_table(schema, batches)
                    .map_err(engine_error),
                (None, Err(error)) => Err(engine_error(error)),
            };
        }
        let built = plan.build(&mut |scan| self.rows(scan).map_err(|e| set_aside(&mut failed, e)));
        match failed {
            Some(error) => Err(error),
            None => built.map_err(engine_error),
        }
    }

    /// A static scan's rows for the executor: a table in memory, or a stored table.
    fn source(&self, scan: &Scan) -> PyResult<kermodes_core::exec::Source> {
        match self.passed.get(&scan.name) {
            Some(table) => {
                let batches = table.snapshot().map_err(engine_error)?;
                Ok(kermodes_core::exec::Source {
                    schema: table.schema().clone(),
                    parts: vec![stored::in_memory(batches)],
                })
            }
            None => stored::source(self.stores()?, scan),
        }
    }

    /// A ticking scan's rows: a table in memory, or a stored table.
    fn rows(&self, scan: &Scan) -> PyResult<kermodes_core::Table> {
        match self.passed.get(&scan.name) {
            Some(table) => match &scan.predicate {
                Some(predicate) => table.filter(predicate).map_err(engine_error),
                None => Ok(table.clone()),
            },
            None if scan.live => stored::live_rows(&self.engine, self.stores()?, scan),
            None => {
                // A static scan in a ticking plan (a static table joined to a live one): its rows
                // now, read by the executor.
                let mut source = Some(stored::source(self.stores()?, scan)?);
                let (schema, batches) =
                    kermodes_core::exec::execute(&Plan::Scan(scan.clone()), &mut |_| {
                        Ok(source.take().expect("a plan of one scan asks once"))
                    })
                    .map_err(engine_error)?;
                self.engine
                    .computed_table(schema, batches)
                    .map_err(engine_error)
            }
        }
    }

    fn explain(&self) -> PyResult<String> {
        let plan = self.plan.clone().pushed_down();
        let mut failed = None;
        let explained = plan.explain(&mut |scan| {
            let described = match self.passed.get(&scan.name) {
                Some(table) => Ok(("in memory".to_owned(), table.is_refreshing())),
                None => self
                    .stores()
                    .and_then(|stores| stored::describe(stores, scan)),
            };
            described.map_err(|e| set_aside(&mut failed, e))
        });
        match failed {
            Some(error) => Err(error),
            None => explained.map_err(engine_error),
        }
    }

    fn stores(&self) -> PyResult<&Stores> {
        self.stores.as_ref().ok_or_else(no_stores)
    }

    /// The plan's operators, each static or ticking, without reading anything.
    fn outline(&self) -> String {
        let mut ticks = |scan: &Scan| {
            let ticks = match self.passed.get(&scan.name) {
                Some(table) => table.is_refreshing(),
                None => scan.live,
            };
            Ok((String::new(), ticks))
        };
        self.plan.explain(&mut ticks).unwrap_or_default()
    }
}

/// The stored table `name`: as it is when used, or `live`, all its rows then every change.
pub(crate) fn stored_table(
    py: Python<'_>,
    engine: &kermodes_core::Session,
    stores: Stores,
    name: &str,
    live: bool,
) -> PyResult<Table> {
    let plan = Plan::Scan(Scan { live, ..scan(name) });
    let planned = py.detach(|| Planned::new(plan, engine.clone(), Some(stores), HashMap::new()))?;
    Ok(Table(Inner::Planned(Box::new(planned))))
}

/// The SQL `query` as a table: its names are stored tables (read ticking where `live`), or the
/// tables passed by keyword under them.
pub(crate) fn sql(
    py: Python<'_>,
    engine: &kermodes_core::Session,
    stores: Option<Stores>,
    query: &str,
    tables: Option<&Bound<'_, PyDict>>,
    live: bool,
) -> PyResult<Table> {
    let parsed = kermodes_core::sql::parse(query).map_err(engine_error)?;
    if parsed.explain {
        return Err(PyValueError::new_err(
            "a query's table explains itself: con.sql(query).explain()",
        ));
    }
    let mut given: HashMap<String, Parts> = HashMap::new();
    for (name, table) in tables.into_iter().flat_map(|tables| tables.iter()) {
        let table = table.extract::<PyRef<'_, Table>>()?;
        let table: &Table = &table;
        let parts = py.detach(|| table.parts(engine))?;
        given.insert(name.extract::<String>()?, parts);
    }
    let (mut plan, mut stores, mut passed, mut missing) =
        (parsed.plan, stores, HashMap::new(), None);
    plan.replace_scans(&mut |scan| {
        if scan.name.contains('.') {
            return live.then(|| {
                Plan::Scan(Scan {
                    live: true,
                    ..scan.clone()
                })
            });
        }
        let Some(parts) = given.get(&scan.name) else {
            missing.get_or_insert_with(|| scan.name.clone());
            return None;
        };
        stores = stores.clone().or(parts.stores.clone());
        passed.extend(parts.passed.clone());
        Some(match &scan.predicate {
            Some(predicate) => Plan::Filter {
                input: Box::new(parts.plan.clone()),
                predicate: predicate.clone(),
            },
            None => parts.plan.clone(),
        })
    });
    if let Some(name) = missing {
        return Err(PyValueError::new_err(format!(
            "no table {name}: name a stored table (ns.table) or pass it, as in \
             con.sql(query, {name}=table)"
        )));
    }
    let planned = py.detach(|| Planned::new(plan, engine.clone(), stores, passed))?;
    Ok(Table(Inner::Planned(Box::new(planned))))
}

/// Every row of every table, which need the same column names and types in the same order. Rows
/// keep their table's order and tables follow one another; nothing is copied. Ticks if any table
/// does.
#[pyfunction]
#[pyo3(signature = (*tables))]
pub(crate) fn merge(py: Python<'_>, tables: Vec<PyRef<'_, Table>>) -> PyResult<Table> {
    let tables: Vec<&Table> = tables.iter().map(|t| &**t).collect();
    Table::combine(py, &tables, Plan::Merge, kermodes_core::Table::merge)
}

/// A rolling time span: a period for timestamps, or a number in the `at` column's units.
#[derive(FromPyObject)]
enum Span {
    Period(String),
    Units(i64),
}

/// A built table as text: whether it ticks, its row count, how it was built, its columns with
/// their types, and its first rows; or its error.
fn repr_built(py: Python<'_>, table: &kermodes_core::Table) -> String {
    let (head, status, description) = py.detach(|| {
        let status =
            kermodes_core::viewer::Status::new(table.is_loading(), table.is_refreshing()).as_str();
        (table.rows_at(0, PREVIEW_ROWS), status, table.description())
    });
    match head {
        Ok(window) => {
            let header = format!(
                "kermodes.Table · {status} · {}\n{description}",
                row_count(window.total)
            );
            preview_text(&window.rows, window.total, &header)
                .unwrap_or_else(|e| format!("{header}\n(its rows cannot be shown: {e})"))
        }
        Err(error) => failed(&description, &error.to_string()),
    }
}

fn failed(description: &str, error: &str) -> String {
    format!("kermodes.Table · failed\n{description}\n{error}")
}

/// A table shaped as `table`, with no rows: static, or ticking if it ticks.
fn empty_like(
    engine: &kermodes_core::Session,
    table: &kermodes_core::Table,
) -> PyResult<kermodes_core::Table> {
    if table.is_refreshing() {
        engine.input_table(table.schema())
    } else {
        engine.static_table(table.schema().clone(), Vec::new())
    }
    .map_err(engine_error)
}

/// A name for a table in memory that a plan reads, unlike any other: a plan joins plans whose
/// tables were named apart.
fn passed_name() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!("__table{}", NEXT.fetch_add(1, Ordering::Relaxed))
}

fn scan(name: &str) -> Scan {
    Scan {
        name: name.to_owned(),
        predicate: None,
        columns: None,
        limit: None,
        live: false,
    }
}

fn names(names: &[String]) -> Vec<&str> {
    names.iter().map(String::as_str).collect()
}

fn pairs(pairs: &[(String, String)]) -> Vec<(&str, &str)> {
    pairs
        .iter()
        .map(|(a, b)| (a.as_str(), b.as_str()))
        .collect()
}

fn specs(aggregations: Option<&Bound<'_, PyDict>>) -> PyResult<Vec<(String, String)>> {
    let mut specs = Vec::new();
    for (name, spec) in aggregations.into_iter().flat_map(|d| d.iter()) {
        specs.push((name.extract()?, spec.extract()?));
    }
    Ok(specs)
}

fn no_stores() -> PyErr {
    KermodesError::new_err(
        "this session keeps no tables: open a store with km.connect(path), or connect to one \
         with km.connect(url)",
    )
}

/// The engine's error for a scan that failed; the scan's own `PyErr` is kept aside and raised in
/// its place, so a `ValueError` stays one.
fn set_aside(failed: &mut Option<PyErr>, error: PyErr) -> kermodes_core::Error {
    *failed = Some(error);
    kermodes_core::Error::Unsupported("a table the plan reads failed".to_owned())
}
