//! `ursid._native`: the engine exposed to Python. Engine work runs with the interpreter released.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::{Array, ArrayIterator, ArrayRef, AsArray, RecordBatch, StructArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit, TimestampNanosecondType};
use arrow::error::ArrowError;
use arrow::util::display::{ArrayFormatter, FormatOptions};
use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyCapsule, PyDict};
use pyo3_arrow::error::PyArrowError;
use pyo3_arrow::ffi::to_stream_pycapsule;
use pyo3_arrow::input::AnyRecordBatch;

/// mimalloc for every allocation the engine makes. It keeps freed memory for reuse only briefly
/// and returns what stays idle to the OS, where the system allocators keep large freed blocks
/// resident (macOS) or in their heaps (glibc): a process's footprint then follows the tables it
/// holds, which keeps container memory predictable.
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Returns free memory to the OS: freed pages from every thread's heap, including those of
/// worker threads that have exited. Registered as the engine's idle hook (see
/// `ursid_core::set_idle_hook`): about 1 µs with nothing to return, about 14 ms after freeing
/// 900 MB.
fn release_free_memory() {
    // SAFETY: mi_collect has no preconditions; it only hands free pages back to the OS.
    unsafe { libmimalloc_sys::mi_collect(true) }
}

create_exception!(
    ursid,
    UrsidError,
    PyException,
    "An engine error: a bad expression, mismatched data, or a failed table."
);

fn engine_error(error: ursid_core::Error) -> PyErr {
    UrsidError::new_err(error.to_string())
}

fn arrow_error(error: ArrowError) -> PyErr {
    PyArrowError::from(error).into()
}

/// Reads every batch from an object implementing the Arrow PyCapsule interface.
fn read_batches(
    data: AnyRecordBatch,
) -> PyResult<(
    arrow::datatypes::SchemaRef,
    Vec<arrow::record_batch::RecordBatch>,
)> {
    let reader = data.into_reader()?;
    let schema = reader.schema();
    let batches = reader.collect::<Result<Vec<_>, _>>().map_err(arrow_error)?;
    Ok((schema, batches))
}

/// A key given as one column name or a list of them.
#[derive(FromPyObject)]
enum Key {
    One(String),
    Many(Vec<String>),
}

impl Key {
    fn names(self) -> Vec<String> {
        match self {
            Key::One(name) => vec![name],
            Key::Many(names) => names,
        }
    }
}

/// An engine running inside this Python process.
#[pyclass(module = "ursid", name = "Session", frozen)]
pub struct Session(ursid_core::Session);

#[pymethods]
impl Session {
    #[new]
    fn new() -> Self {
        Self(ursid_core::Session::new())
    }

    /// A refreshing table with the given column names and types. Without `key`, rows arrive
    /// through `append`; with `key`, the table holds one row per key value, written through
    /// `upsert` and `delete`.
    #[pyo3(signature = (columns, key=None))]
    fn input_table(
        &self,
        py: Python<'_>,
        columns: &Bound<'_, PyDict>,
        key: Option<Key>,
    ) -> PyResult<Table> {
        let mut fields = Vec::with_capacity(columns.len());
        for (name, type_name) in columns.iter() {
            let name: String = name.extract()?;
            let type_name: String = type_name.extract()?;
            let data_type = ursid_core::parse_type(&type_name).map_err(engine_error)?;
            fields.push(Field::new(name, data_type, true));
        }
        let schema = Schema::new(fields);
        let names = key.map(Key::names);
        let table = py.detach(|| match &names {
            None => self.0.input_table(&schema),
            Some(names) => {
                let names: Vec<&str> = names.iter().map(String::as_str).collect();
                self.0.keyed_input_table(&schema, &names)
            }
        });
        Ok(Table(table.map_err(engine_error)?))
    }

    /// A table that never changes, from any Arrow table or record batch.
    fn static_table(&self, py: Python<'_>, data: AnyRecordBatch) -> PyResult<Table> {
        let (schema, batches) = read_batches(data)?;
        let table = py
            .detach(|| self.0.static_table(schema, batches))
            .map_err(engine_error)?;
        Ok(Table(table))
    }

    /// Runs one cycle and returns its number.
    fn step(&self, py: Python<'_>) -> u64 {
        py.detach(|| self.0.step())
    }

    /// Runs cycles in the background: one as soon as writes arrive, folding writes that arrive
    /// during a cycle into the next, until `stop`. `min_period` (such as `"100ms"` or `"1s"`)
    /// spaces cycles at least that far apart.
    #[pyo3(signature = (min_period=None))]
    fn start(&self, py: Python<'_>, min_period: Option<&str>) -> PyResult<()> {
        let min_period = min_period.map(parse_period).transpose()?;
        py.detach(|| self.0.start(min_period)).map_err(engine_error)
    }

    /// Stops the background cycles, after any cycle in progress.
    fn stop(&self, py: Python<'_>) {
        py.detach(|| self.0.stop());
    }

    #[getter]
    fn is_running(&self, py: Python<'_>) -> bool {
        py.detach(|| self.0.is_running())
    }

    /// How many tables the session holds: those Python refers to, and those they are computed
    /// from. A table goes as soon as neither is true.
    #[getter]
    fn table_count(&self, py: Python<'_>) -> usize {
        py.detach(|| self.0.table_count())
    }

    /// Serves this session's published tables to the web UI on `host:port` (port 0 picks a
    /// free one): the app at `server.url`, tables published with `server.publish(name, table)`.
    /// Local use: there is no authentication, so it listens on 127.0.0.1 unless told otherwise.
    #[pyo3(signature = (port=0, host="127.0.0.1"))]
    fn serve(&self, py: Python<'_>, port: u16, host: &str) -> PyResult<Server> {
        let addr: SocketAddr = format!("{host}:{port}")
            .parse()
            .map_err(|e| PyValueError::new_err(format!("cannot listen on {host}:{port}: {e}")))?;
        // The web app ships in the package (`bun run build` in web/ writes it there).
        let package: PathBuf = py.import("ursid")?.getattr("__file__")?.extract()?;
        let app = package.parent().map(|dir| dir.join("static").join("app"));
        let session = self.0.clone();
        let server = py
            .detach(|| ursid_gateway::serve(session, addr, app))
            .map_err(|e| PyValueError::new_err(format!("cannot listen on {addr}: {e}")))?;
        Ok(Server(Mutex::new(server)))
    }
}

/// The web UI's server for a session (`Session.serve`). Published tables are held until
/// unpublished or the server stops; it stops when this object goes, or with `stop()`.
#[pyclass(module = "ursid", name = "Server", frozen)]
pub struct Server(Mutex<ursid_gateway::Server>);

impl Server {
    fn lock(&self) -> std::sync::MutexGuard<'_, ursid_gateway::Server> {
        self.0
            .lock()
            .expect("ursid server poisoned by an earlier panic")
    }
}

#[pymethods]
impl Server {
    /// The web UI's address.
    #[getter]
    fn url(&self) -> String {
        format!("http://{}/", self.lock().addr())
    }

    /// Shows `table` in the web UI as `name`, replacing any table of that name.
    fn publish(&self, py: Python<'_>, name: &str, table: PyRef<'_, Table>) {
        let table = &table.0;
        py.detach(|| self.lock().publish(name, table));
    }

    /// Removes `name` from the web UI; whether it was published.
    fn unpublish(&self, py: Python<'_>, name: &str) -> bool {
        py.detach(|| self.lock().unpublish(name))
    }

    /// Published names, in order.
    #[getter]
    fn published(&self) -> Vec<String> {
        self.lock().published()
    }

    /// Stops serving: connections close and published tables are let go.
    fn stop(&self, py: Python<'_>) {
        py.detach(|| self.lock().stop());
    }

    fn __repr__(&self) -> String {
        format!("<ursid.Server {}>", self.url())
    }
}

/// A period such as `"100ms"`, `"0.5s"`, `"5m"`, or `"1h"`: a positive number and one of the
/// units ns, us, ms, s, m, h, d.
fn parse_period(text: &str) -> PyResult<Duration> {
    let text = text.trim();
    let units = [
        ("ns", 1e-9),
        ("us", 1e-6),
        ("ms", 1e-3),
        ("s", 1.0),
        ("m", 60.0),
        ("h", 3600.0),
        ("d", 86400.0),
    ];
    let parsed = units.iter().find_map(|(unit, scale)| {
        let number = text.strip_suffix(unit)?.trim().parse::<f64>().ok()?;
        (number.is_finite() && number > 0.0).then(|| Duration::from_secs_f64(number * scale))
    });
    parsed.ok_or_else(|| {
        PyValueError::new_err(format!(
            "period must look like \"100ms\", \"5m\", or \"1h\" (units ns, us, ms, s, m, h, d), \
             not {text:?}"
        ))
    })
}

/// A rolling time span: a period for timestamps, or a number in the `at` column's units.
#[derive(FromPyObject)]
enum Span {
    Period(String),
    Units(i64),
}

/// A table: static, or ticking with the session's cycles. It lives while Python refers to it or
/// a live table is computed from it; weak references (the notebook view's) don't count.
///
/// Every method that reaches the session releases the interpreter first. A cycle can free rows
/// that Python owns (inputs are imported without copying), which takes the interpreter while
/// the cycle holds the session; waiting for the session while holding the interpreter would
/// deadlock.
#[pyclass(module = "ursid", name = "Table", frozen, weakref)]
pub struct Table(ursid_core::Table);

#[pymethods]
impl Table {
    /// Column names and type names.
    #[getter]
    fn columns(&self) -> Vec<(String, String)> {
        self.0
            .schema()
            .fields()
            .iter()
            .map(|f| (f.name().clone(), ursid_core::type_name(f.data_type())))
            .collect()
    }

    #[getter]
    fn is_refreshing(&self, py: Python<'_>) -> bool {
        py.detach(|| self.0.is_refreshing())
    }

    /// Queues rows for the next cycle, from any Arrow table or record batch.
    fn append(&self, py: Python<'_>, data: AnyRecordBatch) -> PyResult<()> {
        let (_, batches) = read_batches(data)?;
        py.detach(|| self.0.append(&batches)).map_err(engine_error)
    }

    /// Queues rows that replace the row with the same key, or add one. Keyed tables only.
    fn upsert(&self, py: Python<'_>, data: AnyRecordBatch) -> PyResult<()> {
        let (_, batches) = read_batches(data)?;
        py.detach(|| self.0.upsert(&batches)).map_err(engine_error)
    }

    /// Queues removal of the rows with these keys; `data` holds only the key columns.
    fn delete(&self, py: Python<'_>, data: AnyRecordBatch) -> PyResult<()> {
        let (_, batches) = read_batches(data)?;
        py.detach(|| self.0.delete(&batches)).map_err(engine_error)
    }

    /// Rows for which `predicate` is true; NULL counts as false.
    #[pyo3(name = "where")]
    fn where_(&self, py: Python<'_>, predicate: &str) -> PyResult<Table> {
        let table = py
            .detach(|| self.0.filter(predicate))
            .map_err(engine_error)?;
        Ok(Table(table))
    }

    /// Only the listed columns, in that order: each an existing column's name or
    /// `name = expression`, as in `update`.
    #[pyo3(signature = (*columns))]
    fn select(&self, py: Python<'_>, columns: Vec<String>) -> PyResult<Table> {
        let columns: Vec<&str> = columns.iter().map(String::as_str).collect();
        let table = py
            .detach(|| self.0.select(&columns))
            .map_err(engine_error)?;
        Ok(Table(table))
    }

    /// Adds or replaces columns, each given as `name = expression`.
    #[pyo3(signature = (*assignments))]
    fn update(&self, py: Python<'_>, assignments: Vec<String>) -> PyResult<Table> {
        let assignments: Vec<&str> = assignments.iter().map(String::as_str).collect();
        let table = py
            .detach(|| self.0.update(&assignments))
            .map_err(engine_error)?;
        Ok(Table(table))
    }

    /// The last row, in arrival order, for each value of `columns`.
    #[pyo3(signature = (*columns))]
    fn last_by(&self, py: Python<'_>, columns: Vec<String>) -> PyResult<Table> {
        let columns: Vec<&str> = columns.iter().map(String::as_str).collect();
        let table = py
            .detach(|| self.0.last_by(&columns))
            .map_err(engine_error)?;
        Ok(Table(table))
    }

    /// The first `n` rows in key order (arrival order for appended rows), kept current.
    fn head(&self, py: Python<'_>, n: usize) -> PyResult<Table> {
        let table = py.detach(|| self.0.head(n)).map_err(engine_error)?;
        Ok(Table(table))
    }

    /// The last `n` rows in key order (the newest, for appended rows), kept current.
    fn tail(&self, py: Python<'_>, n: usize) -> PyResult<Table> {
        let table = py.detach(|| self.0.tail(n)).map_err(engine_error)?;
        Ok(Table(table))
    }

    /// Rows ordered by `by`, each `"name"` or `"name desc"`, earlier entries first. NULL sorts as
    /// the largest value; ties keep the input's order. A ticking table's sort keeps its rows in
    /// that order for reading; nothing can be built on it.
    #[pyo3(signature = (*by))]
    fn sort(&self, py: Python<'_>, by: Vec<String>) -> PyResult<Table> {
        let by: Vec<&str> = by.iter().map(String::as_str).collect();
        let table = py.detach(|| self.0.sort(&by)).map_err(engine_error)?;
        Ok(Table(table))
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
        let on: Vec<&str> = on.iter().map(String::as_str).collect();
        let joins: Option<Vec<&str>> = joins
            .as_ref()
            .map(|j| j.iter().map(String::as_str).collect());
        let table = py
            .detach(|| self.0.natural_join(&right.0, &on, joins.as_deref()))
            .map_err(engine_error)?;
        Ok(Table(table))
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
        let on: Vec<&str> = on.iter().map(String::as_str).collect();
        let joins: Option<Vec<&str>> = joins
            .as_ref()
            .map(|j| j.iter().map(String::as_str).collect());
        let table = py
            .detach(|| self.0.as_of_join(&right.0, &on, at, joins.as_deref()))
            .map_err(engine_error)?;
        Ok(Table(table))
    }

    /// One row per value of `columns`, with each `name="function(column)"` aggregation.
    #[pyo3(signature = (*columns, **aggregations))]
    fn agg_by(
        &self,
        py: Python<'_>,
        columns: Vec<String>,
        aggregations: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Table> {
        let mut specs: Vec<(String, String)> = Vec::new();
        for (name, spec) in aggregations.into_iter().flat_map(|d| d.iter()) {
            specs.push((name.extract()?, spec.extract()?));
        }
        let columns: Vec<&str> = columns.iter().map(String::as_str).collect();
        let specs: Vec<(&str, &str)> = specs
            .iter()
            .map(|(n, s)| (n.as_str(), s.as_str()))
            .collect();
        let table = py
            .detach(|| self.0.agg_by(&columns, &specs))
            .map_err(engine_error)?;
        Ok(Table(table))
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
        let window = match (rows, time, &at) {
            (Some(n), None, None) => ursid_core::Window::Rows(n),
            (None, Some(time), Some(at)) => ursid_core::Window::Time {
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
        let mut specs: Vec<(String, String)> = Vec::new();
        for (name, spec) in aggregations.into_iter().flat_map(|d| d.iter()) {
            specs.push((name.extract()?, spec.extract()?));
        }
        let by: Vec<&str> = by.iter().map(String::as_str).collect();
        let specs: Vec<(&str, &str)> = specs
            .iter()
            .map(|(n, s)| (n.as_str(), s.as_str()))
            .collect();
        let table = py
            .detach(|| self.0.rolling(&by, window, &specs))
            .map_err(engine_error)?;
        Ok(Table(table))
    }

    fn __len__(&self, py: Python<'_>) -> PyResult<usize> {
        py.detach(|| self.0.num_rows()).map_err(engine_error)
    }

    /// Exports a snapshot of the current rows through the Arrow PyCapsule interface.
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyCapsule>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let batches = py.detach(|| self.0.snapshot()).map_err(engine_error)?;
        let schema = self.0.schema();
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
        py.detach(|| self.0.changed())
    }

    /// The first `rows` rows as an HTML table, with the row count, or the table's error.
    fn _preview_html(&self, py: Python<'_>, rows: usize) -> PyResult<String> {
        let (preview, status) = py.detach(|| {
            let status = if self.0.is_refreshing() {
                format!("ticking · updated in cycle {}", self.0.changed())
            } else {
                "static".to_owned()
            };
            (self.0.rows_at(0, rows), status)
        });
        match preview {
            Ok(window) => preview_html(&window.rows, window.total, &status).map_err(arrow_error),
            Err(error) => Ok(failure_html(&error.to_string())),
        }
    }

    /// Notebook display: a live view for ticking tables (see `ursid._live`).
    fn _ipython_display_(slf: &Bound<'_, Self>) -> PyResult<()> {
        slf.py()
            .import("ursid._live")?
            .call_method1("show", (slf,))?;
        Ok(())
    }

    fn __repr__(&self, py: Python<'_>) -> String {
        let columns = self
            .columns()
            .iter()
            .map(|(name, type_name)| format!("{name}: {type_name}"))
            .collect::<Vec<_>>()
            .join(", ");
        let (refreshing, description) =
            py.detach(|| (self.0.is_refreshing(), self.0.description()));
        let kind = if refreshing { "ticking" } else { "static" };
        format!("<ursid.Table {description} [{kind}] {{{columns}}}>")
    }
}

/// Every row of every table, which need the same column names and types in the same order. Rows
/// keep their table's order and tables follow one another; nothing is copied. Ticks if any table
/// does.
#[pyfunction]
#[pyo3(signature = (*tables))]
fn merge(py: Python<'_>, tables: Vec<PyRef<'_, Table>>) -> PyResult<Table> {
    let tables: Vec<&ursid_core::Table> = tables.iter().map(|t| &t.0).collect();
    let table = py
        .detach(|| ursid_core::Table::merge(&tables))
        .map_err(engine_error)?;
    Ok(Table(table))
}

/// One open grid's server side (`ursid_core::viewer`): `ursid._widget` carries its messages over
/// the notebook comm. Frames are `(header, rows, keys)`: a dict, an Arrow IPC stream, and the
/// rows' keys as little-endian u64. Holds its table weakly.
#[pyclass(module = "ursid", name = "Viewer", frozen)]
pub struct Viewer(Mutex<ursid_core::viewer::Viewer>);

type PyFrame<'py> = (Bound<'py, PyDict>, Bound<'py, PyBytes>, Bound<'py, PyBytes>);

impl Viewer {
    fn lock(&self) -> std::sync::MutexGuard<'_, ursid_core::viewer::Viewer> {
        self.0
            .lock()
            .expect("ursid viewer poisoned by an earlier panic")
    }
}

fn py_frame(py: Python<'_>, frame: ursid_core::viewer::Frame) -> PyResult<PyFrame<'_>> {
    let header = PyDict::new(py);
    match frame.fetched {
        Some(id) => {
            header.set_item("type", "fetched")?;
            header.set_item("id", id)?;
        }
        None => header.set_item("type", "rows")?,
    }
    header.set_item("start", frame.start)?;
    header.set_item("total", frame.total)?;
    header.set_item("cycle", frame.cycle)?;
    Ok((
        header,
        PyBytes::new(py, &frame.rows),
        PyBytes::new(py, &frame.keys),
    ))
}

/// A grid message: {"type": "viewport", "top", "bottom"}, {"type": "sort", "by": [{"column",
/// "descending"}]}, {"type": "filter", "filters": {column: text}}, or {"type": "fetch", "id",
/// "start", "count"}. `None` for other types.
fn request(message: &Bound<'_, PyDict>) -> PyResult<Option<ursid_core::viewer::Request>> {
    use ursid_core::viewer::{Request, SortKey};
    let item = |name: &str| -> PyResult<Bound<'_, PyAny>> {
        message
            .get_item(name)?
            .ok_or_else(|| PyValueError::new_err(format!("viewer message without `{name}`")))
    };
    let kind: String = item("type")?.extract()?;
    Ok(Some(match kind.as_str() {
        "viewport" => Request::Viewport {
            top: item("top")?.extract()?,
            bottom: item("bottom")?.extract()?,
        },
        "sort" => Request::Sort(
            item("by")?
                .try_iter()?
                .map(|key| {
                    let key = key?;
                    Ok(SortKey {
                        column: key.get_item("column")?.extract()?,
                        descending: key.get_item("descending")?.extract()?,
                    })
                })
                .collect::<PyResult<_>>()?,
        ),
        "filter" => Request::Filter(item("filters")?.extract()?),
        "fetch" => Request::Fetch {
            id: item("id")?.extract()?,
            start: item("start")?.extract()?,
            count: item("count")?.extract()?,
        },
        _ => return Ok(None),
    }))
}

#[pymethods]
impl Viewer {
    #[new]
    fn new(py: Python<'_>, table: PyRef<'_, Table>) -> Self {
        let table = &table.0;
        Self(Mutex::new(
            py.detach(|| ursid_core::viewer::Viewer::new(table)),
        ))
    }

    /// Handles a grid message; returns the frame to send back, if any.
    fn receive<'py>(
        &self,
        py: Python<'py>,
        message: &Bound<'py, PyDict>,
    ) -> PyResult<Option<PyFrame<'py>>> {
        let Some(request) = request(message)? else {
            return Ok(None);
        };
        let frame = py
            .detach(|| self.lock().receive(request))
            .map_err(engine_error)?;
        frame.map(|f| py_frame(py, f)).transpose()
    }

    /// The window the grid shows, as it is now; `None` once the table is gone.
    fn rows<'py>(&self, py: Python<'py>) -> PyResult<Option<PyFrame<'py>>> {
        let frame = py.detach(|| self.lock().rows()).map_err(engine_error)?;
        frame.map(|f| py_frame(py, f)).transpose()
    }

    /// The viewed table's last changing cycle; `None` once it is gone.
    #[getter]
    fn changed(&self, py: Python<'_>) -> Option<u64> {
        py.detach(|| self.lock().changed())
    }

    /// Drops the sorted or filtered table, so the engine can free it with the viewed table.
    fn release(&self, py: Python<'_>) {
        py.detach(|| self.lock().release());
    }

    /// Sort keys as (column, descending), most significant first.
    #[getter]
    fn sort(&self) -> Vec<(String, bool)> {
        let viewer = self.lock();
        viewer
            .sort()
            .iter()
            .map(|key| (key.column.clone(), key.descending))
            .collect()
    }

    #[getter]
    fn filters(&self) -> std::collections::BTreeMap<String, String> {
        self.lock().filters().clone()
    }

    /// Why filter entries were not applied, by column.
    #[getter]
    fn filter_errors(&self) -> std::collections::BTreeMap<String, String> {
        self.lock().filter_errors().clone()
    }

    /// "static", or "ticking · updated in cycle N".
    fn status(&self, cycle: u64) -> String {
        self.lock().status(cycle)
    }
}

#[pymodule]
mod _native {
    #[pymodule_export]
    use super::{Server, Session, Table, Viewer, merge};

    use pyo3::prelude::*;

    #[pymodule_init]
    fn init(module: &Bound<'_, PyModule>) -> PyResult<()> {
        ursid_core::set_idle_hook(super::release_free_memory);
        module.add("UrsidError", module.py().get_type::<super::UrsidError>())
    }
}

const CAPTION: &str = "font:12px ui-monospace,monospace;opacity:0.65;margin:2px 0";
const CELL: &str = "padding:2px 10px;white-space:nowrap";

/// HTML for a table's first rows: column names and types, then the rows, then how many more.
fn preview_html(head: &RecordBatch, total: usize, status: &str) -> Result<String, ArrowError> {
    let options = FormatOptions::default()
        .with_null("null")
        .with_timestamp_tz_format(Some("%Y-%m-%d %H:%M:%S%.f"));
    let columns: Vec<ArrayRef> = head.columns().iter().map(displayable).collect();
    let formatters = columns
        .iter()
        .map(|c| ArrayFormatter::try_new(c.as_ref(), &options))
        .collect::<Result<Vec<_>, _>>()?;
    let schema = head.schema();
    let mut html = format!(
        "<div><div style=\"{CAPTION}\">{} · {} rows</div>",
        escape(status),
        thousands(total)
    );
    html.push_str("<table style=\"border-collapse:collapse;font:12px ui-monospace,monospace\">");
    html.push_str("<thead><tr>");
    for field in schema.fields() {
        html.push_str(&format!(
            "<th style=\"{CELL};text-align:{};border-bottom:1px solid #8886\">{} \
             <span style=\"opacity:0.5;font-weight:normal\">{}</span></th>",
            alignment(field.data_type()),
            escape(field.name()),
            escape(&ursid_core::type_name(field.data_type())),
        ));
    }
    html.push_str("</tr></thead><tbody>");
    for row in 0..head.num_rows() {
        html.push_str("<tr>");
        for ((formatter, column), field) in formatters.iter().zip(&columns).zip(schema.fields()) {
            let value = escape(&formatter.value(row).try_to_string()?);
            let value = if column.is_null(row) {
                format!("<span style=\"opacity:0.4\">{value}</span>")
            } else {
                value
            };
            html.push_str(&format!(
                "<td style=\"{CELL};text-align:{}\">{value}</td>",
                alignment(field.data_type())
            ));
        }
        html.push_str("</tr>");
    }
    html.push_str("</tbody></table>");
    if total > head.num_rows() {
        html.push_str(&format!(
            "<div style=\"{CAPTION}\">… {} more rows</div>",
            thousands(total - head.num_rows())
        ));
    }
    html.push_str("</div>");
    Ok(html)
}

fn failure_html(message: &str) -> String {
    format!(
        "<div style=\"font:12px ui-monospace,monospace;color:#d33\">{}</div>",
        escape(message)
    )
}

fn alignment(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::Int32 | DataType::Int64 | DataType::Float64 => "right",
        _ => "left",
    }
}

/// UTC timestamps relabelled with the fixed offset `+00:00`: same instants, but they format
/// without a time zone database.
fn displayable(column: &ArrayRef) -> ArrayRef {
    match column.data_type() {
        DataType::Timestamp(TimeUnit::Nanosecond, Some(tz)) if tz.as_ref() == "UTC" => Arc::new(
            column
                .as_primitive::<TimestampNanosecondType>()
                .clone()
                .with_timezone("+00:00"),
        ),
        _ => column.clone(),
    }
}

fn escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            _ => out.push(c),
        }
    }
    out
}

/// `1234567` as `1,234,567`.
fn thousands(n: usize) -> String {
    let digits = n.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}
