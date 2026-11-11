//! `ursid._native`: the engine exposed to Python. Engine work runs with the interpreter released.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, ArrayIterator, ArrayRef, AsArray, RecordBatch, StructArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit, TimestampNanosecondType};
use arrow::error::ArrowError;
use arrow::util::display::{ArrayFormatter, FormatOptions};
use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyDict};
use pyo3_arrow::error::PyArrowError;
use pyo3_arrow::ffi::to_stream_pycapsule;
use pyo3_arrow::input::AnyRecordBatch;

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
    fn input_table(&self, columns: &Bound<'_, PyDict>, key: Option<Key>) -> PyResult<Table> {
        let mut fields = Vec::with_capacity(columns.len());
        for (name, type_name) in columns.iter() {
            let name: String = name.extract()?;
            let type_name: String = type_name.extract()?;
            let data_type = ursid_core::parse_type(&type_name).map_err(engine_error)?;
            fields.push(Field::new(name, data_type, true));
        }
        let schema = Schema::new(fields);
        let table = match key {
            None => self.0.input_table(&schema),
            Some(key) => {
                let names = key.names();
                let names: Vec<&str> = names.iter().map(String::as_str).collect();
                self.0.keyed_input_table(&schema, &names)
            }
        };
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
    fn start(&self, min_period: Option<&str>) -> PyResult<()> {
        let min_period = min_period.map(parse_period).transpose()?;
        self.0.start(min_period).map_err(engine_error)
    }

    /// Stops the background cycles, after any cycle in progress.
    fn stop(&self, py: Python<'_>) {
        py.detach(|| self.0.stop());
    }

    #[getter]
    fn is_running(&self) -> bool {
        self.0.is_running()
    }
}

/// A period such as `"100ms"`, `"0.5s"`, or `"2s"`.
fn parse_period(text: &str) -> PyResult<Duration> {
    let text = text.trim();
    let (number, scale) = if let Some(ms) = text.strip_suffix("ms") {
        (ms, 1e-3)
    } else if let Some(s) = text.strip_suffix('s') {
        (s, 1.0)
    } else {
        (text, 0.0)
    };
    match number.trim().parse::<f64>() {
        Ok(value) if scale > 0.0 && value.is_finite() && value > 0.0 => {
            Ok(Duration::from_secs_f64(value * scale))
        }
        _ => Err(PyValueError::new_err(format!(
            "period must look like \"100ms\" or \"1s\", not {text:?}"
        ))),
    }
}

/// A table: static, or ticking with the session's cycles.
#[pyclass(module = "ursid", name = "Table", frozen)]
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
    fn is_refreshing(&self) -> bool {
        self.0.is_refreshing()
    }

    /// Queues rows for the next cycle, from any Arrow table or record batch.
    fn append(&self, data: AnyRecordBatch) -> PyResult<()> {
        let (_, batches) = read_batches(data)?;
        self.0.append(&batches).map_err(engine_error)
    }

    /// Queues rows that replace the row with the same key, or add one. Keyed tables only.
    fn upsert(&self, data: AnyRecordBatch) -> PyResult<()> {
        let (_, batches) = read_batches(data)?;
        self.0.upsert(&batches).map_err(engine_error)
    }

    /// Queues removal of the rows with these keys; `data` holds only the key columns.
    fn delete(&self, data: AnyRecordBatch) -> PyResult<()> {
        let (_, batches) = read_batches(data)?;
        self.0.delete(&batches).map_err(engine_error)
    }

    /// Rows for which `predicate` is true; NULL counts as false.
    #[pyo3(name = "where")]
    fn where_(&self, py: Python<'_>, predicate: &str) -> PyResult<Table> {
        let table = py
            .detach(|| self.0.filter(predicate))
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

    fn __len__(&self) -> PyResult<usize> {
        self.0.num_rows().map_err(engine_error)
    }

    /// Exports a snapshot of the current rows through the Arrow PyCapsule interface.
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyCapsule>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let batches = self.0.snapshot().map_err(engine_error)?;
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
    fn _changed(&self) -> u64 {
        self.0.changed()
    }

    /// The first `rows` rows as an HTML table, with the row count, or the table's error.
    fn _preview_html(&self, py: Python<'_>, rows: usize) -> PyResult<String> {
        let preview = py
            .detach(|| Ok::<_, ursid_core::Error>((self.0.first_rows(rows)?, self.0.num_rows()?)));
        let status = if self.0.is_refreshing() {
            format!("ticking · updated in cycle {}", self.0.changed())
        } else {
            "static".to_owned()
        };
        match preview {
            Ok((head, total)) => preview_html(&head, total, &status).map_err(arrow_error),
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

    fn __repr__(&self) -> String {
        let columns = self
            .columns()
            .iter()
            .map(|(name, type_name)| format!("{name}: {type_name}"))
            .collect::<Vec<_>>()
            .join(", ");
        let kind = if self.0.is_refreshing() {
            "ticking"
        } else {
            "static"
        };
        format!(
            "<ursid.Table {} [{kind}] {{{columns}}}>",
            self.0.description()
        )
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

#[pymodule]
mod _native {
    #[pymodule_export]
    use super::{Session, Table, merge};

    use pyo3::prelude::*;

    #[pymodule_init]
    fn init(module: &Bound<'_, PyModule>) -> PyResult<()> {
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
