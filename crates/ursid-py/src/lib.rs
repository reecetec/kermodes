//! `ursid._native`: the engine exposed to Python. Engine work runs with the interpreter released.

use std::sync::Arc;

use arrow::array::{ArrayIterator, ArrayRef, StructArray};
use arrow::datatypes::{Field, Schema};
use arrow::error::ArrowError;
use pyo3::create_exception;
use pyo3::exceptions::PyException;
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

/// An engine running inside this Python process.
#[pyclass(module = "ursid", name = "Session", frozen)]
pub struct Session(ursid_core::Session);

#[pymethods]
impl Session {
    #[new]
    fn new() -> Self {
        Self(ursid_core::Session::new())
    }

    /// A refreshing table with the given column names and types; rows arrive through `append`.
    fn input_table(&self, columns: &Bound<'_, PyDict>) -> PyResult<Table> {
        let mut fields = Vec::with_capacity(columns.len());
        for (name, type_name) in columns.iter() {
            let name: String = name.extract()?;
            let type_name: String = type_name.extract()?;
            let data_type = ursid_core::parse_type(&type_name).map_err(engine_error)?;
            fields.push(Field::new(name, data_type, true));
        }
        let table = self
            .0
            .input_table(&Schema::new(fields))
            .map_err(engine_error)?;
        Ok(Table(table))
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

#[pymodule]
mod _native {
    #[pymodule_export]
    use super::{Session, Table};

    use pyo3::prelude::*;

    #[pymodule_init]
    fn init(module: &Bound<'_, PyModule>) -> PyResult<()> {
        module.add("UrsidError", module.py().get_type::<super::UrsidError>())
    }
}
