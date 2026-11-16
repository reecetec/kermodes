//! Column IDs (architecture.md §5.4): each column of a stored table has an ID that names it for
//! good, across renames and in every weir segment and history file, so data written under one
//! version of a table's columns reads under another. A table's columns at one version are a
//! `Shape`; a batch moves between shapes by `Shape::project`.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{RecordBatch, RecordBatchOptions, new_null_array};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use crate::Error;

/// A column's ID, unique within its table and never reused. A table written before tables had
/// IDs has its columns' positions, counting from 1, as theirs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ColumnId(pub u32);

/// The field metadata key a weir segment's schema keeps each column's ID under.
const ID: &str = "kermodes:id";

/// A table's columns at one version: the schema readers see (no IDs in it), and each column's
/// ID, in the schema's order.
#[derive(Clone, Debug, PartialEq)]
pub struct Shape {
    pub schema: SchemaRef,
    pub ids: Arc<[ColumnId]>,
}

impl Shape {
    pub fn new(schema: SchemaRef, ids: Arc<[ColumnId]>) -> Self {
        debug_assert_eq!(schema.fields().len(), ids.len());
        Self { schema, ids }
    }

    /// The shape of columns written with `ids` beside them, or by position where a column has
    /// none (written before tables had IDs): `schema` with its metadata dropped.
    pub fn with_ids(schema: &Schema, ids: impl Fn(usize, &Field) -> Option<ColumnId>) -> Self {
        let fields: Vec<Field> = schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone().with_metadata(HashMap::new()))
            .collect();
        let ids: Arc<[ColumnId]> = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| ids(i, f).unwrap_or(ColumnId(i as u32 + 1)))
            .collect();
        Self::new(Arc::new(Schema::new(fields)), ids)
    }

    /// The shape a weir segment's schema says it was written in.
    pub(crate) fn of_segment(schema: &Schema) -> Self {
        Self::with_ids(schema, |_, field| {
            field
                .metadata()
                .get(ID)
                .and_then(|id| id.parse().ok())
                .map(ColumnId)
        })
    }

    /// The schema a weir segment is written with: each field with its ID.
    pub(crate) fn segment_schema(&self) -> SchemaRef {
        let fields: Vec<Field> = self
            .schema
            .fields()
            .iter()
            .zip(self.ids.iter())
            .map(|(f, id)| {
                f.as_ref()
                    .clone()
                    .with_metadata(HashMap::from([(ID.to_owned(), id.0.to_string())]))
            })
            .collect();
        Arc::new(Schema::new(fields))
    }

    /// Whether a batch of this shape is already one of `to`: the same columns, of the same
    /// types, in the same order (names may differ: a rename is metadata).
    pub fn reads_as(&self, to: &Shape) -> bool {
        self.ids == to.ids
            && self
                .schema
                .fields()
                .iter()
                .zip(to.schema.fields().iter())
                .all(|(a, b)| a.data_type() == b.data_type())
    }

    /// `batch`, written in this shape, as one of `to`, column by ID: a column `batch` lacks
    /// (added since) is null, a narrower type is widened, and a column `to` lacks is left out.
    /// A column that `to` has narrower than `batch` (widened after `to` was taken) cannot be
    /// read as `to`: whoever holds `to` must take the table's columns again.
    pub fn project(&self, batch: &RecordBatch, to: &Shape) -> Result<RecordBatch, Error> {
        let columns = to
            .schema
            .fields()
            .iter()
            .zip(to.ids.iter())
            .map(|(field, id)| match self.ids.iter().position(|i| i == id) {
                None => Ok(new_null_array(field.data_type(), batch.num_rows())),
                Some(at) => {
                    let column = batch.column(at);
                    let written = column.data_type();
                    if written == field.data_type() {
                        Ok(column.clone())
                    } else if widens(written, field.data_type()) {
                        cast(column, field.data_type()).map_err(|e| Error::Arrow(e.to_string()))
                    } else {
                        Err(Error::Schema(format!(
                            "column `{}` is {} where it was written, not {}: its table's \
                             columns changed since they were read here; read the table again",
                            field.name(),
                            kermodes_core::type_name(written),
                            kermodes_core::type_name(field.data_type()),
                        )))
                    }
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        RecordBatch::try_new_with_options(
            to.schema.clone(),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
        )
        .map_err(|e| Error::Arrow(e.to_string()))
    }
}

/// Whether every value of `from` is exactly one of `to`: the type changes a table may make
/// (architecture.md §5.4).
pub fn widens(from: &DataType, to: &DataType) -> bool {
    matches!(
        (from, to),
        (DataType::Int32, DataType::Int64) | (DataType::Int32, DataType::Float64)
    )
}
