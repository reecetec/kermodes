//! The catalog (architecture.md §8.2): which tables exist, their IDs, storage locations, and
//! columns, and each source's lease epoch (§7.4). Locally it is SQLite in the store's directory;
//! in the cluster it is Postgres, shared by every weir (and later the control plane), so weirs
//! agree on tables and epochs rise across them. Both backends hold the same rows and pass the
//! same tests (`tests/catalog.rs`).

mod postgres;
mod sqlite;

use std::path::Path;
use std::sync::Arc;

use arrow::datatypes::{Field, Schema, SchemaRef};
use kermodes_core::{parse_type, type_name};
use serde_json::Value;

use crate::{Error, SourceId, TableName};

/// The catalog's format; a newer one is a migration, not a silent reinterpretation.
const FORMAT: i64 = 1;

/// A table the catalog knows.
#[derive(Clone, Debug)]
pub struct TableEntry {
    pub id: i64,
    pub name: TableName,
    /// Where its data lives, relative to the store: `{branch}/{namespace}/{table}.{id}`, fixed
    /// when the table is created (architecture.md §5.1).
    pub location: String,
    pub schema: SchemaRef,
}

pub(crate) enum Catalog {
    Sqlite(sqlite::Sqlite),
    // Boxed: its client is several times the SQLite connection's size.
    Postgres(Box<postgres::Postgres>),
}

/// A table's row: id, namespace, name, location, and columns as stored.
type Row = (i64, String, String, String, String);

impl Catalog {
    pub(crate) fn sqlite(path: &Path) -> Result<Self, Error> {
        Ok(Self::Sqlite(sqlite::Sqlite::open(path)?))
    }

    pub(crate) fn postgres(url: &str) -> Result<Self, Error> {
        Ok(Self::Postgres(Box::new(postgres::Postgres::open(url)?)))
    }

    /// Creates `name` on `branch` with `schema`, or returns it if it exists with the same
    /// columns. A table with other columns is an error: schema changes are planned (§5.4).
    pub(crate) fn create(
        &mut self,
        branch: &str,
        name: &TableName,
        schema: &Schema,
    ) -> Result<TableEntry, Error> {
        if let Some(existing) = self.get(branch, name)? {
            return same_columns(name, existing, schema);
        }
        for field in schema.fields() {
            let kind = type_name(field.data_type());
            if parse_type(&kind).ok().as_ref() != Some(field.data_type()) {
                return Err(Error::Schema(format!(
                    "column `{}` of {name} is {kind}, which tables cannot store",
                    field.name()
                )));
            }
        }
        let columns = encode(schema).to_string();
        let inserted = match self {
            Self::Sqlite(c) => c.insert(branch, name, &columns)?,
            Self::Postgres(c) => c.insert(branch, name, &columns)?,
        };
        match inserted {
            Some((id, location)) => Ok(TableEntry {
                id,
                name: name.clone(),
                location,
                schema: Arc::new(nullable(schema)),
            }),
            // Another catalog client created it first: it must agree.
            None => {
                let existing = self.get(branch, name)?.ok_or_else(|| {
                    Error::Catalog(format!("{name} was created and then vanished"))
                })?;
                same_columns(name, existing, schema)
            }
        }
    }

    pub(crate) fn get(
        &mut self,
        branch: &str,
        name: &TableName,
    ) -> Result<Option<TableEntry>, Error> {
        let row = match self {
            Self::Sqlite(c) => c.get(branch, name)?,
            Self::Postgres(c) => c.get(branch, name)?,
        };
        row.map(|(id, location, columns)| {
            Ok(TableEntry {
                id,
                name: name.clone(),
                location,
                schema: Arc::new(decode(&columns)?),
            })
        })
        .transpose()
    }

    /// Every table on `branch`, by name.
    pub(crate) fn list(&mut self, branch: &str) -> Result<Vec<TableEntry>, Error> {
        let rows = match self {
            Self::Sqlite(c) => c.list(branch)?,
            Self::Postgres(c) => c.list(branch)?,
        };
        rows.into_iter()
            .map(|(id, namespace, table, location, columns)| {
                Ok(TableEntry {
                    id,
                    name: TableName::parse(&format!("{namespace}.{table}"))?,
                    location,
                    schema: Arc::new(decode(&columns)?),
                })
            })
            .collect()
    }

    /// Takes the next lease on a table's source: its epoch, one above the last ever taken.
    pub(crate) fn next_epoch(&mut self, table_id: i64, source: &SourceId) -> Result<u64, Error> {
        match self {
            Self::Sqlite(c) => c.next_epoch(table_id, source),
            Self::Postgres(c) => c.next_epoch(table_id, source),
        }
    }

    /// The epoch of a table source's latest lease; `None` before its first writer.
    pub(crate) fn epoch(&mut self, table_id: i64, source: &SourceId) -> Result<Option<u64>, Error> {
        match self {
            Self::Sqlite(c) => c.epoch(table_id, source),
            Self::Postgres(c) => c.epoch(table_id, source),
        }
    }
}

fn location(branch: &str, name: &TableName, id: i64) -> String {
    format!("{branch}/{}/{}.{id}", name.namespace(), name.table())
}

fn same_columns(
    name: &TableName,
    existing: TableEntry,
    schema: &Schema,
) -> Result<TableEntry, Error> {
    if columns(&existing.schema) != columns(schema) {
        return Err(Error::Schema(format!(
            "{name} already exists with columns ({}), not ({})",
            listed(&existing.schema),
            listed(schema)
        )));
    }
    Ok(existing)
}

/// Column names and type names, in order: what makes two definitions the same.
fn columns(schema: &Schema) -> Vec<(String, String)> {
    schema
        .fields()
        .iter()
        .map(|f| (f.name().clone(), type_name(f.data_type())))
        .collect()
}

fn listed(schema: &Schema) -> String {
    columns(schema)
        .iter()
        .map(|(name, kind)| format!("{name} {kind}"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn encode(schema: &Schema) -> Value {
    Value::Array(
        columns(schema)
            .into_iter()
            .map(|(name, kind)| serde_json::json!([name, kind]))
            .collect(),
    )
}

fn decode(text: &str) -> Result<Schema, Error> {
    let corrupt = || Error::Corrupt(format!("catalog columns `{text}` are unreadable"));
    let value: Value = serde_json::from_str(text).map_err(|_| corrupt())?;
    let fields = value
        .as_array()
        .ok_or_else(corrupt)?
        .iter()
        .map(|column| {
            let name = column[0].as_str().ok_or_else(corrupt)?;
            let kind = column[1].as_str().ok_or_else(corrupt)?;
            let data_type = parse_type(kind).map_err(|_| corrupt())?;
            Ok(Field::new(name, data_type, true))
        })
        .collect::<Result<Vec<_>, Error>>()?;
    Ok(Schema::new(fields))
}

fn nullable(schema: &Schema) -> Schema {
    Schema::new(
        schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone().with_nullable(true))
            .collect::<Vec<_>>(),
    )
}
