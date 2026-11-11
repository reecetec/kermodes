//! The local catalog (architecture.md §8.2): which tables exist, their IDs, storage locations, and
//! columns, in SQLite. The cluster's catalog is Postgres; both sit behind the control plane's
//! storage interface once it exists.

use std::path::Path;
use std::sync::Arc;

use arrow::datatypes::{Field, Schema, SchemaRef};
use kermodes_core::{parse_type, type_name};
use rusqlite::{Connection, OptionalExtension, params};
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

pub(crate) struct Catalog {
    conn: Connection,
}

impl Catalog {
    pub(crate) fn open(path: &Path) -> Result<Self, Error> {
        let conn = Connection::open(path).map_err(|e| Error::catalog(path, e))?;
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = FULL;
             CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value INTEGER NOT NULL);
             CREATE TABLE IF NOT EXISTS tables (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 branch TEXT NOT NULL,
                 namespace TEXT NOT NULL,
                 name TEXT NOT NULL,
                 location TEXT NOT NULL,
                 columns TEXT NOT NULL,
                 UNIQUE (branch, namespace, name)
             );
             -- Each source's lease epoch (§7.4). Added after format 1 shipped: a table of its
             -- own, so catalogs without it still open, and older versions ignore it.
             CREATE TABLE IF NOT EXISTS sources (
                 table_id INTEGER NOT NULL REFERENCES tables (id),
                 source TEXT NOT NULL,
                 epoch INTEGER NOT NULL,
                 PRIMARY KEY (table_id, source)
             );",
        )
        .map_err(|e| Error::catalog(path, e))?;
        let format: Option<i64> = conn
            .query_row("SELECT value FROM meta WHERE key = 'format'", [], |row| {
                row.get(0)
            })
            .optional()
            .map_err(|e| Error::catalog(path, e))?;
        match format {
            None => {
                conn.execute("INSERT INTO meta VALUES ('format', ?1)", [FORMAT])
                    .map_err(|e| Error::catalog(path, e))?;
            }
            Some(FORMAT) => {}
            Some(other) => {
                return Err(Error::Corrupt(format!(
                    "{}: catalog format {other}; this version reads {FORMAT}",
                    path.display()
                )));
            }
        }
        Ok(Self { conn })
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
            if columns(&existing.schema) != columns(schema) {
                return Err(Error::Schema(format!(
                    "{name} already exists with columns ({}), not ({})",
                    listed(&existing.schema),
                    listed(schema)
                )));
            }
            return Ok(existing);
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
        let tx = self.conn.transaction().map_err(Error::sql)?;
        tx.execute(
            "INSERT INTO tables (branch, namespace, name, location, columns)
             VALUES (?1, ?2, ?3, '', ?4)",
            params![branch, name.namespace(), name.table(), columns],
        )
        .map_err(Error::sql)?;
        let id = tx.last_insert_rowid();
        let location = format!("{branch}/{}/{}.{id}", name.namespace(), name.table());
        tx.execute(
            "UPDATE tables SET location = ?1 WHERE id = ?2",
            params![location, id],
        )
        .map_err(Error::sql)?;
        tx.commit().map_err(Error::sql)?;
        Ok(TableEntry {
            id,
            name: name.clone(),
            location,
            schema: Arc::new(nullable(schema)),
        })
    }

    pub(crate) fn get(&self, branch: &str, name: &TableName) -> Result<Option<TableEntry>, Error> {
        let row = self
            .conn
            .query_row(
                "SELECT id, location, columns FROM tables
                 WHERE branch = ?1 AND namespace = ?2 AND name = ?3",
                params![branch, name.namespace(), name.table()],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(Error::sql)?;
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
    pub(crate) fn list(&self, branch: &str) -> Result<Vec<TableEntry>, Error> {
        let mut statement = self
            .conn
            .prepare(
                "SELECT id, namespace, name, location, columns FROM tables
                 WHERE branch = ?1 ORDER BY namespace, name",
            )
            .map_err(Error::sql)?;
        let rows = statement
            .query_map([branch], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                ))
            })
            .map_err(Error::sql)?;
        let mut tables = Vec::new();
        for row in rows {
            let (id, namespace, table, location, columns) = row.map_err(Error::sql)?;
            tables.push(TableEntry {
                id,
                name: TableName::parse(&format!("{namespace}.{table}"))?,
                location,
                schema: Arc::new(decode(&columns)?),
            });
        }
        Ok(tables)
    }

    /// Takes the next lease on a table's source: its epoch, one above the last ever taken.
    pub(crate) fn next_epoch(&mut self, table_id: i64, source: &SourceId) -> Result<u64, Error> {
        self.conn
            .query_row(
                "INSERT INTO sources (table_id, source, epoch) VALUES (?1, ?2, 1)
                 ON CONFLICT (table_id, source) DO UPDATE SET epoch = epoch + 1
                 RETURNING epoch",
                params![table_id, source.as_str()],
                |row| row.get::<_, i64>(0),
            )
            .map(|epoch| epoch as u64)
            .map_err(Error::sql)
    }

    /// The epoch of a table source's latest lease; `None` before its first writer.
    pub(crate) fn epoch(&self, table_id: i64, source: &SourceId) -> Result<Option<u64>, Error> {
        self.conn
            .query_row(
                "SELECT epoch FROM sources WHERE table_id = ?1 AND source = ?2",
                params![table_id, source.as_str()],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map(|epoch| epoch.map(|e| e as u64))
            .map_err(Error::sql)
    }
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
