//! The catalog (architecture.md §8.2): which tables exist, their IDs, storage locations, and
//! columns, each source's lease epoch (§7.4), and which files hold each table's history (§8).
//! Locally it is SQLite in the store's directory; in the cluster it is Postgres, shared by every
//! weir (and later the control plane), so weirs agree on tables and epochs rise across them.
//! Both backends hold the same rows and pass the same tests (`tests/catalog.rs`).

mod postgres;
mod sqlite;

use std::path::Path;
use std::sync::Arc;

use arrow::datatypes::{Field, Schema, SchemaRef};
use kermodes_core::{parse_type, type_name};
use serde_json::Value;

use crate::{Date, Error, SourceId, TableName};

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
    /// A keyed table's key columns (§5.2), fixed when it is created; empty for an append table.
    pub key: Vec<String>,
}

/// A file of a table's history: its name within the table's location, the dates it holds, and
/// its size.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoryFile {
    pub name: String,
    pub first: Date,
    pub last: Date,
    pub rows: u64,
    pub bytes: u64,
}

/// A date of a table's history in one of its files, with its rows and bytes there; a date past
/// the target file size lies in several.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoryDate {
    pub date: Date,
    pub file: String,
    pub rows: u64,
    pub bytes: u64,
}

/// One commit of a table's history: the whole dates written (each replacing what the history
/// held for it; one with no rows in `dates` is removed), the files replaced, and the new files
/// with their dates.
#[derive(Clone, Debug, Default)]
pub struct HistoryCommit {
    pub written: Vec<Date>,
    pub replaced: Vec<String>,
    pub files: Vec<HistoryFile>,
    pub dates: Vec<HistoryDate>,
}

/// A file replaced by a commit, deleted once no read can still be using it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Garbage {
    pub table_id: i64,
    /// The table's location, which the file's name is within.
    pub location: String,
    pub file: String,
}

pub(crate) enum Catalog {
    Sqlite(sqlite::Sqlite),
    // Boxed: its client is several times the SQLite connection's size.
    Postgres(Box<postgres::Postgres>),
}

/// A table's row: id, namespace, name, location, columns, and key columns as stored.
type Row = (i64, String, String, String, String, Option<String>);

/// A table found by name: id, location, columns, and key columns as stored.
type Found = (i64, String, String, Option<String>);

impl Catalog {
    pub(crate) fn sqlite(path: &Path) -> Result<Self, Error> {
        Ok(Self::Sqlite(sqlite::Sqlite::open(path)?))
    }

    pub(crate) fn postgres(url: &str) -> Result<Self, Error> {
        Ok(Self::Postgres(Box::new(postgres::Postgres::open(url)?)))
    }

    /// Creates `name` on `branch` with `schema`, keyed by `key` (empty for an append table), or
    /// returns it if it exists with the same columns and key. A table with other columns or
    /// another key is an error: schema changes are planned (§5.4).
    pub(crate) fn create(
        &mut self,
        branch: &str,
        name: &TableName,
        schema: &Schema,
        key: &[String],
    ) -> Result<TableEntry, Error> {
        if let Some(existing) = self.get(branch, name)? {
            return same_definition(name, existing, schema, key);
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
        for (i, column) in key.iter().enumerate() {
            if schema.column_with_name(column).is_none() {
                return Err(Error::Schema(format!(
                    "{name}'s key column `{column}` is not one of its columns ({})",
                    listed(schema)
                )));
            }
            if key[..i].contains(column) {
                return Err(Error::Schema(format!(
                    "{name}'s key names `{column}` twice"
                )));
            }
        }
        let columns = encode(schema).to_string();
        let key_json = (!key.is_empty()).then(|| serde_json::json!(key).to_string());
        let inserted = match self {
            Self::Sqlite(c) => c.insert(branch, name, &columns, key_json.as_deref())?,
            Self::Postgres(c) => c.insert(branch, name, &columns, key_json.as_deref())?,
        };
        match inserted {
            Some((id, location)) => Ok(TableEntry {
                id,
                name: name.clone(),
                location,
                schema: Arc::new(nullable(schema)),
                key: key.to_vec(),
            }),
            // Another catalog client created it first: it must agree.
            None => {
                let existing = self.get(branch, name)?.ok_or_else(|| {
                    Error::Catalog(format!("{name} was created and then vanished"))
                })?;
                same_definition(name, existing, schema, key)
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
        row.map(|(id, location, columns, key)| {
            Ok(TableEntry {
                id,
                name: name.clone(),
                location,
                schema: Arc::new(decode(&columns)?),
                key: decode_key(key.as_deref())?,
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
            .map(|(id, namespace, table, location, columns, key)| {
                Ok(TableEntry {
                    id,
                    name: TableName::parse(&format!("{namespace}.{table}"))?,
                    location,
                    schema: Arc::new(decode(&columns)?),
                    key: decode_key(key.as_deref())?,
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

    /// Every date of a table's history, by date then file.
    pub(crate) fn history(&mut self, table_id: i64) -> Result<Vec<HistoryDate>, Error> {
        match self {
            Self::Sqlite(c) => c.history(table_id),
            Self::Postgres(c) => c.history(table_id),
        }
    }

    /// The files of a table's history holding any date from `first` to `last`, by first date.
    pub(crate) fn history_files(
        &mut self,
        table_id: i64,
        first: Date,
        last: Date,
    ) -> Result<Vec<HistoryFile>, Error> {
        match self {
            Self::Sqlite(c) => c.history_files(table_id, first, last),
            Self::Postgres(c) => c.history_files(table_id, first, last),
        }
    }

    /// Commits `commit` in one transaction, checked against the history as it is then
    /// (`check_commit`); the replaced files become garbage as of `now` (seconds since the epoch).
    pub(crate) fn commit_history(
        &mut self,
        table_id: i64,
        commit: &HistoryCommit,
        now: i64,
    ) -> Result<(), Error> {
        match self {
            Self::Sqlite(c) => c.commit_history(table_id, commit, now),
            Self::Postgres(c) => c.commit_history(table_id, commit, now),
        }
    }

    /// Files replaced at or before `before` (seconds since the epoch), across every table.
    pub(crate) fn garbage(&mut self, before: i64) -> Result<Vec<Garbage>, Error> {
        match self {
            Self::Sqlite(c) => c.garbage(before),
            Self::Postgres(c) => c.garbage(before),
        }
    }

    /// Forgets a garbage file once it is deleted.
    pub(crate) fn forget_garbage(&mut self, garbage: &Garbage) -> Result<(), Error> {
        match self {
            Self::Sqlite(c) => c.forget_garbage(garbage),
            Self::Postgres(c) => c.forget_garbage(garbage),
        }
    }
}

/// Whether `commit` may apply to a history holding `current` (every date, in its files) in
/// `files`. A commit planned on a history that has since changed is `Busy`: plan it again. One
/// that is wrong in itself is refused outright.
fn check_commit(
    current: &[HistoryDate],
    files: &[String],
    commit: &HistoryCommit,
) -> Result<(), Error> {
    let changed = |why: String| Error::Busy(format!("the history changed since the plan: {why}"));
    for file in &commit.replaced {
        if !files.contains(file) {
            return Err(changed(format!("{file} is no longer in it")));
        }
    }
    // Each date in one place: a date the commit touches leaves every file holding it, and a date
    // a replaced file holds is written or rewritten, never lost.
    for held in current {
        let replaced = commit.replaced.contains(&held.file);
        let touched =
            commit.written.contains(&held.date) || commit.dates.iter().any(|d| d.date == held.date);
        if touched && !replaced {
            return Err(changed(format!(
                "{} is also in {}, which the commit keeps",
                held.date, held.file
            )));
        }
        if replaced && !touched {
            return Err(Error::Catalog(format!(
                "a commit replacing {} must rewrite its date {}",
                held.file, held.date
            )));
        }
    }
    for date in &commit.dates {
        let file = commit.files.iter().find(|f| f.name == date.file);
        if !file.is_some_and(|f| f.first <= date.date && date.date <= f.last) {
            return Err(Error::Catalog(format!(
                "{} of a commit is not within a file it adds ({})",
                date.date, date.file
            )));
        }
    }
    for file in &commit.files {
        if files.contains(&file.name) {
            return Err(Error::Catalog(format!(
                "{} is already in the history",
                file.name
            )));
        }
    }
    Ok(())
}

fn location(branch: &str, name: &TableName, id: i64) -> String {
    format!("{branch}/{}/{}.{id}", name.namespace(), name.table())
}

fn same_definition(
    name: &TableName,
    existing: TableEntry,
    schema: &Schema,
    key: &[String],
) -> Result<TableEntry, Error> {
    if columns(&existing.schema) != columns(schema) {
        return Err(Error::Schema(format!(
            "{name} already exists with columns ({}), not ({})",
            listed(&existing.schema),
            listed(schema)
        )));
    }
    if existing.key != key {
        let keyed = |key: &[String]| match key {
            [] => "as an append table".to_owned(),
            key => format!("keyed by ({})", key.join(", ")),
        };
        return Err(Error::Schema(format!(
            "{name} already exists {}, not {}",
            keyed(&existing.key),
            keyed(key)
        )));
    }
    Ok(existing)
}

fn decode_key(text: Option<&str>) -> Result<Vec<String>, Error> {
    text.map_or(Ok(Vec::new()), |text| {
        serde_json::from_str(text)
            .map_err(|_| Error::Corrupt(format!("catalog key columns `{text}` are unreadable")))
    })
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
