//! The catalog (architecture.md §8.2): which tables exist, their IDs, storage locations, and
//! columns, and which files hold each table's history (§8). Locally it is SQLite in the store's
//! directory; in the cluster it is Postgres, shared by every weir (and later the control plane),
//! so weirs agree on tables.
//! Both backends hold the same rows and pass the same tests (`tests/catalog.rs`).

mod postgres;
mod sqlite;

use std::path::Path;
use std::sync::Arc;

use arrow::datatypes::{Field, Schema, SchemaRef};
use kermodes_core::{parse_type, type_name};
use serde_json::Value;

use crate::shape::{ColumnId, Shape, former, widens};
use crate::{Date, Error, TableName};

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
    /// Each column's ID (§5.4), in `schema`'s order.
    pub ids: Arc<[ColumnId]>,
    /// The names each column had before, in `schema`'s order: a rename keeps the column's ID.
    pub was: Vec<Vec<String>>,
    /// A keyed table's key columns (§5.2), fixed when it is created; empty for an append table.
    pub key: Vec<String>,
    /// The columns history sorts an append table's dates by (§8.1), as it was created; empty
    /// for none, and for a keyed table, whose history is its log in commit order.
    pub sort: Vec<String>,
}

impl TableEntry {
    /// The table's columns now, with their IDs.
    pub fn shape(&self) -> Shape {
        Shape::new(self.schema.clone(), self.ids.clone())
    }
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

/// A table's row: id, namespace, name, location, columns, and key and sort columns as stored.
type Row = (
    i64,
    String,
    String,
    String,
    String,
    Option<String>,
    Option<String>,
);

/// A table found by name: id, location, columns, and key and sort columns as stored.
type Found = (i64, String, String, Option<String>, Option<String>);

impl Catalog {
    pub(crate) fn sqlite(path: &Path) -> Result<Self, Error> {
        Ok(Self::Sqlite(sqlite::Sqlite::open(path)?))
    }

    pub(crate) fn postgres(url: &str) -> Result<Self, Error> {
        Ok(Self::Postgres(Box::new(postgres::Postgres::open(url)?)))
    }

    /// Creates `name` on `branch` with `schema`, keyed by `key` (empty for an append table) or,
    /// for an append table, sorted in history by `sort`. One that exists is brought to
    /// `schema`'s columns where that is a compatible change (§5.4: `synced`), and returned; its
    /// key and sort must be the same.
    pub(crate) fn create(
        &mut self,
        branch: &str,
        name: &TableName,
        schema: &Schema,
        key: &[String],
        sort: &[String],
    ) -> Result<TableEntry, Error> {
        if !key.is_empty() && !sort.is_empty() {
            return Err(Error::Schema(format!(
                "{name} is keyed: its history is its log, kept in commit order"
            )));
        }
        if let Some(found) = self.found(branch, name)? {
            return self.sync(branch, name, found, schema, key, sort);
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
        named_columns(name, schema, "key", key)?;
        named_columns(name, schema, "sort", sort)?;
        let schema = plain(schema);
        let ids: Arc<[ColumnId]> = (1..=schema.fields().len() as u32).map(ColumnId).collect();
        let was = vec![Vec::new(); ids.len()];
        let columns = encode(&schema, &ids, &was).to_string();
        let json =
            |names: &[String]| (!names.is_empty()).then(|| serde_json::json!(names).to_string());
        let (key_json, sort_json) = (json(key), json(sort));
        let inserted = match self {
            Self::Sqlite(c) => c.insert(
                branch,
                name,
                &columns,
                key_json.as_deref(),
                sort_json.as_deref(),
            )?,
            Self::Postgres(c) => c.insert(
                branch,
                name,
                &columns,
                key_json.as_deref(),
                sort_json.as_deref(),
            )?,
        };
        match inserted {
            Some((id, location)) => Ok(TableEntry {
                id,
                name: name.clone(),
                location,
                schema: Arc::new(schema),
                ids,
                was,
                key: key.to_vec(),
                sort: sort.to_vec(),
            }),
            // Another catalog client created it first: it must agree.
            None => {
                let found = self.found(branch, name)?.ok_or_else(|| {
                    Error::Catalog(format!("{name} was created and then vanished"))
                })?;
                self.sync(branch, name, found, &schema, key, sort)
            }
        }
    }

    /// Brings `found`'s columns to `schema`'s, if they differ: written only if no one changed
    /// them meanwhile, else worked out again from what they became.
    fn sync(
        &mut self,
        branch: &str,
        name: &TableName,
        found: (TableEntry, String),
        schema: &Schema,
        key: &[String],
        sort: &[String],
    ) -> Result<TableEntry, Error> {
        let (mut existing, mut stored) = found;
        for _ in 0..3 {
            same_layout(name, &existing, key, sort)?;
            let Some((schema, ids, was)) = synced(name, &existing, schema)? else {
                return Ok(existing);
            };
            let columns = encode(&schema, &ids, &was).to_string();
            let updated = match self {
                Self::Sqlite(c) => c.update_columns(existing.id, &stored, &columns)?,
                Self::Postgres(c) => c.update_columns(existing.id, &stored, &columns)?,
            };
            if updated {
                return Ok(TableEntry {
                    schema: Arc::new(schema),
                    ids,
                    was,
                    ..existing
                });
            }
            (existing, stored) = self.found(branch, name)?.ok_or_else(|| {
                Error::Catalog(format!("{name} vanished while its columns changed"))
            })?;
        }
        Err(Error::Busy(format!(
            "{name}'s columns kept changing under this change; try again"
        )))
    }

    pub(crate) fn get(
        &mut self,
        branch: &str,
        name: &TableName,
    ) -> Result<Option<TableEntry>, Error> {
        Ok(self.found(branch, name)?.map(|(entry, _)| entry))
    }

    /// A table, and its columns as stored: what a change of them must find still there.
    fn found(
        &mut self,
        branch: &str,
        name: &TableName,
    ) -> Result<Option<(TableEntry, String)>, Error> {
        let row = match self {
            Self::Sqlite(c) => c.get(branch, name)?,
            Self::Postgres(c) => c.get(branch, name)?,
        };
        row.map(|(id, location, columns, key, sort)| {
            let (schema, ids, was) = decode(&columns)?;
            let entry = TableEntry {
                id,
                name: name.clone(),
                location,
                schema: Arc::new(schema),
                ids,
                was,
                key: decode_names(key.as_deref())?,
                sort: decode_names(sort.as_deref())?,
            };
            Ok((entry, columns))
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
            .map(|(id, namespace, table, location, columns, key, sort)| {
                let (schema, ids, was) = decode(&columns)?;
                Ok(TableEntry {
                    id,
                    name: TableName::parse(&format!("{namespace}.{table}"))?,
                    location,
                    schema: Arc::new(schema),
                    ids,
                    was,
                    key: decode_names(key.as_deref())?,
                    sort: decode_names(sort.as_deref())?,
                })
            })
            .collect()
    }

    /// Whether replica `replica` of `branch`'s shard has had a log (`start_replica`): from then
    /// on, finding none, it has lost it, and whom it voted for and what it acknowledged with it.
    pub(crate) fn replica_started(&mut self, branch: &str, replica: u64) -> Result<bool, Error> {
        match self {
            Self::Sqlite(c) => c.replica_started(branch, replica),
            Self::Postgres(c) => c.replica_started(branch, replica),
        }
    }

    /// Records that replica `replica` of `branch`'s shard has a log (`replica_started`).
    pub(crate) fn start_replica(&mut self, branch: &str, replica: u64) -> Result<(), Error> {
        match self {
            Self::Sqlite(c) => c.start_replica(branch, replica),
            Self::Postgres(c) => c.start_replica(branch, replica),
        }
    }

    /// The next incarnation of slot `slot` of `branch`'s shard, from 1: a replica whose disk was
    /// lost or replaced joins as it, a new member (`replica_started`).
    pub(crate) fn next_incarnation(&mut self, branch: &str, slot: u64) -> Result<u64, Error> {
        match self {
            Self::Sqlite(c) => c.next_incarnation(branch, slot),
            Self::Postgres(c) => c.next_incarnation(branch, slot),
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

/// The key in the catalog's metadata that says a replica of `branch`'s shard has had a log
/// (`replica_started`).
fn started_key(branch: &str, replica: u64) -> String {
    format!("replica started: {branch} {replica}")
}

/// The key in the catalog's metadata counting a slot's incarnations (`next_incarnation`).
fn incarnation_key(branch: &str, slot: u64) -> String {
    format!("replica incarnation: {branch} {slot}")
}

/// Refuses a definition whose key or sort is not the table's: those are fixed when it is
/// created.
fn same_layout(
    name: &TableName,
    existing: &TableEntry,
    key: &[String],
    sort: &[String],
) -> Result<(), Error> {
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
    if existing.sort != sort {
        let sorted = |sort: &[String]| match sort {
            [] => "unsorted".to_owned(),
            sort => format!("sorted by ({})", sort.join(", ")),
        };
        return Err(Error::Schema(format!(
            "{name} already exists {}, not {}",
            sorted(&existing.sort),
            sorted(sort)
        )));
    }
    Ok(())
}

/// The columns `existing` takes to be `desired`'s (§5.4), `None` when it has them already.
/// Each desired column is the existing one of its name, or else of a name it says it was (a
/// rename), keeping that column's ID; or a new column, with an ID never used before. A type
/// may only widen (`widens`). Leaving a column out would drop it, and a key or sort column
/// cannot be renamed or retyped (keys hash, and history files are sorted, by them): those are
/// refused, naming the change.
fn synced(
    name: &TableName,
    existing: &TableEntry,
    desired: &Schema,
) -> Result<Option<Columns>, Error> {
    let current = &existing.schema;
    let mut taken = vec![false; current.fields().len()];
    let mut next = existing.ids.iter().map(|id| id.0).max().unwrap_or(0) + 1;
    let (mut fields, mut ids, mut was) = (Vec::new(), Vec::new(), Vec::new());
    for field in desired.fields() {
        let declared = Field::new(field.name(), field.data_type().clone(), true);
        let at = current.index_of(field.name()).ok().or_else(|| {
            former(field)
                .iter()
                .find_map(|old| current.index_of(old).ok())
        });
        let Some(at) = at else {
            fields.push(declared);
            ids.push(ColumnId(next));
            was.push(Vec::new());
            next += 1;
            continue;
        };
        let column = current.field(at);
        if std::mem::replace(&mut taken[at], true) {
            return Err(Error::Schema(format!(
                "{name}: two of the columns given are its column `{}`",
                column.name()
            )));
        }
        let fixed = [("key", &existing.key), ("sort", &existing.sort)]
            .into_iter()
            .find(|(_, names)| names.contains(column.name()))
            .map(|(what, _)| what);
        let renamed = column.name() != field.name();
        let retyped = column.data_type() != field.data_type();
        if let Some(what) = fixed.filter(|_| renamed || retyped) {
            return Err(Error::Schema(format!(
                "{name}'s {what} column `{}` cannot be renamed or change type: that is a new \
                 table",
                column.name()
            )));
        }
        if retyped && !widens(column.data_type(), field.data_type()) {
            return Err(Error::Schema(format!(
                "{name}'s column `{}` is {}: making it {} is not a compatible change (int32 \
                 widens to int64 or float64; anything else is a new table)",
                column.name(),
                type_name(column.data_type()),
                type_name(field.data_type())
            )));
        }
        let mut names = existing.was[at].clone();
        if renamed {
            names.push(column.name().clone());
        }
        names.retain(|old| old != field.name());
        fields.push(declared);
        ids.push(existing.ids[at]);
        was.push(names);
    }
    if let Some(at) = taken.iter().position(|taken| !taken) {
        return Err(Error::Schema(format!(
            "{name} has a column `{}` the columns given leave out: dropping a column is not a \
             compatible change; keep it, or rename it with `was`",
            current.field(at).name()
        )));
    }
    let schema = Schema::new(fields);
    if columns(&schema) == columns(current) && ids[..] == existing.ids[..] && was == existing.was {
        return Ok(None);
    }
    Ok(Some((schema, ids.into(), was)))
}

/// Checks that `names` (a table's key or sort) are columns of `schema`, each once.
fn named_columns(
    name: &TableName,
    schema: &Schema,
    what: &str,
    names: &[String],
) -> Result<(), Error> {
    for (i, column) in names.iter().enumerate() {
        if schema.column_with_name(column).is_none() {
            return Err(Error::Schema(format!(
                "{name}'s {what} column `{column}` is not one of its columns ({})",
                listed(schema)
            )));
        }
        if names[..i].contains(column) {
            return Err(Error::Schema(format!(
                "{name}'s {what} names `{column}` twice"
            )));
        }
    }
    Ok(())
}

fn decode_names(text: Option<&str>) -> Result<Vec<String>, Error> {
    text.map_or(Ok(Vec::new()), |text| {
        serde_json::from_str(text)
            .map_err(|_| Error::Corrupt(format!("catalog column names `{text}` are unreadable")))
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

/// `[[name, type, id, [former names]], ...]`, the former names left out when there are none.
/// Readers from before column IDs read the name and type of each and ignore the rest.
fn encode(schema: &Schema, ids: &[ColumnId], was: &[Vec<String>]) -> Value {
    Value::Array(
        columns(schema)
            .into_iter()
            .zip(ids.iter().zip(was))
            .map(|((name, kind), (id, was))| match was.as_slice() {
                [] => serde_json::json!([name, kind, id.0]),
                was => serde_json::json!([name, kind, id.0, was]),
            })
            .collect(),
    )
}

/// A table's columns as the catalog keeps them: schema, each column's ID, its former names.
type Columns = (Schema, Arc<[ColumnId]>, Vec<Vec<String>>);

/// A table's columns, IDs, and former names; one stored before columns had IDs has their
/// positions, counting from 1, and no former names.
fn decode(text: &str) -> Result<Columns, Error> {
    let corrupt = || Error::Corrupt(format!("catalog columns `{text}` are unreadable"));
    let value: Value = serde_json::from_str(text).map_err(|_| corrupt())?;
    let (mut fields, mut ids, mut was) = (Vec::new(), Vec::new(), Vec::new());
    for (position, column) in value.as_array().ok_or_else(corrupt)?.iter().enumerate() {
        let name = column[0].as_str().ok_or_else(corrupt)?;
        let kind = column[1].as_str().ok_or_else(corrupt)?;
        let data_type = parse_type(kind).map_err(|_| corrupt())?;
        fields.push(Field::new(name, data_type, true));
        ids.push(match &column[2] {
            Value::Null => ColumnId(position as u32 + 1),
            id => ColumnId(
                id.as_u64()
                    .and_then(|id| u32::try_from(id).ok())
                    .ok_or_else(corrupt)?,
            ),
        });
        was.push(match &column[3] {
            Value::Null => Vec::new(),
            names => serde_json::from_value(names.clone()).map_err(|_| corrupt())?,
        });
    }
    Ok((Schema::new(fields), ids.into(), was))
}

/// `schema`'s columns as a table has them: every column nullable, and nothing beside its name
/// and type (a declaration's former names are the catalog's to keep, not the schema's).
fn plain(schema: &Schema) -> Schema {
    Schema::new(
        schema
            .fields()
            .iter()
            .map(|f| Field::new(f.name(), f.data_type().clone(), true))
            .collect::<Vec<_>>(),
    )
}
