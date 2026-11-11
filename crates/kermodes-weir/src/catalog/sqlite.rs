//! The local catalog: SQLite in the store's directory (architecture.md §16.6).

use std::path::Path;

use rusqlite::{Connection, OptionalExtension, params};

use super::{FORMAT, Row, location};
use crate::{Error, SourceId, TableName};

pub(crate) struct Sqlite {
    conn: Connection,
}

impl Sqlite {
    pub(crate) fn open(path: &Path) -> Result<Self, Error> {
        let at = |e: rusqlite::Error| Error::Catalog(format!("{}: {e}", path.display()));
        let conn = Connection::open(path).map_err(at)?;
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
        .map_err(at)?;
        let format: Option<i64> = conn
            .query_row("SELECT value FROM meta WHERE key = 'format'", [], |row| {
                row.get(0)
            })
            .optional()
            .map_err(at)?;
        match format {
            None => {
                conn.execute("INSERT INTO meta VALUES ('format', ?1)", [FORMAT])
                    .map_err(at)?;
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

    /// Inserts a table, returning its id and location; one process owns a local store, so it
    /// never meets another's insert.
    pub(crate) fn insert(
        &mut self,
        branch: &str,
        name: &TableName,
        columns: &str,
    ) -> Result<Option<(i64, String)>, Error> {
        let tx = self.conn.transaction().map_err(sql)?;
        tx.execute(
            "INSERT INTO tables (branch, namespace, name, location, columns)
             VALUES (?1, ?2, ?3, '', ?4)",
            params![branch, name.namespace(), name.table(), columns],
        )
        .map_err(sql)?;
        let id = tx.last_insert_rowid();
        let location = location(branch, name, id);
        tx.execute(
            "UPDATE tables SET location = ?1 WHERE id = ?2",
            params![location, id],
        )
        .map_err(sql)?;
        tx.commit().map_err(sql)?;
        Ok(Some((id, location)))
    }

    /// A table's id, location, and columns.
    pub(crate) fn get(
        &mut self,
        branch: &str,
        name: &TableName,
    ) -> Result<Option<(i64, String, String)>, Error> {
        self.conn
            .query_row(
                "SELECT id, location, columns FROM tables
                 WHERE branch = ?1 AND namespace = ?2 AND name = ?3",
                params![branch, name.namespace(), name.table()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(sql)
    }

    pub(crate) fn list(&mut self, branch: &str) -> Result<Vec<Row>, Error> {
        let mut statement = self
            .conn
            .prepare(
                "SELECT id, namespace, name, location, columns FROM tables
                 WHERE branch = ?1 ORDER BY namespace, name",
            )
            .map_err(sql)?;
        statement
            .query_map([branch], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            })
            .map_err(sql)?
            .map(|row| row.map_err(sql))
            .collect()
    }

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
            .map_err(sql)
    }

    pub(crate) fn epoch(&mut self, table_id: i64, source: &SourceId) -> Result<Option<u64>, Error> {
        self.conn
            .query_row(
                "SELECT epoch FROM sources WHERE table_id = ?1 AND source = ?2",
                params![table_id, source.as_str()],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map(|epoch| epoch.map(|e| e as u64))
            .map_err(sql)
    }
}

fn sql(error: rusqlite::Error) -> Error {
    Error::Catalog(error.to_string())
}
