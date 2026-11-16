//! The local catalog: SQLite in the store's directory (architecture.md §16.6).

use std::path::Path;

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

use super::{
    FORMAT, Found, Garbage, HistoryCommit, HistoryDate, HistoryFile, Row, check_commit, location,
};
use crate::{Date, Error, SourceId, TableName};

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
             );
             -- A keyed table's key columns (§5.2), added after format 1 shipped, like `sources`.
             CREATE TABLE IF NOT EXISTS table_keys (
                 table_id INTEGER PRIMARY KEY REFERENCES tables (id),
                 key TEXT NOT NULL
             );
             -- An append table's sort columns (§8.1), added after format 1 shipped, like `sources`.
             CREATE TABLE IF NOT EXISTS table_sort (
                 table_id INTEGER PRIMARY KEY REFERENCES tables (id),
                 sort TEXT NOT NULL
             );
             -- History (§8), added after format 1 shipped, like `sources`: which files hold
             -- each table's history, which dates each holds, and files replaced but not yet
             -- deleted.
             CREATE TABLE IF NOT EXISTS history_files (
                 table_id INTEGER NOT NULL REFERENCES tables (id),
                 file TEXT NOT NULL,
                 first_date INTEGER NOT NULL,
                 last_date INTEGER NOT NULL,
                 rows INTEGER NOT NULL,
                 bytes INTEGER NOT NULL,
                 PRIMARY KEY (table_id, file)
             );
             CREATE TABLE IF NOT EXISTS history_dates (
                 table_id INTEGER NOT NULL,
                 date INTEGER NOT NULL,
                 file TEXT NOT NULL,
                 rows INTEGER NOT NULL,
                 bytes INTEGER NOT NULL,
                 PRIMARY KEY (table_id, date, file),
                 FOREIGN KEY (table_id, file) REFERENCES history_files (table_id, file)
             );
             CREATE TABLE IF NOT EXISTS history_garbage (
                 table_id INTEGER NOT NULL REFERENCES tables (id),
                 file TEXT NOT NULL,
                 since INTEGER NOT NULL,
                 PRIMARY KEY (table_id, file)
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
        key: Option<&str>,
        sort: Option<&str>,
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
        if let Some(key) = key {
            tx.execute(
                "INSERT INTO table_keys (table_id, key) VALUES (?1, ?2)",
                params![id, key],
            )
            .map_err(sql)?;
        }
        if let Some(sort) = sort {
            tx.execute(
                "INSERT INTO table_sort (table_id, sort) VALUES (?1, ?2)",
                params![id, sort],
            )
            .map_err(sql)?;
        }
        tx.commit().map_err(sql)?;
        Ok(Some((id, location)))
    }

    /// A table's id, location, columns, key columns, and sort columns.
    pub(crate) fn get(&mut self, branch: &str, name: &TableName) -> Result<Option<Found>, Error> {
        self.conn
            .query_row(
                "SELECT t.id, t.location, t.columns, k.key, s.sort FROM tables t
                 LEFT JOIN table_keys k ON k.table_id = t.id
                 LEFT JOIN table_sort s ON s.table_id = t.id
                 WHERE t.branch = ?1 AND t.namespace = ?2 AND t.name = ?3",
                params![branch, name.namespace(), name.table()],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .optional()
            .map_err(sql)
    }

    /// Replaces a table's columns if they are still `current`; whether they were.
    pub(crate) fn update_columns(
        &mut self,
        id: i64,
        current: &str,
        columns: &str,
    ) -> Result<bool, Error> {
        self.conn
            .execute(
                "UPDATE tables SET columns = ?1 WHERE id = ?2 AND columns = ?3",
                params![columns, id, current],
            )
            .map(|updated| updated == 1)
            .map_err(sql)
    }

    pub(crate) fn list(&mut self, branch: &str) -> Result<Vec<Row>, Error> {
        let mut statement = self
            .conn
            .prepare(
                "SELECT t.id, t.namespace, t.name, t.location, t.columns, k.key, s.sort
                 FROM tables t
                 LEFT JOIN table_keys k ON k.table_id = t.id
                 LEFT JOIN table_sort s ON s.table_id = t.id
                 WHERE t.branch = ?1 ORDER BY t.namespace, t.name",
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
                    row.get(5)?,
                    row.get(6)?,
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

    pub(crate) fn history(&mut self, table_id: i64) -> Result<Vec<HistoryDate>, Error> {
        history(&self.conn, table_id)
    }

    pub(crate) fn history_files(
        &mut self,
        table_id: i64,
        first: Date,
        last: Date,
    ) -> Result<Vec<HistoryFile>, Error> {
        let mut statement = self
            .conn
            .prepare(
                "SELECT file, first_date, last_date, rows, bytes FROM history_files
                 WHERE table_id = ?1 AND first_date <= ?3 AND last_date >= ?2
                 ORDER BY first_date, file",
            )
            .map_err(sql)?;
        statement
            .query_map(params![table_id, first.days(), last.days()], |row| {
                Ok(HistoryFile {
                    name: row.get(0)?,
                    first: Date::from_days(row.get(1)?),
                    last: Date::from_days(row.get(2)?),
                    rows: row.get::<_, i64>(3)? as u64,
                    bytes: row.get::<_, i64>(4)? as u64,
                })
            })
            .map_err(sql)?
            .map(|row| row.map_err(sql))
            .collect()
    }

    pub(crate) fn commit_history(
        &mut self,
        table_id: i64,
        commit: &HistoryCommit,
        now: i64,
    ) -> Result<(), Error> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql)?;
        let current = history(&tx, table_id)?;
        let files: Vec<String> = tx
            .prepare("SELECT file FROM history_files WHERE table_id = ?1")
            .map_err(sql)?
            .query_map([table_id], |row| row.get(0))
            .map_err(sql)?
            .collect::<Result<_, _>>()
            .map_err(sql)?;
        check_commit(&current, &files, commit)?;
        for file in &commit.replaced {
            tx.execute(
                "DELETE FROM history_dates WHERE table_id = ?1 AND file = ?2",
                params![table_id, file],
            )
            .map_err(sql)?;
            tx.execute(
                "DELETE FROM history_files WHERE table_id = ?1 AND file = ?2",
                params![table_id, file],
            )
            .map_err(sql)?;
            tx.execute(
                "INSERT INTO history_garbage (table_id, file, since) VALUES (?1, ?2, ?3)",
                params![table_id, file, now],
            )
            .map_err(sql)?;
        }
        for file in &commit.files {
            tx.execute(
                "INSERT INTO history_files (table_id, file, first_date, last_date, rows, bytes)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    table_id,
                    file.name,
                    file.first.days(),
                    file.last.days(),
                    file.rows as i64,
                    file.bytes as i64
                ],
            )
            .map_err(sql)?;
        }
        for date in &commit.dates {
            tx.execute(
                "INSERT INTO history_dates (table_id, date, file, rows, bytes)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    table_id,
                    date.date.days(),
                    date.file,
                    date.rows as i64,
                    date.bytes as i64
                ],
            )
            .map_err(sql)?;
        }
        tx.commit().map_err(sql)
    }

    pub(crate) fn garbage(&mut self, before: i64) -> Result<Vec<Garbage>, Error> {
        let mut statement = self
            .conn
            .prepare(
                "SELECT g.table_id, t.location, g.file FROM history_garbage g
                 JOIN tables t ON t.id = g.table_id
                 WHERE g.since <= ?1 ORDER BY g.since, g.file",
            )
            .map_err(sql)?;
        statement
            .query_map([before], |row| {
                Ok(Garbage {
                    table_id: row.get(0)?,
                    location: row.get(1)?,
                    file: row.get(2)?,
                })
            })
            .map_err(sql)?
            .map(|row| row.map_err(sql))
            .collect()
    }

    pub(crate) fn forget_garbage(&mut self, garbage: &Garbage) -> Result<(), Error> {
        self.conn
            .execute(
                "DELETE FROM history_garbage WHERE table_id = ?1 AND file = ?2",
                params![garbage.table_id, garbage.file],
            )
            .map(|_| ())
            .map_err(sql)
    }
}

fn history(conn: &Connection, table_id: i64) -> Result<Vec<HistoryDate>, Error> {
    let mut statement = conn
        .prepare(
            "SELECT date, file, rows, bytes FROM history_dates
             WHERE table_id = ?1 ORDER BY date, file",
        )
        .map_err(sql)?;
    statement
        .query_map([table_id], |row| {
            Ok(HistoryDate {
                date: Date::from_days(row.get(0)?),
                file: row.get(1)?,
                rows: row.get::<_, i64>(2)? as u64,
                bytes: row.get::<_, i64>(3)? as u64,
            })
        })
        .map_err(sql)?
        .map(|row| row.map_err(sql))
        .collect()
}

fn sql(error: rusqlite::Error) -> Error {
    Error::Catalog(error.to_string())
}
