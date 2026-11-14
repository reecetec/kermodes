//! The cluster's catalog: Postgres, shared by every weir (architecture.md §8.2). Concurrent
//! clients agree because Postgres decides: a table is created once (`ON CONFLICT DO NOTHING`, the
//! loser reads the winner's), and an epoch is bumped atomically (`ON CONFLICT DO UPDATE ...
//! RETURNING`).

use postgres::{Client, GenericClient, NoTls};

use super::{
    FORMAT, Garbage, HistoryCommit, HistoryDate, HistoryFile, Row, check_commit, location,
};
use crate::{Date, Error, SourceId, TableName};

/// Held while creating the catalog's tables, so clients starting together do not collide on
/// `CREATE TABLE IF NOT EXISTS` (which is not atomic in Postgres).
const SCHEMA_LOCK: i64 = 0x6b65_726d_6f64_6573; // "kermodes"

pub(crate) struct Postgres {
    url: String,
    client: Client,
}

impl Postgres {
    pub(crate) fn open(url: &str) -> Result<Self, Error> {
        let mut client = connect(url)?;
        let mut tx = client.transaction().map_err(sql)?;
        tx.execute("SELECT pg_advisory_xact_lock($1)", &[&SCHEMA_LOCK])
            .map_err(sql)?;
        tx.batch_execute(
            "CREATE TABLE IF NOT EXISTS kermodes_meta (key TEXT PRIMARY KEY, value BIGINT NOT NULL);
             CREATE TABLE IF NOT EXISTS kermodes_tables (
                 id BIGSERIAL PRIMARY KEY,
                 branch TEXT NOT NULL,
                 namespace TEXT NOT NULL,
                 name TEXT NOT NULL,
                 location TEXT NOT NULL,
                 columns TEXT NOT NULL,
                 UNIQUE (branch, namespace, name)
             );
             CREATE TABLE IF NOT EXISTS kermodes_sources (
                 table_id BIGINT NOT NULL REFERENCES kermodes_tables (id),
                 source TEXT NOT NULL,
                 epoch BIGINT NOT NULL,
                 PRIMARY KEY (table_id, source)
             );
             CREATE TABLE IF NOT EXISTS kermodes_history_files (
                 table_id BIGINT NOT NULL REFERENCES kermodes_tables (id),
                 file TEXT NOT NULL,
                 first_date INTEGER NOT NULL,
                 last_date INTEGER NOT NULL,
                 rows BIGINT NOT NULL,
                 bytes BIGINT NOT NULL,
                 PRIMARY KEY (table_id, file)
             );
             CREATE TABLE IF NOT EXISTS kermodes_history_dates (
                 table_id BIGINT NOT NULL,
                 date INTEGER NOT NULL,
                 file TEXT NOT NULL,
                 rows BIGINT NOT NULL,
                 bytes BIGINT NOT NULL,
                 PRIMARY KEY (table_id, date, file),
                 FOREIGN KEY (table_id, file) REFERENCES kermodes_history_files (table_id, file)
             );
             CREATE TABLE IF NOT EXISTS kermodes_history_garbage (
                 table_id BIGINT NOT NULL REFERENCES kermodes_tables (id),
                 file TEXT NOT NULL,
                 since BIGINT NOT NULL,
                 PRIMARY KEY (table_id, file)
             );
             INSERT INTO kermodes_meta VALUES ('format', 1) ON CONFLICT (key) DO NOTHING;",
        )
        .map_err(sql)?;
        let format: i64 = tx
            .query_one("SELECT value FROM kermodes_meta WHERE key = 'format'", &[])
            .map_err(sql)?
            .get(0);
        tx.commit().map_err(sql)?;
        if format != FORMAT {
            return Err(Error::Corrupt(format!(
                "catalog format {format}; this version reads {FORMAT}"
            )));
        }
        Ok(Self {
            url: url.to_owned(),
            client,
        })
    }

    /// Inserts a table, returning its id and location, or `None` if another client created it
    /// first.
    pub(crate) fn insert(
        &mut self,
        branch: &str,
        name: &TableName,
        columns: &str,
    ) -> Result<Option<(i64, String)>, Error> {
        let mut tx = self.client()?.transaction().map_err(sql)?;
        let Some(row) = tx
            .query_opt(
                "INSERT INTO kermodes_tables (branch, namespace, name, location, columns)
                 VALUES ($1, $2, $3, '', $4)
                 ON CONFLICT (branch, namespace, name) DO NOTHING
                 RETURNING id",
                &[&branch, &name.namespace(), &name.table(), &columns],
            )
            .map_err(sql)?
        else {
            return Ok(None);
        };
        let id: i64 = row.get(0);
        let location = location(branch, name, id);
        tx.execute(
            "UPDATE kermodes_tables SET location = $1 WHERE id = $2",
            &[&location, &id],
        )
        .map_err(sql)?;
        tx.commit().map_err(sql)?;
        Ok(Some((id, location)))
    }

    pub(crate) fn get(
        &mut self,
        branch: &str,
        name: &TableName,
    ) -> Result<Option<(i64, String, String)>, Error> {
        Ok(self
            .client()?
            .query_opt(
                "SELECT id, location, columns FROM kermodes_tables
                 WHERE branch = $1 AND namespace = $2 AND name = $3",
                &[&branch, &name.namespace(), &name.table()],
            )
            .map_err(sql)?
            .map(|row| (row.get(0), row.get(1), row.get(2))))
    }

    pub(crate) fn list(&mut self, branch: &str) -> Result<Vec<Row>, Error> {
        Ok(self
            .client()?
            .query(
                "SELECT id, namespace, name, location, columns FROM kermodes_tables
                 WHERE branch = $1 ORDER BY namespace, name",
                &[&branch],
            )
            .map_err(sql)?
            .iter()
            .map(|row| (row.get(0), row.get(1), row.get(2), row.get(3), row.get(4)))
            .collect())
    }

    pub(crate) fn next_epoch(&mut self, table_id: i64, source: &SourceId) -> Result<u64, Error> {
        let epoch: i64 = self
            .client()?
            .query_one(
                "INSERT INTO kermodes_sources (table_id, source, epoch) VALUES ($1, $2, 1)
                 ON CONFLICT (table_id, source)
                 DO UPDATE SET epoch = kermodes_sources.epoch + 1
                 RETURNING epoch",
                &[&table_id, &source.as_str()],
            )
            .map_err(sql)?
            .get(0);
        Ok(epoch as u64)
    }

    pub(crate) fn epoch(&mut self, table_id: i64, source: &SourceId) -> Result<Option<u64>, Error> {
        Ok(self
            .client()?
            .query_opt(
                "SELECT epoch FROM kermodes_sources WHERE table_id = $1 AND source = $2",
                &[&table_id, &source.as_str()],
            )
            .map_err(sql)?
            .map(|row| row.get::<_, i64>(0) as u64))
    }

    pub(crate) fn history(&mut self, table_id: i64) -> Result<Vec<HistoryDate>, Error> {
        history(self.client()?, table_id)
    }

    pub(crate) fn history_files(
        &mut self,
        table_id: i64,
        first: Date,
        last: Date,
    ) -> Result<Vec<HistoryFile>, Error> {
        Ok(self
            .client()?
            .query(
                "SELECT file, first_date, last_date, rows, bytes FROM kermodes_history_files
                 WHERE table_id = $1 AND first_date <= $3 AND last_date >= $2
                 ORDER BY first_date, file",
                &[&table_id, &first.days(), &last.days()],
            )
            .map_err(sql)?
            .iter()
            .map(|row| HistoryFile {
                name: row.get(0),
                first: Date::from_days(row.get(1)),
                last: Date::from_days(row.get(2)),
                rows: row.get::<_, i64>(3) as u64,
                bytes: row.get::<_, i64>(4) as u64,
            })
            .collect())
    }

    /// Commits run one at a time per table: each locks the table's row first, so the history
    /// one checks is the one it changes.
    pub(crate) fn commit_history(
        &mut self,
        table_id: i64,
        commit: &HistoryCommit,
        now: i64,
    ) -> Result<(), Error> {
        let mut tx = self.client()?.transaction().map_err(sql)?;
        tx.execute(
            "SELECT id FROM kermodes_tables WHERE id = $1 FOR UPDATE",
            &[&table_id],
        )
        .map_err(sql)?;
        let current = history(&mut tx, table_id)?;
        let files: Vec<String> = tx
            .query(
                "SELECT file FROM kermodes_history_files WHERE table_id = $1",
                &[&table_id],
            )
            .map_err(sql)?
            .iter()
            .map(|row| row.get(0))
            .collect();
        check_commit(&current, &files, commit)?;
        for file in &commit.replaced {
            tx.execute(
                "DELETE FROM kermodes_history_dates WHERE table_id = $1 AND file = $2",
                &[&table_id, file],
            )
            .map_err(sql)?;
            tx.execute(
                "DELETE FROM kermodes_history_files WHERE table_id = $1 AND file = $2",
                &[&table_id, file],
            )
            .map_err(sql)?;
            tx.execute(
                "INSERT INTO kermodes_history_garbage (table_id, file, since) VALUES ($1, $2, $3)",
                &[&table_id, file, &now],
            )
            .map_err(sql)?;
        }
        for file in &commit.files {
            tx.execute(
                "INSERT INTO kermodes_history_files
                 (table_id, file, first_date, last_date, rows, bytes)
                 VALUES ($1, $2, $3, $4, $5, $6)",
                &[
                    &table_id,
                    &file.name,
                    &file.first.days(),
                    &file.last.days(),
                    &(file.rows as i64),
                    &(file.bytes as i64),
                ],
            )
            .map_err(sql)?;
        }
        for date in &commit.dates {
            tx.execute(
                "INSERT INTO kermodes_history_dates (table_id, date, file, rows, bytes)
                 VALUES ($1, $2, $3, $4, $5)",
                &[
                    &table_id,
                    &date.date.days(),
                    &date.file,
                    &(date.rows as i64),
                    &(date.bytes as i64),
                ],
            )
            .map_err(sql)?;
        }
        tx.commit().map_err(sql)
    }

    pub(crate) fn garbage(&mut self, before: i64) -> Result<Vec<Garbage>, Error> {
        Ok(self
            .client()?
            .query(
                "SELECT g.table_id, t.location, g.file FROM kermodes_history_garbage g
                 JOIN kermodes_tables t ON t.id = g.table_id
                 WHERE g.since <= $1 ORDER BY g.since, g.file",
                &[&before],
            )
            .map_err(sql)?
            .iter()
            .map(|row| Garbage {
                table_id: row.get(0),
                location: row.get(1),
                file: row.get(2),
            })
            .collect())
    }

    pub(crate) fn forget_garbage(&mut self, garbage: &Garbage) -> Result<(), Error> {
        self.client()?
            .execute(
                "DELETE FROM kermodes_history_garbage WHERE table_id = $1 AND file = $2",
                &[&garbage.table_id, &garbage.file],
            )
            .map(|_| ())
            .map_err(sql)
    }

    /// The connection, reopened if Postgres closed it (a restart or a CNPG switchover): the call
    /// that met the closed connection failed, the next one works.
    fn client(&mut self) -> Result<&mut Client, Error> {
        if self.client.is_closed() {
            self.client = connect(&self.url)?;
        }
        Ok(&mut self.client)
    }
}

fn connect(url: &str) -> Result<Client, Error> {
    Client::connect(url, NoTls).map_err(|e| Error::Catalog(format!("cannot open the catalog: {e}")))
}

fn history(client: &mut impl GenericClient, table_id: i64) -> Result<Vec<HistoryDate>, Error> {
    Ok(client
        .query(
            "SELECT date, file, rows, bytes FROM kermodes_history_dates
             WHERE table_id = $1 ORDER BY date, file",
            &[&table_id],
        )
        .map_err(sql)?
        .iter()
        .map(|row| HistoryDate {
            date: Date::from_days(row.get(0)),
            file: row.get(1),
            rows: row.get::<_, i64>(2) as u64,
            bytes: row.get::<_, i64>(3) as u64,
        })
        .collect())
}

fn sql(error: postgres::Error) -> Error {
    Error::Catalog(match error.as_db_error() {
        Some(db) => db.message().to_owned(),
        None => error.to_string(),
    })
}
