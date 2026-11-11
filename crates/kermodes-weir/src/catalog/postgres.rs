//! The cluster's catalog: Postgres, shared by every weir (architecture.md §8.2). Concurrent
//! clients agree because Postgres decides: a table is created once (`ON CONFLICT DO NOTHING`, the
//! loser reads the winner's), and an epoch is bumped atomically (`ON CONFLICT DO UPDATE ...
//! RETURNING`).

use postgres::{Client, NoTls};

use super::{FORMAT, Row, location};
use crate::{Error, SourceId, TableName};

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

fn sql(error: postgres::Error) -> Error {
    Error::Catalog(match error.as_db_error() {
        Some(db) => db.message().to_owned(),
        None => error.to_string(),
    })
}
