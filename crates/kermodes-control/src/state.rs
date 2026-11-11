//! What the controller knows, in Postgres beside the catalog (architecture.md §10.1): each synced
//! commit with its code snapshot and what it declares, and the commit each resource runs. Git
//! holds the definitions; this records what was seen and done with them.

use std::collections::BTreeMap;

use tokio_postgres::{Client, NoTls};

/// Held while creating the controller's tables, so replicas starting together do not collide.
const SCHEMA_LOCK: i64 = 0x6b6d_5f63_6f6e_7472; // "km_contr"

pub struct State {
    url: String,
    client: Client,
}

/// A synced commit: its manifest once discovered, or why discovery failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Commit {
    pub commit: String,
    pub manifest: Option<String>,
    pub error: Option<String>,
}

impl State {
    pub async fn open(url: &str) -> Result<Self, String> {
        let mut client = connect(url).await?;
        let tx = client.transaction().await.map_err(sql)?;
        tx.execute("SELECT pg_advisory_xact_lock($1)", &[&SCHEMA_LOCK])
            .await
            .map_err(sql)?;
        tx.batch_execute(
            "CREATE TABLE IF NOT EXISTS kermodes_control_commits (
                 project TEXT NOT NULL,
                 commit TEXT NOT NULL,
                 synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                 snapshot BYTEA NOT NULL,
                 manifest TEXT,
                 error TEXT,
                 PRIMARY KEY (project, commit)
             );
             CREATE TABLE IF NOT EXISTS kermodes_control_deployed (
                 project TEXT NOT NULL,
                 resource TEXT NOT NULL,
                 commit TEXT NOT NULL,
                 applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                 PRIMARY KEY (project, resource)
             );",
        )
        .await
        .map_err(sql)?;
        tx.commit().await.map_err(sql)?;
        Ok(Self {
            url: url.to_owned(),
            client,
        })
    }

    /// Records a synced commit's snapshot, before its discovery; does nothing if it is known.
    pub async fn add_commit(
        &mut self,
        project: &str,
        commit: &str,
        snapshot: &[u8],
    ) -> Result<(), String> {
        self.client()
            .await?
            .execute(
                "INSERT INTO kermodes_control_commits (project, commit, snapshot)
                 VALUES ($1, $2, $3) ON CONFLICT (project, commit) DO NOTHING",
                &[&project, &commit, &snapshot],
            )
            .await
            .map_err(sql)?;
        Ok(())
    }

    /// Records what discovery found for a commit: its manifest, or why there is none.
    pub async fn discovered(
        &mut self,
        project: &str,
        commit: &str,
        result: &Result<String, String>,
    ) -> Result<(), String> {
        let (manifest, error) = match result {
            Ok(manifest) => (Some(manifest.as_str()), None),
            Err(error) => (None, Some(error.as_str())),
        };
        self.client()
            .await?
            .execute(
                "UPDATE kermodes_control_commits SET manifest = $3, error = $4
                 WHERE project = $1 AND commit = $2",
                &[&project, &commit, &manifest, &error],
            )
            .await
            .map_err(sql)?;
        Ok(())
    }

    pub async fn commit(&mut self, project: &str, commit: &str) -> Result<Option<Commit>, String> {
        Ok(self
            .client()
            .await?
            .query_opt(
                "SELECT commit, manifest, error FROM kermodes_control_commits
                 WHERE project = $1 AND commit = $2",
                &[&project, &commit],
            )
            .await
            .map_err(sql)?
            .map(|row| Commit {
                commit: row.get(0),
                manifest: row.get(1),
                error: row.get(2),
            }))
    }

    /// The newest synced commit whose discovery succeeded: what the project should run.
    pub async fn latest_discovered(&mut self, project: &str) -> Result<Option<Commit>, String> {
        Ok(self
            .client()
            .await?
            .query_opt(
                "SELECT commit, manifest, error FROM kermodes_control_commits
                 WHERE project = $1 AND manifest IS NOT NULL
                 ORDER BY synced_at DESC LIMIT 1",
                &[&project],
            )
            .await
            .map_err(sql)?
            .map(|row| Commit {
                commit: row.get(0),
                manifest: row.get(1),
                error: row.get(2),
            }))
    }

    pub async fn snapshot(
        &mut self,
        project: &str,
        commit: &str,
    ) -> Result<Option<Vec<u8>>, String> {
        Ok(self
            .client()
            .await?
            .query_opt(
                "SELECT snapshot FROM kermodes_control_commits WHERE project = $1 AND commit = $2",
                &[&project, &commit],
            )
            .await
            .map_err(sql)?
            .map(|row| row.get(0)))
    }

    /// The commit each of a project's resources runs.
    pub async fn deployed(&mut self, project: &str) -> Result<BTreeMap<String, String>, String> {
        Ok(self
            .client()
            .await?
            .query(
                "SELECT resource, commit FROM kermodes_control_deployed WHERE project = $1",
                &[&project],
            )
            .await
            .map_err(sql)?
            .iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect())
    }

    pub async fn set_deployed(
        &mut self,
        project: &str,
        resource: &str,
        commit: &str,
    ) -> Result<(), String> {
        self.client()
            .await?
            .execute(
                "INSERT INTO kermodes_control_deployed (project, resource, commit)
                 VALUES ($1, $2, $3)
                 ON CONFLICT (project, resource)
                 DO UPDATE SET commit = $3, applied_at = now()",
                &[&project, &resource, &commit],
            )
            .await
            .map_err(sql)?;
        Ok(())
    }

    pub async fn remove_deployed(&mut self, project: &str, resource: &str) -> Result<(), String> {
        self.client()
            .await?
            .execute(
                "DELETE FROM kermodes_control_deployed WHERE project = $1 AND resource = $2",
                &[&project, &resource],
            )
            .await
            .map_err(sql)?;
        Ok(())
    }

    /// The connection, reopened if Postgres closed it (a restart, a CNPG switchover).
    async fn client(&mut self) -> Result<&mut Client, String> {
        if self.client.is_closed() {
            self.client = connect(&self.url).await?;
        }
        Ok(&mut self.client)
    }
}

async fn connect(url: &str) -> Result<Client, String> {
    let (client, connection) = tokio_postgres::connect(url, NoTls)
        .await
        .map_err(|e| format!("cannot open the controller's database: {e}"))?;
    tokio::spawn(async move {
        // Ends when the connection closes; the next call reconnects.
        let _ = connection.await;
    });
    Ok(client)
}

fn sql(error: tokio_postgres::Error) -> String {
    match error.as_db_error() {
        Some(db) => db.message().to_owned(),
        None => error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::State;

    /// A fresh schema of the test Postgres, or `None` without one (CI always has one).
    async fn database() -> Option<String> {
        let Ok(url) = std::env::var("KERMODES_TEST_POSTGRES") else {
            assert!(
                std::env::var_os("CI").is_none(),
                "CI must set KERMODES_TEST_POSTGRES"
            );
            return None;
        };
        let schema = format!("kermodes_control_test_{}", std::process::id());
        let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(connection);
        client
            .batch_execute(&format!(
                "DROP SCHEMA IF EXISTS {schema} CASCADE; CREATE SCHEMA {schema}"
            ))
            .await
            .unwrap();
        let separator = if url.contains('?') { '&' } else { '?' };
        Some(format!("{url}{separator}options=-csearch_path%3D{schema}"))
    }

    /// A commit that fails discovery is never what a project runs: the newest deployable commit
    /// stays the one before it.
    #[tokio::test]
    async fn a_failed_commit_never_replaces_the_deployable_one() {
        let Some(url) = database().await else { return };
        let mut state = State::open(&url).await.unwrap();
        state.add_commit("ticker", "a", b"tar a").await.unwrap();
        // Known but not yet discovered: not deployable.
        assert_eq!(state.latest_discovered("ticker").await.unwrap(), None);
        state
            .discovered("ticker", "a", &Ok("{\"manifest\": \"a\"}".into()))
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        state.add_commit("ticker", "b", b"tar b").await.unwrap();
        state
            .discovered("ticker", "b", &Err("SyntaxError".into()))
            .await
            .unwrap();
        let latest = state.latest_discovered("ticker").await.unwrap().unwrap();
        assert_eq!(latest.commit, "a");
        assert_eq!(
            state
                .commit("ticker", "b")
                .await
                .unwrap()
                .unwrap()
                .error
                .as_deref(),
            Some("SyntaxError")
        );
        assert_eq!(
            state.snapshot("ticker", "b").await.unwrap().as_deref(),
            Some(&b"tar b"[..])
        );

        state.set_deployed("ticker", "ticks", "a").await.unwrap();
        state.set_deployed("ticker", "ticks", "c").await.unwrap();
        assert_eq!(state.deployed("ticker").await.unwrap()["ticks"], "c");
        state.remove_deployed("ticker", "ticks").await.unwrap();
        assert!(state.deployed("ticker").await.unwrap().is_empty());
    }
}
