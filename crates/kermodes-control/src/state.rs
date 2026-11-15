//! What the controller knows, in Postgres beside the catalog (architecture.md §10.1): each synced
//! commit with its code snapshot and what it declares, and the commit each resource runs. Git
//! holds the definitions; this records what was seen and done with them.

use std::collections::{BTreeMap, BTreeSet};

use tokio_postgres::{Client, NoTls};

use crate::plan;

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
    /// The git tree of the project's directory: equal trees are the same code. Unknown for
    /// commits synced before it was recorded.
    pub tree: Option<String>,
    pub manifest: Option<String>,
    pub error: Option<String>,
    /// The platform image whose `kermodes manifest` read it; unknown for commits read before
    /// this was recorded.
    pub discovered_by: Option<String>,
    /// The hash of the project's `uv.lock` and `pyproject.toml` (`plan::lock_of`), which its
    /// environment is built from; none for a project without a lockfile.
    pub lock: Option<String>,
}

const COMMIT_COLUMNS: &str = "commit, tree, manifest, error, discovered_by, lock";

fn commit_row(row: tokio_postgres::Row) -> Commit {
    Commit {
        commit: row.get(0),
        tree: row.get(1),
        manifest: row.get(2),
        error: row.get(3),
        discovered_by: row.get(4),
        lock: row.get(5),
    }
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
             ALTER TABLE kermodes_control_commits ADD COLUMN IF NOT EXISTS tree TEXT;
             ALTER TABLE kermodes_control_commits ADD COLUMN IF NOT EXISTS discovered_by TEXT;
             ALTER TABLE kermodes_control_commits ADD COLUMN IF NOT EXISTS lock TEXT;
             CREATE TABLE IF NOT EXISTS kermodes_control_environments (
                 key TEXT PRIMARY KEY,
                 started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                 built BOOLEAN NOT NULL DEFAULT false,
                 error TEXT
             );
             CREATE TABLE IF NOT EXISTS kermodes_control_deployed (
                 project TEXT NOT NULL,
                 resource TEXT NOT NULL,
                 commit TEXT NOT NULL,
                 applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                 PRIMARY KEY (project, resource)
             );
             ALTER TABLE kermodes_control_deployed
                 ADD COLUMN IF NOT EXISTS stopped BOOLEAN NOT NULL DEFAULT false;
             CREATE TABLE IF NOT EXISTS kermodes_control_builds (
                 project TEXT NOT NULL,
                 app TEXT NOT NULL,
                 commit TEXT NOT NULL,
                 started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                 built BOOLEAN NOT NULL DEFAULT false,
                 error TEXT,
                 PRIMARY KEY (project, app, commit)
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

    /// Records a synced commit's snapshot and lock, before its discovery; does nothing if it is
    /// known.
    pub async fn add_commit(
        &mut self,
        project: &str,
        commit: &str,
        tree: &str,
        lock: Option<&str>,
        snapshot: &[u8],
    ) -> Result<(), String> {
        self.client()
            .await?
            .execute(
                "INSERT INTO kermodes_control_commits (project, commit, tree, lock, snapshot)
                 VALUES ($1, $2, $3, $4, $5) ON CONFLICT (project, commit) DO NOTHING",
                &[&project, &commit, &tree, &lock, &snapshot],
            )
            .await
            .map_err(sql)?;
        Ok(())
    }

    /// How the environment `key` stands, if it was ever built (`plan::environment_key`). The
    /// environment itself is in the artifacts store; this records only how its build went.
    pub async fn environment(&mut self, key: &str) -> Result<Option<plan::Build>, String> {
        Ok(self
            .client()
            .await?
            .query_opt(
                "SELECT built, error FROM kermodes_control_environments WHERE key = $1",
                &[&key],
            )
            .await
            .map_err(sql)?
            .map(|row| build_state(row.get(0), row.get(1))))
    }

    /// Every environment ever started, and how each stands.
    pub async fn environments(&mut self) -> Result<Vec<(String, plan::Build)>, String> {
        Ok(self
            .client()
            .await?
            .query(
                "SELECT key, built, error FROM kermodes_control_environments",
                &[],
            )
            .await
            .map_err(sql)?
            .iter()
            .map(|row| (row.get(0), build_state(row.get(1), row.get(2))))
            .collect())
    }

    pub async fn start_environment(&mut self, key: &str) -> Result<(), String> {
        self.client()
            .await?
            .execute(
                "INSERT INTO kermodes_control_environments (key) VALUES ($1)
                 ON CONFLICT DO NOTHING",
                &[&key],
            )
            .await
            .map_err(sql)?;
        Ok(())
    }

    pub async fn finish_environment(
        &mut self,
        key: &str,
        result: &Result<(), String>,
    ) -> Result<(), String> {
        let (built, error) = match result {
            Ok(()) => (true, None),
            Err(e) => (false, Some(e.as_str())),
        };
        self.client()
            .await?
            .execute(
                "UPDATE kermodes_control_environments SET built = $2, error = $3 WHERE key = $1",
                &[&key, &built, &error],
            )
            .await
            .map_err(sql)?;
        Ok(())
    }

    pub async fn remove_environment(&mut self, key: &str) -> Result<(), String> {
        self.client()
            .await?
            .execute(
                "DELETE FROM kermodes_control_environments WHERE key = $1",
                &[&key],
            )
            .await
            .map_err(sql)?;
        Ok(())
    }

    /// The locks of a project's newest `count` commits: the environments worth keeping.
    pub async fn recent_locks(&mut self, project: &str, count: i64) -> Result<Vec<String>, String> {
        Ok(self
            .client()
            .await?
            .query(
                "SELECT lock FROM kermodes_control_commits
                 WHERE project = $1 AND lock IS NOT NULL
                 ORDER BY synced_at DESC LIMIT $2",
                &[&project, &count],
            )
            .await
            .map_err(sql)?
            .iter()
            .map(|row| row.get(0))
            .collect())
    }

    /// Records what discovery under `image` found for a commit: its manifest, or why there is
    /// none.
    pub async fn discovered(
        &mut self,
        project: &str,
        commit: &str,
        image: &str,
        result: &Result<String, String>,
    ) -> Result<(), String> {
        let (manifest, error) = match result {
            Ok(manifest) => (Some(manifest.as_str()), None),
            Err(error) => (None, Some(error.as_str())),
        };
        self.client()
            .await?
            .execute(
                "UPDATE kermodes_control_commits
                 SET manifest = $3, error = $4, discovered_by = $5
                 WHERE project = $1 AND commit = $2",
                &[&project, &commit, &manifest, &error, &image],
            )
            .await
            .map_err(sql)?;
        Ok(())
    }

    /// Records a deployable commit read again under `image`: its new manifest, or, when the new
    /// platform could not read it, its old one kept, so what runs keeps running.
    pub async fn rediscovered(
        &mut self,
        project: &str,
        commit: &str,
        image: &str,
        manifest: Option<&str>,
    ) -> Result<(), String> {
        self.client()
            .await?
            .execute(
                "UPDATE kermodes_control_commits
                 SET manifest = COALESCE($3, manifest), discovered_by = $4
                 WHERE project = $1 AND commit = $2",
                &[&project, &commit, &manifest, &image],
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
                &format!(
                    "SELECT {COMMIT_COLUMNS} FROM kermodes_control_commits
                     WHERE project = $1 AND commit = $2"
                ),
                &[&project, &commit],
            )
            .await
            .map_err(sql)?
            .map(commit_row))
    }

    /// The newest synced commit, discovered or not: the mainline as the controller last saw it.
    pub async fn latest(&mut self, project: &str) -> Result<Option<Commit>, String> {
        Ok(self
            .client()
            .await?
            .query_opt(
                &format!(
                    "SELECT {COMMIT_COLUMNS} FROM kermodes_control_commits
                     WHERE project = $1 ORDER BY synced_at DESC LIMIT 1"
                ),
                &[&project],
            )
            .await
            .map_err(sql)?
            .map(commit_row))
    }

    /// The newest synced commit whose discovery succeeded: what the project should run.
    pub async fn latest_discovered(&mut self, project: &str) -> Result<Option<Commit>, String> {
        Ok(self
            .client()
            .await?
            .query_opt(
                &format!(
                    "SELECT {COMMIT_COLUMNS} FROM kermodes_control_commits
                     WHERE project = $1 AND manifest IS NOT NULL
                     ORDER BY synced_at DESC LIMIT 1"
                ),
                &[&project],
            )
            .await
            .map_err(sql)?
            .map(commit_row))
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

    /// The resources of a project someone stopped: they keep following commits, with no run.
    pub async fn stopped(&mut self, project: &str) -> Result<BTreeSet<String>, String> {
        Ok(self
            .client()
            .await?
            .query(
                "SELECT resource FROM kermodes_control_deployed
                 WHERE project = $1 AND stopped",
                &[&project],
            )
            .await
            .map_err(sql)?
            .iter()
            .map(|row| row.get(0))
            .collect())
    }

    /// Stops or starts a deployed resource; an error if it never ran.
    pub async fn set_stopped(
        &mut self,
        project: &str,
        resource: &str,
        stopped: bool,
    ) -> Result<(), String> {
        let changed = self
            .client()
            .await?
            .execute(
                "UPDATE kermodes_control_deployed SET stopped = $3
                 WHERE project = $1 AND resource = $2",
                &[&project, &resource, &stopped],
            )
            .await
            .map_err(sql)?;
        if changed == 0 {
            return Err(format!("{project} has no deployed `{resource}`"));
        }
        Ok(())
    }

    /// A project's app builds, newest commit first. The built files are in the artifacts store,
    /// at `plan::app_object(project, app, commit)`; this records only how each build went.
    pub async fn builds(&mut self, project: &str) -> Result<Vec<plan::BuildRecord>, String> {
        Ok(self
            .client()
            .await?
            .query(
                "SELECT b.app, b.commit, b.built, b.error
                 FROM kermodes_control_builds b
                 JOIN kermodes_control_commits c ON c.project = b.project AND c.commit = b.commit
                 WHERE b.project = $1
                 ORDER BY c.synced_at DESC, b.app",
                &[&project],
            )
            .await
            .map_err(sql)?
            .iter()
            .map(|row| plan::BuildRecord {
                app: row.get(0),
                commit: row.get(1),
                state: build_state(row.get(2), row.get(3)),
            })
            .collect())
    }

    /// Records that `app` of `project` is building at `commit`.
    pub async fn start_build(
        &mut self,
        project: &str,
        app: &str,
        commit: &str,
    ) -> Result<(), String> {
        self.client()
            .await?
            .execute(
                "INSERT INTO kermodes_control_builds (project, app, commit) VALUES ($1, $2, $3)
                 ON CONFLICT DO NOTHING",
                &[&project, &app, &commit],
            )
            .await
            .map_err(sql)?;
        Ok(())
    }

    /// Records how a build ended: built, or why not.
    pub async fn finish_build(
        &mut self,
        project: &str,
        app: &str,
        commit: &str,
        result: &Result<(), String>,
    ) -> Result<(), String> {
        let (built, error) = match result {
            Ok(()) => (true, None),
            Err(e) => (false, Some(e.as_str())),
        };
        self.client()
            .await?
            .execute(
                "UPDATE kermodes_control_builds SET built = $4, error = $5
                 WHERE project = $1 AND app = $2 AND commit = $3",
                &[&project, &app, &commit, &built, &error],
            )
            .await
            .map_err(sql)?;
        Ok(())
    }

    pub async fn remove_build(
        &mut self,
        project: &str,
        app: &str,
        commit: &str,
    ) -> Result<(), String> {
        self.client()
            .await?
            .execute(
                "DELETE FROM kermodes_control_builds
                 WHERE project = $1 AND app = $2 AND commit = $3",
                &[&project, &app, &commit],
            )
            .await
            .map_err(sql)?;
        Ok(())
    }

    /// Asks the leader, whichever replica it is, to sync now (the push webhook).
    pub async fn request_sync(&mut self) -> Result<(), String> {
        self.client()
            .await?
            .batch_execute(&format!("NOTIFY {}", crate::leader::SYNC_CHANNEL))
            .await
            .map_err(sql)
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

/// How a build stands, from its row's `built` and `error`.
fn build_state(built: bool, error: Option<String>) -> plan::Build {
    match (built, error) {
        (true, _) => plan::Build::Built,
        (false, Some(error)) => plan::Build::Failed(error),
        (false, None) => plan::Build::Building,
    }
}

/// A fresh schema of the test Postgres for the test `name`, or `None` without one (CI always has
/// one).
#[cfg(test)]
pub(crate) async fn test_database(name: &str) -> Option<String> {
    let Ok(url) = std::env::var("KERMODES_TEST_POSTGRES") else {
        assert!(
            std::env::var_os("CI").is_none(),
            "CI must set KERMODES_TEST_POSTGRES"
        );
        return None;
    };
    let schema = format!("kermodes_control_{name}_{}", std::process::id());
    let (client, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
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

#[cfg(test)]
mod tests {
    use super::{State, plan};

    /// A commit that fails discovery is never what a project runs: the newest deployable commit
    /// stays the one before it.
    #[tokio::test]
    async fn a_failed_commit_never_replaces_the_deployable_one() {
        let Some(url) = super::test_database("state").await else {
            return;
        };
        let mut state = State::open(&url).await.unwrap();
        state
            .add_commit("ticker", "a", "tree a", None, b"tar a")
            .await
            .unwrap();
        // Known but not yet discovered: not deployable.
        assert_eq!(state.latest_discovered("ticker").await.unwrap(), None);
        state
            .discovered("ticker", "a", "img:1", &Ok("{\"manifest\": \"a\"}".into()))
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        state
            .add_commit("ticker", "b", "tree b", Some("lock b"), b"tar b")
            .await
            .unwrap();
        state
            .discovered("ticker", "b", "img:1", &Err("SyntaxError".into()))
            .await
            .unwrap();
        let latest = state.latest_discovered("ticker").await.unwrap().unwrap();
        assert_eq!(latest.commit, "a");
        assert_eq!(latest.discovered_by.as_deref(), Some("img:1"));
        // Read again by a new platform: a new manifest replaces the old; one it could not read
        // keeps the old, so what runs keeps running.
        state
            .rediscovered("ticker", "a", "img:2", None)
            .await
            .unwrap();
        let kept = state.latest_discovered("ticker").await.unwrap().unwrap();
        assert_eq!(
            (kept.manifest.as_deref(), kept.discovered_by.as_deref()),
            (Some("{\"manifest\": \"a\"}"), Some("img:2"))
        );
        state
            .rediscovered("ticker", "a", "img:3", Some("{\"manifest\": \"a3\"}"))
            .await
            .unwrap();
        let read = state.latest_discovered("ticker").await.unwrap().unwrap();
        assert_eq!(read.manifest.as_deref(), Some("{\"manifest\": \"a3\"}"));
        // The mainline is at the failed commit; it shows as failed, not as what runs.
        let head = state.latest("ticker").await.unwrap().unwrap();
        assert_eq!(
            (
                head.commit.as_str(),
                head.tree.as_deref(),
                head.error.as_deref()
            ),
            ("b", Some("tree b"), Some("SyntaxError"))
        );
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

        // Builds list newest commit first, by when the commit synced: what decides which
        // build an app serves.
        state.start_build("ticker", "market", "a").await.unwrap();
        state
            .finish_build("ticker", "market", "a", &Ok(()))
            .await
            .unwrap();
        state.start_build("ticker", "market", "b").await.unwrap();
        state.start_build("ticker", "market", "b").await.unwrap();
        let why = "vite: Could not resolve './Missing'".to_owned();
        state
            .finish_build("ticker", "market", "b", &Err(why.clone()))
            .await
            .unwrap();
        let record = |commit: &str, state| plan::BuildRecord {
            app: "market".into(),
            commit: commit.into(),
            state,
        };
        assert_eq!(
            state.builds("ticker").await.unwrap(),
            [
                record("b", plan::Build::Failed(why)),
                record("a", plan::Build::Built)
            ]
        );
        state.remove_build("ticker", "market", "b").await.unwrap();
        assert_eq!(
            state.builds("ticker").await.unwrap(),
            [record("a", plan::Build::Built)]
        );

        state.set_deployed("ticker", "ticks", "a").await.unwrap();
        state.set_deployed("ticker", "ticks", "c").await.unwrap();
        assert_eq!(state.deployed("ticker").await.unwrap()["ticks"], "c");

        // Stopped stays stopped as commits roll on, until started; only what ran can stop.
        state.set_stopped("ticker", "ticks", true).await.unwrap();
        state.set_deployed("ticker", "ticks", "d").await.unwrap();
        assert!(state.stopped("ticker").await.unwrap().contains("ticks"));
        state.set_stopped("ticker", "ticks", false).await.unwrap();
        assert!(state.stopped("ticker").await.unwrap().is_empty());
        assert!(state.set_stopped("ticker", "never", true).await.is_err());
        state.remove_deployed("ticker", "ticks").await.unwrap();
        assert!(state.deployed("ticker").await.unwrap().is_empty());
    }
}
