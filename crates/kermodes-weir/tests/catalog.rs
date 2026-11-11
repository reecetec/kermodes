//! The catalog's behaviour through the store, the same against SQLite (a local store) and Postgres
//! (the cluster's, shared by every weir). Postgres runs when `KERMODES_TEST_POSTGRES` names a
//! server (CI provides one, and fails without it); each test gets a schema of its own.

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};

use arrow::datatypes::{DataType, Field, Schema};
use kermodes_weir::{Error, SourceId, Store, TableName};

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "kermodes-catalog-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&dir);
        Self(dir)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// A catalog URL on a fresh schema of the test Postgres, or `None` when there is none.
fn postgres() -> Option<String> {
    let Ok(url) = std::env::var("KERMODES_TEST_POSTGRES") else {
        assert!(
            std::env::var_os("CI").is_none(),
            "CI must set KERMODES_TEST_POSTGRES: the Postgres catalog is untested without it"
        );
        eprintln!("KERMODES_TEST_POSTGRES is not set: skipping the Postgres catalog");
        return None;
    };
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let schema = format!(
        "kermodes_test_{}_{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    );
    let mut admin = postgres::Client::connect(&url, postgres::NoTls).unwrap();
    admin
        .batch_execute(&format!(
            "DROP SCHEMA IF EXISTS {schema} CASCADE; CREATE SCHEMA {schema}"
        ))
        .unwrap();
    let separator = if url.contains('?') { '&' } else { '?' };
    Some(format!("{url}{separator}options=-csearch_path%3D{schema}"))
}

enum Backend {
    Sqlite,
    Postgres(String),
}

fn backends() -> Vec<Backend> {
    let mut backends = vec![Backend::Sqlite];
    backends.extend(postgres().map(Backend::Postgres));
    backends
}

fn open(dir: &TempDir, backend: &Backend) -> Store {
    match backend {
        Backend::Sqlite => Store::open(&dir.0).unwrap(),
        Backend::Postgres(url) => Store::open_with_catalog(&dir.0, url, "prod").unwrap(),
    }
}

fn ticks() -> TableName {
    TableName::parse("md.ticks").unwrap()
}

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("n", DataType::Int64, true),
        Field::new("sym", DataType::Utf8, true),
    ])
}

#[test]
fn both_catalogs_keep_tables_and_epochs() {
    for backend in backends() {
        let dir = TempDir::new();
        let feed = SourceId::parse("feed").unwrap();
        let (id, location) = {
            let store = open(&dir, &backend);
            let created = store.create_table(&ticks(), &schema()).unwrap();
            // The same definition again is the same table; other columns are refused.
            assert_eq!(
                store.create_table(&ticks(), &schema()).unwrap().id,
                created.id
            );
            let other = Schema::new(vec![Field::new("n", DataType::Float64, true)]);
            assert!(matches!(
                store.create_table(&ticks(), &other),
                Err(Error::Schema(_))
            ));
            let names: Vec<String> = store
                .tables()
                .unwrap()
                .iter()
                .map(|t| t.name.to_string())
                .collect();
            assert_eq!(names, ["md.ticks"]);
            assert_eq!(store.writer(&ticks(), &feed, false).unwrap().epoch(), 1);
            assert_eq!(store.writer(&ticks(), &feed, false).unwrap().epoch(), 2);
            (created.id, created.location)
        };
        // Reopened, the table is the same and epochs go on rising.
        let store = open(&dir, &backend);
        let entry = store.table(&ticks()).unwrap();
        assert_eq!((entry.id, entry.location), (id, location));
        assert_eq!(store.writer(&ticks(), &feed, false).unwrap().epoch(), 3);
        assert_eq!(
            store.source_state(&ticks(), &feed).unwrap(),
            (None, Some(3))
        );
    }
}

/// Weirs sharing a Postgres catalog: one definition per table, however they race to create it,
/// and epochs that rise across all of them.
#[test]
fn weirs_sharing_a_postgres_catalog_agree() {
    let Some(url) = postgres() else { return };
    let (a_dir, b_dir) = (TempDir::new(), TempDir::new());
    let a = Arc::new(Store::open_with_catalog(&a_dir.0, &url, "prod").unwrap());
    let b = Arc::new(Store::open_with_catalog(&b_dir.0, &url, "prod").unwrap());

    for i in 0..20 {
        let name = TableName::parse(&format!("md.t{i}")).unwrap();
        let start = Arc::new(Barrier::new(2));
        let racers: Vec<_> = [a.clone(), b.clone()]
            .into_iter()
            .map(|store| {
                let (name, start) = (name.clone(), start.clone());
                std::thread::spawn(move || {
                    start.wait();
                    store.create_table(&name, &schema()).unwrap().id
                })
            })
            .collect();
        let ids: Vec<i64> = racers.into_iter().map(|r| r.join().unwrap()).collect();
        assert_eq!(ids[0], ids[1], "md.t{i} was created twice");
    }

    let feed = SourceId::parse("feed").unwrap();
    let name = TableName::parse("md.t0").unwrap();
    assert_eq!(a.writer(&name, &feed, false).unwrap().epoch(), 1);
    assert_eq!(b.writer(&name, &feed, false).unwrap().epoch(), 2);
    assert_eq!(b.source_state(&name, &feed).unwrap().1, Some(2));
}
