//! The catalog's behaviour through the store, the same against SQLite (a local store) and Postgres
//! (the cluster's, shared by every weir). Postgres runs when `KERMODES_TEST_POSTGRES` names a
//! server (CI provides one, and fails without it); each test gets a schema of its own.

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use kermodes_weir::{
    Date, Error, HistoryCommit, HistoryDate, HistoryFile, SourceId, Store, TableName,
};

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
fn both_catalogs_keep_tables() {
    for backend in backends() {
        let dir = TempDir::new();
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
            (created.id, created.location)
        };
        // Reopened, the table is the same.
        let store = open(&dir, &backend);
        let entry = store.table(&ticks()).unwrap();
        assert_eq!((entry.id, entry.location), (id, location));
    }
}

/// A keyed table's key columns are part of its definition: kept across a reopen, required to
/// match when it is created again, and checked against its columns.
#[test]
fn both_catalogs_keep_a_keyed_tables_key() {
    let key = |columns: &[&str]| columns.iter().map(|c| c.to_string()).collect::<Vec<_>>();
    for backend in backends() {
        let dir = TempDir::new();
        {
            let store = open(&dir, &backend);
            let created = store
                .create_keyed_table(&ticks(), &schema(), &key(&["sym"]))
                .unwrap();
            assert_eq!(created.key, ["sym"]);
            let quotes = TableName::parse("md.quotes").unwrap();
            assert!(
                store
                    .create_table(&quotes, &schema())
                    .unwrap()
                    .key
                    .is_empty()
            );
        }
        let store = open(&dir, &backend);
        assert_eq!(store.table(&ticks()).unwrap().key, ["sym"]);
        let listed: Vec<Vec<String>> = store.tables().unwrap().into_iter().map(|t| t.key).collect();
        assert_eq!(listed, [vec![], vec!["sym".to_string()]]);
        // The same definition is the same table.
        store
            .create_keyed_table(&ticks(), &schema(), &key(&["sym"]))
            .unwrap();
        let refused = [
            (
                "another key",
                store.create_keyed_table(&ticks(), &schema(), &key(&["n"])),
            ),
            (
                "as an append table",
                store.create_table(&ticks(), &schema()),
            ),
            (
                "a key that is not a column",
                store.create_keyed_table(
                    &TableName::parse("md.x").unwrap(),
                    &schema(),
                    &key(&["px"]),
                ),
            ),
            (
                "a column twice",
                store.create_keyed_table(
                    &TableName::parse("md.y").unwrap(),
                    &schema(),
                    &key(&["sym", "sym"]),
                ),
            ),
            (
                "no key",
                store.create_keyed_table(&TableName::parse("md.z").unwrap(), &schema(), &[]),
            ),
        ];
        for (case, result) in refused {
            assert!(
                matches!(result, Err(Error::Schema(_))),
                "{case}: {result:?}"
            );
        }
    }
}

/// Weirs sharing a Postgres catalog: one definition per table, however they race to create it.
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
}

/// While the catalog fails (Postgres unreachable, §7.6), a weir goes on writing and reading the
/// tables it holds, a new writer's lease included; creating a table waits for the catalog.
#[test]
fn writing_goes_on_while_the_catalog_fails() {
    let Some(url) = postgres() else { return };
    let dir = TempDir::new();
    let store = Store::open_with_catalog(&dir.0, &url, "prod").unwrap();
    store.create_table(&ticks(), &schema()).unwrap();
    let rows = |n: i64| {
        RecordBatch::try_from_iter([
            ("n", Arc::new(Int64Array::from(vec![n])) as ArrayRef),
            ("sym", Arc::new(StringArray::from(vec!["A"])) as ArrayRef),
        ])
        .unwrap()
    };
    let feed = SourceId::parse("feed").unwrap();
    let writer = store.writer(&ticks(), &feed, false).unwrap();
    assert!(writer.write(&rows(1), None).unwrap());
    drop(writer);

    // The catalog's tables gone, as when Postgres cannot be reached: every query of it fails.
    let schema_name = url.rsplit("search_path%3D").next().unwrap();
    let admin_url = std::env::var("KERMODES_TEST_POSTGRES").unwrap();
    let mut admin = postgres::Client::connect(&admin_url, postgres::NoTls).unwrap();
    admin
        .batch_execute(&format!("DROP SCHEMA {schema_name} CASCADE"))
        .unwrap();

    let names: Vec<String> = store
        .tables()
        .unwrap()
        .iter()
        .map(|t| t.name.to_string())
        .collect();
    assert_eq!(names, ["md.ticks"]);
    let other = SourceId::parse("other").unwrap();
    let writer = store.writer(&ticks(), &other, false).unwrap();
    assert!(writer.write(&rows(2), None).unwrap());
    assert_eq!(store.source_state(&ticks(), &other).unwrap().0, Some(1));
    let quotes = TableName::parse("md.quotes").unwrap();
    assert!(matches!(
        store.create_table(&quotes, &schema()),
        Err(Error::Catalog(_))
    ));
}

fn date(text: &str) -> Date {
    Date::parse(text).unwrap()
}

/// A history file named `name` holding `first` to `last`, ten bytes a row.
fn file(name: &str, first: &str, last: &str, rows: u64) -> HistoryFile {
    HistoryFile {
        name: name.into(),
        first: date(first),
        last: date(last),
        rows,
        bytes: rows * 10,
    }
}

fn on(day: &str, file: &str, rows: u64) -> HistoryDate {
    HistoryDate {
        date: date(day),
        file: file.into(),
        rows,
        bytes: rows * 10,
    }
}

/// A commit writing `written` as the new file `name`, replacing `replaced`.
fn commit(written: &[&str], replaced: &[&str], name: &str, dates: &[(&str, u64)]) -> HistoryCommit {
    let rows = dates.iter().map(|(_, rows)| rows).sum();
    let (first, last) = (dates[0].0, dates[dates.len() - 1].0);
    HistoryCommit {
        written: written.iter().map(|d| date(d)).collect(),
        replaced: replaced.iter().map(|f| f.to_string()).collect(),
        files: vec![file(name, first, last, rows)],
        dates: dates
            .iter()
            .map(|&(day, rows)| on(day, name, rows))
            .collect(),
    }
}

/// History commits through either catalog: dates move between files in one commit; a commit
/// planned on a history that has since changed is `Busy`; one that would lose a date is refused
/// outright; refused commits change nothing; replaced files wait as garbage until forgotten; and
/// the history outlives the process.
#[test]
fn both_catalogs_commit_history_checked_against_what_it_holds() {
    for backend in backends() {
        let dir = TempDir::new();
        let store = open(&dir, &backend);
        store.create_table(&ticks(), &schema()).unwrap();
        assert!(store.history(&ticks()).unwrap().is_empty());

        let first = commit(&["2026-11-01"], &[], "a", &[("2026-11-01", 1)]);
        store.commit_history(&ticks(), &first).unwrap();
        // The next small date joins it in a new pack.
        let pack = commit(
            &["2026-11-02"],
            &["a"],
            "b",
            &[("2026-11-01", 1), ("2026-11-02", 1)],
        );
        store.commit_history(&ticks(), &pack).unwrap();
        assert_eq!(store.history(&ticks()).unwrap(), pack.dates);
        let after = |first: &str| {
            store
                .history_files(&ticks(), date(first), date("2026-11-30"))
                .unwrap()
        };
        assert_eq!(after("2026-11-02"), pack.files);
        assert!(after("2026-11-03").is_empty());

        // Planned when `a` was the file before 2026-11-05: `a` is gone.
        let stale = commit(&["2026-11-05"], &["a"], "c", &[("2026-11-05", 1)]);
        // Planned when 2026-11-01 was in no file: `b` now holds it.
        let twice = commit(
            &["2026-11-05"],
            &[],
            "f",
            &[("2026-11-01", 1), ("2026-11-05", 1)],
        );
        // Writing 2026-11-01 beside `b`, which holds it.
        let beside = commit(&["2026-11-01"], &[], "d", &[("2026-11-01", 5)]);
        // Replacing `b` while rewriting only 2026-11-02 would lose 2026-11-01.
        let lossy = commit(&["2026-11-02"], &["b"], "e", &[("2026-11-02", 1)]);
        let cases = [
            ("stale", &stale),
            ("twice", &twice),
            ("beside", &beside),
            ("lossy", &lossy),
        ];
        for (case, refused) in cases.map(|(case, c)| (case, store.commit_history(&ticks(), c))) {
            match (case, refused) {
                ("stale" | "twice" | "beside", Err(Error::Busy(_)))
                | ("lossy", Err(Error::Catalog(_))) => {}
                (case, other) => panic!("{case}: {other:?}"),
            }
        }
        assert_eq!(store.history(&ticks()).unwrap(), pack.dates);

        let garbage = store.history_garbage(Duration::ZERO).unwrap();
        let files: Vec<&str> = garbage.iter().map(|g| g.file.as_str()).collect();
        assert_eq!(files, ["a"]);
        assert!(
            store
                .history_garbage(Duration::from_secs(3600))
                .unwrap()
                .is_empty()
        );
        store.forget_garbage(&garbage[0]).unwrap();
        assert!(store.history_garbage(Duration::ZERO).unwrap().is_empty());

        drop(store);
        assert_eq!(open(&dir, &backend).history(&ticks()).unwrap(), pack.dates);
    }
}

/// Two weirs committing to one table's history at once through a shared catalog: one commit
/// wins, the other is told the history changed, never a broken history or a database error.
#[test]
fn racing_history_commits_on_a_shared_catalog_one_wins() {
    let Some(url) = postgres() else { return };
    let (a_dir, b_dir) = (TempDir::new(), TempDir::new());
    let a = Arc::new(Store::open_with_catalog(&a_dir.0, &url, "prod").unwrap());
    let b = Arc::new(Store::open_with_catalog(&b_dir.0, &url, "prod").unwrap());

    for i in 0..20 {
        let name = TableName::parse(&format!("md.h{i}")).unwrap();
        a.create_table(&name, &schema()).unwrap();
        let first = commit(&["2026-11-01"], &[], "a", &[("2026-11-01", 1)]);
        a.commit_history(&name, &first).unwrap();
        let start = Arc::new(Barrier::new(2));
        let racers: Vec<_> = [(a.clone(), "b"), (b.clone(), "c")]
            .into_iter()
            .map(|(store, file)| {
                let (name, start) = (name.clone(), start.clone());
                std::thread::spawn(move || {
                    let pack = commit(
                        &["2026-11-02"],
                        &["a"],
                        file,
                        &[("2026-11-01", 1), ("2026-11-02", 1)],
                    );
                    start.wait();
                    store.commit_history(&name, &pack)
                })
            })
            .collect();
        let results: Vec<_> = racers.into_iter().map(|r| r.join().unwrap()).collect();
        let won = results.iter().filter(|r| r.is_ok()).count();
        let busy = results
            .iter()
            .filter(|r| matches!(r, Err(Error::Busy(_))))
            .count();
        assert_eq!((won, busy), (1, 1), "md.h{i}: {results:?}");
        assert_eq!(a.history(&name).unwrap().len(), 2);
    }
}

/// History sorts each date of an append table by its declared sort (a keyed table's log keeps
/// commit order); both catalogs keep it, and a table asked for again with another sort is refused.
#[test]
fn both_catalogs_keep_a_tables_sort() {
    let names = |columns: &[&str]| columns.iter().map(|c| c.to_string()).collect::<Vec<_>>();
    let quotes = TableName::parse("md.quotes").unwrap();
    for backend in backends() {
        let dir = TempDir::new();
        {
            let store = open(&dir, &backend);
            let sorted = store
                .create_sorted_table(&ticks(), &schema(), &names(&["sym", "n"]))
                .unwrap();
            assert_eq!((sorted.key.len(), sorted.sort), (0, names(&["sym", "n"])));
            let keyed = store
                .create_keyed_table(&quotes, &schema(), &names(&["sym"]))
                .unwrap();
            assert!(
                keyed.sort.is_empty(),
                "a keyed table's log keeps commit order"
            );
        }
        let store = open(&dir, &backend);
        assert_eq!(store.table(&ticks()).unwrap().sort, ["sym", "n"]);
        let listed: Vec<Vec<String>> = store
            .tables()
            .unwrap()
            .into_iter()
            .map(|t| t.sort)
            .collect();
        assert_eq!(listed, [names(&[]), names(&["sym", "n"])]);
        store
            .create_sorted_table(&ticks(), &schema(), &names(&["sym", "n"]))
            .unwrap();
        let refused = [
            store.create_sorted_table(&ticks(), &schema(), &names(&["n"])),
            store.create_table(&ticks(), &schema()),
            store.create_sorted_table(
                &TableName::parse("md.x").unwrap(),
                &schema(),
                &names(&["px"]),
            ),
        ];
        for result in refused {
            assert!(matches!(result, Err(Error::Schema(_))), "{result:?}");
        }
    }
}

/// What each table takes, for the catalog: its rows and bytes in weir by date, over every
/// source; its history by date, a date spread over files counted once with its files; and the
/// store's volume.
#[test]
fn stats_say_what_each_table_takes_in_weir_and_in_history() {
    for backend in backends() {
        let dir = TempDir::new();
        let store = open(&dir, &backend);
        store.create_table(&ticks(), &schema()).unwrap();
        let refs = TableName::parse("md.refs").unwrap();
        store
            .create_keyed_table(&refs, &schema(), &["n".to_owned()])
            .unwrap();
        let rows = arrow::record_batch::RecordBatch::try_from_iter([
            (
                "n",
                Arc::new(arrow::array::Int64Array::from(vec![1, 2, 3])) as arrow::array::ArrayRef,
            ),
            (
                "sym",
                Arc::new(arrow::array::StringArray::from(vec!["a", "b", "c"])),
            ),
        ])
        .unwrap();
        for source in ["feed", "backfill"] {
            let writer = store
                .writer(&ticks(), &SourceId::parse(source).unwrap(), false)
                .unwrap();
            writer.write(&rows, None).unwrap();
        }
        // 2026-11-02 is in two files (a pack being rewritten), 2026-11-01 in one.
        let history = HistoryCommit {
            written: vec![date("2026-11-01"), date("2026-11-02")],
            replaced: Vec::new(),
            files: vec![
                file("a", "2026-11-01", "2026-11-02", 3),
                file("b", "2026-11-02", "2026-11-02", 4),
            ],
            dates: vec![
                on("2026-11-01", "a", 1),
                on("2026-11-02", "a", 2),
                on("2026-11-02", "b", 4),
            ],
        };
        store.commit_history(&ticks(), &history).unwrap();

        let stats = store.stats_json().unwrap();
        let table = |name: &str| {
            stats["tables"]
                .as_array()
                .unwrap()
                .iter()
                .find(|t| t["table"] == name)
                .unwrap()
                .clone()
        };
        let ticks = table("md.ticks");
        assert_eq!(ticks["kind"], "append");
        assert_eq!(ticks["columns"][1]["type"], "utf8");
        let today = &ticks["weir"].as_array().unwrap()[..];
        assert_eq!(today.len(), 1, "{today:?}");
        assert_eq!(today[0]["rows"], 6, "both sources' rows");
        assert!(today[0]["bytes"].as_u64().unwrap() > 0);
        assert_eq!(
            ticks["history"],
            serde_json::json!([
                {"date": "2026-11-01", "rows": 1, "bytes": 10, "files": 1},
                {"date": "2026-11-02", "rows": 6, "bytes": 60, "files": 2},
            ])
        );
        let refs = table("md.refs");
        assert_eq!(
            (refs["kind"].clone(), refs["key"].clone()),
            (serde_json::json!("keyed"), serde_json::json!(["n"]))
        );
        assert_eq!(refs["weir"], serde_json::json!([]));
        let volume = &stats["volume"];
        let (size, free) = (
            volume["bytes"].as_u64().unwrap(),
            volume["free"].as_u64().unwrap(),
        );
        assert!(size > 0 && free > 0 && free <= size, "{volume}");
    }
}
