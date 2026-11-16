//! A table's columns changing (architecture.md §5.4), through `Store::create_table` on real files:
//! which changes apply and which are refused, and readers and writers on either side of one.

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::Schema;
use arrow::util::display::{ArrayFormatter, FormatOptions};
use kermodes_weir::{ColumnId, Error, SourceId, Store, TableName, column};

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "kermodes-columns-{}-{}",
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

/// Columns as `create_table` is given them: `(name, spec)`, a spec as `km.float64(was=...)`
/// writes it.
fn declared(columns: &[(&str, &str)]) -> Schema {
    Schema::new(
        columns
            .iter()
            .map(|(name, spec)| column(name, spec).unwrap())
            .collect::<Vec<_>>(),
    )
}

fn ticks() -> TableName {
    TableName::parse("md.ticks").unwrap()
}

/// The table's columns as `name type` and their IDs.
fn columns(store: &Store) -> (Vec<String>, Vec<u32>) {
    let entry = store.table(&ticks()).unwrap();
    let names = entry
        .schema
        .fields()
        .iter()
        .map(|f| format!("{} {}", f.name(), kermodes_core::type_name(f.data_type())))
        .collect();
    (names, entry.ids.iter().map(|ColumnId(id)| *id).collect())
}

/// One step of a table's columns changing: the columns declared, and the table's columns and
/// IDs after, or the words of the refusal.
type Step<'a> = (
    &'a [(&'a str, &'a str)],
    Result<(&'a [&'a str], &'a [u32]), &'a str>,
);

#[test]
fn a_table_takes_compatible_changes_and_refuses_the_rest_by_name() {
    let dir = TempDir::new();
    let store = Store::open(&dir.0).unwrap();
    let base = [("n", "int64"), ("sym", "utf8"), ("q", "int32")];
    store.create_table(&ticks(), &declared(&base)).unwrap();
    let steps: &[Step] = &[
        (&base, Ok((&["n int64", "sym utf8", "q int32"], &[1, 2, 3]))),
        (
            &[
                ("n", "int64"),
                ("sym", "utf8"),
                ("q", "int32"),
                ("px", "float64"),
            ],
            Ok((
                &["n int64", "sym utf8", "q int32", "px float64"],
                &[1, 2, 3, 4],
            )),
        ),
        (
            &[
                ("n", "int64"),
                ("symbol", "utf8 was sym"),
                ("q", "int32"),
                ("px", "float64"),
            ],
            Ok((
                &["n int64", "symbol utf8", "q int32", "px float64"],
                &[1, 2, 3, 4],
            )),
        ),
        // Declared again, `was` and all: nothing to change.
        (
            &[
                ("n", "int64"),
                ("symbol", "utf8 was sym"),
                ("q", "int32"),
                ("px", "float64"),
            ],
            Ok((
                &["n int64", "symbol utf8", "q int32", "px float64"],
                &[1, 2, 3, 4],
            )),
        ),
        (
            &[
                ("px", "float64"),
                ("n", "int64"),
                ("symbol", "utf8"),
                ("q", "int64"),
            ],
            Ok((
                &["px float64", "n int64", "symbol utf8", "q int64"],
                &[4, 1, 2, 3],
            )),
        ),
        (
            &[
                ("px", "float64"),
                ("n", "int64"),
                ("symbol", "utf8"),
                ("q", "int32"),
            ],
            Err("making it int32 is not a compatible change"),
        ),
        (
            &[("n", "int64"), ("symbol", "utf8"), ("q", "int64")],
            Err("column `px` the columns given leave out"),
        ),
        (
            &[
                ("px", "float64"),
                ("n", "utf8"),
                ("symbol", "utf8"),
                ("q", "int64"),
            ],
            Err("making it utf8 is not a compatible change"),
        ),
        (
            &[
                ("px", "float64"),
                ("n", "int64"),
                ("a", "utf8 was symbol"),
                ("b", "utf8 was symbol"),
                ("q", "int64"),
            ],
            Err("two of the columns given are its column `symbol`"),
        ),
    ];
    for (i, (columns_given, want)) in steps.iter().enumerate() {
        let got = store.create_table(&ticks(), &declared(columns_given));
        match (want, got) {
            (Ok((names, ids)), Ok(_)) => {
                let (got_names, got_ids) = columns(&store);
                assert_eq!(got_names, *names, "step {i}");
                assert_eq!(got_ids, *ids, "step {i}");
            }
            (Err(words), Err(Error::Schema(message))) => {
                assert!(message.contains(words), "step {i}: {message}");
            }
            (want, got) => panic!("step {i}: wanted {want:?}, got {got:?}"),
        }
    }
    // A keyed table's key column cannot change: keys hash by it.
    let quotes = TableName::parse("md.quotes").unwrap();
    let key = ["k".to_owned()];
    store
        .create_keyed_table(
            &quotes,
            &declared(&[("k", "int32"), ("v", "float64")]),
            &key,
        )
        .unwrap();
    let Err(Error::Schema(message)) = store.create_keyed_table(
        &quotes,
        &declared(&[("k", "int64"), ("v", "float64")]),
        &key,
    ) else {
        panic!("a key column's type cannot change");
    };
    assert!(message.contains("key column `k` cannot"), "{message}");
    // The catalog keeps it all.
    drop(store);
    let store = Store::open(&dir.0).unwrap();
    assert_eq!(
        columns(&store),
        (
            vec!["px float64", "n int64", "symbol utf8", "q int64"]
                .into_iter()
                .map(String::from)
                .collect(),
            vec![4, 1, 2, 3]
        )
    );
    assert_eq!(store.table(&ticks()).unwrap().was[2], vec!["sym"]);
}

/// A batch's rows as text, column by column, sorted: what a reader saw, whatever its order.
fn rows(batches: &[RecordBatch]) -> Vec<String> {
    let options = FormatOptions::default().with_null("∅");
    let mut rows = Vec::new();
    for batch in batches {
        let formatters: Vec<_> = batch
            .columns()
            .iter()
            .map(|c| ArrayFormatter::try_new(c.as_ref(), &options).unwrap())
            .collect();
        for row in 0..batch.num_rows() {
            let fields: Vec<String> = batch
                .schema()
                .fields()
                .iter()
                .zip(&formatters)
                .map(|(f, v)| format!("{}={}", f.name(), v.value(row)))
                .collect();
            rows.push(fields.join(" "));
        }
    }
    rows.sort();
    rows
}

/// A subscription's batches, and why it failed if it did.
struct Seen {
    batches: Arc<Mutex<Vec<RecordBatch>>>,
    failed: Arc<Mutex<Option<String>>>,
}

fn subscribe(store: &Store) -> Seen {
    let batches: Arc<Mutex<Vec<RecordBatch>>> = Arc::default();
    let failed: Arc<Mutex<Option<String>>> = Arc::default();
    let (sink, why) = (Arc::clone(&batches), Arc::clone(&failed));
    store
        .subscribe(
            &ticks(),
            Box::new(move |batch, _, _| {
                sink.lock().unwrap().push(batch.clone());
                true
            }),
            Box::new(move |reason| *why.lock().unwrap() = Some(reason)),
        )
        .unwrap();
    Seen { batches, failed }
}

impl Seen {
    fn rows(&self) -> Vec<String> {
        rows(&self.batches.lock().unwrap())
    }

    fn failed(&self) -> Option<String> {
        self.failed.lock().unwrap().clone()
    }
}

fn batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
    RecordBatch::try_from_iter(columns).unwrap()
}

#[test]
fn readers_and_writers_keep_their_columns_across_a_change() {
    let dir = TempDir::new();
    let store = Store::open(&dir.0).unwrap();
    let first = [("n", "int64"), ("sym", "utf8"), ("q", "int32")];
    store.create_table(&ticks(), &declared(&first)).unwrap();
    let source = |name: &str| SourceId::parse(name).unwrap();
    let old = store.writer(&ticks(), &source("west"), false).unwrap();
    let v1 = |n: i64, sym: &str, q: i32| {
        batch(vec![
            ("n", Arc::new(Int64Array::from(vec![n])) as ArrayRef),
            ("sym", Arc::new(StringArray::from(vec![sym]))),
            ("q", Arc::new(Int32Array::from(vec![q]))),
        ])
    };
    old.write(&v1(0, "a", 10), None).unwrap();
    let before = subscribe(&store);

    // Add `px`, rename `sym` to `symbol`.
    let second = [
        ("n", "int64"),
        ("symbol", "utf8 was sym"),
        ("q", "int32"),
        ("px", "float64"),
    ];
    store.create_table(&ticks(), &declared(&second)).unwrap();
    // The writer opened before writes as it did: its rows come out with the new columns too.
    old.write(&v1(1, "b", 11), None).unwrap();
    let new = store.writer(&ticks(), &source("east"), false).unwrap();
    new.write(
        &batch(vec![
            ("n", Arc::new(Int64Array::from(vec![2])) as ArrayRef),
            ("symbol", Arc::new(StringArray::from(vec!["c"]))),
            ("q", Arc::new(Int32Array::from(vec![12]))),
            ("px", Arc::new(Float64Array::from(vec![1.5]))),
        ]),
        None,
    )
    .unwrap();
    let after = subscribe(&store);
    // The reader from before keeps its columns, the rename followed by ID.
    assert_eq!(
        before.rows(),
        ["n=0 sym=a q=10", "n=1 sym=b q=11", "n=2 sym=c q=12"]
    );
    assert_eq!(
        after.rows(),
        [
            "n=0 symbol=a q=10 px=∅",
            "n=1 symbol=b q=11 px=∅",
            "n=2 symbol=c q=12 px=1.5"
        ]
    );

    // Widen `q`: readers holding it as int32 cannot follow, and are told so.
    let third = [
        ("n", "int64"),
        ("symbol", "utf8"),
        ("q", "int64"),
        ("px", "float64"),
    ];
    store.create_table(&ticks(), &declared(&third)).unwrap();
    old.write(&v1(3, "d", 13), None).unwrap();
    for reader in [&before, &after] {
        let failed = reader.failed().expect("a reader of int32 `q` fails");
        assert!(failed.contains("read the table again"), "{failed}");
    }
    let widened = subscribe(&store);
    assert!(
        widened
            .rows()
            .contains(&"n=3 symbol=d q=13 px=∅".to_owned())
    );

    // A writer taking over the old one's source writes into the same partition: the segment
    // the old one left open had the old columns, so these start a new one.
    drop(old);
    let handover = store.writer(&ticks(), &source("west"), false).unwrap();
    handover
        .write(
            &batch(vec![
                ("n", Arc::new(Int64Array::from(vec![4])) as ArrayRef),
                ("symbol", Arc::new(StringArray::from(vec!["e"]))),
                ("q", Arc::new(Int64Array::from(vec![5_000_000_000]))),
                ("px", Arc::new(Float64Array::from(vec![2.5]))),
            ]),
            None,
        )
        .unwrap();

    // From disk, the day reads in the table's columns now, across segments of three shapes.
    drop((handover, new));
    drop(store);
    let store = Store::open(&dir.0).unwrap();
    assert_eq!(
        subscribe(&store).rows(),
        [
            "n=0 symbol=a q=10 px=∅",
            "n=1 symbol=b q=11 px=∅",
            "n=2 symbol=c q=12 px=1.5",
            "n=3 symbol=d q=13 px=∅",
            "n=4 symbol=e q=5000000000 px=2.5"
        ]
    );
}
