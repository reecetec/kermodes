//! The table viewer's server side, as the grid drives it: windows of rows with their keys, quick
//! filters and sorts built in the engine, fetches for copying, and letting go of tables.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    ArrayRef, AsArray, BooleanArray, Date32Array, Float64Array, Int64Array, RecordBatch,
    StringArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, Float64Type, Schema, TimeUnit};
use arrow::ipc::reader::StreamReader;
use ursid_core::viewer::{FETCH_LIMIT, Frame, MARGIN, Request, SortKey, Viewer};
use ursid_core::{Session, Table};

fn rows(frame: &Frame) -> RecordBatch {
    let mut reader = StreamReader::try_new(frame.rows.as_slice(), None).unwrap();
    let batch = reader.next().unwrap().unwrap();
    assert!(reader.next().is_none(), "one batch per frame");
    batch
}

fn keys(frame: &Frame) -> Vec<u64> {
    let (chunks, rest) = frame.keys.as_chunks::<8>();
    assert!(rest.is_empty());
    chunks.iter().map(|k| u64::from_le_bytes(*k)).collect()
}

fn strings(batch: &RecordBatch, column: &str) -> Vec<Option<String>> {
    let values = batch.column_by_name(column).unwrap().as_string::<i32>();
    values.iter().map(|v| v.map(str::to_owned)).collect()
}

fn filter(viewer: &mut Viewer, entries: &[(&str, &str)]) -> RecordBatch {
    let entries: BTreeMap<String, String> = entries
        .iter()
        .map(|(c, t)| ((*c).to_owned(), (*t).to_owned()))
        .collect();
    rows(&viewer.receive(Request::Filter(entries)).unwrap().unwrap())
}

/// One row per kind of value, and a row of NULLs.
fn every_type(session: &Session) -> Table {
    let schema = Arc::new(Schema::new(vec![
        Field::new("sym", DataType::Utf8, true),
        Field::new("price", DataType::Float64, true),
        Field::new("n", DataType::Int64, true),
        Field::new("flag", DataType::Boolean, true),
        Field::new("day", DataType::Date32, true),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            true,
        ),
    ]));
    let day = 20_454; // 2026-01-01
    let ns = 1_767_225_600_000_000_000_i64; // 2026-01-01T00:00:00Z
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![
            Some("Apple"),
            Some("it's"),
            Some("pear"),
            None,
        ])),
        Arc::new(Float64Array::from(vec![
            Some(1.5),
            Some(-2.0),
            Some(10.0),
            None,
        ])),
        Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3), None])),
        Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(false),
            Some(true),
            None,
        ])),
        Arc::new(Date32Array::from(vec![
            Some(day - 1),
            Some(day),
            Some(day + 1),
            None,
        ])),
        Arc::new(
            TimestampNanosecondArray::from(vec![Some(ns - 1), Some(ns), Some(ns + 1), None])
                .with_timezone("UTC"),
        ),
    ];
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
    session.static_table(schema, vec![batch]).unwrap()
}

#[test]
fn quick_filters_follow_each_column_type() {
    let session = Session::new();
    let table = every_type(&session);
    let mut viewer = Viewer::new(&table);
    let cases: &[(&str, &str, &[&str])] = &[
        ("sym", "p", &["Apple", "pear"]),
        ("sym", "APP", &["Apple"]),
        ("sym", "=pear", &["pear"]),
        ("sym", "!=pear", &["Apple", "it's"]),
        ("sym", "!p", &["it's"]),
        ("sym", "it's", &["it's"]),
        ("sym", "null", &[]),
        ("price", "> 1", &["Apple", "pear"]),
        ("price", "-2", &["it's"]),
        ("price", "<= 1.5e0", &["Apple", "it's"]),
        ("n", "!= 2", &["Apple", "pear"]),
        ("flag", "FALSE", &["it's"]),
        ("day", ">= 2026-01-01", &["it's", "pear"]),
        ("ts", "< 2026-01-01T00:00:00Z", &["Apple"]),
        ("price", "!null", &["Apple", "it's", "pear"]),
    ];
    for (column, text, want) in cases {
        let got = filter(&mut viewer, &[(column, text)]);
        let got: Vec<String> = strings(&got, "sym").into_iter().flatten().collect();
        let errors = viewer.filter_errors();
        assert!(errors.is_empty(), "{column} {text:?}: {errors:?}");
        assert_eq!(got, *want, "{column} {text:?}");
    }
    // `sym` is NULL only in the last row.
    assert_eq!(filter(&mut viewer, &[("sym", "null")]).num_rows(), 1);

    let errors: &[(&str, &str, &str)] = &[
        ("price", "> lots", "`lots` is not a number"),
        ("price", "inf", "`inf` is not a number"),
        ("n", "1_000", "`1_000` is not a number"),
        ("n", ">=", "needs a value"),
        ("flag", "yes", "takes true, false, null, or !null"),
        ("day", "2026-13-01", "not an ISO date or time"),
        ("nope", "1", "no such column"),
    ];
    for (column, text, message) in errors {
        // A bad entry is reported for its column and skipped; the good one still applies.
        let got = filter(&mut viewer, &[(column, text), ("sym", "=pear")]);
        assert_eq!(got.num_rows(), 1, "{column} {text:?}");
        let error = &viewer.filter_errors()[*column];
        assert!(error.contains(message), "{column} {text:?}: {error}");
    }
}

#[test]
fn sorted_filtered_windows_follow_the_table_and_keep_their_keys() {
    let session = Session::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("sym", DataType::Utf8, true),
        Field::new("price", DataType::Float64, true),
    ]));
    let input = session.keyed_input_table(&schema, &["sym"]).unwrap();
    let batch = |syms: Vec<&str>, prices: Vec<f64>| {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(syms)) as ArrayRef,
                Arc::new(Float64Array::from(prices)),
            ],
        )
        .unwrap()
    };
    input
        .upsert(&[batch(vec!["aa", "AB", "b", "ca"], vec![3.0, 1.0, 4.0, 2.0])])
        .unwrap();
    session.step();
    let mut viewer = Viewer::new(&input);
    let first = viewer.rows().unwrap().unwrap();
    let key_of: BTreeMap<String, u64> = strings(&rows(&first), "sym")
        .into_iter()
        .flatten()
        .zip(keys(&first))
        .collect();

    filter(&mut viewer, &[("sym", "a")]);
    let sorted = viewer
        .receive(Request::Sort(vec![SortKey {
            column: "price".into(),
            descending: true,
        }]))
        .unwrap()
        .unwrap();
    let names = ["aa", "ca", "AB"];
    assert_eq!(
        strings(&rows(&sorted), "sym"),
        names.map(|s| Some(s.to_owned()))
    );
    assert_eq!(keys(&sorted), names.map(|s| key_of[s]));
    assert_eq!(sorted.total, 3);

    // A cycle moves a row; its key moves with it.
    input.upsert(&[batch(vec!["AB"], vec![9.0])]).unwrap();
    let cycle = session.step();
    let moved = viewer.rows().unwrap().unwrap();
    assert_eq!(keys(&moved)[0], key_of["AB"]);
    assert_eq!((moved.cycle, viewer.changed()), (cycle, Some(cycle)));
    let prices = rows(&moved);
    let prices = prices.column(1).as_primitive::<Float64Type>();
    assert_eq!(prices.values().to_vec(), vec![9.0, 3.0, 2.0]);

    // No sort and no filters show the table as it is.
    viewer.receive(Request::Sort(Vec::new())).unwrap();
    let all = filter(&mut viewer, &[("sym", " ")]);
    assert_eq!(all.num_rows(), 4);
    assert!(viewer.filters().is_empty() && viewer.sort().is_empty());
}

#[test]
fn windows_cover_the_rows_in_view_and_fetches_return_exactly_what_was_asked() {
    let session = Session::new();
    let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));
    let n = Int64Array::from_iter_values(0..300_000);
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(n)]).unwrap();
    let table = session.static_table(schema, vec![batch]).unwrap();
    let mut viewer = Viewer::new(&table);

    let window = viewer
        .receive(Request::Viewport {
            top: 600,
            bottom: 619,
        })
        .unwrap()
        .unwrap();
    let values = rows(&window);
    let values = values
        .column(0)
        .as_primitive::<arrow::datatypes::Int64Type>();
    assert_eq!(window.start, 600 - MARGIN);
    assert_eq!(values.len(), 20 + 2 * MARGIN);
    assert_eq!(values.value(0), (600 - MARGIN) as i64);

    let fetched = viewer
        .receive(Request::Fetch {
            id: 7,
            start: 250,
            count: 5,
        })
        .unwrap()
        .unwrap();
    assert_eq!(
        (fetched.fetched, fetched.start, rows(&fetched).num_rows()),
        (Some(7), 250, 5)
    );
    let capped = viewer
        .receive(Request::Fetch {
            id: 8,
            start: 0,
            count: usize::MAX,
        })
        .unwrap()
        .unwrap();
    assert_eq!(rows(&capped).num_rows(), FETCH_LIMIT);
    // A fetch leaves the window alone.
    assert_eq!(viewer.rows().unwrap().unwrap().start, 600 - MARGIN);
}

#[test]
fn a_viewer_does_not_keep_tables_alive() {
    let session = Session::new();
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Float64, true)]));
    let input = session.input_table(&schema).unwrap();
    let mut viewer = Viewer::new(&input);
    filter(&mut viewer, &[("x", "> 0")]);
    assert_eq!(
        session.table_count(),
        2,
        "the input and the viewer's filter"
    );
    drop(input);
    // The filter still holds the input until the viewer lets go of what it built.
    assert!(viewer.changed().is_none());
    assert_eq!(session.table_count(), 2);
    viewer.release();
    assert_eq!(session.table_count(), 0);
    assert!(viewer.rows().unwrap().is_none());
}

#[test]
fn waiting_for_a_cycle_returns_the_last_finished_one() {
    let session = Session::new();
    assert_eq!(session.wait_cycle(0, Duration::ZERO), 0);
    session.step();
    session.step();
    assert_eq!(session.wait_cycle(0, Duration::from_secs(60)), 2);
    assert_eq!(session.wait_cycle(2, Duration::from_millis(1)), 2);
}
