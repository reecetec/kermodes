use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int32Array, Int64Array, StringArray};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::display::{ArrayFormatter, FormatOptions};
use ursid_core::{Error, Session, Table};

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("sym", DataType::Utf8, true),
        Field::new("price", DataType::Float64, true),
    ])
}

fn quotes(rows: &[(&str, f64)]) -> RecordBatch {
    let columns: Vec<(&str, ArrayRef)> = vec![
        (
            "sym",
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.0))),
        ),
        (
            "price",
            Arc::new(Float64Array::from_iter_values(rows.iter().map(|r| r.1))),
        ),
    ];
    RecordBatch::try_from_iter(columns).unwrap()
}

fn keys(syms: &[&str]) -> RecordBatch {
    let column: ArrayRef = Arc::new(StringArray::from(syms.to_vec()));
    RecordBatch::try_from_iter([("sym", column)]).unwrap()
}

/// Each row rendered as `a=1, b=2`, in table order.
fn rows(table: &Table) -> Vec<String> {
    let all = concat_batches(table.schema(), &table.snapshot().unwrap()).unwrap();
    let options = FormatOptions::default().with_null("null");
    let formatters: Vec<_> = all
        .columns()
        .iter()
        .map(|c| ArrayFormatter::try_new(c.as_ref(), &options).unwrap())
        .collect();
    (0..all.num_rows())
        .map(|row| {
            all.schema()
                .fields()
                .iter()
                .zip(&formatters)
                .map(|(field, f)| format!("{}={}", field.name(), f.value(row)))
                .collect::<Vec<_>>()
                .join(", ")
        })
        .collect()
}

#[test]
fn the_last_write_to_a_key_in_a_cycle_wins() {
    let session = Session::new();
    let book = session.keyed_input_table(&schema(), &["sym"]).unwrap();
    book.upsert(&[quotes(&[("a", 1.0), ("b", 2.0)])]).unwrap();
    session.step();

    book.upsert(&[quotes(&[("a", 1.5), ("a", 1.6), ("c", 3.0)])])
        .unwrap();
    book.delete(&[keys(&["c", "b"])]).unwrap();
    book.upsert(&[quotes(&[("b", 2.5)])]).unwrap();
    session.step();

    assert_eq!(rows(&book), ["sym=a, price=1.6", "sym=b, price=2.5"]);
}

#[test]
fn rows_leave_and_reenter_a_filter_as_they_change() {
    let session = Session::new();
    let book = session.keyed_input_table(&schema(), &["sym"]).unwrap();
    let rich = book.filter("price > 10").unwrap();
    book.upsert(&[quotes(&[("a", 5.0), ("b", 20.0)])]).unwrap();
    session.step();
    assert_eq!(rows(&rich), ["sym=b, price=20.0"]);

    book.upsert(&[quotes(&[("a", 15.0), ("b", 8.0)])]).unwrap();
    session.step();
    assert_eq!(rows(&rich), ["sym=a, price=15.0"]);

    book.delete(&[keys(&["a"])]).unwrap();
    session.step();
    assert!(rows(&rich).is_empty());
}

#[test]
fn aggregations_follow_upserts_and_deletes() {
    let session = Session::new();
    let book = session
        .keyed_input_table(
            &Schema::new(vec![
                Field::new("id", DataType::Int64, true),
                Field::new("sector", DataType::Utf8, true),
                Field::new("size", DataType::Int64, true),
            ]),
            &["id"],
        )
        .unwrap();
    let by_sector = book
        .agg_by(
            &["sector"],
            &[
                ("n", "count(*)"),
                ("total", "sum(size)"),
                ("big", "max(size)"),
            ],
        )
        .unwrap();
    let positions = |rows: &[(i64, &str, i64)]| {
        let columns: Vec<(&str, ArrayRef)> = vec![
            (
                "id",
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
            ),
            (
                "sector",
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1))),
            ),
            (
                "size",
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.2))),
            ),
        ];
        RecordBatch::try_from_iter(columns).unwrap()
    };

    book.upsert(&[positions(&[
        (1, "tech", 10),
        (2, "tech", 30),
        (3, "energy", 5),
    ])])
    .unwrap();
    session.step();
    // agg_by row order follows its shards, so compare as sets.
    assert_eq!(
        sorted(rows(&by_sector)),
        [
            "sector=energy, n=1, total=5, big=5",
            "sector=tech, n=2, total=40, big=30"
        ]
    );

    // Moving id 2 to energy updates both groups; deleting id 3 leaves energy with id 2 only.
    book.upsert(&[positions(&[(2, "energy", 7)])]).unwrap();
    book.delete(&[RecordBatch::try_from_iter([(
        "id",
        Arc::new(Int64Array::from(vec![3])) as ArrayRef,
    )])
    .unwrap()])
        .unwrap();
    session.step();
    assert_eq!(
        sorted(rows(&by_sector)),
        [
            "sector=energy, n=1, total=7, big=7",
            "sector=tech, n=1, total=10, big=10"
        ]
    );
}

fn sorted(mut rows: Vec<String>) -> Vec<String> {
    rows.sort();
    rows
}

#[test]
fn last_by_keeps_the_newest_row_per_group() {
    let session = Session::new();
    let ticks = session.input_table(&schema()).unwrap();
    let latest = ticks.last_by(&["sym"]).unwrap();
    ticks
        .append(&[quotes(&[("a", 1.0), ("b", 2.0), ("a", 3.0)])])
        .unwrap();
    session.step();
    ticks.append(&[quotes(&[("b", 4.0)])]).unwrap();
    session.step();
    assert_eq!(rows(&latest), ["sym=a, price=3.0", "sym=b, price=4.0"]);
}

#[test]
fn writes_must_suit_the_table() {
    let session = Session::new();
    let ticks = session.input_table(&schema()).unwrap();
    let book = session.keyed_input_table(&schema(), &["sym"]).unwrap();
    assert!(matches!(
        ticks.upsert(&[quotes(&[("a", 1.0)])]),
        Err(Error::Unsupported(_))
    ));
    assert!(matches!(
        book.append(&[quotes(&[("a", 1.0)])]),
        Err(Error::Unsupported(_))
    ));
    assert_eq!(
        book.delete(&[quotes(&[("a", 1.0)])])
            .unwrap_err()
            .to_string(),
        "unexpected columns: price"
    );
}

#[test]
fn bad_aggregations_are_explained() {
    let session = Session::new();
    let ticks = session.input_table(&schema()).unwrap();
    let cases = [
        (("x", "median(price)"), "unknown aggregation `median`"),
        (("x", "sum(sym)"), "`sum` needs a number, but `sym` is utf8"),
        (("x", "max(*)"), "only count accepts `*`"),
        (("x", "mean(prise)"), "unknown column `prise`"),
        (("sym", "count(*)"), "duplicate column `sym`"),
    ];
    for (aggregation, message) in cases {
        let Err(error) = ticks.agg_by(&["sym"], &[aggregation]) else {
            panic!("{aggregation:?} should be rejected");
        };
        assert!(error.to_string().contains(message), "{error}");
    }
}

#[test]
fn distinct_keys_stay_distinct_groups() {
    // Keys of every length, differing in one byte: first, middle, or last, and long keys that
    // share their first and last eight bytes.
    let distinct = [
        None,
        Some(""),
        Some("a"),
        Some("b"),
        Some("ab"),
        Some("ba"),
        Some("aaa"),
        Some("aba"),
        Some("abb"),
        Some("bba"),
        Some("abcd"),
        Some("abce"),
        Some("abcdefg"),
        Some("abcxefg"),
        Some("abcdefgh"),
        Some("abcdefgh1"),
        Some("0123456789abcdef"),
        Some("0123456789abcdeF"),
        Some("01234567x9abcdef"),
        Some("aaaaaaaa-x-bbbbbbbb"),
        Some("aaaaaaaa-y-bbbbbbbb"),
        Some("aaaaaaaa-xx-bbbbbbbb"),
    ];
    let twice: Vec<Option<&str>> = distinct.iter().chain(&distinct).copied().collect();
    let column: ArrayRef = Arc::new(StringArray::from(twice));
    let batch = RecordBatch::try_from_iter([("sym", column)]).unwrap();
    let session = Session::new();
    let table = session.static_table(batch.schema(), vec![batch]).unwrap();
    let counts = table.agg_by(&["sym"], &[("n", "count(*)")]).unwrap();
    let expected: Vec<String> = distinct
        .iter()
        .map(|sym| format!("sym={}, n=2", sym.unwrap_or("null")))
        .collect();
    assert_eq!(sorted(rows(&counts)), sorted(expected));
}

#[test]
fn integer_keys_stay_distinct_groups() {
    let session = Session::new();
    let int32 = [
        None,
        Some(0),
        Some(1),
        Some(-1),
        Some(i32::MIN),
        Some(i32::MAX),
    ];
    let int64 = [
        None,
        Some(0),
        Some(1),
        Some(-1),
        Some(i64::MIN),
        Some(i64::MAX),
    ];
    let render = |key: Option<String>| format!("k={}, n=2", key.as_deref().unwrap_or("null"));
    let cases: [(ArrayRef, Vec<String>); 2] = [
        (
            Arc::new(Int32Array::from_iter(int32.iter().chain(&int32).copied())),
            int32
                .iter()
                .map(|k| render(k.map(|v| v.to_string())))
                .collect(),
        ),
        (
            Arc::new(Int64Array::from_iter(int64.iter().chain(&int64).copied())),
            int64
                .iter()
                .map(|k| render(k.map(|v| v.to_string())))
                .collect(),
        ),
    ];
    for (column, expected) in cases {
        let batch = RecordBatch::try_from_iter([("k", column)]).unwrap();
        let table = session.static_table(batch.schema(), vec![batch]).unwrap();
        let counts = table.agg_by(&["k"], &[("n", "count(*)")]).unwrap();
        assert_eq!(sorted(rows(&counts)), sorted(expected));
    }
}
