//! Aggregations whose answers are exact: integer sums and means over 128-bit sums, distinct
//! counts, and `std` over double-double sums, ticking under upserts and deletes as well as
//! computed once.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Date32Array, Float64Array, Int32Array, Int64Array,
    StringArray, TimestampNanosecondArray,
};
use arrow::compute::{SortColumn, concat_batches, lexsort_to_indices, take_record_batch};
use arrow::datatypes::{DataType, Field, Float64Type, Int64Type, Schema, TimeUnit};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use kermodes_core::{Error, Session, Table};

/// Rows sorted by every column: tables are compared as sets of rows.
fn contents(table: &Table) -> RecordBatch {
    let all = concat_batches(table.schema(), &table.snapshot().unwrap()).unwrap();
    if all.num_rows() == 0 {
        return all;
    }
    let columns: Vec<SortColumn> = all
        .columns()
        .iter()
        .map(|values| SortColumn {
            values: values.clone(),
            options: None,
        })
        .collect();
    let order = lexsort_to_indices(&columns, None).unwrap();
    take_record_batch(&all, &order).unwrap()
}

/// The same aggregation over a static copy of `input`.
fn recomputed(session: &Session, input: &Table, by: &[&str], aggs: &[(&str, &str)]) -> Table {
    session
        .static_table(input.schema().clone(), input.snapshot().unwrap())
        .unwrap()
        .agg_by(by, aggs)
        .unwrap()
}

fn int(batch: &RecordBatch, row: usize, name: &str) -> i64 {
    batch
        .column_by_name(name)
        .unwrap()
        .as_primitive::<Int64Type>()
        .value(row)
}

fn float(batch: &RecordBatch, row: usize, name: &str) -> f64 {
    batch
        .column_by_name(name)
        .unwrap()
        .as_primitive::<Float64Type>()
        .value(row)
}

const BASE: i64 = 1_000_000_000_000_000_000;

fn values(rows: &[(i64, i64)]) -> RecordBatch {
    let columns: Vec<(&str, ArrayRef)> = vec![
        (
            "id",
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
        ),
        (
            "v",
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.1))),
        ),
    ];
    RecordBatch::try_from_iter(columns).unwrap()
}

fn ids(ids: &[i64]) -> RecordBatch {
    let column: ArrayRef = Arc::new(Int64Array::from(ids.to_vec()));
    RecordBatch::try_from_iter([("id", column)]).unwrap()
}

#[test]
fn integer_means_divide_the_exact_sum() {
    let rows = [
        (0, BASE + 43),
        (1, BASE + 43),
        (2, BASE + 43),
        (3, BASE + 129),
    ];
    // A float64 running sum rounds each value to a multiple of 128 and then the total to one of
    // 512, losing the 258 over 4e18; the exact mean, 1e18 + 64.5, rounds to 1e18 + 128.
    let running = rows.iter().map(|r| r.1 as f64).sum::<f64>() / rows.len() as f64;
    assert_eq!(running, BASE as f64);
    let exact = (BASE + 128) as f64;
    assert_ne!(running, exact);

    let aggs = [("total", "sum(v)"), ("avg", "mean(v)")];
    let session = Session::new();
    let fixed = session
        .static_table(values(&rows).schema(), vec![values(&rows)])
        .unwrap()
        .agg_by(&[], &aggs)
        .unwrap();
    let all = contents(&fixed);
    assert_eq!(float(&all, 0, "avg"), exact);
    assert_eq!(int(&all, 0, "total"), 4 * BASE + 258);

    let input = session
        .keyed_input_table(&values(&rows).schema(), &["id"])
        .unwrap();
    let ticking = input.agg_by(&[], &aggs).unwrap();
    input.upsert(&[values(&rows[..2])]).unwrap();
    session.step();
    input.upsert(&[values(&rows[2..])]).unwrap();
    session.step();
    let all = contents(&ticking);
    assert_eq!(float(&all, 0, "avg"), exact);
    assert_eq!(int(&all, 0, "total"), 4 * BASE + 258);

    // Removing and modifying rows subtracts exactly: 3e18 + 385 over 3 is 1e18 + 128.33.
    input.delete(&[ids(&[3])]).unwrap();
    input.upsert(&[values(&[(0, BASE + 299)])]).unwrap();
    session.step();
    let all = contents(&ticking);
    assert_eq!(float(&all, 0, "avg"), (BASE + 128) as f64);
    assert_eq!(int(&all, 0, "total"), 3 * BASE + 385);
    assert_eq!(all, contents(&recomputed(&session, &input, &[], &aggs)));

    // Negative sums round the same way, mirrored.
    input
        .upsert(&[values(&[(0, -BASE - 43), (1, -BASE - 43), (2, -BASE - 43)])])
        .unwrap();
    input.upsert(&[values(&[(3, -BASE - 129)])]).unwrap();
    session.step();
    assert_eq!(float(&contents(&ticking), 0, "avg"), -exact);
}

#[test]
fn an_integer_sum_beyond_int64_fails_its_table() {
    let session = Session::new();
    let schema = values(&[]).schema();
    let fixed = |rows: &[(i64, i64)]| session.static_table(schema.clone(), vec![values(rows)]);

    // Only the result must fit: a sum may pass beyond int64 on the way.
    let through = fixed(&[(0, i64::MAX), (1, 1), (2, -2)]).unwrap();
    let sums = through.agg_by(&[], &[("total", "sum(v)")]).unwrap();
    assert_eq!(int(&contents(&sums), 0, "total"), i64::MAX - 1);

    let beyond = fixed(&[(0, i64::MAX), (1, 1)]).unwrap();
    let means = beyond.agg_by(&[], &[("avg", "mean(v)")]).unwrap();
    assert_eq!(
        float(&contents(&means), 0, "avg"),
        4_611_686_018_427_387_904.0
    );
    let Err(error) = beyond.agg_by(&[], &[("total", "sum(v)")]) else {
        panic!("a sum beyond int64 must fail");
    };
    assert!(matches!(error, Error::Compute { .. }), "{error}");
    let message = "sum of `v` is 9223372036854775808, which does not fit in int64";
    assert!(error.to_string().contains(message), "{error}");

    let input = session.input_table(&schema).unwrap();
    let ticking = input.agg_by(&[], &[("total", "sum(v)")]).unwrap();
    input.append(&[values(&[(0, i64::MAX)])]).unwrap();
    session.step();
    assert_eq!(int(&contents(&ticking), 0, "total"), i64::MAX);
    input.append(&[values(&[(1, 1)])]).unwrap();
    session.step();
    let error = ticking.snapshot().unwrap_err().to_string();
    assert!(error.contains(message), "{error}");
}

fn kinds_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("g", DataType::Utf8, true),
        Field::new("i", DataType::Int32, true),
        Field::new("f", DataType::Float64, true),
        Field::new("s", DataType::Utf8, true),
        Field::new("b", DataType::Boolean, true),
        Field::new("d", DataType::Date32, true),
        Field::new(
            "t",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            true,
        ),
    ])
}

type Kinds<'a> = (
    i64,
    &'a str,
    Option<i32>,
    Option<f64>,
    Option<&'a str>,
    Option<bool>,
    Option<i32>,
    Option<i64>,
);

fn kinds(rows: &[Kinds<'_>]) -> RecordBatch {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
        Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1))),
        Arc::new(Int32Array::from_iter(rows.iter().map(|r| r.2))),
        Arc::new(Float64Array::from_iter(rows.iter().map(|r| r.3))),
        Arc::new(StringArray::from_iter(rows.iter().map(|r| r.4))),
        Arc::new(BooleanArray::from_iter(rows.iter().map(|r| r.5))),
        Arc::new(Date32Array::from_iter(rows.iter().map(|r| r.6))),
        Arc::new(
            TimestampNanosecondArray::from_iter(rows.iter().map(|r| r.7)).with_timezone("UTC"),
        ),
    ];
    RecordBatch::try_new(Arc::new(kinds_schema()), columns).unwrap()
}

const DISTINCT: &[(&str, &str)] = &[
    ("n", "count(*)"),
    ("i", "count_distinct(i)"),
    ("f", "count_distinct(f)"),
    ("s", "count_distinct(s)"),
    ("b", "count_distinct(b)"),
    ("d", "count_distinct(d)"),
    ("t", "count_distinct(t)"),
];

/// Each group's counts, in `DISTINCT` order after `n`, groups sorted.
fn distinct_counts(table: &Table) -> Vec<Vec<i64>> {
    let all = contents(table);
    (0..all.num_rows())
        .map(|row| {
            DISTINCT
                .iter()
                .map(|(name, _)| int(&all, row, name))
                .collect()
        })
        .collect()
}

#[test]
fn count_distinct_counts_each_value_once_over_every_type() {
    let other_nan = f64::from_bits(0xfff8_0000_0000_0001);
    let rows: [Kinds<'_>; 6] = [
        (
            0,
            "a",
            Some(1),
            Some(f64::NAN),
            Some("x"),
            Some(true),
            Some(10),
            Some(100),
        ),
        (
            1,
            "a",
            Some(1),
            Some(other_nan),
            Some("x"),
            Some(false),
            Some(10),
            Some(100),
        ),
        (
            2,
            "a",
            Some(-1),
            Some(0.0),
            Some(""),
            Some(true),
            Some(11),
            Some(200),
        ),
        (3, "a", None, Some(-0.0), None, None, None, None),
        (
            4,
            "a",
            Some(2),
            Some(1.5),
            Some("y"),
            None,
            Some(12),
            Some(100),
        ),
        (5, "b", None, None, None, None, None, None),
    ];
    let session = Session::new();
    let fixed = session
        .static_table(Arc::new(kinds_schema()), vec![kinds(&rows)])
        .unwrap();
    // NaNs of any bits are one value, as are 0.0 and -0.0; NULLs are no value; '' is one.
    let a = vec![5, 3, 3, 3, 2, 3, 2];
    let b = vec![1, 0, 0, 0, 0, 0, 0];
    assert_eq!(
        distinct_counts(&fixed.agg_by(&["g"], DISTINCT).unwrap()),
        [a.clone(), b]
    );
    assert_eq!(
        distinct_counts(&fixed.agg_by(&[], DISTINCT).unwrap()),
        [vec![6, 3, 3, 3, 2, 3, 2]]
    );

    let input = session.keyed_input_table(&kinds_schema(), &["id"]).unwrap();
    let grouped = input.agg_by(&["g"], DISTINCT).unwrap();
    let all = input.agg_by(&[], DISTINCT).unwrap();
    let check = |want: &[Vec<i64>]| {
        assert_eq!(distinct_counts(&grouped), want);
        let again = recomputed(&session, &input, &["g"], DISTINCT);
        assert_eq!(contents(&grouped), contents(&again));
        let again = recomputed(&session, &input, &[], DISTINCT);
        assert_eq!(contents(&all), contents(&again));
    };
    input.upsert(&[kinds(&rows[..3])]).unwrap();
    session.step();
    input.upsert(&[kinds(&rows[3..])]).unwrap();
    session.step();
    check(&[a, vec![1, 0, 0, 0, 0, 0, 0]]);

    // Removing a value's last row drops it; removing one of several keeps it.
    input.delete(&[ids(&[0, 2])]).unwrap();
    session.step();
    check(&[vec![3, 2, 3, 2, 1, 2, 1], vec![1, 0, 0, 0, 0, 0, 0]]);

    // A row moving group leaves its values behind only if no other row holds them.
    let moved: Kinds<'_> = (
        4,
        "b",
        Some(7),
        Some(0.0),
        Some("x"),
        Some(true),
        Some(10),
        Some(100),
    );
    input.upsert(&[kinds(&[moved])]).unwrap();
    session.step();
    check(&[vec![2, 1, 2, 1, 1, 1, 1], vec![2, 1, 1, 1, 1, 1, 1]]);

    // Values come back after leaving.
    input.upsert(&[kinds(&rows[..1])]).unwrap();
    session.step();
    check(&[vec![3, 1, 2, 1, 2, 1, 1], vec![2, 1, 1, 1, 1, 1, 1]]);
}

/// Many long strings arriving and leaving, so interned strings are freed, their ids reused, and
/// their buffer compacted; `min` beside them makes groups that lose their minimum recompute,
/// dropping their distinct values wholesale.
#[test]
fn count_distinct_of_churning_strings_matches_its_recomputation() {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("g", DataType::Int64, true),
        Field::new("s", DataType::Utf8, true),
    ]);
    let batch = |rows: Vec<(i64, Option<String>)>| {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0 % 5))),
            Arc::new(StringArray::from_iter(rows.iter().map(|r| r.1.as_deref()))),
        ];
        RecordBatch::try_new(Arc::new(schema.clone()), columns).unwrap()
    };
    let aggs = [("n", "count_distinct(s)"), ("low", "min(s)")];
    let session = Session::new();
    let input = session.keyed_input_table(&schema, &["id"]).unwrap();
    let grouped = input.agg_by(&["g"], &aggs).unwrap();
    let all = input.agg_by(&[], &aggs).unwrap();
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    let mut next = |n: u64| {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state % n
    };
    for cycle in 0..40 {
        let rows: Vec<(i64, Option<String>)> = (0..200)
            .map(|_| {
                let id = next(400) as i64;
                let text = (next(10) > 0).then(|| {
                    format!(
                        "{}-{}",
                        "long string value ".repeat(4),
                        next(60 + cycle * 10)
                    )
                });
                (id, text)
            })
            .collect();
        input.upsert(&[batch(rows)]).unwrap();
        let gone: Vec<i64> = (0..next(300)).map(|_| next(400) as i64).collect();
        input.delete(&[ids(&gone)]).unwrap();
        session.step();
        for (table, by) in [(&grouped, &["g"][..]), (&all, &[][..])] {
            let again = recomputed(&session, &input, by, &aggs);
            assert_eq!(contents(table), contents(&again), "cycle {cycle}");
        }
    }
}

fn readings(rows: &[(i64, &str, f64)]) -> RecordBatch {
    let columns: Vec<(&str, ArrayRef)> = vec![
        (
            "id",
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
        ),
        (
            "g",
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1))),
        ),
        (
            "x",
            Arc::new(Float64Array::from_iter_values(rows.iter().map(|r| r.2))),
        ),
    ];
    RecordBatch::try_from_iter(columns).unwrap()
}

/// Each group's `std(x)`, groups sorted; `None` for a group of one row.
fn stds(table: &Table) -> Vec<(String, Option<f64>)> {
    let all = contents(table);
    let (g, s) = (
        all.column_by_name("g").unwrap().as_string::<i32>(),
        all.column_by_name("s")
            .unwrap()
            .as_primitive::<Float64Type>(),
    );
    (0..all.num_rows())
        .map(|row| {
            (
                g.value(row).to_owned(),
                s.is_valid(row).then(|| s.value(row)),
            )
        })
        .collect()
}

#[test]
fn std_is_exactly_zero_once_the_values_that_differed_leave() {
    let rows = [
        (0, "a", 5.0),
        (1, "a", 5.0),
        (2, "a", 9.0),
        (3, "a", 5.0),
        (4, "a", 1.0),
        (5, "b", 1e9),
        (6, "b", 1e9 + 3.0),
        (7, "b", 1e9),
    ];
    let aggs = [("s", "std(x)")];
    let session = Session::new();
    let input = session
        .keyed_input_table(&readings(&rows).schema(), &["id"])
        .unwrap();
    let ticking = input.agg_by(&["g"], &aggs).unwrap();
    input.upsert(&[readings(&rows[..4])]).unwrap();
    session.step();
    input.upsert(&[readings(&rows[4..])]).unwrap();
    session.step();
    assert_eq!(
        stds(&ticking),
        stds(&recomputed(&session, &input, &["g"], &aggs))
    );

    input.delete(&[ids(&[2, 4, 6])]).unwrap();
    session.step();
    let zero = vec![("a".to_owned(), Some(0.0)), ("b".to_owned(), Some(0.0))];
    assert_eq!(stds(&ticking), zero);
    assert_eq!(stds(&recomputed(&session, &input, &["g"], &aggs)), zero);

    // A value modified away and back leaves no trace either.
    input.upsert(&[readings(&[(3, "a", 7.25)])]).unwrap();
    session.step();
    input.upsert(&[readings(&[(3, "a", 5.0)])]).unwrap();
    session.step();
    assert_eq!(stds(&ticking), zero);
}

/// Values near 1e9 that differ in their thousandths: their squares need all of a double-double,
/// so ticking sums round differently from a static one, but only near 2^-106 of the sums.
#[test]
fn std_of_values_with_a_large_offset_matches_its_recomputation() {
    let aggs = [("s", "std(x)")];
    let session = Session::new();
    let input = session
        .keyed_input_table(&readings(&[]).schema(), &["id"])
        .unwrap();
    let ticking = input.agg_by(&["g"], &aggs).unwrap();
    let mut state = 0x2545_f491_4f6c_dd1d_u64;
    let mut next = |n: u64| {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state % n
    };
    let groups = ["a", "b", "c"];
    for cycle in 0..40 {
        let rows: Vec<(i64, &str, f64)> = (0..30)
            .map(|_| {
                let id = next(60) as i64;
                let x = 1e9 + next(2000) as f64 * 0.001;
                (id, groups[(id % 3) as usize], x)
            })
            .collect();
        input.upsert(&[readings(&rows)]).unwrap();
        let gone: Vec<i64> = (0..next(20)).map(|_| next(60) as i64).collect();
        input.delete(&[ids(&gone)]).unwrap();
        session.step();
        let want = stds(&recomputed(&session, &input, &["g"], &aggs));
        let got = stds(&ticking);
        assert_eq!(got.len(), want.len(), "cycle {cycle}");
        for ((group, a), (_, b)) in got.iter().zip(&want) {
            match (a, b) {
                (Some(a), Some(b)) => assert!(
                    (a - b).abs() <= 1e-9 * a.abs().max(*b),
                    "cycle {cycle}, group {group}: {a} vs {b}"
                ),
                _ => assert_eq!(a, b, "cycle {cycle}, group {group}"),
            }
        }
    }
}

/// `count(*)` reads no column of its table, so a stored table's scan gives it rows without any:
/// they are still counted, static and ticking.
#[test]
fn count_of_a_table_without_columns_counts_its_rows() {
    let session = Session::new();
    let schema = Arc::new(Schema::empty());
    let rows = |n| {
        let options = RecordBatchOptions::new().with_row_count(Some(n));
        RecordBatch::try_new_with_options(schema.clone(), Vec::new(), &options).unwrap()
    };
    let fixed = session
        .static_table(schema.clone(), vec![rows(3), rows(2)])
        .unwrap();
    let counted = fixed.agg_by(&[], &[("n", "count(*)")]).unwrap();
    let count = |table: &Table| {
        contents(table)
            .column(0)
            .as_primitive::<Int64Type>()
            .value(0)
    };
    assert_eq!(count(&counted), 5);

    let ticking = session.input_table(&schema).unwrap();
    let counted = ticking.agg_by(&[], &[("n", "count(*)")]).unwrap();
    ticking.append(&[rows(4)]).unwrap();
    session.step();
    ticking.append(&[rows(1)]).unwrap();
    session.step();
    assert_eq!(count(&counted), 5);
}
