//! Aggregations whose answers are exact: integer sums and means over 128-bit sums, distinct
//! counts, and float sums, means, and `std` over exact sums that round once, ticking under
//! upserts and deletes as well as computed once, in any chunking and on any number of threads.

use std::process::Command;
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

const EXTREMES: &[(&str, &str)] = &[
    ("n", "count(*)"),
    ("fs", "count(f)"),
    ("i_low", "min(i)"),
    ("i_high", "max(i)"),
    ("f_low", "min(f)"),
    ("f_high", "max(f)"),
    ("s_low", "min(s)"),
    ("s_high", "max(s)"),
    ("b_low", "min(b)"),
    ("b_high", "max(b)"),
    ("d_low", "min(d)"),
    ("t_high", "max(t)"),
    ("i_first", "first(i)"),
    ("f_last", "last(f)"),
    ("s_first", "first(s)"),
    ("b_last", "last(b)"),
    ("d_last", "last(d)"),
    ("t_first", "first(t)"),
];

fn present(rng: &mut Rng) -> bool {
    rng.below(10) > 0
}

/// Rows of `kinds_schema` with keys `ids`, in groups "a" to "c", one value in ten NULL and the
/// rest drawn from a few, so they tie; floats include NaNs of two bit patterns and both zeros.
fn random_kinds(rng: &mut Rng, ids: impl Iterator<Item = i64>) -> RecordBatch {
    let floats = [
        f64::NAN,
        f64::from_bits(0xfff8_0000_0000_0001),
        0.0,
        -0.0,
        1.5,
        -1.5,
        f64::INFINITY,
        f64::NEG_INFINITY,
    ];
    let rows: Vec<Kinds<'_>> = ids
        .map(|id| {
            (
                id,
                ["a", "b", "c"][rng.below(3) as usize],
                present(rng).then(|| rng.below(5) as i32 - 2),
                present(rng).then(|| floats[rng.below(8) as usize]),
                present(rng).then(|| ["", "x", "xy", "y"][rng.below(4) as usize]),
                present(rng).then(|| rng.below(2) == 0),
                present(rng).then(|| rng.below(4) as i32),
                present(rng).then(|| rng.below(4) as i64 * 1_000),
            )
        })
        .collect();
    kinds(&rows)
}

/// Without group columns every row is in one group, whose batches fold whole (`min` and `max`
/// through Arrow's kernels); with them, rows fold one by one. Each group's rows aggregated alone
/// give that group's row of the aggregation by group, over NULLs, ties, NaNs of two bit patterns,
/// and both zeros, computed once and ticking under upserts and deletes.
#[test]
fn one_group_aggregates_as_each_group_among_many() {
    let session = Session::new();
    let mut rng = Rng(7);
    let groups = ["a", "b", "c"];
    let built = |input: &Table| {
        let grouped = input.agg_by(&["g"], EXTREMES).unwrap();
        let alone: Vec<Table> = groups
            .iter()
            .map(|g| {
                input
                    .filter(&format!("g = '{g}'"))
                    .unwrap()
                    .agg_by(&[], EXTREMES)
                    .unwrap()
            })
            .collect();
        (grouped, alone)
    };
    let check = |(grouped, alone): &(Table, Vec<Table>), context: &str| {
        for (g, alone) in groups.iter().zip(alone) {
            let among = grouped.filter(&format!("g = '{g}'")).unwrap();
            let rows = |table: &Table| concat_batches(table.schema(), &table.snapshot().unwrap());
            let (alone, among) = (rows(alone).unwrap(), rows(&among).unwrap());
            for (name, _) in EXTREMES {
                assert_eq!(
                    alone.column_by_name(name),
                    among.column_by_name(name),
                    "{context}: {name} of {g}"
                );
            }
        }
    };

    let batches = (0..3)
        .map(|part| random_kinds(&mut rng, part * 5_000..(part + 1) * 5_000))
        .collect();
    let fixed = session
        .static_table(Arc::new(kinds_schema()), batches)
        .unwrap();
    check(&built(&fixed), "static");

    let input = session.keyed_input_table(&kinds_schema(), &["id"]).unwrap();
    let ticking = built(&input);
    for cycle in 0..20 {
        let keys: Vec<i64> = (0..300).map(|_| rng.below(2_000) as i64).collect();
        input
            .upsert(&[random_kinds(&mut rng, keys.into_iter())])
            .unwrap();
        let gone: Vec<i64> = (0..rng.below(200))
            .map(|_| rng.below(2_000) as i64)
            .collect();
        input.delete(&[ids(&gone)]).unwrap();
        session.step();
        check(&ticking, &format!("cycle {cycle}"));
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

/// Values near 1e9 that differ in their thousandths, whose squares need more than a double's
/// bits: ticking sums, added and removed in another order, still give the static result exactly.
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
        assert_eq!(stds(&ticking), want, "cycle {cycle}");
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

/// xorshift64*: small, seedable.
struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D) % n
    }
}

/// `num / den · 2^exp` rounded once, for a positive `num`, `den` below 2^72, and a normal result:
/// `num` shifted to 128 bits divides to a quotient of at least 56 bits, whose low bit, set for a
/// remainder, lies below the rounding bit, so converting it rounds as the true quotient does.
fn rounded(num: u128, den: u128, exp: i32) -> f64 {
    if num == 0 {
        return 0.0;
    }
    let shift = num.leading_zeros();
    let scaled = num << shift;
    let quotient = (scaled / den) | u128::from(!scaled.is_multiple_of(den));
    quotient as f64 * 2f64.powi(exp - shift as i32)
}

/// `sqrt(num / den · 2^exp)` rounded once, for an even `exp`, `den` below 2^18, and a normal
/// result: an even shift of `num` to 127 bits or more leaves a quotient of at least 2^108, whose
/// integer root of 55 bits or more takes a low bit for any inexactness, as in `rounded`.
fn rounded_root(num: u128, den: u128, exp: i32) -> f64 {
    if num == 0 {
        return 0.0;
    }
    let shift = num.leading_zeros() & !1;
    let scaled = num << shift;
    let quotient = scaled / den;
    let root = quotient.isqrt();
    let exact = scaled.is_multiple_of(den) && root * root == quotient;
    (root | u128::from(!exact)) as f64 * 2f64.powi((exp - shift as i32) / 2)
}

fn signed(negative: bool, value: f64) -> f64 {
    if negative { -value } else { value }
}

fn floats_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("g", DataType::Int64, false),
        Field::new("x", DataType::Float64, true),
        Field::new("y", DataType::Float64, false),
    ]))
}

/// Rows `(id, g, x, y)`.
type FloatRow = (i64, i64, Option<f64>, f64);

fn floats(rows: &[FloatRow]) -> RecordBatch {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.1))),
        Arc::new(Float64Array::from_iter(rows.iter().map(|r| r.2))),
        Arc::new(Float64Array::from_iter_values(rows.iter().map(|r| r.3))),
    ];
    RecordBatch::try_new(floats_schema(), columns).unwrap()
}

const FLOAT_AGGS: [(&str, &str); 3] = [("s", "sum(x)"), ("a", "mean(x)"), ("d", "std(y)")];

/// Each output row as its group and the bits of `s`, `a`, and `d`, in the table's order.
fn float_rows(batch: &RecordBatch) -> Vec<(i64, [Option<u64>; 3])> {
    let g = batch.column(0).as_primitive::<Int64Type>();
    let bits = |name: &str, row: usize| {
        let column = batch
            .column_by_name(name)
            .unwrap()
            .as_primitive::<Float64Type>();
        column.is_valid(row).then(|| column.value(row).to_bits())
    };
    (0..batch.num_rows())
        .map(|row| {
            (
                g.value(row),
                [bits("s", row), bits("a", row), bits("d", row)],
            )
        })
        .collect()
}

fn table_rows(table: &Table) -> Vec<(i64, [Option<u64>; 3])> {
    float_rows(&concat_batches(table.schema(), &table.snapshot().unwrap()).unwrap())
}

/// Rows in random batches of up to `most` rows.
fn chunked(rng: &mut Rng, rows: &[FloatRow], most: u64) -> Vec<RecordBatch> {
    let mut batches = Vec::new();
    let mut start = 0;
    while start < rows.len() {
        let end = (start + 1 + rng.below(most) as usize).min(rows.len());
        batches.push(floats(&rows[start..end]));
        start = end;
    }
    batches
}

/// Sums over 100k random rows in 400 groups, against sums kept in exact integers: `x` is
/// `±m·2^e` with `m` below 2^30 and `e` in -40..=20 (a multiple of 2^-40 below 2^50, with 5%
/// nulls), `y` is `m·2^e` with `m` below 2^12 and `e` in -8..=4 (a multiple of 2^-8). Static
/// tables over several chunkings, an appended table, and an upserted one, after values that
/// come and go, all give the exactly rounded sums, means, and standard deviations, with groups
/// in the order their keys first appear.
fn check_float_aggregates() {
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    let mut rows = Vec::new();
    let mut sums: Vec<(i128, u128, i128, u128, u128)> = vec![(0, 0, 0, 0, 0); 400];
    let mut order = Vec::new();
    for id in 0..100_000 {
        let g = rng.below(400) as i64;
        let sum = &mut sums[g as usize];
        if !order.contains(&g) {
            order.push(g);
        }
        let x = (rng.below(20) != 0).then(|| {
            let (m, e) = (rng.below(1 << 30), rng.below(61) as i32 - 40);
            let negative = rng.below(2) == 0;
            let exact = i128::from(m) << (e + 40);
            sum.0 += if negative { -exact } else { exact };
            sum.1 += 1;
            signed(negative, m as f64 * 2f64.powi(e))
        });
        let (m, e) = (rng.below(1 << 12), rng.below(13) as i32 - 8);
        let exact = i128::from(m) << (e + 8);
        sum.2 += exact;
        sum.3 += (exact * exact) as u128;
        sum.4 += 1;
        rows.push((id, g, x, m as f64 * 2f64.powi(e)));
    }
    let want: Vec<(i64, [Option<u64>; 3])> = order
        .iter()
        .map(|&g| {
            let (s, n, sy, sq, ny) = sums[g as usize];
            let sum = signed(s < 0, rounded(s.unsigned_abs(), 1, -40));
            let mean = signed(s < 0, rounded(s.unsigned_abs(), n, -40));
            let spread = ny * sq - (sy * sy) as u128;
            let std = rounded_root(spread, ny * (ny - 1), -16);
            let bits = |value: f64| Some(value.to_bits());
            let (sum, mean) = if n == 0 {
                (None, None)
            } else {
                (bits(sum), bits(mean))
            };
            (g, [sum, mean, (ny > 1).then(|| std.to_bits())])
        })
        .collect();

    let session = Session::new();
    for most in [1_000, 30_000, 100_000] {
        let fixed = session
            .static_table(floats_schema(), chunked(&mut rng, &rows, most))
            .unwrap()
            .agg_by(&["g"], &FLOAT_AGGS)
            .unwrap();
        assert_eq!(table_rows(&fixed), want, "static, batches of up to {most}");
    }

    let appended = session.input_table(&floats_schema()).unwrap();
    let ticking = appended.agg_by(&["g"], &FLOAT_AGGS).unwrap();
    for batch in chunked(&mut rng, &rows, 20_000) {
        appended.append(&[batch]).unwrap();
        session.step();
    }
    assert_eq!(table_rows(&ticking), want, "appended");

    let upserted = session
        .keyed_input_table(&floats_schema(), &["id"])
        .unwrap();
    let ticking = upserted.agg_by(&["g"], &FLOAT_AGGS).unwrap();
    for batch in chunked(&mut rng, &rows, 20_000) {
        upserted.upsert(&[batch]).unwrap();
        session.step();
    }
    assert_eq!(table_rows(&ticking), want, "upserted");

    // Values far from the rest, NaNs, and infinities arrive as new rows and as modifies of
    // existing ones, then leave: every sum is again exactly what it was.
    let extremes = [
        1e300,
        -1e300,
        1e-300,
        f64::MAX,
        f64::NAN,
        f64::INFINITY,
        -0.0,
    ];
    let mut noise = Vec::new();
    for (i, &value) in extremes.iter().enumerate() {
        let row = rows[rng.below(rows.len() as u64) as usize];
        noise.push((100_000 + i as i64, row.1, Some(value), value));
        noise.push((row.0, row.1, Some(value), -value));
    }
    upserted.upsert(&[floats(&noise)]).unwrap();
    session.step();
    let rebuilt = recomputed(&session, &upserted, &["g"], &FLOAT_AGGS);
    let sorted = |mut rows: Vec<(i64, [Option<u64>; 3])>| {
        rows.sort();
        rows
    };
    assert_eq!(sorted(table_rows(&ticking)), sorted(table_rows(&rebuilt)));
    let new: Vec<i64> = (0..extremes.len() as i64).map(|i| 100_000 + i).collect();
    upserted.delete(&[ids(&new)]).unwrap();
    let restored: Vec<FloatRow> = noise
        .iter()
        .filter(|row| row.0 < 100_000)
        .map(|row| rows[row.0 as usize])
        .collect();
    upserted.upsert(&[floats(&restored)]).unwrap();
    session.step();
    assert_eq!(table_rows(&ticking), want, "after values came and went");
}

#[test]
#[ignore = "run under each thread count by float_aggregates_are_exact_on_every_thread_count"]
fn float_aggregates_are_exact_under_the_thread_count() {
    check_float_aggregates();
}

#[test]
fn float_aggregates_are_exact_on_every_thread_count() {
    for threads in ["1", "3", "8"] {
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "float_aggregates_are_exact_under_the_thread_count",
                "--exact",
                "--ignored",
            ])
            .env("KERMODES_THREADS", threads)
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            output.status.success() && stdout.contains("1 passed"),
            "on {threads} threads:\n{stdout}\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

/// One group's `sum`, `mean`, and `std` of `values`, computed once.
fn aggregated(values: &[f64]) -> (f64, f64, f64) {
    let rows: Vec<FloatRow> = (0..values.len() as i64)
        .map(|i| (i, 0, Some(values[i as usize]), values[i as usize]))
        .collect();
    let session = Session::new();
    let table = session
        .static_table(floats_schema(), vec![floats(&rows)])
        .unwrap()
        .agg_by(&["g"], &FLOAT_AGGS)
        .unwrap();
    let all = contents(&table);
    (
        float(&all, 0, "s"),
        float(&all, 0, "a"),
        float(&all, 0, "d"),
    )
}

#[test]
fn float_sums_are_the_exact_sum_rounded_once() {
    // A float64 running sum loses the 1 to rounding at 1e16.
    assert_eq!(1e16 + 1.0 - 1e16, 0.0);
    assert_eq!(aggregated(&[1e16, 1.0, -1e16]).0, 1.0);
    assert_eq!(aggregated(&[1e16, 1.0, -1e16]).1, 1.0 / 3.0);
    assert_eq!(aggregated(&[1e300, 1e-300, -1e300]).0, 1e-300);
    // Too large for float64 on the way, but not at the end.
    assert_eq!(aggregated(&[f64::MAX, f64::MAX, -f64::MAX]).0, f64::MAX);
    let (sum, mean, std) = aggregated(&[f64::MAX, f64::MAX]);
    assert_eq!((sum, mean, std), (f64::INFINITY, f64::MAX, 0.0));
    // The variance, 1e400, is beyond float64; its root is not.
    assert_eq!(aggregated(&[1e200, -1e200, 0.0]).2, 1e200);
    // An exact zero is +0; the sum of 0.1 ten times rounds once, to 1.
    assert_eq!(aggregated(&[-0.0, -0.0]).0.to_bits(), 0f64.to_bits());
    assert_eq!(aggregated(&[0.1; 10]).0, 1.0);
    assert_eq!(aggregated(&[0.1; 10]).1, 0.1);
}

#[test]
fn float_sums_of_nans_and_infinities_follow_ieee() {
    let inf = f64::INFINITY;
    assert!(aggregated(&[1.0, f64::NAN]).0.is_nan());
    assert_eq!(aggregated(&[inf, 1.0, inf]).0, inf);
    assert_eq!(aggregated(&[-inf, 2.0]).1, -inf);
    assert!(aggregated(&[inf, -inf]).0.is_nan());
    assert!(aggregated(&[inf, 1.0]).2.is_nan());
    assert!(aggregated(&[f64::NAN, 1.0]).2.is_nan());
}
