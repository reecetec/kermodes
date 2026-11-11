//! Static sort against a reference built from Arrow's per-column comparators: a stable sort of
//! row positions, NULL as the largest value. Ticking sort against the static sort of the same
//! rows, cycle by cycle.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray, UInt32Array,
    make_comparator,
};
use arrow::compute::{SortOptions, concat_batches, take_record_batch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use kermodes_core::{Error, Session, Table};

/// xorshift64*: small and seedable.
struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D) % n
    }

    /// `value` most of the time, NULL otherwise.
    fn maybe<T>(&mut self, value: T) -> Option<T> {
        (self.below(6) != 0).then_some(value)
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("sym", DataType::Utf8, true),
        Field::new("price", DataType::Float64, true),
        Field::new("size", DataType::Int64, true),
        Field::new("flag", DataType::Boolean, true),
    ]))
}

fn random_batch(rng: &mut Rng, rows: usize) -> RecordBatch {
    let (mut sym, mut price, mut size, mut flag) = (vec![], vec![], vec![], vec![]);
    for _ in 0..rows {
        let s = ["a", "b", "bb", "c"][rng.below(4) as usize];
        sym.push(rng.maybe(s));
        let p = rng.below(9) as f64 / 2.0 - 2.0;
        price.push(rng.maybe(p));
        let z = rng.below(5) as i64 - 2;
        size.push(rng.maybe(z));
        let f = rng.below(2) == 0;
        flag.push(rng.maybe(f));
    }
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(sym)),
        Arc::new(Float64Array::from(price)),
        Arc::new(Int64Array::from(size)),
        Arc::new(BooleanArray::from(flag)),
    ];
    RecordBatch::try_new(schema(), columns).unwrap()
}

fn rows(table: &Table) -> RecordBatch {
    concat_batches(table.schema(), &table.snapshot().unwrap()).unwrap()
}

/// `table`'s rows ordered by `keys` (column name, descending), compared column by column.
fn reference(table: &Table, keys: &[(&str, bool)]) -> RecordBatch {
    let all = rows(table);
    let comparators: Vec<_> = keys
        .iter()
        .map(|(name, descending)| {
            let column = all.column(all.schema().index_of(name).unwrap());
            let options = SortOptions {
                descending: *descending,
                nulls_first: *descending,
            };
            make_comparator(column.as_ref(), column.as_ref(), options).unwrap()
        })
        .collect();
    let mut order: Vec<u32> = (0..all.num_rows() as u32).collect();
    order.sort_by(|a, b| {
        comparators
            .iter()
            .map(|compare| compare(*a as usize, *b as usize))
            .find(|o| o.is_ne())
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    take_record_batch(&all, &UInt32Array::from(order)).unwrap()
}

const ORDERS: &[&[(&str, bool)]] = &[
    &[("sym", false)],
    &[("price", true)],
    &[("sym", true), ("price", false)],
    &[("flag", false), ("size", true), ("sym", false)],
    &[
        ("size", false),
        ("flag", true),
        ("price", true),
        ("sym", true),
    ],
];

#[test]
fn sort_matches_a_column_by_column_stable_sort() {
    for seed in 1..=40 {
        let mut rng = Rng(seed);
        let session = Session::new();
        let batches: Vec<RecordBatch> = (0..1 + rng.below(4))
            .map(|_| {
                let rows = rng.below(60) as usize;
                random_batch(&mut rng, rows)
            })
            .collect();
        let input = session.static_table(schema(), batches).unwrap();
        // Inputs read from every kind of storage: chunks, slot columns (an aggregation's
        // output), and a merge that reads through its tables.
        let grouped = input
            .agg_by(
                &["sym", "flag"],
                &[("price", "max(price)"), ("size", "sum(size)")],
            )
            .unwrap();
        let merged = Table::merge(&[&input, &input]).unwrap();
        for table in [&input, &grouped, &merged] {
            for keys in ORDERS {
                let by: Vec<String> = keys
                    .iter()
                    .map(|(name, desc)| {
                        if *desc {
                            format!("{name} desc")
                        } else {
                            (*name).to_owned()
                        }
                    })
                    .collect();
                let by: Vec<&str> = by.iter().map(String::as_str).collect();
                let sorted = table.sort(&by).unwrap();
                assert_eq!(
                    rows(&sorted),
                    reference(table, keys),
                    "seed {seed}, {} sorted by {by:?}",
                    table.description()
                );
            }
        }
    }
}

/// `table` sorted by `keys`, as `sort` entries.
fn sort_by(table: &Table, keys: &[(&str, bool)]) -> Table {
    let by: Vec<String> = keys
        .iter()
        .map(|(name, desc)| match desc {
            true => format!("{name} desc"),
            false => (*name).to_owned(),
        })
        .collect();
    table
        .sort(&by.iter().map(String::as_str).collect::<Vec<_>>())
        .unwrap()
}

/// Rows by position (the viewer's reads) agree with the sorted snapshot.
fn assert_windows_match(sorted: &Table, context: &str) {
    let all = rows(sorted);
    let len = all.num_rows();
    for (start, count) in [(0, 7), (len / 2, 9), (len.saturating_sub(3), 9)] {
        let window = sorted.rows_at(start, count).unwrap();
        assert_eq!(window.total, len, "{context}");
        assert_eq!(
            window.rows,
            all.slice(start, count.min(len - start)),
            "{context}"
        );
    }
}

#[test]
fn ticking_sort_reads_like_the_static_sort_of_its_rows() {
    let keyed_schema = schema();
    for seed in 1..=40 {
        let mut rng = Rng(seed);
        let session = Session::new();
        let keyed = session
            .keyed_input_table(&keyed_schema, &["sym", "size"])
            .unwrap();
        let appended = session.input_table(&schema()).unwrap();
        let grouped = appended
            .agg_by(
                &["sym", "flag"],
                &[("price", "max(price)"), ("size", "sum(size)")],
            )
            .unwrap();
        // Each input with its sort by every order.
        let sorted: Vec<(&Table, Vec<Table>)> = [&keyed, &grouped]
            .into_iter()
            .map(|table| {
                (
                    table,
                    ORDERS.iter().map(|keys| sort_by(table, keys)).collect(),
                )
            })
            .collect();
        for cycle in 0..12 {
            for _ in 0..rng.below(4) {
                let rows = rng.below(30) as usize;
                let batch = random_batch(&mut rng, rows);
                match rng.below(3) {
                    0 => keyed.delete(&[batch.project(&[0, 2]).unwrap()]).unwrap(),
                    _ => keyed.upsert(std::slice::from_ref(&batch)).unwrap(),
                }
                appended.append(&[batch]).unwrap();
            }
            session.step();
            for (input, sorts) in &sorted {
                let fixed = session
                    .static_table(input.schema().clone(), input.snapshot().unwrap())
                    .unwrap();
                for (sorted, keys) in sorts.iter().zip(ORDERS) {
                    let context = format!(
                        "seed {seed}, cycle {cycle}, {} by {keys:?}",
                        input.description()
                    );
                    assert_eq!(rows(sorted), rows(&sort_by(&fixed, keys)), "{context}");
                    assert_windows_match(sorted, &context);
                }
            }
        }
    }
}

/// Enough rows for many blocks: a bulk build, blocks that gain rows and are cut again, and blocks
/// that lose rows and join their neighbours.
#[test]
fn large_ticking_sort_follows_heavy_churn() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("value", DataType::Int64, true),
    ]));
    let batch = |ids: Vec<i64>, values: Vec<i64>| {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)) as ArrayRef,
                Arc::new(Int64Array::from(values)),
            ],
        )
        .unwrap()
    };
    let mut rng = Rng(11);
    let session = Session::new();
    let input = session.keyed_input_table(&schema, &["id"]).unwrap();
    let sorted = input.sort(&["value desc"]).unwrap();
    // Each key's row id, as the viewer sees it: a key names one row while its position moves.
    let mut key_ids: std::collections::HashMap<u64, i64> = std::collections::HashMap::new();
    for cycle in 0..25 {
        // After the first cycle, values crowd into a narrow range: rows pile into a few blocks,
        // which split, and leave the others, which fold.
        let (writes, spread) = if cycle == 0 {
            (20_000, 5_000)
        } else {
            (600, 10)
        };
        let ids: Vec<i64> = (0..writes).map(|_| rng.below(25_000) as i64).collect();
        let values: Vec<i64> = (0..writes)
            .map(|_| 2_500 + rng.below(spread) as i64)
            .collect();
        input.upsert(&[batch(ids, values)]).unwrap();
        let gone: Vec<i64> = (0..rng.below(400))
            .map(|_| rng.below(25_000) as i64)
            .collect();
        let ids =
            RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(gone)) as ArrayRef)])
                .unwrap();
        input.delete(&[ids]).unwrap();
        session.step();
        let fixed = session
            .static_table(schema.clone(), input.snapshot().unwrap())
            .unwrap();
        assert_eq!(
            rows(&sorted),
            rows(&fixed.sort(&["value desc"]).unwrap()),
            "cycle {cycle}"
        );
        let window = sorted.rows_at(0, usize::MAX).unwrap();
        let ids = window
            .rows
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for (key, id) in window.keys.iter().zip(ids.values()) {
            let known = *key_ids.entry(*key).or_insert(*id);
            assert_eq!(known, *id, "cycle {cycle}: key {key} moved to another row");
        }
    }
}

#[test]
fn nothing_is_built_on_a_ticking_sort_and_entries_are_clear() {
    let session = Session::new();
    let ticking = session.input_table(&schema()).unwrap();
    let sorted = ticking.sort(&["sym"]).unwrap();
    let Err(Error::Unsupported(message)) = sorted.filter("size > 0") else {
        panic!("building on a ticking sort should fail");
    };
    assert!(message.contains("sort last"), "{message}");
    let fixed = session.static_table(schema(), vec![]).unwrap();
    assert!(fixed.sort(&["sym"]).unwrap().snapshot().unwrap().is_empty());
    for (entry, expected) in [("sym upward", "\"name desc\""), ("prise", "price")] {
        let Err(error) = fixed.sort(&[entry]) else {
            panic!("{entry} should be rejected");
        };
        let error = error.to_string();
        assert!(error.contains(expected), "{entry}: {error}");
    }
}

/// 300k rows sort in parallel partitions, and two full-range columns with NULLs (65 bits each)
/// need two words, sorted one after the other.
#[test]
fn large_and_wide_sorts_match_the_reference() {
    let mut rng = Rng(7);
    let wide = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, true),
        Field::new("sym", DataType::Utf8, true),
    ]));
    let batches: Vec<RecordBatch> = (0..3)
        .map(|_| {
            let (mut a, mut b, mut sym) = (vec![], vec![], vec![]);
            for _ in 0..100_000 {
                let x = rng.below(u64::MAX) as i64;
                let x = if rng.below(2) == 0 { x } else { x % 50 };
                a.push(rng.maybe(x));
                let y = rng.below(u64::MAX) as i64;
                b.push(rng.maybe(y));
                let s = format!("s{}", rng.below(5000));
                sym.push(rng.maybe(s));
            }
            let columns: Vec<ArrayRef> = vec![
                Arc::new(Int64Array::from(a)),
                Arc::new(Int64Array::from(b)),
                Arc::new(StringArray::from(sym)),
            ];
            RecordBatch::try_new(wide.clone(), columns).unwrap()
        })
        .collect();
    let session = Session::new();
    let input = session.static_table(wide, batches).unwrap();
    for keys in [
        &[("a", false), ("b", true)][..],
        &[("sym", true), ("a", false)][..],
    ] {
        let by: Vec<String> = keys
            .iter()
            .map(|(name, desc)| format!("{name}{}", if *desc { " desc" } else { "" }))
            .collect();
        let by: Vec<&str> = by.iter().map(String::as_str).collect();
        assert_eq!(
            rows(&input.sort(&by).unwrap()),
            reference(&input, keys),
            "{by:?}"
        );
    }
}
