//! Static sort against a reference built from Arrow's per-column comparators: a stable sort of
//! row positions, NULL as the largest value.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray, UInt32Array,
    make_comparator,
};
use arrow::compute::{SortOptions, concat_batches, take_record_batch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use ursid_core::{Error, Session, Table};

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

#[test]
fn sort_needs_a_static_table_and_clear_entries() {
    let session = Session::new();
    let ticking = session.input_table(&schema()).unwrap();
    let Err(Error::Unsupported(message)) = ticking.sort(&["sym"]) else {
        panic!("sorting a ticking table should fail");
    };
    assert!(message.contains("ORDER BY"), "{message}");

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
