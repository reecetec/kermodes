//! The static path is the oracle: after every cycle, each ticking table must equal the same
//! pipeline computed once over a static copy of its input.

use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use ursid_core::{Session, Table};

type Pipeline = fn(&Table) -> Table;

const PIPELINES: &[(&str, Pipeline)] = &[
    ("where", |t| t.filter("price > 0 AND flag").unwrap()),
    ("update", |t| {
        t.update(&[
            "notional = price * size",
            "half = size / 2",
            "rest = size % 3",
            "neg = -size",
            "known = sym IS NOT NULL",
        ])
        .unwrap()
    }),
    ("where on nulls", |t| {
        t.filter("sym <> 'a' OR size IS NULL").unwrap()
    }),
    ("chain", |t| {
        t.filter("price > -1")
            .unwrap()
            .update(&["notional = price * size", "size = size + 1"])
            .unwrap()
            .filter("notional > 1 OR NOT flag")
            .unwrap()
    }),
];

/// xorshift64*: small, seedable, and enough to shake out ordering and chunking bugs.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn null(&mut self) -> bool {
        self.below(10) == 0
    }
}

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("sym", DataType::Utf8, true),
        Field::new("price", DataType::Float64, true),
        Field::new("size", DataType::Int64, true),
        Field::new("flag", DataType::Boolean, true),
    ])
}

fn random_batch(rng: &mut Rng, rows: usize) -> RecordBatch {
    let mut sym = Vec::with_capacity(rows);
    let mut price = Vec::with_capacity(rows);
    let mut size = Vec::with_capacity(rows);
    let mut flag = Vec::with_capacity(rows);
    for _ in 0..rows {
        sym.push((!rng.null()).then(|| ["a", "b", "c", "d"][rng.below(4) as usize]));
        price.push((!rng.null()).then(|| (rng.below(1000) as f64 - 500.0) / 100.0));
        size.push((!rng.null()).then(|| rng.below(13) as i64 - 3));
        flag.push((!rng.null()).then(|| rng.below(2) == 0));
    }
    let columns: Vec<(&str, ArrayRef)> = vec![
        ("sym", Arc::new(StringArray::from(sym))),
        ("price", Arc::new(Float64Array::from(price))),
        ("size", Arc::new(Int64Array::from(size))),
        ("flag", Arc::new(BooleanArray::from(flag))),
    ];
    RecordBatch::try_from_iter(columns).unwrap()
}

fn contents(table: &Table) -> RecordBatch {
    concat_batches(table.schema(), &table.snapshot().unwrap()).unwrap()
}

#[test]
fn ticking_tables_match_their_static_recomputation() {
    for seed in 1..=200 {
        let mut rng = Rng(seed);
        let session = Session::new();
        let input = session.input_table(&schema()).unwrap();
        let ticking: Vec<Table> = PIPELINES.iter().map(|(_, build)| build(&input)).collect();

        for cycle in 0..12 {
            let appends = rng.below(4);
            let batches: Vec<RecordBatch> = (0..appends)
                .map(|_| {
                    let rows = rng.below(40) as usize;
                    random_batch(&mut rng, rows)
                })
                .collect();
            input.append(&batches).unwrap();
            session.step();

            let snapshot = session
                .static_table(input.schema().clone(), input.snapshot().unwrap())
                .unwrap();
            for ((name, build), table) in PIPELINES.iter().zip(&ticking) {
                let expected = contents(&build(&snapshot));
                assert_eq!(
                    contents(table),
                    expected,
                    "pipeline `{name}` diverged: seed {seed}, cycle {cycle}"
                );
            }
        }
    }
}
