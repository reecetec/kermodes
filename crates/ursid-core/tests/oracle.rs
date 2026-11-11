//! The static path is the oracle: after every cycle, each ticking table must equal the same
//! pipeline computed once over a static copy of its input. Float aggregates may differ in the
//! last bits (partials merge where piece boundaries fall), so float columns are compared to a
//! relative 1e-9; everything else must match exactly.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::compute::{SortColumn, concat_batches, lexsort_to_indices, take_record_batch};
use arrow::datatypes::{DataType, Field, Float64Type, Schema};
use arrow::record_batch::RecordBatch;
use ursid_core::{Session, Table};

type Pipeline = fn(&Table) -> Table;

const AGGREGATIONS: &[(&str, &str)] = &[
    ("n", "count(*)"),
    ("sizes", "count(size)"),
    ("total", "sum(size)"),
    ("value", "sum(price)"),
    ("avg", "mean(price)"),
    ("avg_size", "mean(size)"),
    ("vol", "std(price)"),
    ("low", "min(price)"),
    ("high", "max(sym)"),
    ("first_price", "first(price)"),
    ("last_size", "last(size)"),
];

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
    ("last_by", |t| t.last_by(&["sym"]).unwrap()),
    ("last_by two keys", |t| t.last_by(&["sym", "flag"]).unwrap()),
    ("agg_by", |t| t.agg_by(&["sym"], AGGREGATIONS).unwrap()),
    ("agg_by all rows", |t| t.agg_by(&[], AGGREGATIONS).unwrap()),
    ("where then agg_by", |t| {
        t.filter("size > 2")
            .unwrap()
            .agg_by(&["flag"], AGGREGATIONS)
            .unwrap()
    }),
    ("agg_by then where", |t| {
        t.agg_by(&["sym"], AGGREGATIONS)
            .unwrap()
            .filter("n > 2")
            .unwrap()
    }),
    ("last_by then update", |t| {
        t.last_by(&["sym"])
            .unwrap()
            .update(&["double = size * 2"])
            .unwrap()
    }),
    ("where then last_by", |t| {
        t.filter("flag").unwrap().last_by(&["sym"]).unwrap()
    }),
    ("merge", |t| {
        let flagged = t.filter("flag").unwrap();
        Table::merge(&[t, &flagged, t]).unwrap()
    }),
    ("merge then last_by", |t| {
        let flagged = t.filter("flag").unwrap();
        let rest = t.filter("NOT flag OR flag IS NULL").unwrap();
        Table::merge(&[&flagged, &rest])
            .unwrap()
            .last_by(&["sym"])
            .unwrap()
    }),
    ("merge then agg_by", |t| {
        let doubled = t.update(&["size = size * 2"]).unwrap();
        Table::merge(&[t, &doubled])
            .unwrap()
            .agg_by(&["flag"], AGGREGATIONS)
            .unwrap()
    }),
    ("merge of merges", |t| {
        let latest = t.last_by(&["sym"]).unwrap();
        let inner = Table::merge(&[t, &latest]).unwrap();
        Table::merge(&[&latest, &inner]).unwrap()
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
        Field::new("id", DataType::Int64, true),
        Field::new("sym", DataType::Utf8, true),
        Field::new("price", DataType::Float64, true),
        Field::new("size", DataType::Int64, true),
        Field::new("flag", DataType::Boolean, true),
    ])
}

/// Ids come from a small range so keyed writes collide, within and across cycles.
fn random_batch(rng: &mut Rng, rows: usize) -> RecordBatch {
    let mut id = Vec::with_capacity(rows);
    let mut sym = Vec::with_capacity(rows);
    let mut price = Vec::with_capacity(rows);
    let mut size = Vec::with_capacity(rows);
    let mut flag = Vec::with_capacity(rows);
    for _ in 0..rows {
        id.push(Some(rng.below(12) as i64));
        sym.push((!rng.null()).then(|| ["a", "b", "c", "d"][rng.below(4) as usize]));
        price.push((!rng.null()).then(|| (rng.below(1000) as f64 - 500.0) / 100.0));
        size.push((!rng.null()).then(|| rng.below(13) as i64 - 3));
        flag.push((!rng.null()).then(|| rng.below(2) == 0));
    }
    let columns: Vec<(&str, ArrayRef)> = vec![
        ("id", Arc::new(Int64Array::from(id))),
        ("sym", Arc::new(StringArray::from(sym))),
        ("price", Arc::new(Float64Array::from(price))),
        ("size", Arc::new(Int64Array::from(size))),
        ("flag", Arc::new(BooleanArray::from(flag))),
    ];
    RecordBatch::try_from_iter(columns).unwrap()
}

fn random_ids(rng: &mut Rng, rows: usize) -> RecordBatch {
    let ids: Vec<i64> = (0..rows).map(|_| rng.below(12) as i64).collect();
    RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(ids)) as ArrayRef)]).unwrap()
}

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

/// Asserts two sorted tables hold the same rows, floats to within a relative 1e-9.
fn assert_same(got: &RecordBatch, want: &RecordBatch, context: &str) {
    assert_eq!(got.schema(), want.schema(), "{context}: schemas differ");
    assert_eq!(
        got.num_rows(),
        want.num_rows(),
        "{context}: row counts differ"
    );
    for (index, field) in got.schema().fields().iter().enumerate() {
        let (a, b) = (got.column(index), want.column(index));
        if field.data_type() != &DataType::Float64 {
            assert_eq!(a, b, "{context}: column `{}` differs", field.name());
            continue;
        }
        let (a, b) = (
            a.as_primitive::<Float64Type>(),
            b.as_primitive::<Float64Type>(),
        );
        for row in 0..a.len() {
            assert_eq!(
                a.is_null(row),
                b.is_null(row),
                "{context}: `{}` row {row} nulls differ",
                field.name()
            );
            if a.is_null(row) {
                continue;
            }
            let (x, y): (f64, f64) = (a.value(row), b.value(row));
            let close = (x - y).abs() <= 1e-9 * x.abs().max(y.abs()) + 1e-12;
            assert!(close, "{context}: `{}` row {row}: {x} vs {y}", field.name());
        }
    }
}

fn check(session: &Session, input: &Table, ticking: &[Table], seed: u64, cycle: usize) {
    let snapshot = session
        .static_table(input.schema().clone(), input.snapshot().unwrap())
        .unwrap();
    for ((name, build), table) in PIPELINES.iter().zip(ticking) {
        let context = format!("pipeline `{name}`, seed {seed}, cycle {cycle}");
        assert_same(&contents(table), &contents(&build(&snapshot)), &context);
    }
}

#[test]
fn ticking_tables_over_appends_match_their_static_recomputation() {
    for seed in 1..=150 {
        let mut rng = Rng(seed);
        let session = Session::new();
        let input = session.input_table(&schema()).unwrap();
        let ticking: Vec<Table> = PIPELINES.iter().map(|(_, build)| build(&input)).collect();
        for cycle in 0..10 {
            let batches: Vec<RecordBatch> = (0..rng.below(4))
                .map(|_| {
                    let rows = rng.below(40) as usize;
                    random_batch(&mut rng, rows)
                })
                .collect();
            input.append(&batches).unwrap();
            session.step();
            check(&session, &input, &ticking, seed, cycle);
        }
    }
}

/// Large deltas aggregate on every core; small ones on one. Both must give the same result.
#[test]
fn parallel_aggregation_matches_cycle_by_cycle_aggregation() {
    let mut rng = Rng(7);
    let session = Session::new();
    let input = session.input_table(&schema()).unwrap();
    let grouped = [
        input.agg_by(&["sym"], AGGREGATIONS).unwrap(),
        input.agg_by(&["id"], AGGREGATIONS).unwrap(),
        input.agg_by(&["sym", "flag"], AGGREGATIONS).unwrap(),
        input.agg_by(&[], AGGREGATIONS).unwrap(),
    ];
    for _ in 0..100 {
        input.append(&[random_batch(&mut rng, 2_000)]).unwrap();
        session.step();
    }
    let everything = session
        .static_table(input.schema().clone(), input.snapshot().unwrap())
        .unwrap();
    let all_at_once = [
        everything.agg_by(&["sym"], AGGREGATIONS).unwrap(),
        everything.agg_by(&["id"], AGGREGATIONS).unwrap(),
        everything.agg_by(&["sym", "flag"], AGGREGATIONS).unwrap(),
        everything.agg_by(&[], AGGREGATIONS).unwrap(),
    ];
    for (ticking, fixed) in grouped.iter().zip(&all_at_once) {
        assert_same(&contents(ticking), &contents(fixed), &fixed.description());
    }
}

/// The same for row-at-a-time operators: a 300k-row static input runs `where`, `update`,
/// `last_by`, and joins against a static right side in parallel pieces; 2k-row cycles run on one
/// thread. An append-only `where` must also keep its input's row order.
#[test]
fn parallel_row_operators_match_cycle_by_cycle_results() {
    let mut rng = Rng(11);
    let session = Session::new();
    let refs = {
        let sym: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), Some("c"), None]));
        let price: ArrayRef = Arc::new(Float64Array::from(vec![1.5, 2.5, 3.5]));
        let batch = RecordBatch::try_from_iter([("sym", sym), ("price", price)]).unwrap();
        session.static_table(batch.schema(), vec![batch]).unwrap()
    };
    let quotes = {
        let batch = random_batch(&mut rng, 5_000);
        session.static_table(batch.schema(), vec![batch]).unwrap()
    };
    let build = |table: &Table| {
        [
            table
                .filter("size > 2")
                .unwrap()
                .update(&["x = price * 2"])
                .unwrap(),
            table
                .natural_join(&refs, &["sym"], Some(&["ref_price=price"]))
                .unwrap(),
            table
                .as_of_join(
                    &quotes,
                    &["sym"],
                    "size",
                    Some(&["q_price=price", "q_id=id"]),
                )
                .unwrap(),
            table.last_by(&["sym"]).unwrap(),
            table.last_by(&["sym", "flag"]).unwrap(),
        ]
    };
    let input = session.input_table(&schema()).unwrap();
    let ticking = build(&input);
    for _ in 0..150 {
        input.append(&[random_batch(&mut rng, 2_000)]).unwrap();
        session.step();
    }
    let everything = session
        .static_table(input.schema().clone(), input.snapshot().unwrap())
        .unwrap();
    let fixed = build(&everything);
    for (ticking, fixed) in ticking.iter().zip(&fixed) {
        assert_same(&contents(ticking), &contents(fixed), &fixed.description());
    }
    let in_order = |table: &Table| concat_batches(table.schema(), &table.snapshot().unwrap());
    assert_eq!(
        in_order(&ticking[0]).unwrap(),
        in_order(&fixed[0]).unwrap(),
        "where keeps row order"
    );
}

#[test]
fn ticking_tables_over_upserts_and_deletes_match_their_static_recomputation() {
    for seed in 1..=150 {
        let mut rng = Rng(seed);
        let session = Session::new();
        let input = session.keyed_input_table(&schema(), &["id"]).unwrap();
        let ticking: Vec<Table> = PIPELINES.iter().map(|(_, build)| build(&input)).collect();
        for cycle in 0..15 {
            for _ in 0..rng.below(4) {
                let rows = rng.below(8) as usize;
                if rng.below(3) == 0 {
                    input.delete(&[random_ids(&mut rng, rows)]).unwrap();
                } else {
                    input.upsert(&[random_batch(&mut rng, rows)]).unwrap();
                }
            }
            session.step();
            check(&session, &input, &ticking, seed, cycle);
        }
    }
}
