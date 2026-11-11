//! Joins against a brute-force reference: after every cycle of random writes, each ticking join
//! must hold exactly the rows a nested-loop join of its inputs' current rows gives.

use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::util::display::{ArrayFormatter, FormatOptions};
use ursid_core::{Error, Session, Table};

/// xorshift: reproducible runs without a dependency.
struct Random(u64);

impl Random {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D) % n
    }

    fn sym(&mut self) -> Option<&'static str> {
        [None, Some("a"), Some("b"), Some("c"), Some("d")][self.below(5) as usize]
    }

    fn time(&mut self) -> Option<i64> {
        (self.below(8) != 0).then(|| self.below(20) as i64)
    }
}

fn batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
    RecordBatch::try_from_iter_with_nullable(columns.into_iter().map(|(n, a)| (n, a, true)))
        .unwrap()
}

fn orders_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("sym", DataType::Utf8, true),
        Field::new("ts", DataType::Int64, true),
        Field::new("x", DataType::Int64, true),
    ])
}

fn orders(rng: &mut Random, n: usize) -> RecordBatch {
    let (mut id, mut sym, mut ts, mut x) = (vec![], vec![], vec![], vec![]);
    for _ in 0..n {
        id.push(rng.below(12) as i64);
        sym.push(rng.sym());
        ts.push(rng.time());
        x.push(Some(rng.below(100) as i64));
    }
    batch(vec![
        ("id", Arc::new(Int64Array::from(id))),
        ("sym", Arc::new(StringArray::from(sym))),
        ("ts", Arc::new(Int64Array::from(ts))),
        ("x", Arc::new(Int64Array::from(x))),
    ])
}

fn ids(rng: &mut Random) -> RecordBatch {
    let id: ArrayRef = Arc::new(Int64Array::from_iter_values(
        (0..rng.below(3)).map(|_| rng.below(12) as i64),
    ));
    batch(vec![("id", id)])
}

/// Writes a random cycle's upserts and deletes to the keyed `orders` table; `log`, an
/// append-only table, gains every upserted row.
fn churn(rng: &mut Random, table: &Table, log: &Table) {
    for _ in 0..rng.below(3) {
        let n = rng.below(4) as usize + 1;
        let rows = orders(rng, n);
        table.upsert(std::slice::from_ref(&rows)).unwrap();
        log.append(&[rows]).unwrap();
    }
    if rng.below(2) == 0 {
        table.delete(&[ids(rng)]).unwrap();
    }
}

/// Every row rendered as one string per column, `null` for NULL.
fn cells(table: &Table) -> Vec<Vec<String>> {
    let all = concat_batches(table.schema(), &table.snapshot().unwrap()).unwrap();
    let options = FormatOptions::default().with_null("null");
    let formatters: Vec<_> = all
        .columns()
        .iter()
        .map(|c| ArrayFormatter::try_new(c.as_ref(), &options).unwrap())
        .collect();
    (0..all.num_rows())
        .map(|row| {
            formatters
                .iter()
                .map(|f| f.value(row).to_string())
                .collect()
        })
        .collect()
}

fn sorted(mut rows: Vec<Vec<String>>) -> Vec<Vec<String>> {
    rows.sort();
    rows
}

/// Left rows followed by the right columns `right_columns` of each one's match, or NULLs.
fn reference(
    left: &[Vec<String>],
    right: &[Vec<String>],
    right_columns: &[usize],
    matches: impl Fn(&[String]) -> Option<usize>,
) -> Vec<Vec<String>> {
    left.iter()
        .map(|row| {
            let matched = matches(row);
            let mut out = row.clone();
            out.extend(right_columns.iter().map(|c| match matched {
                Some(r) => right[r][*c].clone(),
                None => "null".into(),
            }));
            out
        })
        .collect()
}

#[test]
fn natural_join_matches_a_nested_loop_join_through_random_updates() {
    let refs_schema = Schema::new(vec![
        Field::new("sym", DataType::Utf8, true),
        Field::new("sector", DataType::Utf8, true),
        Field::new("w", DataType::Float64, true),
    ]);
    for seed in 1..=25 {
        let mut rng = Random(seed);
        let session = Session::new();
        let left = session
            .keyed_input_table(&orders_schema(), &["id"])
            .unwrap();
        let refs = session.keyed_input_table(&refs_schema, &["sym"]).unwrap();
        let log = session.input_table(&orders_schema()).unwrap();
        let joined = left.natural_join(&refs, &["sym"], None).unwrap();
        let logged = log.natural_join(&refs, &["sym"], None).unwrap();
        for cycle in 0..30 {
            churn(&mut rng, &left, &log);
            if rng.below(2) == 0 {
                let n = rng.below(3) as usize + 1;
                let (mut sym, mut sector, mut w) = (vec![], vec![], vec![]);
                for _ in 0..n {
                    sym.push(rng.sym());
                    sector.push(["tech", "energy", "banks"][rng.below(3) as usize]);
                    w.push(rng.below(10) as f64 / 4.0);
                }
                refs.upsert(&[batch(vec![
                    ("sym", Arc::new(StringArray::from(sym))),
                    ("sector", Arc::new(StringArray::from(sector))),
                    ("w", Arc::new(Float64Array::from(w))),
                ])])
                .unwrap();
            }
            if rng.below(4) == 0 {
                let gone: ArrayRef = Arc::new(StringArray::from(vec![rng.sym()]));
                refs.delete(&[batch(vec![("sym", gone)])]).unwrap();
            }
            session.step();

            for (input, output) in [(&left, &joined), (&log, &logged)] {
                let (l, r) = (cells(input), cells(&refs));
                let expected = reference(&l, &r, &[1, 2], |row| {
                    r.iter().position(|ref_row| ref_row[0] == row[1])
                });
                assert_eq!(
                    sorted(cells(output)),
                    sorted(expected),
                    "seed {seed}, cycle {cycle}"
                );
            }
        }
        let fixed_left = session
            .static_table(left.schema().clone(), left.snapshot().unwrap())
            .unwrap();
        let fixed_refs = session
            .static_table(refs.schema().clone(), refs.snapshot().unwrap())
            .unwrap();
        let fixed = fixed_left
            .natural_join(&fixed_refs, &["sym"], None)
            .unwrap();
        assert_eq!(sorted(cells(&fixed)), sorted(cells(&joined)), "seed {seed}");
    }
}

#[test]
fn as_of_join_matches_a_nested_loop_join_through_random_updates() {
    let quotes_schema = Schema::new(vec![
        Field::new("sym", DataType::Utf8, true),
        Field::new("ts", DataType::Int64, true),
        Field::new("q", DataType::Int64, true),
    ]);
    for seed in 1..=25 {
        let mut rng = Random(seed);
        let session = Session::new();
        let left = session
            .keyed_input_table(&orders_schema(), &["id"])
            .unwrap();
        let quotes = session.input_table(&quotes_schema).unwrap();
        let log = session.input_table(&orders_schema()).unwrap();
        let joined = left.as_of_join(&quotes, &["sym"], "ts", None).unwrap();
        let logged = log.as_of_join(&quotes, &["sym"], "ts", None).unwrap();
        for cycle in 0..30 {
            churn(&mut rng, &left, &log);
            if rng.below(3) != 0 {
                let n = rng.below(3) as usize + 1;
                let (mut sym, mut ts, mut q) = (vec![], vec![], vec![]);
                for _ in 0..n {
                    sym.push(rng.sym());
                    ts.push(rng.time());
                    q.push(rng.below(1000) as i64);
                }
                quotes
                    .append(&[batch(vec![
                        ("sym", Arc::new(StringArray::from(sym))),
                        ("ts", Arc::new(Int64Array::from(ts))),
                        ("q", Arc::new(Int64Array::from(q))),
                    ])])
                    .unwrap();
            }
            session.step();

            // The right table's snapshot is in arrival order, so among equal times the last
            // candidate arrived last.
            for (input, output) in [(&left, &joined), (&log, &logged)] {
                let (l, r) = (cells(input), cells(&quotes));
                let expected = reference(&l, &r, &[2], |row| {
                    let at: i64 = row[2].parse().ok()?;
                    r.iter()
                        .enumerate()
                        .filter(|(_, quote)| quote[0] == row[1])
                        .filter_map(|(i, quote)| Some((quote[1].parse::<i64>().ok()?, i)))
                        .filter(|(time, _)| *time <= at)
                        .max()
                        .map(|(_, i)| i)
                });
                assert_eq!(
                    sorted(cells(output)),
                    sorted(expected),
                    "seed {seed}, cycle {cycle}"
                );
            }
        }
        let fixed_left = session
            .static_table(left.schema().clone(), left.snapshot().unwrap())
            .unwrap();
        let fixed_quotes = session
            .static_table(quotes.schema().clone(), quotes.snapshot().unwrap())
            .unwrap();
        let fixed = fixed_left
            .as_of_join(&fixed_quotes, &["sym"], "ts", None)
            .unwrap();
        assert_eq!(sorted(cells(&fixed)), sorted(cells(&joined)), "seed {seed}");
    }
}

#[test]
fn a_second_right_row_for_a_key_fails_a_natural_join() {
    let session = Session::new();
    let left = session.input_table(&orders_schema()).unwrap();
    let refs_schema = Schema::new(vec![
        Field::new("sym", DataType::Utf8, true),
        Field::new("w", DataType::Float64, true),
    ]);
    let refs = session.input_table(&refs_schema).unwrap();
    let joined = left.natural_join(&refs, &["sym"], None).unwrap();
    let quote = |w: f64| {
        batch(vec![
            ("sym", Arc::new(StringArray::from(vec!["a"]))),
            ("w", Arc::new(Float64Array::from(vec![w]))),
        ])
    };
    refs.append(&[quote(1.0)]).unwrap();
    session.step();
    assert_eq!(joined.num_rows().unwrap(), 0);
    refs.append(&[quote(2.0)]).unwrap();
    session.step();
    assert!(matches!(joined.num_rows(), Err(Error::Compute { .. })));
}

#[test]
fn joins_reject_unclear_columns_and_unsuitable_tables() {
    let session = Session::new();
    let left = session.input_table(&orders_schema()).unwrap();
    let other = Schema::new(vec![
        Field::new("sym", DataType::Utf8, true),
        Field::new("x", DataType::Int64, true),
        Field::new("ts", DataType::Float64, true),
    ]);
    let right = session.input_table(&other).unwrap();
    let keyed = session.keyed_input_table(&other, &["sym"]).unwrap();
    let cases = [
        // `x` and `ts` are on both sides.
        left.natural_join(&right, &["sym"], None),
        // Keys of different types.
        left.natural_join(&right, &["x=sym"], Some(&["ts"])),
        // A float time column.
        left.as_of_join(&right, &["sym"], "ts", Some(&["x2=x"])),
        // A right table that can lose rows.
        left.as_of_join(&keyed, &["sym"], "x", Some(&["x2=x"])),
        // An unknown column.
        left.natural_join(&right, &["symbol"], None),
    ];
    for (i, case) in cases.into_iter().enumerate() {
        assert!(
            matches!(case, Err(Error::Schema(_) | Error::Unsupported(_))),
            "case {i} should be rejected"
        );
    }
    let renamed = left
        .natural_join(&right, &["sym"], Some(&["right_x=x", "right_ts=ts"]))
        .unwrap();
    let names: Vec<_> = renamed
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(names, ["id", "sym", "ts", "x", "right_x", "right_ts"]);
}
