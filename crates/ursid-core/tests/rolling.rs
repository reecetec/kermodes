//! `rolling` against a naive reference: each row's window recomputed from scratch over the
//! input's rows, after every cycle of random appends.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Float64Type, Int64Type, Schema};
use ursid_core::{Error, Session, Table, Window};

/// xorshift64*: small and seedable.
struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D) % n
    }

    fn maybe<T>(&mut self, value: T) -> Option<T> {
        (self.below(8) != 0).then_some(value)
    }
}

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("sym", DataType::Utf8, true),
        Field::new("ts", DataType::Int64, true),
        Field::new("price", DataType::Float64, true),
        Field::new("size", DataType::Int64, true),
    ])
}

const AGGREGATIONS: &[(&str, &str)] = &[
    ("n", "count(*)"),
    ("priced", "count(price)"),
    ("total", "sum(size)"),
    ("avg", "mean(price)"),
    ("vol", "std(price)"),
    ("low", "min(price)"),
    ("high", "max(size)"),
];

/// Appends a batch whose times grow with `clock`, so every group's times never decrease.
fn append(rng: &mut Rng, input: &Table, clock: &mut i64) {
    let rows = rng.below(30) as usize;
    let (mut sym, mut ts, mut price, mut size) = (vec![], vec![], vec![], vec![]);
    for _ in 0..rows {
        *clock += rng.below(6) as i64;
        let s = ["a", "b", "c"][rng.below(3) as usize];
        sym.push(rng.maybe(s));
        ts.push(rng.maybe(*clock));
        let p = (rng.below(400) as f64 - 200.0) / 8.0;
        price.push(rng.maybe(p));
        let z = rng.below(20) as i64 - 5;
        size.push(rng.maybe(z));
    }
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(sym)),
        Arc::new(Int64Array::from(ts)),
        Arc::new(Float64Array::from(price)),
        Arc::new(Int64Array::from(size)),
    ];
    input
        .append(&[RecordBatch::try_new(Arc::new(schema()), columns).unwrap()])
        .unwrap();
}

fn rows(table: &Table) -> RecordBatch {
    concat_batches(table.schema(), &table.snapshot().unwrap()).unwrap()
}

/// Each row's expected aggregates, from its window recomputed over every earlier row.
fn reference(input: &RecordBatch, window: Window<'_>) -> Vec<[Option<f64>; 7]> {
    let sym = input.column(0).as_string::<i32>();
    let ts = input.column(1).as_primitive::<Int64Type>();
    let price = input.column(2).as_primitive::<Float64Type>();
    let size = input.column(3).as_primitive::<Int64Type>();
    let get = |a: &dyn Array, row: usize| a.is_valid(row).then_some(row);
    (0..input.num_rows())
        .map(|row| {
            let same = |other: usize| {
                get(sym, other).map(|o| sym.value(o)) == get(sym, row).map(|r| sym.value(r))
            };
            let members: Vec<usize> = match window {
                Window::Rows(n) => {
                    let group: Vec<usize> = (0..=row).filter(|o| same(*o)).collect();
                    group[group.len().saturating_sub(n)..].to_vec()
                }
                Window::Time { span, .. } => {
                    if ts.is_null(row) {
                        return [None; 7];
                    }
                    let now = ts.value(row);
                    (0..=row)
                        .filter(|o| same(*o) && ts.is_valid(*o))
                        .filter(|o| ts.value(*o) > now - span && ts.value(*o) <= now)
                        .collect()
                }
            };
            let prices: Vec<f64> = members
                .iter()
                .filter(|o| price.is_valid(**o))
                .map(|o| price.value(*o))
                .collect();
            let sizes: Vec<i64> = members
                .iter()
                .filter(|o| size.is_valid(**o))
                .map(|o| size.value(*o))
                .collect();
            let mean =
                (!prices.is_empty()).then(|| prices.iter().sum::<f64>() / prices.len() as f64);
            let std = (prices.len() > 1).then(|| {
                let m = mean.unwrap();
                let squares: f64 = prices.iter().map(|p| (p - m) * (p - m)).sum();
                (squares / (prices.len() - 1) as f64).sqrt()
            });
            [
                Some(members.len() as f64),
                Some(prices.len() as f64),
                (!sizes.is_empty()).then(|| sizes.iter().sum::<i64>() as f64),
                mean,
                std,
                prices.iter().copied().reduce(f64::min),
                sizes.iter().copied().max().map(|v| v as f64),
            ]
        })
        .collect()
}

/// The output's aggregate columns (after the input's four) as floats.
fn results(output: &RecordBatch) -> Vec<[Option<f64>; 7]> {
    (0..output.num_rows())
        .map(|row| {
            std::array::from_fn(|i| {
                let column = output.column(4 + i);
                if column.is_null(row) {
                    return None;
                }
                Some(match column.data_type() {
                    DataType::Float64 => column.as_primitive::<Float64Type>().value(row),
                    _ => column.as_primitive::<Int64Type>().value(row) as f64,
                })
            })
        })
        .collect()
}

#[test]
fn rolling_matches_windows_recomputed_from_scratch() {
    for seed in 1..=30 {
        let mut rng = Rng(seed);
        let session = Session::new();
        let input = session.input_table(&schema()).unwrap();
        let windows = [
            Window::Rows(1),
            Window::Rows(3),
            Window::Time { at: "ts", span: 40 },
        ];
        let rolled: Vec<Table> = windows
            .iter()
            .map(|w| input.rolling(&["sym"], *w, AGGREGATIONS).unwrap())
            .collect();
        let mut clock = 0;
        for cycle in 0..20 {
            for _ in 0..rng.below(3) {
                append(&mut rng, &input, &mut clock);
            }
            session.step();
            let all = rows(&input);
            for (window, table) in windows.iter().zip(&rolled) {
                let (got, want) = (results(&rows(table)), reference(&all, *window));
                assert_eq!(got.len(), want.len());
                for (row, (g, w)) in got.iter().zip(&want).enumerate() {
                    for (column, (g, w)) in g.iter().zip(w).enumerate() {
                        let same = match (g, w) {
                            (Some(g), Some(w)) => {
                                (g - w).abs() <= 1e-9 * g.abs().max(w.abs()) + 1e-9
                            }
                            (None, None) => true,
                            _ => false,
                        };
                        assert!(
                            same,
                            "seed {seed}, cycle {cycle}, {window:?}, row {row}, {}: got {g:?}, want {w:?}",
                            AGGREGATIONS[column].0
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn rolling_needs_rows_in_order() {
    let session = Session::new();
    let keyed = session.keyed_input_table(&schema(), &["sym"]).unwrap();
    let merged = Table::merge(&[
        &session.input_table(&schema()).unwrap(),
        &session.input_table(&schema()).unwrap(),
    ])
    .unwrap();
    for table in [&keyed, &merged, &merged.update(&["x = size + 1"]).unwrap()] {
        let Err(Error::Unsupported(message)) =
            table.rolling(&["sym"], Window::Rows(2), AGGREGATIONS)
        else {
            panic!("rolling over `{}` should be rejected", table.description());
        };
        assert!(message.contains("only appends"), "{message}");
    }

    let input = session.input_table(&schema()).unwrap();
    let windowed = input
        .rolling(
            &["sym"],
            Window::Time { at: "ts", span: 5 },
            &[("n", "count(*)")],
        )
        .unwrap();
    let batch = |ts: Vec<i64>| {
        let n = ts.len();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["a"; n])),
            Arc::new(Int64Array::from(ts)),
            Arc::new(Float64Array::from(vec![1.0; n])),
            Arc::new(Int64Array::from(vec![1; n])),
        ];
        RecordBatch::try_new(Arc::new(schema()), columns).unwrap()
    };
    input.append(&[batch(vec![10, 12])]).unwrap();
    session.step();
    input.append(&[batch(vec![11])]).unwrap();
    session.step();
    let error = windowed.snapshot().unwrap_err().to_string();
    assert!(error.contains("went backwards"), "{error}");

    for (spec, message) in [("first(price)", "not `first`"), ("min(sym)", "number")] {
        let Err(error) = input.rolling(&[], Window::Rows(2), &[("x", spec)]) else {
            panic!("{spec} should be rejected");
        };
        assert!(error.to_string().contains(message), "{spec}: {error}");
    }
}

/// Values that move far from where a window started (0, then near 1e8 with a spread of 0.1)
/// keep an accurate std: shifted sums around the old values would cancel catastrophically.
#[test]
fn rolling_std_stays_accurate_as_values_move_away() {
    let session = Session::new();
    let input = session.input_table(&schema()).unwrap();
    let rolled = input
        .rolling(&[], Window::Rows(3), &[("vol", "std(price)")])
        .unwrap();
    let prices: Vec<f64> = std::iter::once(0.0)
        .chain((1..50).map(|k| 1e8 + 0.1 * k as f64))
        .collect();
    let n = prices.len();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec!["a"; n])),
        Arc::new(Int64Array::from((0..n as i64).collect::<Vec<_>>())),
        Arc::new(Float64Array::from(prices.clone())),
        Arc::new(Int64Array::from(vec![1; n])),
    ];
    input
        .append(&[RecordBatch::try_new(Arc::new(schema()), columns).unwrap()])
        .unwrap();
    session.step();
    let output = rows(&rolled);
    let vol = output.column(4).as_primitive::<Float64Type>();
    for row in 3..n {
        let window = &prices[row - 2..=row];
        let mean = window.iter().sum::<f64>() / 3.0;
        let want = (window.iter().map(|p| (p - mean) * (p - mean)).sum::<f64>() / 2.0).sqrt();
        let got = vol.value(row);
        assert!(
            (got - want).abs() <= 1e-6 * want,
            "row {row}: got {got}, want {want}"
        );
    }
}

/// A 1.2M-row batch rolls in chunks (over a million rows each), shard by shard on every core;
/// 2k-row cycles roll on one thread. Each group sees its rows in the same order either way, so
/// results match exactly.
#[test]
fn parallel_rolling_matches_cycle_by_cycle_rolling() {
    let mut rng = Rng(11);
    let session = Session::new();
    let ticking = session.input_table(&schema()).unwrap();
    let mut clock = 0;
    let mut batches = Vec::new();
    for _ in 0..600 {
        let (mut sym, mut ts, mut price, mut size) = (vec![], vec![], vec![], vec![]);
        for _ in 0..2_000 {
            clock += rng.below(3) as i64;
            let s = format!("s{}", rng.below(500));
            sym.push(rng.maybe(s));
            ts.push(rng.maybe(clock));
            let p = (rng.below(10_000) as f64 - 5_000.0) / 7.0;
            price.push(rng.maybe(p));
            let z = rng.below(100) as i64;
            size.push(rng.maybe(z));
        }
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(sym)),
            Arc::new(Int64Array::from(ts)),
            Arc::new(Float64Array::from(price)),
            Arc::new(Int64Array::from(size)),
        ];
        batches.push(RecordBatch::try_new(Arc::new(schema()), columns).unwrap());
    }
    let windows = [
        Window::Rows(50),
        Window::Time {
            at: "ts",
            span: 300,
        },
    ];
    let rolled: Vec<Table> = windows
        .iter()
        .map(|w| ticking.rolling(&["sym"], *w, AGGREGATIONS).unwrap())
        .collect();
    for batch in &batches {
        ticking.append(std::slice::from_ref(batch)).unwrap();
        session.step();
    }
    let all = concat_batches(&Arc::new(schema()), &batches).unwrap();
    let fixed = session.static_table(Arc::new(schema()), vec![all]).unwrap();
    for (window, table) in windows.iter().zip(&rolled) {
        let at_once = fixed.rolling(&["sym"], *window, AGGREGATIONS).unwrap();
        assert_eq!(rows(table), rows(&at_once), "{window:?}");
    }
}
