//! A chart's series against a plain recomputation of what it promises (kermodes_core::series):
//! after every cycle of seeded upserts and deletes, the points are every row with an x when they
//! fit the budget, else each x bucket's lowest and highest x and each y's lowest and highest, in
//! x order, earliest row first among equals.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch,
    StringArray, TimestampNanosecondArray,
};
use arrow::compute::{cast, concat_batches};
use arrow::datatypes::{DataType, Field, Float64Type, Schema};
use arrow::ipc::reader::StreamReader;
use kermodes_core::series::Series;
use kermodes_core::{Session, Table};

struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        // xorshift64*
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d) % n
    }

    fn null(&mut self) -> bool {
        self.below(10) == 0
    }
}

fn keyed_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("x", DataType::Float64, true),
        Field::new("a", DataType::Float64, true),
        Field::new("b", DataType::Int32, true),
        Field::new("flag", DataType::Boolean, true),
    ])
}

/// Rows over a range of ids wide enough to need downsampling, narrow enough that writes collide.
/// x repeats (ties) and is sometimes NULL or NaN; so are the ys.
fn random_rows(rng: &mut Rng, rows: usize) -> RecordBatch {
    let mut columns = (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for _ in 0..rows {
        columns.0.push(rng.below(300) as i64);
        columns.1.push(match rng.below(20) {
            0 => None,
            1 => Some(f64::NAN),
            _ => Some(rng.below(120) as f64 / 4.0),
        });
        columns
            .2
            .push((!rng.null()).then(|| rng.below(2000) as f64 / 7.0 - 100.0));
        columns
            .3
            .push((!rng.null()).then(|| rng.below(50) as i32 - 25));
        columns.4.push((!rng.null()).then(|| rng.below(2) == 0));
    }
    RecordBatch::try_new(
        Arc::new(keyed_schema()),
        vec![
            Arc::new(Int64Array::from(columns.0)),
            Arc::new(Float64Array::from(columns.1)),
            Arc::new(Float64Array::from(columns.2)),
            Arc::new(Int32Array::from(columns.3)),
            Arc::new(BooleanArray::from(columns.4)),
        ],
    )
    .unwrap()
}

fn random_ids(rng: &mut Rng, rows: usize) -> RecordBatch {
    let ids: Vec<i64> = (0..rows).map(|_| rng.below(300) as i64).collect();
    RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(ids)) as ArrayRef)]).unwrap()
}

/// A column's values as numbers, as they are drawn: NULL as None.
fn numbers(values: &ArrayRef) -> Vec<Option<f64>> {
    let as_numbers = match values.data_type() {
        DataType::Timestamp(_, _) => {
            cast(&cast(values, &DataType::Int64).unwrap(), &DataType::Float64)
        }
        _ => cast(values, &DataType::Float64),
    }
    .unwrap();
    as_numbers.as_primitive::<Float64Type>().iter().collect()
}

/// The points as rows of numbers, x first; NaN as None, so rows compare.
fn rows_of(batch: &RecordBatch) -> Vec<Vec<Option<f64>>> {
    let columns: Vec<Vec<Option<f64>>> = batch.columns().iter().map(numbers).collect();
    (0..batch.num_rows())
        .map(|row| {
            columns
                .iter()
                .map(|c| c[row].filter(|v| !v.is_nan()))
                .collect()
        })
        .collect()
}

fn points(series: &Series) -> RecordBatch {
    let frame = series.frame().unwrap().unwrap();
    let mut reader = StreamReader::try_new(frame.rows.as_slice(), None).unwrap();
    let batch = reader.next().unwrap().unwrap();
    assert!(reader.next().is_none(), "one batch per frame");
    batch
}

/// The least power of two whose multiples from zero cut `low..=high` into at most `buckets`
/// buckets, found by trying each in turn.
fn width_for(low: f64, high: f64, buckets: usize) -> f64 {
    (-80..200)
        .map(|power| 2f64.powi(power))
        .find(|width| (high / width).floor() - (low / width).floor() + 1.0 <= buckets as f64)
        .unwrap()
}

/// What the series promises for `table`'s rows of `x` and `ys` within `range`, recomputed
/// plainly.
fn expected(
    table: &Table,
    x: &str,
    ys: &[&str],
    points: usize,
    range: Option<(f64, f64)>,
) -> Vec<Vec<Option<f64>>> {
    let all = concat_batches(table.schema(), &table.snapshot().unwrap()).unwrap();
    let mut names = vec![x];
    names.extend(ys);
    let columns: Vec<Vec<Option<f64>>> = names
        .iter()
        .map(|name| numbers(all.column_by_name(name).unwrap()))
        .collect();
    let value = |c: usize, row: usize| columns[c][row].filter(|v| !v.is_nan());
    let with_x: Vec<usize> = (0..all.num_rows())
        .filter(|r| {
            value(0, *r).is_some_and(|x| range.is_none_or(|(from, to)| from <= x && x <= to))
        })
        .collect();
    let mut picked: Vec<usize> = if with_x.len() <= points {
        with_x.clone()
    } else {
        let xs = with_x.iter().map(|r| value(0, *r).unwrap());
        let low = xs.clone().fold(f64::INFINITY, f64::min);
        let high = xs.fold(f64::NEG_INFINITY, f64::max);
        let width = width_for(low, high, (points / (2 + 2 * ys.len())).max(1));
        let bucket = |row: usize| (value(0, row).unwrap() / width).floor() as i64;
        let mut buckets: Vec<i64> = with_x.iter().map(|r| bucket(*r)).collect();
        buckets.sort_unstable();
        buckets.dedup();
        let mut picked = Vec::new();
        for b in buckets {
            let members: Vec<usize> = with_x.iter().copied().filter(|r| bucket(*r) == b).collect();
            for c in 0..names.len() {
                let valued: Vec<(usize, f64)> = members
                    .iter()
                    .filter_map(|r| value(c, *r).map(|v| (*r, v)))
                    .collect();
                // The earliest row of the least and of the greatest value
                if let Some(least) =
                    valued
                        .iter()
                        .fold(None::<(usize, f64)>, |best, p| match best {
                            Some(b) if b.1 <= p.1 => Some(b),
                            _ => Some(*p),
                        })
                {
                    picked.push(least.0);
                }
                if let Some(most) = valued
                    .iter()
                    .fold(None::<(usize, f64)>, |best, p| match best {
                        Some(b) if b.1 >= p.1 => Some(b),
                        _ => Some(*p),
                    })
                {
                    picked.push(most.0);
                }
            }
        }
        picked
    };
    picked.sort_by(|a, b| {
        value(0, *a)
            .unwrap()
            .total_cmp(&value(0, *b).unwrap())
            .then(a.cmp(b))
    });
    picked.dedup();
    picked
        .iter()
        .map(|row| (0..names.len()).map(|c| value(c, *row)).collect())
        .collect()
}

/// A chart under test: its series, the table it should match, its ys, budget, and range.
type Charted<'a> = (Series, &'a Table, Vec<&'a str>, usize, Option<(f64, f64)>);

#[test]
fn series_over_upserts_and_deletes_match_their_recomputation() {
    let ys = ["a".to_owned(), "b".to_owned()];
    for seed in 1..=60 {
        let mut rng = Rng(seed);
        let session = Session::new();
        let input = session.keyed_input_table(&keyed_schema(), &["id"]).unwrap();
        let filtered = input.filter("flag").unwrap();
        // Budgets below, around, and above the row count, by one column and by two, over every
        // x and over a range of it
        let charts: Vec<Charted> = [16, 40, 120, 400]
            .into_iter()
            .flat_map(|points| {
                let mut ranged = Series::new(&input, "x", &ys, None, points).unwrap();
                ranged.set_range(Some((5.0, 20.25))).unwrap();
                [
                    (
                        Series::new(&input, "x", &ys, None, points).unwrap(),
                        &input,
                        vec!["a", "b"],
                        points,
                        None,
                    ),
                    (
                        Series::new(&input, "x", &ys[..1], None, points).unwrap(),
                        &input,
                        vec!["a"],
                        points,
                        None,
                    ),
                    (
                        Series::new(&input, "x", &ys[1..], Some("flag"), points).unwrap(),
                        &filtered,
                        vec!["b"],
                        points,
                        None,
                    ),
                    (ranged, &input, vec!["a", "b"], points, Some((5.0, 20.25))),
                ]
            })
            .collect();
        for cycle in 0..12 {
            for _ in 0..1 + rng.below(4) {
                if rng.below(4) == 0 {
                    let rows = rng.below(40) as usize;
                    input.delete(&[random_ids(&mut rng, rows)]).unwrap();
                } else {
                    let rows = rng.below(120) as usize;
                    input.upsert(&[random_rows(&mut rng, rows)]).unwrap();
                }
            }
            session.step();
            for (series, table, ys, budget, range) in &charts {
                let got = points(series);
                assert!(
                    got.num_rows() <= *budget,
                    "seed {seed} cycle {cycle}: over budget"
                );
                assert_eq!(
                    rows_of(&got),
                    expected(table, "x", ys, *budget, *range),
                    "seed {seed} cycle {cycle}: {ys:?} in {budget} points, range {range:?}"
                );
                assert_eq!(
                    series.frame().unwrap().unwrap().total,
                    table.num_rows().unwrap()
                );
            }
        }
    }
}

fn ticks(n: usize) -> RecordBatch {
    let start = 1_767_225_600_000_000_000_i64;
    let ts: Vec<i64> = (0..n as i64).map(|i| start + i * 1_000_000).collect();
    let price: Vec<f64> = (0..n)
        .map(|i| 100.0 + ((i * 7919) % 1000) as f64 / 10.0)
        .collect();
    let sym: Vec<&str> = (0..n).map(|i| ["AAPL", "MSFT"][i % 2]).collect();
    RecordBatch::try_from_iter([
        (
            "ts",
            Arc::new(TimestampNanosecondArray::from(ts).with_timezone("UTC")) as ArrayRef,
        ),
        ("sym", Arc::new(StringArray::from(sym))),
        ("price", Arc::new(Float64Array::from(price))),
    ])
    .unwrap()
}

#[test]
fn a_ticking_time_series_downsamples_to_its_budget_as_it_grows() {
    let session = Session::new();
    let schema = ticks(0).schema();
    let input = session.input_table(&schema).unwrap();
    let ys = ["price".to_owned()];
    let all = Series::new(&input, "ts", &ys, None, 1000).unwrap();
    let one = Series::new(&input, "ts", &ys, Some("sym = 'MSFT'"), 1000).unwrap();
    let msft = input.filter("sym = 'MSFT'").unwrap();
    for _ in 0..8 {
        input.append(&[ticks(700)]).unwrap();
        session.step();
        assert_eq!(
            rows_of(&points(&all)),
            expected(&input, "ts", &["price"], 1000, None)
        );
        assert_eq!(
            rows_of(&points(&one)),
            expected(&msft, "ts", &["price"], 1000, None)
        );
    }
    // Values are the table's own: the timestamps go out as timestamps
    assert_eq!(
        points(&all).schema().field(0).data_type(),
        schema.field(0).data_type()
    );
}

/// Points as (x, y) for comparing frames: a timestamp's nanoseconds as a float, as bucketed.
fn xy(batch: &RecordBatch) -> Vec<(f64, f64)> {
    rows_of(batch)
        .into_iter()
        .map(|row| (row[0].unwrap(), row[1].unwrap()))
        .collect()
}

#[test]
fn a_growing_series_keeps_the_points_it_has_drawn() {
    // A walk over time: each frame, every point before the last bucket is as the frame before
    // drew it, while the width holds; when the range outgrows it, the points there are a subset
    // of those drawn, never new ones. Only so does a ticking line stay still behind its end.
    let session = Session::new();
    let schema = ticks(0).schema();
    let input = session.input_table(&schema).unwrap();
    let series = Series::new(&input, "ts", &["price".to_owned()], None, 200).unwrap();
    let (mut before, mut drawn) = (None::<(f64, f64)>, Vec::new());
    let mut doublings = 0;
    for (cycle, rows) in [150, 37, 211, 5, 900, 64, 1300, 3, 2500, 410]
        .into_iter()
        .enumerate()
    {
        let start = 1_767_225_600_000_000_000_i64 + input.num_rows().unwrap() as i64 * 1_000_000;
        let batch = ticks(rows);
        let ts: Vec<i64> = (0..rows as i64).map(|i| start + i * 1_000_000).collect();
        let batch = RecordBatch::try_new(
            batch.schema(),
            vec![
                Arc::new(TimestampNanosecondArray::from(ts).with_timezone("UTC")),
                batch.column(1).clone(),
                batch.column(2).clone(),
            ],
        )
        .unwrap();
        input.append(&[batch]).unwrap();
        session.step();
        let now = xy(&points(&series));
        let (low, high) = (now[0].0, now[now.len() - 1].0);
        let width = width_for(low, high, 200 / 4);
        if let Some((width_before, last_before)) = before {
            let kept = |points: &[(f64, f64)]| -> Vec<(f64, f64)> {
                points
                    .iter()
                    .copied()
                    .filter(|p| p.0 < last_before)
                    .collect()
            };
            let (was, is) = (kept(&drawn), kept(&now));
            if width == width_before {
                assert_eq!(
                    is, was,
                    "cycle {cycle}: points before the last bucket moved"
                );
            } else {
                doublings += 1;
                assert!(width > width_before, "cycle {cycle}: the width shrank");
                assert!(
                    is.iter().all(|p| was.contains(p)),
                    "cycle {cycle}: a merge drew a point not drawn before"
                );
            }
        }
        // Where the last bucket started: points from there on may change with the next rows
        before = Some((width, (high / width).floor() * width));
        drawn = now;
    }
    assert!(
        doublings > 0,
        "the walk outgrew no width: it tests nothing of merges"
    );
}

#[test]
fn a_zoomed_series_charts_only_its_range_in_full_detail() {
    let session = Session::new();
    let input = session.input_table(&ticks(0).schema()).unwrap();
    input.append(&[ticks(5000)]).unwrap();
    session.step();
    let mut series = Series::new(&input, "ts", &["price".to_owned()], None, 1000).unwrap();
    let start = 1_767_225_600_000_000_000_f64;
    // 300 ticks of the 5000, a millisecond apart: they fit, so every one is sent
    let range = (start + 1_000.0 * 1e6, start + 1_299.0 * 1e6);
    series.set_range(Some(range)).unwrap();
    let got = xy(&points(&series));
    assert_eq!(got.len(), 300);
    assert_eq!((got[0].0, got[299].0), range);
    assert_eq!(
        rows_of(&points(&series)),
        expected(&input, "ts", &["price"], 1000, Some(range))
    );
    // Back to all of it: downsampled again
    series.set_range(None).unwrap();
    assert!(points(&series).num_rows() <= 1000);
    // A range is two finite numbers, the lower first
    assert!(series.set_range(Some((2.0, 1.0))).is_err());
    assert!(series.set_range(Some((f64::NAN, 1.0))).is_err());
}

#[test]
fn a_text_x_sends_every_row_in_the_tables_order() {
    let session = Session::new();
    let rows = RecordBatch::try_from_iter([
        (
            "sym",
            Arc::new(StringArray::from(vec![
                Some("b"),
                None,
                Some("a"),
                Some("c"),
            ])) as ArrayRef,
        ),
        (
            "change",
            Arc::new(Float64Array::from(vec![1.5, 2.0, -0.5, 3.0])),
        ),
    ])
    .unwrap();
    let board = session.static_table(rows.schema(), vec![rows]).unwrap();
    let got = points(&Series::new(&board, "sym", &["change".to_owned()], None, 16).unwrap());
    let syms: Vec<_> = got.column(0).as_string::<i32>().iter().collect();
    assert_eq!(syms, [Some("b"), Some("a"), Some("c")]);
}

#[test]
fn a_series_refuses_columns_it_cannot_draw() {
    let session = Session::new();
    let input = session.keyed_input_table(&keyed_schema(), &["id"]).unwrap();
    let refused = |x: &str, y: &str| Series::new(&input, x, &[y.to_owned()], None, 100).err();
    assert!(refused("missing", "a").is_some());
    assert!(refused("x", "missing").is_some());
    assert!(refused("flag", "a").is_some(), "a boolean x");
    assert!(refused("x", "flag").is_some(), "a boolean y");
    assert!(Series::new(&input, "x", &[], None, 100).is_err(), "no y");
    assert!(Series::new(&input, "x", &["a".to_owned()], Some("nope >"), 100).is_err());
}
