//! The replay clock against a plain model of what it promises (kermodes_core::replay): each date
//! ordered by event time (ties as written, no time first), time never going back across dates,
//! and each cycle feeding exactly the rows due by its grid time. After every cycle, pipelines
//! over the replayed table equal the same pipelines computed statically over the rows fed so far.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, Float64Array, RecordBatch, StringArray, TimestampNanosecondArray};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::util::display::{ArrayFormatter, FormatOptions};
use kermodes_core::replay::{Chunks, Kind, Replay};
use kermodes_core::{Session, Table};

struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}

const STEP: i64 = 100_000_000; // 100 ms
const DAY: i64 = 86_400_000_000_000;
const START: i64 = 1_767_225_600_000_000_000; // 2026-01-01

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            true,
        ),
        Field::new("sym", DataType::Utf8, true),
        Field::new("px", DataType::Float64, true),
    ]))
}

fn batch(rows: &[(Option<i64>, &str, f64)]) -> RecordBatch {
    let ts: Vec<Option<i64>> = rows.iter().map(|r| r.0).collect();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(TimestampNanosecondArray::from(ts).with_timezone("UTC")) as ArrayRef,
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1))),
            Arc::new(Float64Array::from_iter_values(rows.iter().map(|r| r.2))),
        ],
    )
    .unwrap()
}

type Row = (Option<i64>, &'static str, f64);

/// Dates of rows: mostly within their day, some a little late (yesterday's time), some with no
/// time, ties on purpose; each date's batches as written.
fn random_dates(rng: &mut Rng, dates: usize) -> Vec<Vec<Vec<Row>>> {
    (0..dates)
        .map(|d| {
            (0..1 + rng.below(3))
                .map(|_| {
                    (0..rng.below(40))
                        .map(|_| {
                            let ts = match rng.below(20) {
                                0 => None,
                                // Late: timed early yesterday, before rows already fed
                                1 => Some(
                                    START
                                        + (d as i64 - 1) * DAY
                                        + (rng.below(60) * 50_000_000) as i64,
                                ),
                                _ => Some(
                                    START
                                        + d as i64 * DAY
                                        + (rng.below(60) * 50_000_000) as i64
                                        + (rng.below(3) as i64 * DAY / 4),
                                ),
                            };
                            let sym = ["a", "b", "c"][rng.below(3) as usize];
                            // Whole numbers: sums are exact in any order
                            (ts, sym, rng.below(1000) as f64)
                        })
                        .collect()
                })
                .collect()
        })
        .collect()
}

fn chunks(dates: &[Vec<Vec<Row>>]) -> Chunks {
    let dates: Vec<Vec<RecordBatch>> = dates
        .iter()
        .map(|date| date.iter().map(|rows| batch(rows)).collect())
        .collect();
    Box::new(dates.into_iter().map(Ok))
}

/// When each row is fed, as the replay promises: per date, event-time order (ties as written,
/// no time with its date's first timed row, or at the epoch in a date with none), never earlier
/// than the row before.
fn feeding(dates: &[Vec<Vec<Row>>]) -> Vec<(i64, Row)> {
    let mut floor = i64::MIN;
    let mut fed = Vec::new();
    for date in dates {
        let mut rows: Vec<Row> = date.iter().flatten().copied().collect();
        rows.sort_by_key(|row| row.0.unwrap_or(i64::MIN));
        let first_timed = rows.iter().find_map(|row| row.0);
        let untimed = match first_timed {
            Some(time) => time.max(floor),
            None if floor == i64::MIN => 0,
            None => floor,
        };
        for row in rows {
            floor = row.0.unwrap_or(untimed).max(floor);
            fed.push((floor, row));
        }
    }
    fed
}

/// A table's rows as sorted text, so tables compare as sets of rows.
fn contents(batches: &[RecordBatch], schema: &SchemaRef) -> Vec<Vec<String>> {
    let all = concat_batches(schema, batches).unwrap();
    let options = FormatOptions::default().with_null("∅");
    let formatters: Vec<_> = all
        .columns()
        .iter()
        .map(|c| ArrayFormatter::try_new(c.as_ref(), &options).unwrap())
        .collect();
    let mut rows: Vec<Vec<String>> = (0..all.num_rows())
        .map(|r| formatters.iter().map(|f| f.value(r).to_string()).collect())
        .collect();
    rows.sort();
    rows
}

type Pipeline = fn(&Table) -> Table;

const PIPELINES: &[Pipeline] = &[
    |t| t.filter("px > 20").unwrap(),
    |t| t.update(&["double = px * 2"]).unwrap(),
    |t| {
        t.agg_by(
            &["sym"],
            &[
                ("n", "count(*)"),
                ("total", "sum(px)"),
                ("last", "last(px)"),
            ],
        )
        .unwrap()
    },
    |t| t.last_by(&["sym"]).unwrap(),
];

#[test]
fn a_replay_feeds_each_row_at_its_time_and_matches_the_static_path_every_cycle() {
    for seed in 1..=40 {
        let mut rng = Rng(seed * 7919);
        let dates = random_dates(&mut rng, 3);
        let model = feeding(&dates);
        let mut replay = Replay::new(Duration::from_nanos(STEP as u64)).unwrap();
        let input = replay
            .add("ticks", &schema(), "ts", Kind::Append, chunks(&dates))
            .unwrap();
        let outputs: Vec<Table> = PIPELINES.iter().map(|p| p(&input)).collect();
        let mut last = None;
        while let Some(at) = replay.step().unwrap() {
            assert_eq!(at.rem_euclid(STEP), 0, "seed {seed}: off the grid");
            assert!(
                last.is_none_or(|last| at > last),
                "seed {seed}: time went back"
            );
            last = Some(at);
            let fed: Vec<Row> = model
                .iter()
                .filter(|(t, _)| *t <= at)
                .map(|(_, r)| *r)
                .collect();
            // No idle cycle: the rows due grew since the last one
            let before = model.iter().filter(|(t, _)| *t <= at - STEP).count();
            assert!(
                fed.len() > before,
                "seed {seed}: a cycle at {at} fed nothing"
            );
            let session = Session::new();
            let fixed = session.static_table(schema(), vec![batch(&fed)]).unwrap();
            for (p, (pipeline, output)) in PIPELINES.iter().zip(&outputs).enumerate() {
                let want = pipeline(&fixed);
                assert_eq!(
                    contents(&output.snapshot().unwrap(), output.schema()),
                    contents(&want.snapshot().unwrap(), want.schema()),
                    "seed {seed}, pipeline {p}, cycle at {at}"
                );
            }
        }
        assert_eq!(
            replay.rows(),
            model.len() as u64,
            "seed {seed}: every row fed"
        );
    }
}

#[test]
fn tables_replayed_together_share_one_timeline() {
    let mut rng = Rng(42);
    let (left, right) = (random_dates(&mut rng, 2), random_dates(&mut rng, 2));
    let mut replay = Replay::new(Duration::from_nanos(STEP as u64)).unwrap();
    let a = replay
        .add("a", &schema(), "ts", Kind::Append, chunks(&left))
        .unwrap();
    let b = replay
        .add("b", &schema(), "ts", Kind::Append, chunks(&right))
        .unwrap();
    let (fed_a, fed_b) = (feeding(&left), feeding(&right));
    while let Some(at) = replay.step().unwrap() {
        for (table, model) in [(&a, &fed_a), (&b, &fed_b)] {
            let due = model.iter().filter(|(t, _)| *t <= at).count();
            assert_eq!(table.num_rows().unwrap(), due, "cycle at {at}");
        }
    }
}

#[test]
fn the_same_rows_replay_the_same_way() {
    let run = || {
        let mut rng = Rng(7);
        let dates = random_dates(&mut rng, 3);
        let mut replay = Replay::new(Duration::from_millis(250)).unwrap();
        let input = replay
            .add("t", &schema(), "ts", Kind::Append, chunks(&dates))
            .unwrap();
        let sums = input.agg_by(&["sym"], &[("total", "sum(px)")]).unwrap();
        let mut seen = Vec::new();
        while let Some(at) = replay.step().unwrap() {
            seen.push((at, contents(&sums.snapshot().unwrap(), sums.schema())));
        }
        seen
    };
    assert_eq!(run(), run());
}

fn keyed_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("Op", DataType::Utf8, false),
        Field::new("id", DataType::Utf8, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            true,
        ),
        Field::new("px", DataType::Float64, true),
    ]))
}

/// A log row: change, key, time, value.
type Change = (&'static str, &'static str, Option<i64>, f64);

fn log(changes: &[Change]) -> RecordBatch {
    RecordBatch::try_new(
        keyed_schema(),
        vec![
            Arc::new(StringArray::from_iter_values(changes.iter().map(|c| c.0))) as ArrayRef,
            Arc::new(StringArray::from_iter_values(changes.iter().map(|c| c.1))),
            Arc::new(
                TimestampNanosecondArray::from(changes.iter().map(|c| c.2).collect::<Vec<_>>())
                    .with_timezone("UTC"),
            ),
            Arc::new(Float64Array::from_iter_values(changes.iter().map(|c| c.3))),
        ],
    )
    .unwrap()
}

#[test]
fn a_keyed_log_replays_in_written_order_from_its_first_checkpoint() {
    let mut rng = Rng(99);
    let ids = ["k1", "k2", "k3", "k4"];
    // Three dates of a log; each date after the first restates the state in its checkpoint
    let mut dates: Vec<Vec<Change>> = Vec::new();
    let mut state: BTreeMap<&str, f64> = BTreeMap::new();
    for d in 0..3i64 {
        let mut changes: Vec<Change> = state
            .iter()
            .map(|(id, px)| ("checkpoint", *id, Some(START + d * DAY), *px))
            .collect();
        if d == 0 {
            changes.push(("checkpoint", "k1", Some(START), 1.0));
            state.insert("k1", 1.0);
        }
        for _ in 0..30 {
            let id = ids[rng.below(4) as usize];
            // Out-of-order times within the log, and deletes with none
            let ts = START + d * DAY + rng.below(40) as i64 * 30_000_000;
            if rng.below(4) == 0 {
                changes.push(("delete", id, None, 0.0));
                state.remove(id);
            } else {
                let px = rng.below(100) as f64;
                changes.push(("upsert", id, Some(ts), px));
                state.insert(id, px);
            }
        }
        dates.push(changes);
    }
    // The model: drop later checkpoints; time each change at max(its time, the one before's), a
    // change with no time with the one before it or, before any, its date's first timed change
    let mut timed: Vec<(i64, Change)> = Vec::new();
    let mut floor = i64::MIN;
    for (d, changes) in dates.iter().enumerate() {
        let kept: Vec<&Change> = changes
            .iter()
            .filter(|c| d == 0 || c.0 != "checkpoint")
            .collect();
        let first_timed = kept.iter().find_map(|c| c.2).map(|t| t.max(floor));
        for change in kept {
            floor = change
                .2
                .unwrap_or(floor.max(first_timed.unwrap_or(0)))
                .max(floor);
            timed.push((floor, *change));
        }
    }
    let chunks: Chunks = Box::new(
        dates
            .iter()
            .map(|changes| Ok(vec![log(changes)]))
            .collect::<Vec<_>>()
            .into_iter(),
    );
    let mut replay = Replay::new(Duration::from_nanos(STEP as u64)).unwrap();
    let table = replay
        .add(
            "book",
            &keyed_schema(),
            "ts",
            Kind::Keyed {
                key: &["id"],
                op: "Op",
            },
            chunks,
        )
        .unwrap();
    assert_eq!(
        table.schema().fields().len(),
        3,
        "the replayed table has no Op column"
    );
    let mut cycles = 0;
    while let Some(at) = replay.step().unwrap() {
        cycles += 1;
        let mut want: BTreeMap<&str, f64> = BTreeMap::new();
        for (_, (op, id, _, px)) in timed.iter().filter(|(t, _)| *t <= at) {
            match *op {
                "delete" => {
                    want.remove(id);
                }
                _ => {
                    want.insert(id, *px);
                }
            }
        }
        let got = contents(&table.snapshot().unwrap(), table.schema());
        let got: Vec<(String, String)> = got
            .into_iter()
            .map(|r| (r[0].clone(), r[2].clone()))
            .collect();
        let want: Vec<(String, String)> = want
            .iter()
            .map(|(id, px)| (id.to_string(), format!("{px:?}")))
            .collect();
        assert_eq!(got, want, "cycle at {at}");
    }
    assert!(cycles > 3);
    // Later dates' checkpoints restate the state and are not fed again
    assert_eq!(replay.rows(), timed.len() as u64);
}

#[test]
fn a_replay_refuses_what_it_cannot_order() {
    let mut replay = Replay::new(Duration::from_millis(100)).unwrap();
    let empty: Chunks = Box::new(std::iter::empty());
    assert!(
        replay
            .add("t", &schema(), "px", Kind::Append, empty)
            .is_err(),
        "not a timestamp"
    );
    let empty: Chunks = Box::new(std::iter::empty());
    assert!(
        replay
            .add("t", &schema(), "nope", Kind::Append, empty)
            .is_err(),
        "no column"
    );
    assert!(Replay::new(Duration::ZERO).is_err());
    let mut started = Replay::new(Duration::from_millis(100)).unwrap();
    started
        .add(
            "t",
            &schema(),
            "ts",
            Kind::Append,
            chunks(&[vec![vec![(Some(START), "a", 1.0)]]]),
        )
        .unwrap();
    started.step().unwrap();
    let empty: Chunks = Box::new(std::iter::empty());
    assert!(
        started
            .add("u", &schema(), "ts", Kind::Append, empty)
            .is_err(),
        "added late"
    );
}
