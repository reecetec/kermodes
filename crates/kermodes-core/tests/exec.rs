//! `exec::execute` against `Plan::build` over a static table of the same rows: seeded random
//! data split into random parts, seeded random plans over it, the same schema and rows in the
//! same order, also over a source that reads its parts again by column and row. Plus what
//! streaming promises: a head opens only the parts it needs, an aggregation holds no input batch
//! it has folded, and a top-N or a filter over a source read again reads its other columns for
//! the rows it keeps alone.

use std::hash::{DefaultHasher, Hash, Hasher};
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int16Array, Int64Array, RecordBatch, StringArray,
    TimestampNanosecondArray, UInt64Array, new_empty_array, new_null_array,
};
use arrow::compute::{
    SortOptions, concat, concat_batches, sort_to_indices, take, take_record_batch,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::util::display::{ArrayFormatter, FormatOptions};
use kermodes_core::exec::{Batches, ColumnStats, Part, PartStats, Reread, Source, Stats, execute};
use kermodes_core::plan::{Plan, Scan};
use kermodes_core::{Error, Session, SortKey, Window};

/// xorshift64*: small, seedable.
struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D) % n
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }

    fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len() as u64) as usize]
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("m", DataType::Int16, true),
        Field::new("s", DataType::Utf8, true),
        Field::new("x", DataType::Float64, true),
        Field::new("n", DataType::Int64, true),
        Field::new(
            "t",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            true,
        ),
        Field::new("b", DataType::Boolean, true),
    ]))
}

/// Floats are mostly thousandths, whose sums round, and one in eight a 53-bit value scaled by up
/// to 2^±384, whose sums need more than 128 bits (and whose squares stay finite, so a rolling
/// `std` has no NaN to compare). Each is drawn from one random number, as the plans that follow
/// expect. Times never go back from `clock` on, so rolling time windows over the rows as read
/// hold.
fn random_batch(rng: &mut Rng, rows: usize, clock: &mut i64) -> RecordBatch {
    let null = |rng: &mut Rng| rng.chance(10);
    let k: Int64Array = (0..rows)
        .map(|_| (!null(rng)).then(|| rng.below(12) as i64))
        .collect();
    let m: Int16Array = (0..rows)
        .map(|_| (!null(rng)).then(|| rng.below(5) as i16 - 2))
        .collect();
    let s: StringArray = (0..rows)
        .map(|_| (!null(rng)).then(|| *rng.pick(&["a", "b", "c", "dd", ""])))
        .collect();
    let x: Float64Array = (0..rows)
        .map(|_| {
            (!null(rng)).then(|| {
                let bits = rng.below(u64::MAX);
                let (kind, rest) = (bits & 7, bits >> 3);
                if kind == 0 {
                    let exponent = (rest >> 53 & 63) as i32 * 12 - 384;
                    let value = (rest & ((1 << 53) - 1)) as f64 * 2f64.powi(exponent);
                    if bits >> 63 == 1 { -value } else { value }
                } else {
                    (rest % 2_000_001) as f64 / 1_000.0 - 1_000.0
                }
            })
        })
        .collect();
    let n: Int64Array = (0..rows)
        .map(|_| (!null(rng)).then(|| rng.below(2_000_001) as i64 - 1_000_000))
        .collect();
    let t: TimestampNanosecondArray = (0..rows)
        .map(|_| {
            *clock += rng.below(3) as i64 * 1_000;
            (!null(rng)).then_some(1_700_000_000_000_000_000 + *clock)
        })
        .collect();
    let b: BooleanArray = (0..rows)
        .map(|_| (!null(rng)).then(|| rng.chance(50)))
        .collect();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(k),
        Arc::new(m),
        Arc::new(s),
        Arc::new(x),
        Arc::new(n),
        Arc::new(t.with_timezone("UTC")),
        Arc::new(b),
    ];
    RecordBatch::try_new(schema(), columns).unwrap()
}

/// Random rows as parts of batches, some of either empty.
fn random_parts(rng: &mut Rng) -> Vec<Vec<RecordBatch>> {
    let mut clock = 0;
    (0..rng.below(7))
        .map(|_| {
            (0..rng.below(4))
                .map(|_| {
                    let rows = if rng.chance(20) { 0 } else { rng.below(400) };
                    random_batch(rng, rows as usize, &mut clock)
                })
                .collect()
        })
        .collect()
}

fn source(parts: &[Vec<RecordBatch>]) -> Source {
    Source {
        schema: schema(),
        parts: parts
            .iter()
            .map(|batches| {
                let batches = batches.clone();
                Box::new(move || Ok(Box::new(batches.into_iter().map(Ok)) as Batches)) as Part
            })
            .collect(),
        reread: None,
        stats: None,
    }
}

/// Exact statistics of each of `parts`, as a source of them would give: rows, and each column's
/// least and greatest non-NULL values and NULLs.
fn stats_of(parts: &[Vec<RecordBatch>]) -> Stats {
    let held = Arc::new(parts.to_vec());
    Arc::new(move |part, columns| {
        let batches = &held[part];
        let rows = batches.iter().map(|batch| batch.num_rows() as u64).sum();
        let columns = columns
            .iter()
            .map(|&at| {
                let data_type = schema().field(at).data_type().clone();
                let arrays: Vec<&dyn Array> = batches
                    .iter()
                    .map(|batch| batch.column(at).as_ref())
                    .collect();
                let all = if arrays.is_empty() {
                    new_empty_array(&data_type)
                } else {
                    concat(&arrays).unwrap()
                };
                let first = |descending| -> ArrayRef {
                    let options = SortOptions {
                        descending,
                        nulls_first: false,
                    };
                    let at = sort_to_indices(&all, Some(options), Some(1)).unwrap();
                    if at.is_empty() {
                        new_null_array(&data_type, 1)
                    } else {
                        take(&all, &at, None).unwrap()
                    }
                };
                Some(ColumnStats {
                    min: first(false),
                    max: first(true),
                    nulls: all.null_count() as u64,
                })
            })
            .collect();
        Ok(Some(PartStats {
            rows,
            whole: true,
            columns,
        }))
    })
}

/// Each read of a source read again: its part, its columns, and how many rows it asked for
/// (all where `None`).
type Reads = Arc<Mutex<Vec<(usize, Vec<usize>, Option<usize>)>>>;

/// `parts` as a source that reads them again by column and row, logging each read, and knows
/// each part's statistics.
fn rereadable(parts: &[Vec<RecordBatch>], reads: &Reads) -> Source {
    let held = Arc::new(parts.to_vec());
    let count = held.len();
    let reads = reads.clone();
    let reread: Reread = Arc::new(move |part, columns, rows| {
        let asked = rows.map(<[u64]>::len);
        reads.lock().unwrap().push((part, columns.to_vec(), asked));
        let mut start = 0;
        let batches: Vec<_> = held[part]
            .iter()
            .map(|batch| {
                let batch = batch.project(columns).unwrap();
                let end = start + batch.num_rows() as u64;
                let taken = rows.map(|rows| {
                    let from = rows.partition_point(|&row| row < start);
                    let to = rows.partition_point(|&row| row < end);
                    let indices =
                        UInt64Array::from_iter_values(rows[from..to].iter().map(|row| row - start));
                    take_record_batch(&batch, &indices).unwrap()
                });
                start = end;
                Ok(taken.unwrap_or(batch))
            })
            .collect();
        Ok(Box::new(batches.into_iter()) as Batches)
    });
    let all: Arc<[usize]> = (0..schema().fields().len()).collect();
    Source {
        schema: schema(),
        parts: (0..count)
            .map(|part| {
                let (reread, all) = (reread.clone(), all.clone());
                Box::new(move || reread(part, &all, None)) as Part
            })
            .collect(),
        reread: Some(reread),
        stats: Some(stats_of(parts)),
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Int,
    Short,
    Float,
    Str,
    Time,
    Bool,
}

/// How a plan's rows change, as the engine tracks it: `rolling` needs append-only rows, and
/// `as_of_join` a right side that only gains rows.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Growth {
    Changing,
    AddOnly,
    AppendOnly,
}

/// A plan with the columns it gives and how its rows grow.
#[derive(Clone)]
struct Shaped {
    plan: Plan,
    columns: Vec<(String, Kind)>,
    growth: Growth,
}

fn scan() -> Shaped {
    Shaped {
        plan: Plan::Scan(Scan {
            name: "t".into(),
            predicate: None,
            columns: None,
            limit: None,
            live: false,
        }),
        columns: vec![
            ("k".into(), Kind::Int),
            ("m".into(), Kind::Short),
            ("s".into(), Kind::Str),
            ("x".into(), Kind::Float),
            ("n".into(), Kind::Int),
            ("t".into(), Kind::Time),
            ("b".into(), Kind::Bool),
        ],
        growth: Growth::AppendOnly,
    }
}

/// Builds random plans, naming the columns they add apart.
struct Planner {
    rng: Rng,
    next: usize,
}

impl Planner {
    fn name(&mut self, prefix: &str) -> String {
        self.next += 1;
        format!("{prefix}{}", self.next)
    }

    fn column(&mut self, shaped: &Shaped, kinds: &[Kind]) -> Option<(String, Kind)> {
        let fits: Vec<_> = shaped
            .columns
            .iter()
            .filter(|(_, kind)| kinds.contains(kind))
            .cloned()
            .collect();
        (!fits.is_empty()).then(|| self.rng.pick(&fits).clone())
    }

    fn named(&self, shaped: &Shaped, name: &str, kind: Kind) -> bool {
        shaped.columns.iter().any(|(n, k)| n == name && *k == kind)
    }

    fn predicate(&mut self, shaped: &Shaped) -> String {
        let all = [
            Kind::Int,
            Kind::Short,
            Kind::Float,
            Kind::Str,
            Kind::Time,
            Kind::Bool,
        ];
        let (column, kind) = self.column(shaped, &all).expect("plans keep a column");
        let one = match kind {
            Kind::Int | Kind::Short => {
                let at = if column == "n" {
                    "0".to_owned()
                } else {
                    (self.rng.below(12) as i64 - 2).to_string()
                };
                format!("{column} > {at}")
            }
            Kind::Float => format!("{column} < {}", self.rng.below(200) as f64 - 100.0),
            Kind::Str => match self.rng.below(2) {
                0 => format!("{column} = 'a'"),
                _ => format!("{column} <> 'b' OR {column} IS NULL"),
            },
            Kind::Time => format!("{column} IS NOT NULL"),
            Kind::Bool => match self.rng.below(2) {
                0 => column,
                _ => format!("NOT {column}"),
            },
        };
        if self.rng.chance(25) {
            let other = self.predicate(shaped);
            return format!("({one}) AND ({other})");
        }
        one
    }

    /// `name = expression` over `shaped`'s columns, and the kind of its value.
    fn expression(&mut self, shaped: &Shaped, name: &str) -> Option<(String, Kind)> {
        let numbers = [Kind::Int, Kind::Short, Kind::Float];
        let (column, kind) = self.column(shaped, &numbers)?;
        Some(match (kind, self.rng.below(3)) {
            (Kind::Float, 0) => (format!("{name} = {column} * 3"), Kind::Float),
            (Kind::Float, _) => (format!("{name} = {column} - 1.5"), Kind::Float),
            (_, 0) => (format!("{name} = {column} * 2 + 1"), Kind::Int),
            (_, 1) => (format!("{name} = {column} % 3"), Kind::Int),
            (_, _) => (format!("{name} = {column} * 0.5"), Kind::Float),
        })
    }

    fn aggregation(&mut self, shaped: &Shaped) -> (String, Kind) {
        let any = [
            Kind::Int,
            Kind::Short,
            Kind::Float,
            Kind::Str,
            Kind::Time,
            Kind::Bool,
        ];
        let numbers = [Kind::Int, Kind::Short, Kind::Float];
        loop {
            let function = *self.rng.pick(&[
                "count(*)",
                "count",
                "count_distinct",
                "sum",
                "mean",
                "std",
                "min",
                "max",
                "first",
                "last",
            ]);
            if function == "count(*)" {
                return (function.into(), Kind::Int);
            }
            let kinds: &[Kind] = match function {
                "sum" | "mean" | "std" => &numbers,
                _ => &any,
            };
            let Some((column, kind)) = self.column(shaped, kinds) else {
                continue;
            };
            let out = match function {
                "count" | "count_distinct" => Kind::Int,
                "sum" if kind == Kind::Float => Kind::Float,
                "sum" => Kind::Int,
                "mean" | "std" => Kind::Float,
                _ => kind,
            };
            return (format!("{function}({column})"), out);
        }
    }

    fn plan(&mut self) -> Shaped {
        let mut shaped = scan();
        for _ in 0..1 + self.rng.below(5) {
            shaped = self.step(shaped);
        }
        shaped
    }

    fn step(&mut self, mut shaped: Shaped) -> Shaped {
        let input = Box::new(shaped.plan.clone());
        let any = [
            Kind::Int,
            Kind::Short,
            Kind::Float,
            Kind::Str,
            Kind::Time,
            Kind::Bool,
        ];
        let plan = match self.rng.below(15) {
            0 | 1 => {
                if shaped.growth >= Growth::AddOnly {
                    shaped.growth = Growth::AppendOnly;
                }
                Plan::Filter {
                    input,
                    predicate: self.predicate(&shaped),
                }
            }
            2 => {
                let replace = self.rng.chance(30);
                let name = match self.column(&shaped, &[Kind::Int, Kind::Float]) {
                    Some((column, _)) if replace => column,
                    _ => self.name("u"),
                };
                let Some((assignment, kind)) = self.expression(&shaped, &name) else {
                    return shaped;
                };
                shaped.columns.retain(|(n, _)| *n != name);
                shaped.columns.push((name, kind));
                Plan::Update {
                    input,
                    assignments: vec![assignment],
                }
            }
            3 => {
                let mut columns = Vec::new();
                let mut kept = Vec::new();
                for (name, kind) in &shaped.columns {
                    if self.rng.chance(60) {
                        columns.push(name.clone());
                        kept.push((name.clone(), *kind));
                    }
                }
                let name = self.name("v");
                if let Some((assignment, kind)) = self.expression(&shaped, &name)
                    && (kept.is_empty() || self.rng.chance(30))
                {
                    columns.push(assignment);
                    kept.push((name, kind));
                }
                if kept.is_empty() {
                    return shaped;
                }
                shaped.columns = kept;
                Plan::Select { input, columns }
            }
            4 if shaped.columns.len() > 1 => {
                let at = self.rng.below(shaped.columns.len() as u64) as usize;
                let (dropped, _) = shaped.columns.remove(at);
                Plan::Drop {
                    input,
                    columns: vec![dropped],
                }
            }
            5 | 6 => {
                let mut by = Vec::new();
                let mut columns = Vec::new();
                for _ in 0..self.rng.below(3) {
                    let (name, kind) = self.column(&shaped, &any).expect("a column");
                    if !by.contains(&name) {
                        by.push(name.clone());
                        columns.push((name, kind));
                    }
                }
                let mut aggregations = Vec::new();
                for _ in 0..1 + self.rng.below(5) {
                    let (spec, kind) = self.aggregation(&shaped);
                    let name = self.name("a");
                    aggregations.push((name.clone(), spec));
                    columns.push((name, kind));
                }
                shaped.columns = columns;
                shaped.growth = Growth::Changing;
                Plan::Agg {
                    input,
                    by,
                    aggregations,
                    aliases: Vec::new(),
                }
            }
            7 | 8 => {
                let mut by = Vec::new();
                for _ in 0..1 + self.rng.below(2) {
                    let (column, _) = self.column(&shaped, &any).expect("a column");
                    by.push(SortKey {
                        column,
                        descending: self.rng.chance(50),
                    });
                }
                shaped.growth = Growth::AppendOnly;
                let sort = Plan::Sort { input, by };
                // A sort, then often a head through operators that keep rows: a top-N.
                if self.rng.chance(50) {
                    let mut shaped = Shaped {
                        plan: sort,
                        ..shaped
                    };
                    if self.rng.chance(50) {
                        shaped = self.step(shaped);
                    }
                    shaped.growth = Growth::Changing;
                    let input = Box::new(shaped.plan.clone());
                    shaped.plan = match self.rng.below(2) {
                        0 => Plan::Head {
                            input,
                            rows: self.rng.below(30) as usize,
                        },
                        _ => Plan::Slice {
                            input,
                            start: self.rng.below(10) as usize,
                            stop: Some(self.rng.below(40) as usize),
                        },
                    };
                    return shaped;
                }
                sort
            }
            9 => {
                shaped.growth = Growth::Changing;
                match self.rng.below(3) {
                    0 => Plan::Head {
                        input,
                        rows: self.rng.below(300) as usize,
                    },
                    1 => Plan::Tail {
                        input,
                        rows: self.rng.below(300) as usize,
                    },
                    _ => Plan::Slice {
                        input,
                        start: self.rng.below(200) as usize,
                        stop: self.rng.chance(70).then(|| self.rng.below(600) as usize),
                    },
                }
            }
            10 => {
                let (column, _) = self.column(&shaped, &any).expect("a column");
                shaped.growth = Growth::Changing;
                Plan::LastBy {
                    input,
                    by: vec![column],
                }
            }
            11 if shaped.growth == Growth::AppendOnly => {
                let by = match self.column(&shaped, &[Kind::Int, Kind::Str]) {
                    Some((column, _)) if self.rng.chance(60) => vec![column],
                    _ => Vec::new(),
                };
                let window = match self.column(&shaped, &[Kind::Time]) {
                    Some((at, _)) if self.rng.chance(50) => Window::Time {
                        at,
                        span: 1 + self.rng.below(20_000) as i64,
                    },
                    _ => Window::Rows(1 + self.rng.below(6) as usize),
                };
                let (mut aggregations, mut added) = (Vec::new(), Vec::new());
                for _ in 0..1 + self.rng.below(3) {
                    let numbers = [Kind::Int, Kind::Short, Kind::Float];
                    let Some((column, kind)) = self.column(&shaped, &numbers) else {
                        break;
                    };
                    let function = *self
                        .rng
                        .pick(&["count", "sum", "mean", "std", "min", "max"]);
                    let out = match function {
                        "count" => Kind::Int,
                        "sum" if kind == Kind::Float => Kind::Float,
                        "sum" => Kind::Int,
                        "mean" | "std" => Kind::Float,
                        _ => kind,
                    };
                    let name = self.name("w");
                    aggregations.push((name.clone(), format!("{function}({column})")));
                    added.push((name, out));
                }
                if aggregations.is_empty() {
                    return shaped;
                }
                shaped.columns.extend(added);
                Plan::Rolling {
                    input,
                    by,
                    window,
                    aggregations,
                }
            }
            12 => {
                let filtered = Plan::Filter {
                    input: input.clone(),
                    predicate: self.predicate(&shaped),
                };
                shaped.growth = shaped.growth.min(Growth::AddOnly);
                let again = (*input).clone();
                Plan::Merge(vec![*input, filtered, again])
            }
            13 if self.named(&shaped, "k", Kind::Int) => {
                let name = self.name("j");
                let right = Plan::Agg {
                    input: Box::new(scan().plan),
                    by: vec!["k".into()],
                    aggregations: vec![(name.clone(), "sum(n)".into())],
                    aliases: Vec::new(),
                };
                shaped.columns.push((name, Kind::Int));
                Plan::Join {
                    left: input,
                    right: Box::new(right),
                    on: vec!["k".into()],
                    at: None,
                    columns: None,
                    bare: Vec::new(),
                }
            }
            14 if self.named(&shaped, "k", Kind::Int) && self.named(&shaped, "t", Kind::Time) => {
                let name = self.name("r");
                let right = Plan::Select {
                    input: Box::new(scan().plan),
                    columns: vec!["k".into(), "t".into(), format!("{name} = n")],
                };
                shaped.columns.push((name, Kind::Int));
                Plan::Join {
                    left: input,
                    right: Box::new(right),
                    on: vec!["k".into()],
                    at: Some("t".into()),
                    columns: None,
                    bare: Vec::new(),
                }
            }
            _ => return shaped,
        };
        shaped.plan = plan;
        shaped
    }
}

type Outcome = Result<(SchemaRef, RecordBatch), Error>;

fn executed(plan: &Plan, parts: &[Vec<RecordBatch>]) -> Outcome {
    executed_from(plan, || source(parts))
}

/// `plan` executed, each of its scans reading what `source` gives.
fn executed_from(plan: &Plan, mut source: impl FnMut() -> Source) -> Outcome {
    let (schema, batches) = execute(plan, &mut |_| Ok(source()))?;
    let rows = concat_batches(&schema, &batches).unwrap();
    Ok((schema, rows))
}

fn built(plan: &Plan, parts: &[Vec<RecordBatch>]) -> Outcome {
    let session = Session::new();
    let rows: Vec<RecordBatch> = parts.iter().flatten().cloned().collect();
    let table = plan.build(&mut |scan| {
        let table = session.static_table(schema(), rows.clone())?;
        match &scan.predicate {
            Some(predicate) => table.filter(predicate),
            None => Ok(table),
        }
    })?;
    let schema = table.schema().clone();
    let rows = concat_batches(&schema, &table.snapshot()?).unwrap();
    Ok((schema, rows))
}

/// Every value of an outcome as text, floats to their last bit. A failure counts by the table
/// that failed: which of several bad rows the engine's own operators report (a rolling window's
/// time going back) depends on how they split their work over threads.
fn hash_outcome(outcome: &Outcome, hasher: &mut DefaultHasher) {
    let (schema, rows) = match outcome {
        Err(Error::Compute { table, .. }) => return table.hash(hasher),
        Err(error) => return error.to_string().hash(hasher),
        Ok(outcome) => outcome,
    };
    format!("{schema:?}").hash(hasher);
    rows.num_rows().hash(hasher);
    let options = FormatOptions::default().with_null("NULL");
    for column in rows.columns() {
        let values = ArrayFormatter::try_new(column.as_ref(), &options).unwrap();
        for row in 0..column.len() {
            values.value(row).to_string().hash(hasher);
        }
    }
}

const SEEDS: u64 = 400;

/// Run under each `KERMODES_THREADS` by `executes_plans_as_the_engine_builds_them`: plans must
/// give the engine's results at every thread count, and the same ones on each.
#[test]
#[ignore = "run under each thread count by executes_plans_as_the_engine_builds_them"]
fn executes_plans_under_the_thread_count() {
    let mut hasher = DefaultHasher::new();
    for seed in 1..=SEEDS {
        let mut rng = Rng(seed);
        let parts = random_parts(&mut rng);
        let mut planner = Planner { rng, next: 0 };
        let shaped = planner.plan();
        for plan in [shaped.plan.clone(), shaped.plan.pushed_down()] {
            let got = executed(&plan, &parts);
            assert_eq!(got, built(&plan, &parts), "seed {seed}: {plan:#?}");
            hash_outcome(&got, &mut hasher);
        }
    }
    println!("digest {:016x}", hasher.finish());
}

#[test]
fn executes_plans_as_the_engine_builds_them() {
    let digests: Vec<String> = ["1", "3", "8"]
        .iter()
        .map(|threads| {
            let output = Command::new(std::env::current_exe().unwrap())
                .args([
                    "executes_plans_under_the_thread_count",
                    "--exact",
                    "--ignored",
                    "--nocapture",
                ])
                .env("KERMODES_THREADS", threads)
                .output()
                .unwrap();
            let stdout = String::from_utf8_lossy(&output.stdout);
            assert!(
                output.status.success(),
                "on {threads} threads:\n{stdout}\n{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let digest = stdout
                .lines()
                .find_map(|line| line.strip_prefix("digest "))
                .expect("the run prints its digest");
            digest.to_owned()
        })
        .collect();
    assert!(
        digests.iter().all(|digest| *digest == digests[0]),
        "results differ by thread count: {digests:?}"
    );
}

/// Over a source that reads its parts again by column and row, plans give the engine's results:
/// heads of sorts and scans' predicates read narrowly, then the other columns of the rows they
/// keep.
#[test]
fn executes_plans_over_a_source_read_again_as_the_engine_builds_them() {
    let reads: Reads = Arc::default();
    for seed in 1..=SEEDS {
        let mut rng = Rng(seed);
        let parts = random_parts(&mut rng);
        let mut planner = Planner { rng, next: 0 };
        let shaped = planner.plan();
        for plan in [shaped.plan.clone(), shaped.plan.pushed_down()] {
            let got = executed_from(&plan, || rereadable(&parts, &reads));
            assert_eq!(got, built(&plan, &parts), "seed {seed}: {plan:#?}");
        }
    }
    let by_rows = reads
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, _, asked)| asked.is_some())
        .count();
    assert!(by_rows > 0, "no rows read again by position");
}

/// A head of a sort of a scan, over a source read again, reads the sort's and the predicate's
/// columns of every row, then every column of its winners alone; a scan's predicate reads its
/// own columns of every row, then the others of the rows it keeps alone.
#[test]
fn a_top_n_and_a_filter_read_the_other_columns_of_the_rows_they_keep_alone() {
    let (mut rng, mut clock) = (Rng(7), 0);
    let parts: Vec<Vec<RecordBatch>> = (0..8)
        .map(|_| {
            (0..3)
                .map(|_| random_batch(&mut rng, 500, &mut clock))
                .collect()
        })
        .collect();
    let scan = |predicate: &str| {
        Plan::Scan(Scan {
            name: "t".into(),
            predicate: Some(predicate.into()),
            columns: None,
            limit: None,
            live: false,
        })
    };
    let top = Plan::Head {
        input: Box::new(Plan::Sort {
            input: Box::new(scan("n > 0")),
            by: vec![SortKey {
                column: "x".into(),
                descending: true,
            }],
        }),
        rows: 20,
    };
    // `x` and `n` for the top-N, which reads them again for its winners; `k` for the filter,
    // which does not.
    for (plan, narrow, again) in [(top, vec![3, 4], true), (scan("k = 3"), vec![0], false)] {
        let reads: Reads = Arc::default();
        let got = executed_from(&plan, || rereadable(&parts, &reads));
        assert_eq!(got, built(&plan, &parts));
        let kept = got.unwrap().1.num_rows();
        assert!(kept > 0);
        let reads = reads.lock().unwrap();
        let mut whole: Vec<usize> = reads
            .iter()
            .filter(|(_, _, asked)| asked.is_none())
            .map(|(part, columns, _)| {
                assert_eq!(*columns, narrow);
                *part
            })
            .collect();
        whole.sort_unstable();
        assert_eq!(whole, (0..parts.len()).collect::<Vec<_>>());
        // Then each other column (each column, for winners) of the rows kept alone.
        for column in 0..schema().fields().len() {
            let asked: usize = reads
                .iter()
                .filter(|(_, columns, _)| columns.contains(&column))
                .filter_map(|(_, _, asked)| *asked)
                .sum();
            let wanted = if again || !narrow.contains(&column) {
                kept
            } else {
                0
            };
            assert_eq!(asked, wanted, "column {column}");
        }
    }
}

/// What a counted part source has done: parts opened, batches read.
#[derive(Default)]
struct Counts {
    opened: Arc<AtomicUsize>,
    read: Arc<AtomicUsize>,
}

/// `parts` parts of `batches` random batches of `rows` rows, counting into `counts`.
fn counted(parts: usize, batches: usize, rows: usize, counts: &Counts) -> Source {
    let Counts { opened, read } = counts;
    Source {
        schema: schema(),
        parts: (0..parts)
            .map(|part| {
                let (opened, read) = (opened.clone(), read.clone());
                Box::new(move || {
                    opened.fetch_add(1, Ordering::SeqCst);
                    let (mut rng, mut clock) = (Rng(part as u64 + 1), 0);
                    let batches = (0..batches).map(move |_| {
                        read.fetch_add(1, Ordering::SeqCst);
                        Ok(random_batch(&mut rng, rows, &mut clock))
                    });
                    Ok(Box::new(batches) as Batches)
                }) as Part
            })
            .collect(),
        reread: None,
        stats: None,
    }
}

#[test]
fn a_head_opens_only_the_parts_it_needs() {
    let counts = Counts::default();
    let plan = Plan::Head {
        input: Box::new(scan().plan),
        rows: 50,
    };
    let (_, rows) = execute(&plan, &mut |_| Ok(counted(10, 4, 100, &counts))).unwrap();
    assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 50);
    // The first part's first batch holds the rows.
    assert_eq!(counts.opened.load(Ordering::SeqCst), 1);
    assert_eq!(counts.read.load(Ordering::SeqCst), 1);
}

#[test]
fn an_aggregation_holds_no_batch_it_has_folded() {
    let (parts, batches) = (64, 4);
    // Every batch read, by its first column; the most alive at once.
    let read: Arc<Mutex<Vec<Weak<dyn Array>>>> = Arc::default();
    let most_alive = Arc::new(AtomicUsize::new(0));
    let source = || Source {
        schema: schema(),
        parts: (0..parts)
            .map(|part| {
                let (read, most_alive) = (read.clone(), most_alive.clone());
                Box::new(move || {
                    let (mut rng, mut clock) = (Rng(part as u64 + 1), 0);
                    let batches = (0..batches).map(move |_| {
                        let batch = random_batch(&mut rng, 500, &mut clock);
                        let mut read = read.lock().unwrap();
                        read.push(Arc::downgrade(batch.column(0)));
                        let alive = read.iter().filter(|w| w.strong_count() > 0).count();
                        most_alive.fetch_max(alive, Ordering::SeqCst);
                        Ok(batch)
                    });
                    Ok(Box::new(batches) as Batches)
                }) as Part
            })
            .collect(),
        reread: None,
        stats: None,
    };
    let plan = Plan::Agg {
        input: Box::new(scan().plan),
        by: vec!["s".into(), "k".into()],
        aggregations: [
            "count(*)",
            "count_distinct(n)",
            "sum(n)",
            "std(x)",
            "min(s)",
            "max(t)",
            "first(x)",
            "last(s)",
        ]
        .iter()
        .enumerate()
        .map(|(i, spec)| (format!("a{i}"), spec.to_string()))
        .collect(),
        aliases: Vec::new(),
    };
    let (_, rows) = execute(&plan, &mut |_| Ok(source())).unwrap();
    assert!(rows.iter().map(RecordBatch::num_rows).sum::<usize>() > 0);
    assert_eq!(read.lock().unwrap().len(), parts * batches);
    // At most one batch per thread is alive: the one each is folding.
    let threads = std::env::var("KERMODES_THREADS")
        .ok()
        .and_then(|threads| threads.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(1, usize::from));
    let alive = most_alive.load(Ordering::SeqCst);
    assert!(
        alive <= threads.min(parts),
        "{alive} batches alive at once on {threads} threads"
    );
}

/// An aggregation without keys of `count(*)`, `min`, and `max` takes parts' statistics where a
/// source has them, reading none of those parts, and gives the engine's result.
#[test]
fn a_summary_reads_no_part_its_statistics_answer() {
    let (mut rng, mut clock) = (Rng(11), 0);
    let parts: Vec<Vec<RecordBatch>> = (0..6)
        .map(|_| {
            (0..2)
                .map(|_| random_batch(&mut rng, 300, &mut clock))
                .collect()
        })
        .collect();
    // Parts 0 to 4 are known only by their statistics: reading them fails.
    let source = || {
        let mut source = rereadable(&parts, &Reads::default());
        let read = source.reread.clone().unwrap();
        source.reread = Some(Arc::new(move |part, columns, rows| {
            if part < 5 {
                return Err(Error::Unsupported(format!("part {part} read")));
            }
            read(part, columns, rows)
        }));
        let stats = stats_of(&parts);
        source.stats = Some(Arc::new(move |part, columns| {
            if part < 5 {
                stats(part, columns)
            } else {
                Ok(None)
            }
        }));
        source
    };
    for aggregations in [
        vec!["count(*)", "min(k)", "max(t)", "min(s)", "max(s)", "max(m)"],
        vec!["count(*)"],
    ] {
        let plan = Plan::Agg {
            input: Box::new(scan().plan.pushed_down()),
            by: Vec::new(),
            aggregations: aggregations
                .iter()
                .enumerate()
                .map(|(i, spec)| (format!("a{i}"), spec.to_string()))
                .collect(),
            aliases: Vec::new(),
        };
        assert_eq!(
            executed_from(&plan, source),
            built(&plan, &parts),
            "{aggregations:?}"
        );
    }
}

/// A top-N over a scan whose source has statistics leaves unread the parts that cannot beat its
/// `stop`th best row so far, and gives the engine's rows.
#[test]
fn a_top_n_reads_no_part_its_statistics_rule_out() {
    let (mut rng, mut clock) = (Rng(13), 0);
    // Times only grow from part to part: once the first part is read, the ten earliest times
    // are known, and no later part holds an earlier one.
    let parts: Vec<Vec<RecordBatch>> = (0..40)
        .map(|_| {
            (0..2)
                .map(|_| random_batch(&mut rng, 100, &mut clock))
                .collect()
        })
        .collect();
    let reads = Reads::default();
    let plan = Plan::Head {
        input: Box::new(Plan::Sort {
            input: Box::new(scan().plan),
            by: vec![SortKey {
                column: "t".into(),
                descending: false,
            }],
        }),
        rows: 10,
    };
    assert_eq!(
        executed_from(&plan, || rereadable(&parts, &reads)),
        built(&plan, &parts)
    );
    let read: std::collections::BTreeSet<usize> = reads
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, _, rows)| rows.is_none())
        .map(|(part, _, _)| *part)
        .collect();
    // Parts opened before the first part's winners were known may be read: two per thread.
    let threads = std::env::var("KERMODES_THREADS")
        .ok()
        .and_then(|threads| threads.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(1, usize::from));
    assert!(
        read.len() <= 2 * threads + 1,
        "{} of {} parts read on {threads} threads",
        read.len(),
        parts.len()
    );
}

/// Parts large enough, with keys close enough to unique, that each part stops deduplicating its
/// groups across batches (after 64k rows): later batches repeat keys of earlier ones, of other
/// batches passed through, and of other parts, and the merge still gives the engine's groups, in
/// the engine's order, with the engine's states.
#[test]
fn aggregates_nearly_unique_keys_as_the_engine_does() {
    let (parts, batches, rows) = (3, 20, 5_000);
    let mut rng = Rng(7);
    let mut clock = 0;
    let mut at = 0i64;
    let parts: Vec<Vec<RecordBatch>> = (0..parts)
        .map(|_| {
            (0..batches)
                .map(|_| {
                    let batch = random_batch(&mut rng, rows, &mut clock);
                    // 60k keys over 100k rows a part: more groups than half its rows.
                    let keys: Int64Array = (at..at + rows as i64)
                        .map(|row| Some(row * 7_919 % 60_000))
                        .collect();
                    at += rows as i64;
                    let mut columns = batch.columns().to_vec();
                    columns[0] = Arc::new(keys);
                    RecordBatch::try_new(schema(), columns).unwrap()
                })
                .collect()
        })
        .collect();
    for by in [vec!["k"], vec!["k", "s"]] {
        let plan = Plan::Agg {
            input: Box::new(scan().plan),
            by: by.iter().map(|c| c.to_string()).collect(),
            aggregations: [
                "count(*)",
                "sum(n)",
                "std(x)",
                "min(s)",
                "max(t)",
                "first(x)",
                "last(s)",
                "count_distinct(m)",
            ]
            .iter()
            .enumerate()
            .map(|(i, spec)| (format!("a{i}"), spec.to_string()))
            .collect(),
            aliases: Vec::new(),
        };
        assert_eq!(executed(&plan, &parts), built(&plan, &parts), "by {by:?}");
    }
}
