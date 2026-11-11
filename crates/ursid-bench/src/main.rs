//! Cycle latency and throughput for the engine's core paths.
//!
//! `cargo run --release -p ursid-bench -- [ticks|book] [flags]`
//!
//! `ticks` (default; `--rows 100000 --cycles 100 --symbols 10000`): each cycle appends one batch of
//! a random-walk ticker, then times `step()`: a ticking `where` → `update` → `last_by(sym)` and an
//! `agg_by(sym)` with count, mean, and std. Afterwards the same `agg_by` runs over a static copy of
//! everything appended, `--static-repeats` times (more repeats give a profiler more samples); the
//! fastest run is reported.
//!
//! `book` (`--orders 1000000 --rows 100000 --cycles 100 --symbols 100`): a keyed order book. One
//! cycle loads `--orders` orders; each later cycle writes `--rows` changes (80% modify a live
//! order's price and size, 10% delete one, 10% insert a new one) and times `step()` through an
//! `agg_by(sym)` with count, sum, mean, and std.
//!
//! Batch generation is never timed.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use ursid_core::Session;

struct Args {
    workload: String,
    rows: usize,
    cycles: usize,
    symbols: Option<usize>,
    orders: usize,
    static_repeats: usize,
}

impl Args {
    fn parse() -> Self {
        let mut args = Self {
            workload: "ticks".into(),
            rows: 100_000,
            cycles: 100,
            symbols: None,
            orders: 1_000_000,
            static_repeats: 1,
        };
        let mut words = std::env::args().skip(1).peekable();
        if let Some(name) = words.next_if(|w| !w.starts_with("--")) {
            assert!(
                name == "ticks" || name == "book",
                "unknown workload {name}; expected ticks or book"
            );
            args.workload = name;
        }
        while let Some(flag) = words.next() {
            let value: usize = words
                .next()
                .and_then(|v| v.parse().ok())
                .unwrap_or_else(|| panic!("{flag} needs a number"));
            match flag.as_str() {
                "--rows" => args.rows = value,
                "--cycles" => args.cycles = value,
                "--symbols" => args.symbols = Some(value),
                "--orders" => args.orders = value,
                "--static-repeats" => args.static_repeats = value.max(1),
                other => panic!(
                    "unknown flag {other}; expected --rows, --cycles, --symbols, --orders, \
                     --static-repeats"
                ),
            }
        }
        args
    }
}

/// xorshift: reproducible runs without a dependency.
struct Random(u64);

impl Random {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

fn symbol_names(symbols: usize) -> Vec<String> {
    (0..symbols).map(|i| format!("S{i:05}")).collect()
}

/// Prices that wander per symbol.
struct RandomWalk {
    random: Random,
    symbols: Vec<String>,
    prices: Vec<f64>,
    clock: i64,
}

impl RandomWalk {
    fn new(symbols: usize) -> Self {
        Self {
            random: Random(0x9E37_79B9_7F4A_7C15),
            symbols: symbol_names(symbols),
            prices: vec![100.0; symbols],
            clock: 1_762_819_200_000_000_000,
        }
    }

    fn batch(&mut self, schema: &Arc<Schema>, rows: usize) -> RecordBatch {
        let mut sym = Vec::with_capacity(rows);
        let mut price = Vec::with_capacity(rows);
        let mut size = Vec::with_capacity(rows);
        let mut ts = Vec::with_capacity(rows);
        for _ in 0..rows {
            let s = self.random.below(self.symbols.len());
            let step = self.random.below(2001) as f64 / 1000.0 - 1.0;
            self.prices[s] = (self.prices[s] + step).max(0.01);
            self.clock += 1_000;
            sym.push(s);
            price.push(self.prices[s]);
            size.push(self.random.below(1000) as i64 + 1);
            ts.push(self.clock);
        }
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from_iter_values(
                sym.iter().map(|s| self.symbols[*s].as_str()),
            )),
            Arc::new(Float64Array::from(price)),
            Arc::new(Int64Array::from(size)),
            Arc::new(TimestampNanosecondArray::from(ts).with_timezone("UTC")),
        ];
        RecordBatch::try_new(schema.clone(), columns).expect("columns match the schema")
    }
}

fn millis(d: Duration) -> f64 {
    d.as_secs_f64() * 1_000.0
}

/// Prints throughput and latency percentiles of timed cycles.
fn report(name: &str, rows_per_cycle: usize, mut cycles: Vec<Duration>) {
    let busy: Duration = cycles.iter().sum();
    cycles.sort();
    let percentile = |p: f64| cycles[((cycles.len() - 1) as f64 * p).round() as usize];
    println!("{name}: {} cycles x {rows_per_cycle} rows", cycles.len());
    println!(
        "  throughput {:.0} rows/s of cycle time",
        (rows_per_cycle * cycles.len()) as f64 / busy.as_secs_f64()
    );
    println!(
        "  cycle p50 {:.1} ms, p99 {:.1} ms, max {:.1} ms",
        millis(percentile(0.5)),
        millis(percentile(0.99)),
        millis(percentile(1.0))
    );
}

fn main() {
    let args = Args::parse();
    match args.workload.as_str() {
        "book" => book(&args),
        _ => ticks(&args),
    }
}

fn ticks(args: &Args) {
    let symbols = args.symbols.unwrap_or(10_000);
    let schema = Arc::new(Schema::new(vec![
        Field::new("sym", DataType::Utf8, true),
        Field::new("price", DataType::Float64, true),
        Field::new("size", DataType::Int64, true),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            true,
        ),
    ]));
    let aggregations = [
        ("n", "count(*)"),
        ("avg", "mean(price)"),
        ("vol", "std(price)"),
    ];

    let session = Session::new();
    let ticks = session.input_table(&schema).expect("valid schema");
    let latest = ticks
        .filter("price > 0")
        .and_then(|t| t.update(&["notional = price * size", "lots = size / 100"]))
        .and_then(|t| t.last_by(&["sym"]))
        .expect("valid pipeline");
    let stats = ticks
        .agg_by(&["sym"], &aggregations)
        .expect("valid aggregation");

    let mut walk = RandomWalk::new(symbols);
    let mut cycles = Vec::with_capacity(args.cycles);
    for _ in 0..args.cycles {
        let batch = walk.batch(&schema, args.rows);
        ticks.append(&[batch]).expect("batch matches the table");
        let start = Instant::now();
        session.step();
        cycles.push(start.elapsed());
    }

    report(&format!("ticking, {symbols} symbols"), args.rows, cycles);
    println!(
        "  last_by rows {}, agg_by rows {}",
        latest.num_rows().expect("not failed"),
        stats.num_rows().expect("not failed")
    );

    let total_rows = args.rows * args.cycles;
    let everything = ticks.snapshot().expect("input never fails");
    let fixed = session
        .static_table(schema.clone(), everything)
        .expect("same schema");
    let mut fastest = Duration::MAX;
    let mut groups = 0;
    for _ in 0..args.static_repeats {
        let start = Instant::now();
        let grouped = fixed
            .agg_by(&["sym"], &aggregations)
            .expect("valid aggregation");
        fastest = fastest.min(start.elapsed());
        groups = grouped.num_rows().expect("not failed");
    }
    println!(
        "static agg_by over {} rows: {:.1} ms ({:.0} rows/s), {} groups",
        total_rows,
        millis(fastest),
        total_rows as f64 / fastest.as_secs_f64(),
        groups
    );
}

/// Live orders and the writes of one cycle.
struct Book {
    random: Random,
    symbols: Vec<String>,
    /// Live order ids, and each order's symbol by id.
    live: Vec<i64>,
    symbol_of: Vec<u32>,
    next_id: i64,
}

impl Book {
    fn new(symbols: usize) -> Self {
        Self {
            random: Random(0x2545_F491_4F6C_DD1D),
            symbols: symbol_names(symbols),
            live: Vec::new(),
            symbol_of: Vec::new(),
            next_id: 0,
        }
    }

    fn insert(&mut self) -> i64 {
        let id = self.next_id;
        self.next_id += 1;
        self.symbol_of
            .push(self.random.below(self.symbols.len()) as u32);
        self.live.push(id);
        id
    }

    /// Upserted rows and deleted keys for `rows` writes; `load` only inserts.
    fn writes(
        &mut self,
        schema: &Arc<Schema>,
        rows: usize,
        load: bool,
    ) -> (RecordBatch, RecordBatch) {
        let mut upserted = Vec::with_capacity(rows);
        let mut deleted = Vec::new();
        for _ in 0..rows {
            match if load { 9 } else { self.random.below(10) } {
                8 if !self.live.is_empty() => {
                    let at = self.random.below(self.live.len());
                    deleted.push(self.live.swap_remove(at));
                }
                9 => upserted.push(self.insert()),
                _ if !self.live.is_empty() => {
                    upserted.push(self.live[self.random.below(self.live.len())]);
                }
                _ => upserted.push(self.insert()),
            }
        }
        let price: Vec<f64> = (0..upserted.len())
            .map(|_| 1.0 + self.random.below(100_000) as f64 / 100.0)
            .collect();
        let size: Vec<i64> = (0..upserted.len())
            .map(|_| self.random.below(1000) as i64 + 1)
            .collect();
        let sym = StringArray::from_iter_values(
            upserted
                .iter()
                .map(|id| self.symbols[self.symbol_of[*id as usize] as usize].as_str()),
        );
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(upserted)),
            Arc::new(sym),
            Arc::new(Float64Array::from(price)),
            Arc::new(Int64Array::from(size)),
        ];
        let upserts = RecordBatch::try_new(schema.clone(), columns).expect("matches the schema");
        let key: ArrayRef = Arc::new(Int64Array::from(deleted));
        let deletes = RecordBatch::try_from_iter([("id", key)]).expect("one key column");
        (upserts, deletes)
    }
}

fn book(args: &Args) {
    let symbols = args.symbols.unwrap_or(100);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("sym", DataType::Utf8, true),
        Field::new("price", DataType::Float64, true),
        Field::new("size", DataType::Int64, true),
    ]));
    let session = Session::new();
    let orders = session
        .keyed_input_table(&schema, &["id"])
        .expect("valid schema");
    let stats = orders
        .agg_by(
            &["sym"],
            &[
                ("n", "count(*)"),
                ("size", "sum(size)"),
                ("avg", "mean(price)"),
                ("vol", "std(price)"),
            ],
        )
        .expect("valid aggregation");

    let mut book = Book::new(symbols);
    let (load, _) = book.writes(&schema, args.orders, true);
    orders.upsert(&[load]).expect("matches the table");
    let start = Instant::now();
    session.step();
    println!(
        "book: loaded {} orders over {symbols} symbols in {:.1} ms",
        args.orders,
        millis(start.elapsed())
    );

    let mut cycles = Vec::with_capacity(args.cycles);
    for _ in 0..args.cycles {
        let (upserts, deletes) = book.writes(&schema, args.rows, false);
        orders.upsert(&[upserts]).expect("matches the table");
        orders.delete(&[deletes]).expect("key columns only");
        let start = Instant::now();
        session.step();
        cycles.push(start.elapsed());
    }
    report("book writes", args.rows, cycles);
    println!(
        "  live orders {}, agg_by rows {}",
        orders.num_rows().expect("not failed"),
        stats.num_rows().expect("not failed")
    );
}
