//! Cycle latency and throughput for the engine's core paths, on a random-walk ticker.
//!
//! `cargo run --release -p ursid-bench -- --rows 100000 --cycles 100 --symbols 10000`
//!
//! Each cycle appends one batch, then times `step()`: a ticking `where` → `update` →
//! `last_by(sym)` and an `agg_by(sym)` with count, mean, and std. Afterwards the same `agg_by`
//! runs once over a static copy of everything appended. Batch generation is not timed.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use ursid_core::Session;

struct Args {
    rows: usize,
    cycles: usize,
    symbols: usize,
}

impl Args {
    fn parse() -> Self {
        let mut args = Self {
            rows: 100_000,
            cycles: 100,
            symbols: 10_000,
        };
        let mut words = std::env::args().skip(1);
        while let Some(flag) = words.next() {
            let value: usize = words
                .next()
                .and_then(|v| v.parse().ok())
                .unwrap_or_else(|| panic!("{flag} needs a number"));
            match flag.as_str() {
                "--rows" => args.rows = value,
                "--cycles" => args.cycles = value,
                "--symbols" => args.symbols = value,
                other => panic!("unknown flag {other}; expected --rows, --cycles, --symbols"),
            }
        }
        args
    }
}

/// Prices that wander per symbol; xorshift keeps runs reproducible.
struct RandomWalk {
    state: u64,
    symbols: Vec<String>,
    prices: Vec<f64>,
    clock: i64,
}

impl RandomWalk {
    fn new(symbols: usize) -> Self {
        Self {
            state: 0x9E37_79B9_7F4A_7C15,
            symbols: (0..symbols).map(|i| format!("S{i:05}")).collect(),
            prices: vec![100.0; symbols],
            clock: 1_762_819_200_000_000_000,
        }
    }

    fn next_u64(&mut self) -> u64 {
        self.state ^= self.state >> 12;
        self.state ^= self.state << 25;
        self.state ^= self.state >> 27;
        self.state.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn batch(&mut self, schema: &Arc<Schema>, rows: usize) -> RecordBatch {
        let mut sym = Vec::with_capacity(rows);
        let mut price = Vec::with_capacity(rows);
        let mut size = Vec::with_capacity(rows);
        let mut ts = Vec::with_capacity(rows);
        for _ in 0..rows {
            let s = (self.next_u64() % self.symbols.len() as u64) as usize;
            let step = (self.next_u64() % 2001) as f64 / 1000.0 - 1.0;
            self.prices[s] = (self.prices[s] + step).max(0.01);
            self.clock += 1_000;
            sym.push(s);
            price.push(self.prices[s]);
            size.push((self.next_u64() % 1000 + 1) as i64);
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

fn main() {
    let args = Args::parse();
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

    let mut walk = RandomWalk::new(args.symbols);
    let mut cycles = Vec::with_capacity(args.cycles);
    for _ in 0..args.cycles {
        let batch = walk.batch(&schema, args.rows);
        ticks.append(&[batch]).expect("batch matches the table");
        let start = Instant::now();
        session.step();
        cycles.push(start.elapsed());
    }

    let total_rows = args.rows * args.cycles;
    let busy: Duration = cycles.iter().sum();
    cycles.sort();
    let percentile = |p: f64| cycles[((cycles.len() - 1) as f64 * p).round() as usize];
    println!(
        "ticking: {} cycles x {} rows, {} symbols",
        args.cycles, args.rows, args.symbols
    );
    println!(
        "  throughput {:.0} rows/s of cycle time",
        total_rows as f64 / busy.as_secs_f64()
    );
    println!(
        "  cycle p50 {:.1} ms, p99 {:.1} ms, max {:.1} ms",
        millis(percentile(0.5)),
        millis(percentile(0.99)),
        millis(percentile(1.0))
    );
    println!(
        "  last_by rows {}, agg_by rows {}",
        latest.num_rows().expect("not failed"),
        stats.num_rows().expect("not failed")
    );

    let everything = ticks.snapshot().expect("input never fails");
    let fixed = session
        .static_table(schema.clone(), everything)
        .expect("same schema");
    let start = Instant::now();
    let grouped = fixed
        .agg_by(&["sym"], &aggregations)
        .expect("valid aggregation");
    let elapsed = start.elapsed();
    println!(
        "static agg_by over {} rows: {:.1} ms ({:.0} rows/s), {} groups",
        total_rows,
        millis(elapsed),
        total_rows as f64 / elapsed.as_secs_f64(),
        grouped.num_rows().expect("not failed")
    );
}
