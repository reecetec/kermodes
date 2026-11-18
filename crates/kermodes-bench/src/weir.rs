//! weir's write and delivery latency (architecture.md §7.6), and the fsync it rests on.
//!
//! `kermodes-bench fsync [--dir PATH] [--count 2000]`: appends 4 KiB and `fdatasync`s it,
//! `--count` times, on the disk holding `--dir`: the floor under every durable write there.
//!
//! `kermodes-bench weir [--dir PATH] [--writers 1] [--rate 1000] [--rows 1] [--window 1]
//! [--seconds 10] [--flight]`: `--writers` writers, each writing `--rows`-row batches `--rate`
//! times a second (0: as fast as it can) with up to `--window` writes in flight, to a store in
//! `--dir`, with one subscriber; over Flight on localhost with `--flight`, in-process without.
//! Each batch carries the time it was due, so a writer that falls behind counts its wait (no
//! coordinated omission). Reports, after a second of warm-up: the time from due to durable (the
//! write's acknowledgement), and from due to the subscriber having it; and the process's CPU
//! time per row written, the bench's own writers and subscriber included, so comparable between
//! builds and machines rather than a cost of weir alone.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow::array::{ArrayRef, AsArray, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Int64Type, Schema};
use kermodes_flight::Client;
use kermodes_weir::{Change, Error, Pending, SourceId, Store, TableName};

struct Args {
    dir: PathBuf,
    count: usize,
    writers: usize,
    rate: u64,
    rows: usize,
    window: usize,
    seconds: u64,
    flight: bool,
}

impl Args {
    fn parse(words: impl Iterator<Item = String>) -> Self {
        let mut args = Self {
            dir: std::env::temp_dir(),
            count: 2000,
            writers: 1,
            rate: 1000,
            rows: 1,
            window: 1,
            seconds: 10,
            flight: false,
        };
        let mut words = words.peekable();
        while let Some(flag) = words.next() {
            if flag == "--flight" {
                args.flight = true;
                continue;
            }
            let value = words
                .next()
                .unwrap_or_else(|| panic!("{flag} needs a value"));
            let number = || -> u64 {
                value
                    .parse()
                    .unwrap_or_else(|_| panic!("{flag} needs a number"))
            };
            match flag.as_str() {
                "--dir" => args.dir = PathBuf::from(&value),
                "--count" => args.count = number() as usize,
                "--writers" => args.writers = number() as usize,
                "--rate" => args.rate = number(),
                "--rows" => args.rows = number() as usize,
                "--window" => args.window = (number() as usize).max(1),
                "--seconds" => args.seconds = number(),
                other => panic!(
                    "unknown flag {other}; expected --dir, --count, --writers, --rate, --rows, \
                     --window, --seconds, --flight"
                ),
            }
        }
        args
    }
}

/// `fsync` or `weir`, with the words after it.
pub fn run(workload: &str, words: impl Iterator<Item = String>) {
    let args = Args::parse(words);
    match workload {
        "fsync" => fsync(&args),
        _ => weir(&args),
    }
}

fn fsync(args: &Args) {
    use std::io::Write;
    let path = args
        .dir
        .join(format!("kermodes-fsync-{}", std::process::id()));
    let mut file = std::fs::File::create(&path).expect("a file in --dir");
    let block = vec![7_u8; 4096];
    let mut times = Vec::with_capacity(args.count);
    for _ in 0..args.count {
        let start = Instant::now();
        file.write_all(&block).expect("written");
        file.sync_data().expect("synced");
        times.push(start.elapsed());
    }
    drop(file);
    let _ = std::fs::remove_file(&path);
    println!(
        "fsync on {}: {} x 4 KiB appends",
        args.dir.display(),
        args.count
    );
    print_latency("append + fdatasync", times);
}

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as i64)
}

/// The CPU time, user and system, this process has used.
fn cpu_seconds() -> f64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage writes a whole `rusage` to the pointer it is given, which points at
    // enough memory for one, and reports failure (only for a bad `who`) without writing.
    let usage = unsafe {
        assert_eq!(libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()), 0);
        usage.assume_init()
    };
    let seconds = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
    seconds(usage.ru_utime) + seconds(usage.ru_stime)
}

/// A write to the bench table, local or remote.
type Write = Box<dyn Fn(&RecordBatch) -> Result<Pending, Error> + Send>;

fn weir(args: &Args) {
    let dir = args
        .dir
        .join(format!("kermodes-weir-bench-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let store = Arc::new(Store::open(&dir).expect("a store in --dir"));
    let name = TableName::parse("bench.ticks").expect("a table name");
    let schema = Schema::new(vec![
        Field::new("due", DataType::Int64, true),
        Field::new("n", DataType::Int64, true),
    ]);
    store.create_table(&name, &schema).expect("created");
    let server = args.flight.then(|| {
        kermodes_flight::serve(store.clone(), "127.0.0.1:0".parse().expect("an address"))
            .expect("serving")
    });
    let url = server.as_ref().map(|s| format!("grpc://{}", s.addr()));

    // A second's warm-up: batches due before it are not counted.
    let start_ns = now_ns();
    let start_cpu = cpu_seconds();
    let warm_ns = start_ns + 1_000_000_000;
    let delivered = Arc::new(Mutex::new(Vec::new()));
    let subscriber: kermodes_weir::Subscriber = {
        let delivered = delivered.clone();
        Box::new(move |batch, _, _| {
            if batch.num_rows() > 0 {
                let due = batch.column(0).as_primitive::<Int64Type>().value(0);
                if due >= warm_ns {
                    let waited = (now_ns() - due).max(0) as u64;
                    delivered.lock().unwrap().push(Duration::from_nanos(waited));
                }
            }
            true
        })
    };
    let reader = url
        .as_ref()
        .map(|url| Client::connect(url).expect("connected"));
    match &reader {
        Some(client) => {
            client
                .subscribe(
                    &name,
                    subscriber,
                    Box::new(|| {}),
                    Box::new(|why| panic!("the bench's subscription ended: {why}")),
                    false,
                )
                .expect("subscribed");
        }
        None => {
            store
                .subscribe(&name, subscriber, Box::new(|why| panic!("{why}")))
                .expect("subscribed");
        }
    }

    let stop = Arc::new(AtomicBool::new(false));
    let acked = Arc::new(Mutex::new(Vec::new()));
    let written = Arc::new(Mutex::new(0_u64));
    let threads: Vec<_> = (0..args.writers)
        .map(|w| {
            let source = SourceId::parse(&format!("w{w}")).expect("a source");
            let write: Write = match &url {
                Some(url) => {
                    let client = Client::connect(url).expect("connected");
                    let writer = client.writer(&name, &source, false).expect("a writer");
                    Box::new(move |batch| writer.send(Change::Rows, batch, None))
                }
                None => {
                    let writer = store.writer(&name, &source, false).expect("a writer");
                    Box::new(move |batch| writer.send(Change::Rows, batch, None))
                }
            };
            let (stop, acked, written) = (stop.clone(), acked.clone(), written.clone());
            let (rate, rows, window) = (args.rate, args.rows, args.window);
            std::thread::spawn(move || {
                let begun = Instant::now();
                let interval = (rate > 0).then(|| Duration::from_nanos(1_000_000_000 / rate));
                // A write takes one of `window` slots, given back when it is answered; its own
                // thread waits for the answers, so each is timed as it comes.
                let (free, slots) = std::sync::mpsc::sync_channel(window);
                for _ in 0..window {
                    free.send(()).expect("a slot");
                }
                let (sent, answers) = std::sync::mpsc::channel::<(Instant, i64, Pending)>();
                let waiter = std::thread::spawn(move || {
                    let mut mine = Vec::new();
                    for (due_at, due_ns, pending) in answers {
                        assert!(pending.wait().expect("written"));
                        if due_ns >= warm_ns {
                            mine.push(Instant::now().duration_since(due_at));
                        }
                        let _ = free.send(());
                    }
                    mine
                });
                let mut count = 0_u64;
                for i in 0_u64.. {
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    // When this batch is due: on the schedule, or now when there is none.
                    let due_at = match interval {
                        Some(interval) => {
                            let due_at = begun + interval * i as u32;
                            if let Some(wait) = due_at.checked_duration_since(Instant::now()) {
                                std::thread::sleep(wait);
                            }
                            due_at
                        }
                        None => Instant::now(),
                    };
                    slots.recv().expect("a slot");
                    let due_ns = now_ns() - Instant::now().duration_since(due_at).as_nanos() as i64;
                    let batch = RecordBatch::try_from_iter([
                        (
                            "due",
                            Arc::new(Int64Array::from(vec![due_ns; rows])) as ArrayRef,
                        ),
                        ("n", Arc::new(Int64Array::from(vec![i as i64; rows]))),
                    ])
                    .expect("a batch");
                    let pending = write(&batch).expect("sent");
                    sent.send((due_at, due_ns, pending)).expect("the waiter");
                    count += rows as u64;
                }
                drop(sent);
                acked
                    .lock()
                    .unwrap()
                    .extend(waiter.join().expect("the waiter"));
                *written.lock().unwrap() += count;
            })
        })
        .collect();
    std::thread::sleep(Duration::from_secs(args.seconds));
    stop.store(true, Ordering::Relaxed);
    for thread in threads {
        thread.join().expect("a writer");
    }
    std::thread::sleep(Duration::from_millis(500));
    let elapsed = (now_ns() - start_ns) as f64 / 1e9;
    let cpu = cpu_seconds() - start_cpu;
    let rows = *written.lock().unwrap();
    println!(
        "weir {}: {} writer(s) x {} x {}-row batches, {} in flight, {:.0} rows/s over {:.1} s, \
         {:.2} us CPU per row ({:.1} cores), on {}",
        if args.flight {
            "over Flight"
        } else {
            "in-process"
        },
        args.writers,
        if args.rate == 0 {
            "as fast as it can".to_owned()
        } else {
            format!("{}/s", args.rate)
        },
        args.rows,
        args.window,
        rows as f64 / elapsed,
        elapsed,
        cpu * 1e6 / rows.max(1) as f64,
        cpu / elapsed,
        args.dir.display()
    );
    print_latency("due to durable", std::mem::take(&mut acked.lock().unwrap()));
    print_latency(
        "due to subscriber",
        std::mem::take(&mut delivered.lock().unwrap()),
    );
    drop(reader);
    drop(server);
    drop(store);
    let _ = std::fs::remove_dir_all(&dir);
}

fn print_latency(what: &str, mut times: Vec<Duration>) {
    if times.is_empty() {
        println!("  {what}: no samples");
        return;
    }
    times.sort();
    let at = |p: f64| times[((times.len() - 1) as f64 * p).round() as usize];
    let us = |d: Duration| d.as_secs_f64() * 1e6;
    println!(
        "  {what:<20} n {:>8}  p50 {:>8.0} us  p90 {:>8.0} us  p99 {:>8.0} us  p99.9 {:>8.0} us  \
         max {:>8.0} us",
        times.len(),
        us(at(0.5)),
        us(at(0.9)),
        us(at(0.99)),
        us(at(0.999)),
        us(at(1.0))
    );
}
