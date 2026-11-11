//! Spreading a cycle's work over cores: rows are cut into fixed-size pieces (so results never
//! depend on the number of cores), pieces run on scoped threads, and results come back in order.

use std::num::NonZero;
use std::thread;

use arrow::record_batch::RecordBatch;

use crate::storage::{RowKey, Rows};

/// Below this many rows, work stays on the calling thread: spawning costs more than it saves.
pub(crate) const PARALLEL_ROWS: usize = 1 << 16;

/// Rows per piece for row-at-a-time operators (`where`, `update`, joins): small enough to bound
/// per-row scratch and spread over cores, large enough to amortize per-piece work.
pub(crate) const PIECE_ROWS: usize = 1 << 16;

/// Cores to use: the `URSID_THREADS` environment variable if it holds a positive number (as
/// DuckDB's `threads` setting or `POLARS_MAX_THREADS`), else every core.
pub(crate) fn threads() -> usize {
    std::env::var("URSID_THREADS")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|count| *count > 0)
        .unwrap_or_else(|| thread::available_parallelism().map_or(1, NonZero::get))
}

/// The threads for row-at-a-time work (`where`, `update`, joins) over `rows` rows: one below four
/// pieces, where a pass on one thread costs about what spawning threads does (a 100k-row ticking
/// `where` + `update` took 0.2 ms on one thread and 0.4 ms on two), else `threads()`.
pub(crate) fn threads_for(rows: usize) -> usize {
    if rows < 4 * PIECE_ROWS { 1 } else { threads() }
}

/// Fixed pieces `(offset, len)` of at most `size` rows covering `rows` rows.
pub(crate) fn offsets(rows: usize, size: usize) -> impl Iterator<Item = (usize, usize)> {
    (0..rows)
        .step_by(size)
        .map(move |offset| (offset, size.min(rows - offset)))
}

/// `batches` in pieces of at most `size` rows: each piece's keys and rows, in order.
pub(crate) fn row_pieces(batches: &[Rows], size: usize) -> Vec<(&[RowKey], RecordBatch)> {
    batches
        .iter()
        .flat_map(|rows| {
            offsets(rows.keys.len(), size).map(move |(offset, len)| {
                (
                    &rows.keys[offset..offset + len],
                    rows.batch.slice(offset, len),
                )
            })
        })
        .collect()
}

/// Runs `f` over `items`, spread across up to `threads` scoped threads, returning results in
/// item order. Each thread passes `f` its own scratch state, reused across its items.
pub(crate) fn run<I: Send, T: Send, S: Default>(
    threads: usize,
    items: Vec<I>,
    f: impl Fn(&mut S, I) -> T + Sync,
) -> Vec<T> {
    if threads <= 1 || items.len() <= 1 {
        let mut scratch = S::default();
        return items
            .into_iter()
            .map(|item| f(&mut scratch, item))
            .collect();
    }
    let count = items.len();
    let mut queues: Vec<Vec<(usize, I)>> = (0..threads).map(|_| Vec::new()).collect();
    for (index, item) in items.into_iter().enumerate() {
        queues[index % threads].push((index, item));
    }
    let f = &f;
    let mut results: Vec<Option<T>> = (0..count).map(|_| None).collect();
    thread::scope(|scope| {
        let workers: Vec<_> = queues
            .into_iter()
            .map(|queue| {
                scope.spawn(move || {
                    let mut scratch = S::default();
                    queue
                        .into_iter()
                        .map(|(index, item)| (index, f(&mut scratch, item)))
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        for worker in workers {
            for (index, result) in worker.join().expect("a worker thread panicked") {
                results[index] = Some(result);
            }
        }
    });
    results
        .into_iter()
        .map(|r| r.expect("every item ran"))
        .collect()
}
