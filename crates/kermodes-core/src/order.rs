//! Ticking sort: a table's row keys kept in the order of some of its columns, so the table can be
//! read in that order (snapshots, the viewer's rows by position) while its rows stay its input's.
//!
//! Each row's sort columns become one byte string (`arrow::row`) whose byte order is the sort
//! order: NULL largest, floats by IEEE total order, strings by bytes, descending columns
//! inverted; the row key breaks ties, so ties keep key order, as the static sort does. Entries
//! live in sorted blocks of at most `BLOCK` rows, each holding its rows' bytes back to back, so
//! there is no allocation per row. A cycle's removals and additions are sorted, then the blocks
//! are walked once: only blocks a change falls in are rebuilt, so a cycle costs its changes'
//! sort plus the blocks they touch, and the first cycle (every row) is one sort. Position `p` is
//! found by walking the block sizes.
//!
//! A large cycle (the first holds every row) converts and sorts on every core. Comparing byte
//! strings misses the cache on every comparison, so entries sort by an integer drawn from their
//! bytes instead (`sort_entries`), and the first cycle cuts its blocks in parallel.

use arrow::array::ArrayRef;
use arrow::compute::SortOptions;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, Rows, SortField};

use crate::parallel::{PIECE_ROWS, run, threads_for};
use crate::sort::partitioned_sort;
use crate::storage::{self, Delta, RowKey};

/// Most rows a block holds. Rebuilt runs are cut into blocks of half that, leaving room to grow.
const BLOCK: usize = 2048;

/// A row's place in the order: its sort bytes, then its key.
type Entry<'a> = (&'a [u8], RowKey);

pub(crate) struct Order {
    converter: RowConverter,
    /// The sort columns, most significant first.
    columns: Vec<usize>,
    blocks: Vec<Block>,
    len: usize,
}

/// Sorted entries: row `i`'s sort bytes are `bytes[ends[i - 1]..ends[i]]`, its key `keys[i]`.
#[derive(Default)]
struct Block {
    bytes: Vec<u8>,
    ends: Vec<u32>,
    keys: Vec<RowKey>,
}

impl Block {
    fn len(&self) -> usize {
        self.keys.len()
    }

    fn entry(&self, i: usize) -> Entry<'_> {
        let start = if i == 0 { 0 } else { self.ends[i - 1] as usize };
        (&self.bytes[start..self.ends[i] as usize], self.keys[i])
    }

    fn last(&self) -> Entry<'_> {
        self.entry(self.len() - 1)
    }

    fn entries(&self) -> impl Iterator<Item = Entry<'_>> {
        (0..self.len()).map(|i| self.entry(i))
    }

    fn push(&mut self, (bytes, key): Entry<'_>) {
        self.bytes.extend_from_slice(bytes);
        self.ends.push(self.bytes.len() as u32);
        self.keys.push(key);
    }

    fn shrink(mut self) -> Self {
        self.bytes.shrink_to_fit();
        self.ends.shrink_to_fit();
        self.keys.shrink_to_fit();
        self
    }
}

/// Collects a sorted run of entries into blocks of `BLOCK / 2`.
struct Cutter<'b> {
    out: &'b mut Vec<Block>,
    current: Block,
}

impl<'b> Cutter<'b> {
    fn new(out: &'b mut Vec<Block>) -> Self {
        Self {
            out,
            current: Block::default(),
        }
    }

    fn push(&mut self, entry: Entry<'_>) {
        self.current.push(entry);
        if self.current.len() == BLOCK / 2 {
            self.out.push(std::mem::take(&mut self.current).shrink());
        }
    }

    fn finish(self) {
        if self.current.len() > 0 {
            self.out.push(self.current.shrink());
        }
    }
}

impl Order {
    /// An empty order over `schema` by `keys`: each a column and whether it sorts descending.
    pub(crate) fn new(schema: &SchemaRef, keys: &[(usize, bool)]) -> Result<Self, ArrowError> {
        let fields = keys
            .iter()
            .map(|(column, descending)| {
                SortField::new_with_options(
                    schema.field(*column).data_type().clone(),
                    // NULL is the largest value: last ascending, first descending.
                    SortOptions {
                        descending: *descending,
                        nulls_first: *descending,
                    },
                )
            })
            .collect();
        Ok(Self {
            converter: RowConverter::new(fields)?,
            columns: keys.iter().map(|(column, _)| *column).collect(),
            blocks: Vec::new(),
            len: 0,
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    /// Applies a cycle's changes to the input. A modified row moves only if its sort columns
    /// changed.
    pub(crate) fn apply(&mut self, delta: &Delta) -> Result<(), ArrowError> {
        let sorts = |batches: Vec<&RecordBatch>| {
            let rows = batches.iter().map(|batch| batch.num_rows()).sum();
            run(threads_for(rows), batches, |(), batch| {
                self.sort_rows(batch)
            })
            .into_iter()
            .collect::<Result<Vec<Rows>, _>>()
        };
        let removed = sorts(delta.removed.iter().map(|rows| &rows.batch).collect())?;
        let previous = sorts(delta.modified.iter().map(|m| &m.previous).collect())?;
        let current = sorts(delta.modified.iter().map(|m| &m.current).collect())?;
        let added = sorts(delta.added.iter().map(|rows| &rows.batch).collect())?;

        let mut gone: Vec<Entry> = Vec::new();
        let mut new: Vec<Entry> = Vec::new();
        push_entries(&mut gone, &delta.removed, &removed);
        for ((modified, before), after) in delta.modified.iter().zip(&previous).zip(&current) {
            for (i, key) in modified.keys.iter().enumerate() {
                let (before, after) = (before.row(i).data(), after.row(i).data());
                if before != after {
                    gone.push((before, *key));
                    new.push((after, *key));
                }
            }
        }
        push_entries(&mut new, &delta.added, &added);
        sort_entries(&mut gone);
        sort_entries(&mut new);
        self.remove_all(&gone)?;
        self.insert_all(&new);
        Ok(())
    }

    /// The keys at positions `start..start + count` in sort order (fewer past the end).
    pub(crate) fn keys_at(&self, start: usize, count: usize) -> Vec<RowKey> {
        let mut keys = Vec::with_capacity(count.min(self.len.saturating_sub(start)));
        let mut skip = start;
        for block in &self.blocks {
            if keys.len() == count {
                break;
            }
            if skip >= block.len() {
                skip -= block.len();
                continue;
            }
            let take = (block.len() - skip).min(count - keys.len());
            keys.extend_from_slice(&block.keys[skip..skip + take]);
            skip = 0;
        }
        keys
    }

    fn sort_rows(&self, batch: &RecordBatch) -> Result<Rows, ArrowError> {
        let columns: Vec<ArrayRef> = self
            .columns
            .iter()
            .map(|c| batch.column(*c).clone())
            .collect();
        self.converter.convert_columns(&columns)
    }

    /// Removes sorted `gone` entries, every one of which must be present. Blocks without any
    /// keep as they are; a block left small joins the block before it when both fit in one.
    fn remove_all(&mut self, gone: &[Entry]) -> Result<(), ArrowError> {
        if gone.is_empty() {
            return Ok(());
        }
        let old = std::mem::take(&mut self.blocks);
        let mut blocks: Vec<Block> = Vec::with_capacity(old.len());
        let mut next = 0;
        for block in old {
            let end = next + gone[next..].partition_point(|entry| *entry <= block.last());
            if end == next {
                blocks.push(block);
                continue;
            }
            let mut kept = Block::default();
            let mut at = next;
            for entry in block.entries() {
                if at < end && gone[at] == entry {
                    at += 1;
                } else {
                    kept.push(entry);
                }
            }
            if at != end {
                return Err(missing(gone[at].1));
            }
            next = end;
            match blocks.last_mut() {
                Some(before) if kept.len() < BLOCK / 4 && before.len() + kept.len() <= BLOCK => {
                    kept.entries().for_each(|entry| before.push(entry));
                }
                _ if kept.len() > 0 => blocks.push(kept.shrink()),
                _ => {}
            }
        }
        if next != gone.len() {
            return Err(missing(gone[next].1));
        }
        self.blocks = blocks;
        self.len -= gone.len();
        Ok(())
    }

    /// Inserts sorted `new` entries. An entry belongs to the first block whose last entry is
    /// greater (the last block if none is); each block that gains entries is merged with them
    /// and cut into fresh blocks.
    fn insert_all(&mut self, new: &[Entry]) {
        if new.is_empty() {
            return;
        }
        if self.blocks.is_empty() {
            // Every entry is new: blocks of `BLOCK / 2` cut on every core.
            let pieces: Vec<&[Entry]> = new.chunks(BLOCK / 2).collect();
            self.blocks = run(threads_for(new.len()), pieces, |(), piece: &[Entry]| {
                let mut block = Block {
                    bytes: Vec::with_capacity(piece.iter().map(|(bytes, _)| bytes.len()).sum()),
                    ends: Vec::with_capacity(piece.len()),
                    keys: Vec::with_capacity(piece.len()),
                };
                piece.iter().for_each(|entry| block.push(*entry));
                block
            });
            self.len = new.len();
            return;
        }
        let old = std::mem::take(&mut self.blocks);
        let mut blocks: Vec<Block> = Vec::with_capacity(old.len() + new.len() / (BLOCK / 2) + 1);
        let mut next = 0;
        let count = old.len();
        for (b, block) in old.into_iter().enumerate() {
            let end = match b + 1 == count {
                true => new.len(),
                false => next + new[next..].partition_point(|entry| *entry < block.last()),
            };
            if end == next {
                blocks.push(block);
                continue;
            }
            let mut cut = Cutter::new(&mut blocks);
            let mut incoming = new[next..end].iter().peekable();
            for entry in block.entries() {
                while let Some(add) = incoming.next_if(|add| **add < entry) {
                    cut.push(*add);
                }
                cut.push(entry);
            }
            incoming.for_each(|add| cut.push(*add));
            cut.finish();
            next = end;
        }
        self.blocks = blocks;
        self.len += new.len();
    }
}

fn missing(key: RowKey) -> ArrowError {
    ArrowError::ComputeError(format!("sort: removed row key {key} is not in the order"))
}

/// Appends an entry for each row of `batches`: its key, and its sort bytes in `sorts` (one
/// `Rows` per batch), filling each batch's place on every core.
fn push_entries<'a>(out: &mut Vec<Entry<'a>>, batches: &[storage::Rows], sorts: &'a [Rows]) {
    let total: usize = batches.iter().map(|batch| batch.keys.len()).sum();
    let start = out.len();
    out.resize(start + total, (&[], 0));
    let mut rest = &mut out[start..];
    let mut tasks = Vec::with_capacity(batches.len());
    for (batch, sort) in batches.iter().zip(sorts) {
        let (task, tail) = rest.split_at_mut(batch.keys.len());
        tasks.push((task, batch.keys.as_slice(), sort));
        rest = tail;
    }
    run(threads_for(total), tasks, |(), (task, keys, sort)| {
        for (i, (slot, key)) in task.iter_mut().zip(keys).enumerate() {
            *slot = (sort.row(i).data(), *key);
        }
    });
}

/// Sorts `entries` as `sort_unstable` does, on every core when there are many.
///
/// Each entry's integer is the 8 bytes after the prefix all entries share, big-endian and padded
/// with zeros: it never orders two entries against their bytes, so sorting by it, then each run
/// of equal integers by whole entries, sorts the entries. Those runs are usually in order
/// already (rows arrive in key order, and equal bytes compare by key), which a linear check
/// finds.
fn sort_entries(entries: &mut Vec<Entry<'_>>) {
    let threads = threads_for(entries.len());
    if threads <= 1 {
        entries.sort_unstable();
        return;
    }
    let source: &[Entry] = entries;
    let first = source[0].0;
    let shared = run(
        threads,
        source.chunks(PIECE_ROWS).collect(),
        |(), piece: &[Entry]| {
            piece.iter().fold(first.len(), |shared, (bytes, _)| {
                first[..shared]
                    .iter()
                    .zip(*bytes)
                    .position(|(a, b)| a != b)
                    .unwrap_or(shared.min(bytes.len()))
            })
        },
    )
    .into_iter()
    .min()
    .unwrap_or(0);

    let mut pairs: Vec<(u64, u32)> = vec![(0, 0); source.len()];
    let pieces: Vec<_> = source
        .chunks(PIECE_ROWS)
        .zip(pairs.chunks_mut(PIECE_ROWS))
        .enumerate()
        .map(|(i, (piece, out))| (i * PIECE_ROWS, piece, out))
        .collect();
    let (min, max) = run(threads, pieces, |(), (offset, piece, out)| {
        let (mut min, mut max) = (u64::MAX, 0);
        for (at, ((bytes, _), pair)) in piece.iter().zip(out).enumerate() {
            let key = prefix(bytes, shared);
            min = min.min(key);
            max = max.max(key);
            *pair = (key, (offset + at) as u32);
        }
        (min, max)
    })
    .into_iter()
    .fold((u64::MAX, 0), |(a, b), (c, d)| (a.min(c), b.max(d)));
    run(
        threads,
        pairs.chunks_mut(PIECE_ROWS).collect(),
        |(), piece: &mut [(u64, u32)]| piece.iter_mut().for_each(|(key, _)| *key -= min),
    );
    let width = u64::BITS - (max - min).leading_zeros();
    let pairs = partitioned_sort(pairs, width, threads);
    let mut sorted: Vec<Entry> = vec![(&[], 0); pairs.len()];
    let pieces: Vec<_> = pairs
        .chunks(PIECE_ROWS)
        .zip(sorted.chunks_mut(PIECE_ROWS))
        .collect();
    run(threads, pieces, |(), (from, to)| {
        for ((_, at), slot) in from.iter().zip(to) {
            *slot = source[*at as usize];
        }
    });

    // Runs of equal integers, grouped into about four tasks per thread, cut only between runs.
    let target = sorted.len().div_ceil(threads * 4);
    let mut tasks: Vec<&mut [Entry]> = Vec::new();
    let mut rest = sorted.as_mut_slice();
    let mut start = 0;
    for at in 1..=pairs.len() {
        if at == pairs.len() || (at - start >= target && pairs[at].0 != pairs[at - 1].0) {
            let (task, tail) = rest.split_at_mut(at - start);
            tasks.push(task);
            rest = tail;
            start = at;
        }
    }
    drop(pairs);
    run(threads, tasks, |(), task: &mut [Entry]| {
        let mut run_start = 0;
        for at in 1..=task.len() {
            if at == task.len() || prefix(task[at].0, shared) != prefix(task[at - 1].0, shared) {
                let run = &mut task[run_start..at];
                if !run.is_sorted() {
                    run.sort_unstable();
                }
                run_start = at;
            }
        }
    });
    *entries = sorted;
}

/// The 8 bytes of `bytes` after its first `shared`, as a big-endian integer padded with zeros.
fn prefix(bytes: &[u8], shared: usize) -> u64 {
    let tail = &bytes[shared.min(bytes.len())..];
    let mut word = [0u8; 8];
    let n = tail.len().min(8);
    word[..n].copy_from_slice(&tail[..n]);
    u64::from_be_bytes(word)
}
