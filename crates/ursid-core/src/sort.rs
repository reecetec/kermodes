//! Static sort: a table's rows ordered by some of its columns.
//!
//! Each sort column becomes an order-preserving unsigned integer per row: integers, dates, and
//! timestamps with the sign bit flipped, floats by IEEE total order, booleans as 0/1, strings by
//! rank among the column's distinct values. A column takes as many bits as its values' range
//! needs, plus one above them for NULL, the largest value; a descending column is inverted
//! within its width. Columns are packed, most significant first, into as few 64- or 128-bit
//! words as fit (usually one). Rows sort by their words with their position as the last
//! tie-break, so ties keep the input's order: one pass partitions them by the key's top bits,
//! then the partitions sort on every core.

use ahash::RandomState;
use arrow::array::{Array, ArrayRef, AsArray};
use arrow::compute::{cast, interleave_record_batch};
use arrow::datatypes::{DataType, Float64Type, Int64Type, SchemaRef};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use hashbrown::HashMap;

use crate::parallel::{PIECE_ROWS, run, threads_for};

/// `batches` (rows in key order) ordered by `keys`, each a column and whether it sorts
/// descending; earlier keys first. NULL sorts as the largest value (last ascending, first
/// descending, as in Postgres), and ties keep the input's order.
pub(crate) fn sorted(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    keys: &[(usize, bool)],
) -> Result<Vec<RecordBatch>, ArrowError> {
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    if rows == 0 {
        return Ok(Vec::new());
    }
    let count = u32::try_from(rows)
        .map_err(|_| ArrowError::ComputeError("sort takes at most 2^32 rows".into()))?;
    let fields = keys
        .iter()
        .map(|(column, descending)| {
            Field::new(
                schema.field(*column).data_type(),
                batches,
                *column,
                *descending,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let words = words(&fields);
    let mut order: Vec<u32> = (0..count).collect();
    let mut fields = Some(fields);
    // Least significant word first: each pass is stable, so earlier words decide. Once the most
    // significant word's pairs exist, the normalized columns are no longer needed: they go
    // before that word's pairs are partitioned, and before the output is built.
    for (index, word) in words.iter().enumerate().rev() {
        let all = fields.as_ref().expect("fields live until the last word");
        let word: Vec<&Field> = word.iter().map(|at| &all[*at]).collect();
        let width: u32 = word.iter().map(|f| f.width).sum();
        order = if width <= 64 {
            let pairs = keyed(&order, |row| pack(&word, row) as u64);
            drop(word);
            if index == 0 {
                fields = None;
            }
            sort_pairs(&order, width, pairs)
        } else {
            let pairs = keyed(&order, |row| pack(&word, row));
            drop(word);
            if index == 0 {
                fields = None;
            }
            sort_pairs(&order, width, pairs)
        };
    }
    gather(batches, &order)
}

/// One sort column as an unsigned integer per row.
struct Field {
    /// Order-preserving values, meaningless where the row is NULL.
    values: Vec<u64>,
    valid: Option<Vec<bool>>,
    /// The smallest valid value, subtracted so the field needs only its range's bits.
    min: u64,
    /// Bits for the range, plus one for NULL if any.
    width: u32,
    bits: u32,
    descending: bool,
}

impl Field {
    fn new(
        data_type: &DataType,
        batches: &[RecordBatch],
        column: usize,
        descending: bool,
    ) -> Result<Self, ArrowError> {
        let arrays: Vec<&ArrayRef> = batches.iter().map(|b| b.column(column)).collect();
        let values =
            match data_type {
                DataType::Utf8 => ranks(&arrays),
                DataType::Float64 => {
                    let mut values = Vec::new();
                    for array in &arrays {
                        values.extend(array.as_primitive::<Float64Type>().values().iter().map(
                            |v| {
                                let bits = v.to_bits();
                                if bits >> 63 == 1 {
                                    !bits
                                } else {
                                    bits | 1 << 63
                                }
                            },
                        ));
                    }
                    values
                }
                DataType::Boolean => {
                    let mut values = Vec::new();
                    for array in &arrays {
                        values.extend(array.as_boolean().values().iter().map(u64::from));
                    }
                    values
                }
                _ => {
                    let mut values = Vec::new();
                    for array in &arrays {
                        let wide = cast(array, &DataType::Int64)?;
                        values.extend(
                            wide.as_primitive::<Int64Type>()
                                .values()
                                .iter()
                                .map(|v| (*v as u64) ^ (1 << 63)),
                        );
                    }
                    values
                }
            };
        let valid = arrays.iter().any(|a| a.null_count() > 0).then(|| {
            let mut valid = Vec::with_capacity(values.len());
            for array in &arrays {
                valid.extend((0..array.len()).map(|row| array.is_valid(row)));
            }
            valid
        });
        let present = |row: usize| valid.as_ref().is_none_or(|v| v[row]);
        let (mut min, mut max) = (u64::MAX, 0);
        for (row, value) in values.iter().enumerate() {
            if present(row) {
                min = min.min(*value);
                max = max.max(*value);
            }
        }
        if min > max {
            (min, max) = (0, 0);
        }
        let bits = u64::BITS - (max - min).leading_zeros();
        Ok(Self {
            width: bits + u32::from(valid.is_some()),
            values,
            valid,
            min,
            bits,
            descending,
        })
    }

    fn get(&self, row: usize) -> u128 {
        let value = match &self.valid {
            Some(valid) if !valid[row] => 1u128 << self.bits,
            _ => u128::from(self.values[row] - self.min),
        };
        if self.descending {
            ((1u128 << self.width) - 1) - value
        } else {
            value
        }
    }
}

/// Each string's rank among the column's distinct strings.
fn ranks(arrays: &[&ArrayRef]) -> Vec<u64> {
    let mut ids: HashMap<&str, u32, RandomState> = HashMap::with_hasher(RandomState::new());
    let mut distinct = Vec::new();
    let mut row_ids = Vec::new();
    for array in arrays {
        let strings = array.as_string::<i32>();
        for row in 0..strings.len() {
            // NULL rows get any id: their value is never read.
            let value = strings.value(row);
            let id = *ids.entry(value).or_insert_with(|| {
                distinct.push(value);
                (distinct.len() - 1) as u32
            });
            row_ids.push(id);
        }
    }
    let mut by_value: Vec<u32> = (0..distinct.len() as u32).collect();
    by_value.sort_unstable_by_key(|id| distinct[*id as usize]);
    let mut rank = vec![0; distinct.len()];
    for (position, id) in by_value.iter().enumerate() {
        rank[*id as usize] = position as u64;
    }
    row_ids.iter().map(|id| rank[*id as usize]).collect()
}

/// Consecutive fields, by index, packed into words of at most 128 bits; a field is at most 65
/// bits wide.
fn words(fields: &[Field]) -> Vec<Vec<usize>> {
    let mut words: Vec<Vec<usize>> = Vec::new();
    let mut width = 0;
    for (at, field) in fields.iter().enumerate() {
        match words.last_mut() {
            Some(word) if width + field.width <= 128 => word.push(at),
            _ => {
                words.push(vec![at]);
                width = 0;
            }
        }
        width += field.width;
    }
    words
}

fn pack(word: &[&Field], row: u32) -> u128 {
    word.iter().fold(0, |key, field| {
        (key << field.width) | field.get(row as usize)
    })
}

/// A sort key: an unsigned integer whose top bits pick a partition.
trait Key: Ord + Copy + Default + Send + Sync {
    fn top(self, shift: u32) -> usize;
}

impl Key for u64 {
    fn top(self, shift: u32) -> usize {
        (self >> shift) as usize & 0xFF
    }
}

impl Key for u128 {
    fn top(self, shift: u32) -> usize {
        (self >> shift) as usize & 0xFF
    }
}

/// Each of `order`'s rows keyed by `key`, paired with its place in `order`: unique, so an
/// unstable sort of the pairs is stable in effect.
fn keyed<K: Key>(order: &[u32], key: impl Fn(u32) -> K) -> Vec<(K, u32)> {
    order
        .iter()
        .enumerate()
        .map(|(at, row)| (key(*row), at as u32))
        .collect()
}

/// `order` stably sorted by its rows' `pairs` (keys `width` bits wide).
fn sort_pairs<K: Key>(order: &[u32], width: u32, mut pairs: Vec<(K, u32)>) -> Vec<u32> {
    let threads = threads_for(pairs.len());
    if threads <= 1 {
        pairs.sort_unstable();
    } else {
        pairs = partitioned_sort(pairs, width, threads);
    }
    pairs.iter().map(|(_, at)| order[*at as usize]).collect()
}

/// Partitions `pairs` by their key's top 8 bits, then sorts the partitions on `threads` cores.
fn partitioned_sort<K: Key>(pairs: Vec<(K, u32)>, width: u32, threads: usize) -> Vec<(K, u32)> {
    let shift = width.saturating_sub(8);
    let mut counts = [0usize; 256];
    for (key, _) in &pairs {
        counts[key.top(shift)] += 1;
    }
    let mut starts = [0usize; 256];
    let mut sum = 0;
    for (start, count) in starts.iter_mut().zip(counts) {
        *start = sum;
        sum += count;
    }
    let mut out = vec![(K::default(), 0); pairs.len()];
    let mut next = starts;
    for pair in pairs {
        let bucket = pair.0.top(shift);
        out[next[bucket]] = pair;
        next[bucket] += 1;
    }
    // Consecutive partitions grouped into about four tasks per thread of similar size.
    let target = out.len().div_ceil(threads * 4).max(1);
    let mut tasks = Vec::new();
    let mut rest = out.as_mut_slice();
    let mut taken = 0;
    for bucket in 0..256 {
        let end = starts[bucket] + counts[bucket];
        if end - taken >= target || bucket == 255 {
            let (task, tail) = rest.split_at_mut(end - taken);
            if !task.is_empty() {
                tasks.push(task);
            }
            rest = tail;
            taken = end;
        }
    }
    run(threads, tasks, |(), task: &mut [(K, u32)]| {
        task.sort_unstable()
    });
    out
}

/// The rows at `order` (positions across `batches`), in pieces gathered on every core.
fn gather(batches: &[RecordBatch], order: &[u32]) -> Result<Vec<RecordBatch>, ArrowError> {
    let mut starts = Vec::with_capacity(batches.len());
    let mut sum = 0;
    for batch in batches {
        starts.push(sum);
        sum += batch.num_rows();
    }
    let sources: Vec<&RecordBatch> = batches.iter().collect();
    let pieces: Vec<&[u32]> = order.chunks(PIECE_ROWS).collect();
    run(threads_for(order.len()), pieces, |(), piece: &[u32]| {
        let at: Vec<(usize, usize)> = piece
            .iter()
            .map(|row| {
                let row = *row as usize;
                let batch = starts.partition_point(|start| *start <= row) - 1;
                (batch, row - starts[batch])
            })
            .collect();
        interleave_record_batch(&sources, &at)
    })
    .into_iter()
    .collect()
}
