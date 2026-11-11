//! Grouping rows by key: each batch becomes a vector of group ids in one pass, with direct
//! hashing for a single integer-like or string column and Arrow's row encoding otherwise.

use std::collections::{BTreeSet, HashMap};

use ahash::RandomState;
use arrow::array::{Array, ArrayRef, ArrowPrimitiveType, AsArray, PrimitiveArray, UInt32Array};
use arrow::compute::take_record_batch;
use arrow::datatypes::{
    DataType, Date32Type, Int32Type, Int64Type, Schema, TimeUnit, TimestampNanosecondType,
};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, Rows, SortField};

use crate::Error;
use crate::storage::RowKey;

/// Encodes key columns as comparable bytes, one key per row.
pub(crate) struct Grouper {
    columns: Vec<usize>,
    converter: RowConverter,
}

impl Grouper {
    pub fn new(schema: &Schema, names: &[&str]) -> Result<Self, Error> {
        let columns = resolve(schema, names)?;
        Self::by_position(schema, columns).map_err(|e| Error::Schema(e.to_string()))
    }

    pub fn by_position(schema: &Schema, columns: Vec<usize>) -> Result<Self, ArrowError> {
        let fields = columns
            .iter()
            .map(|i| SortField::new(schema.field(*i).data_type().clone()))
            .collect();
        Ok(Self {
            columns,
            converter: RowConverter::new(fields)?,
        })
    }

    pub fn columns(&self) -> &[usize] {
        &self.columns
    }

    pub fn encode(&self, batch: &RecordBatch) -> Result<Rows, ArrowError> {
        let columns: Vec<ArrayRef> = self
            .columns
            .iter()
            .map(|i| batch.column(*i).clone())
            .collect();
        self.converter.convert_columns(&columns)
    }
}

/// Column indices for `names`, rejecting unknown and repeated names.
pub(crate) fn resolve(schema: &Schema, names: &[&str]) -> Result<Vec<usize>, Error> {
    let mut columns = Vec::with_capacity(names.len());
    for name in names {
        let index = schema.index_of(name).map_err(|_| {
            let known: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
            Error::Schema(format!(
                "unknown column `{name}`; columns are: {}",
                known.join(", ")
            ))
        })?;
        if columns.contains(&index) {
            return Err(Error::Schema(format!("column `{name}` is listed twice")));
        }
        columns.push(index);
    }
    Ok(columns)
}

/// How a key is read from a batch.
enum KeyKind {
    /// No key columns: every row has the same key.
    None,
    /// One integer, date, or timestamp column.
    Int(usize),
    /// One string column.
    Str(usize),
    /// Anything else: Arrow's row encoding.
    Rows(Grouper),
}

impl KeyKind {
    fn new(schema: &Schema, columns: Vec<usize>) -> Result<Self, ArrowError> {
        Ok(match columns.as_slice() {
            [] => KeyKind::None,
            [column] => match schema.field(*column).data_type() {
                DataType::Int32
                | DataType::Int64
                | DataType::Date32
                | DataType::Timestamp(TimeUnit::Nanosecond, _) => KeyKind::Int(*column),
                DataType::Utf8 => KeyKind::Str(*column),
                _ => KeyKind::Rows(Grouper::by_position(schema, columns)?),
            },
            _ => KeyKind::Rows(Grouper::by_position(schema, columns)?),
        })
    }
}

/// Calls `f(row)` for every row of a batch of `len` rows, or only the selected `rows`.
pub(crate) fn for_rows(rows: Option<&[u32]>, len: usize, mut f: impl FnMut(usize)) {
    match rows {
        None => (0..len).for_each(f),
        Some(rows) => rows.iter().for_each(|row| f(*row as usize)),
    }
}

/// Calls `f(value)` for an integer-like column read as `i64`, `None` for nulls, over all rows
/// or the selected ones.
fn each_int(array: &dyn Array, rows: Option<&[u32]>, f: impl FnMut(Option<i64>)) {
    fn walk<T: ArrowPrimitiveType>(
        array: &PrimitiveArray<T>,
        rows: Option<&[u32]>,
        mut f: impl FnMut(Option<i64>),
    ) where
        T::Native: Into<i64>,
    {
        let values = array.values();
        for_rows(rows, array.len(), |row| {
            f(array.is_valid(row).then(|| values[row].into()));
        });
    }
    match array.data_type() {
        DataType::Int32 => walk(array.as_primitive::<Int32Type>(), rows, f),
        DataType::Int64 => walk(array.as_primitive::<Int64Type>(), rows, f),
        DataType::Date32 => walk(array.as_primitive::<Date32Type>(), rows, f),
        _ => walk(array.as_primitive::<TimestampNanosecondType>(), rows, f),
    }
}

/// Splits rows into shards by a hash of their key: the same key always lands in the same shard.
pub(crate) struct Sharder {
    key: KeyKind,
    hasher: RandomState,
    shards: usize,
}

impl Sharder {
    pub fn new(schema: &Schema, columns: Vec<usize>, shards: usize) -> Result<Self, ArrowError> {
        Ok(Self {
            key: KeyKind::new(schema, columns)?,
            hasher: RandomState::with_seeds(
                0x243f_6a88_85a3_08d3,
                0x1319_8a2e_0370_7344,
                0xa409_3822_299f_31d0,
                0x082e_fa98_ec4e_6c89,
            ),
            shards,
        })
    }

    pub fn shards(&self) -> usize {
        self.shards
    }

    /// The rows of `batch` in each shard, as row indices plus `offset`, in row order.
    pub fn split(&self, batch: &RecordBatch, offset: u32) -> Result<Vec<Vec<u32>>, ArrowError> {
        let mut buckets: Vec<Vec<u32>> = vec![Vec::new(); self.shards];
        let mut row = offset;
        let mut put = |hash: u64| {
            buckets[(hash % self.shards as u64) as usize].push(row);
            row += 1;
        };
        match &self.key {
            KeyKind::None => (0..batch.num_rows()).for_each(|_| put(0)),
            KeyKind::Int(column) => each_int(batch.column(*column).as_ref(), None, |value| {
                put(self.hasher.hash_one(value));
            }),
            KeyKind::Str(column) => {
                for value in batch.column(*column).as_string::<i32>().iter() {
                    put(self.hasher.hash_one(value));
                }
            }
            KeyKind::Rows(grouper) => {
                for encoded in grouper.encode(batch)?.iter() {
                    put(self.hasher.hash_one(encoded.data()));
                }
            }
        }
        Ok(buckets)
    }
}

/// The keys of an input's rows within one group. Append-only inputs only need the newest.
pub(crate) enum Members {
    Newest(Option<RowKey>),
    All(BTreeSet<RowKey>),
}

impl Members {
    fn new(input_add_only: bool) -> Self {
        if input_add_only {
            Members::Newest(None)
        } else {
            Members::All(BTreeSet::new())
        }
    }

    pub fn insert(&mut self, key: RowKey) {
        match self {
            Members::Newest(newest) => *newest = Some(newest.map_or(key, |n| n.max(key))),
            Members::All(keys) => {
                keys.insert(key);
            }
        }
    }

    pub fn remove(&mut self, key: RowKey) -> Result<(), ArrowError> {
        match self {
            Members::Newest(_) => Err(ArrowError::InvalidArgumentError(
                "an append-only input removed a row".into(),
            )),
            Members::All(keys) => {
                keys.remove(&key);
                Ok(())
            }
        }
    }

    pub fn last(&self) -> Option<RowKey> {
        match self {
            Members::Newest(newest) => *newest,
            Members::All(keys) => keys.last().copied(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.last().is_none()
    }

    pub fn keys(&self) -> Vec<RowKey> {
        match self {
            Members::Newest(newest) => newest.iter().copied().collect(),
            Members::All(keys) => keys.iter().copied().collect(),
        }
    }
}

/// A group's key, kept so the group can be forgotten when it empties.
enum OwnedKey {
    Null,
    Int(i64),
    Str(Box<str>),
    Bytes(Box<[u8]>),
}

/// Key value → group id.
enum KeyMap {
    All(Option<u32>),
    Int {
        column: usize,
        map: HashMap<i64, u32, RandomState>,
        null: Option<u32>,
    },
    Str {
        column: usize,
        map: HashMap<Box<str>, u32, RandomState>,
        null: Option<u32>,
    },
    Rows {
        grouper: Grouper,
        map: HashMap<Box<[u8]>, u32, RandomState>,
    },
}

/// Groups of an input's rows: ids, membership, output slots, and which groups a cycle touched.
/// Ids of emptied groups are reused; output slots never are.
pub(crate) struct Membership {
    keys: KeyMap,
    owned: Vec<OwnedKey>,
    pub slot: Vec<RowKey>,
    pub members: Vec<Members>,
    touched: Vec<bool>,
    touched_list: Vec<u32>,
    /// Ids created or reused since the last `take_created`.
    created: Vec<u32>,
    free: Vec<u32>,
    /// The next output slot. Slots advance by `slot_stride`, so the shards of one operator,
    /// started at different offsets, never collide.
    next_slot: RowKey,
    slot_stride: RowKey,
    add_only: bool,
}

impl Membership {
    pub fn new(
        schema: &Schema,
        columns: Vec<usize>,
        add_only: bool,
        slot_offset: RowKey,
        slot_stride: RowKey,
    ) -> Result<Self, ArrowError> {
        let keys = match KeyKind::new(schema, columns)? {
            KeyKind::None => KeyMap::All(None),
            KeyKind::Int(column) => KeyMap::Int {
                column,
                map: HashMap::default(),
                null: None,
            },
            KeyKind::Str(column) => KeyMap::Str {
                column,
                map: HashMap::default(),
                null: None,
            },
            KeyKind::Rows(grouper) => KeyMap::Rows {
                grouper,
                map: HashMap::default(),
            },
        };
        Ok(Self {
            keys,
            owned: Vec::new(),
            slot: Vec::new(),
            members: Vec::new(),
            touched: Vec::new(),
            touched_list: Vec::new(),
            created: Vec::new(),
            free: Vec::new(),
            next_slot: slot_offset,
            slot_stride,
            add_only,
        })
    }

    /// Number of ids handed out so far, live or free: the length of per-group vectors.
    pub fn capacity(&self) -> usize {
        self.slot.len()
    }

    /// The group id of every row in `batch`, or of the selected `rows` in that order, creating
    /// groups as needed and marking them touched.
    pub fn assign(
        &mut self,
        batch: &RecordBatch,
        rows: Option<&[u32]>,
    ) -> Result<Vec<u32>, ArrowError> {
        let mut allocator = Allocator {
            owned: &mut self.owned,
            slot: &mut self.slot,
            members: &mut self.members,
            created: &mut self.created,
            free: &mut self.free,
            next_slot: &mut self.next_slot,
            slot_stride: self.slot_stride,
            add_only: self.add_only,
        };
        let count = rows.map_or(batch.num_rows(), <[u32]>::len);
        let mut ids = Vec::with_capacity(count);
        match &mut self.keys {
            KeyMap::All(id) => {
                let id = *id.get_or_insert_with(|| allocator.new_group(OwnedKey::Null));
                ids.resize(count, id);
            }
            KeyMap::Int { column, map, null } => {
                each_int(batch.column(*column).as_ref(), rows, |value| {
                    let id = match value {
                        None => *null.get_or_insert_with(|| allocator.new_group(OwnedKey::Null)),
                        Some(value) => *map
                            .entry(value)
                            .or_insert_with(|| allocator.new_group(OwnedKey::Int(value))),
                    };
                    ids.push(id);
                });
            }
            KeyMap::Str { column, map, null } => {
                let array = batch.column(*column).as_string::<i32>();
                for_rows(rows, array.len(), |row| {
                    let id = if array.is_null(row) {
                        *null.get_or_insert_with(|| allocator.new_group(OwnedKey::Null))
                    } else {
                        let text = array.value(row);
                        match map.get(text) {
                            Some(id) => *id,
                            None => {
                                let id = allocator.new_group(OwnedKey::Str(text.into()));
                                map.insert(text.into(), id);
                                id
                            }
                        }
                    };
                    ids.push(id);
                });
            }
            KeyMap::Rows { grouper, map } => {
                let selected = match rows {
                    None => batch.clone(),
                    Some(rows) => take_record_batch(batch, &UInt32Array::from(rows.to_vec()))?,
                };
                for row in grouper.encode(&selected)?.iter() {
                    let bytes = row.data();
                    let id = match map.get(bytes) {
                        Some(id) => *id,
                        None => {
                            let id = allocator.new_group(OwnedKey::Bytes(bytes.into()));
                            map.insert(bytes.into(), id);
                            id
                        }
                    };
                    ids.push(id);
                }
            }
        }
        self.touched.resize(self.slot.len(), false);
        for &id in &ids {
            let seen = &mut self.touched[id as usize];
            if !*seen {
                *seen = true;
                self.touched_list.push(id);
            }
        }
        Ok(ids)
    }

    /// Groups touched since the last call, in first-touch order.
    pub fn take_touched(&mut self) -> Vec<u32> {
        let touched = std::mem::take(&mut self.touched_list);
        for id in &touched {
            self.touched[*id as usize] = false;
        }
        touched
    }

    /// Groups created (or ids reused) since the last call; their per-group state must be reset.
    pub fn take_created(&mut self) -> Vec<u32> {
        std::mem::take(&mut self.created)
    }

    /// Forgets an emptied group; its id may be reused by a later key.
    pub fn release(&mut self, id: u32) {
        let owned = std::mem::replace(&mut self.owned[id as usize], OwnedKey::Null);
        match (&mut self.keys, owned) {
            (KeyMap::All(group), _) => *group = None,
            (KeyMap::Int { map, .. }, OwnedKey::Int(value)) => {
                map.remove(&value);
            }
            (KeyMap::Str { map, .. }, OwnedKey::Str(text)) => {
                map.remove(&text);
            }
            (KeyMap::Rows { map, .. }, OwnedKey::Bytes(bytes)) => {
                map.remove(&bytes);
            }
            (KeyMap::Int { null, .. } | KeyMap::Str { null, .. }, OwnedKey::Null) => *null = None,
            _ => {}
        }
        self.free.push(id);
    }
}

/// Hands out group ids, reusing freed ones, and sets up their membership.
struct Allocator<'a> {
    owned: &'a mut Vec<OwnedKey>,
    slot: &'a mut Vec<RowKey>,
    members: &'a mut Vec<Members>,
    created: &'a mut Vec<u32>,
    free: &'a mut Vec<u32>,
    next_slot: &'a mut RowKey,
    slot_stride: RowKey,
    add_only: bool,
}

impl Allocator<'_> {
    fn new_group(&mut self, key: OwnedKey) -> u32 {
        let slot = *self.next_slot;
        *self.next_slot += self.slot_stride;
        let id = match self.free.pop() {
            Some(id) => {
                let i = id as usize;
                self.owned[i] = key;
                self.slot[i] = slot;
                self.members[i] = Members::new(self.add_only);
                id
            }
            None => {
                self.owned.push(key);
                self.slot.push(slot);
                self.members.push(Members::new(self.add_only));
                (self.slot.len() - 1) as u32
            }
        };
        self.created.push(id);
        id
    }
}
