//! Grouping rows by key. Every key is hashed with one engine-wide hasher, so a hash computed once
//! serves shard routing, piece-local tables, and the owning shard's table alike.

use std::collections::BTreeSet;
use std::sync::LazyLock;

use ahash::RandomState;
use arrow::array::{Array, ArrayRef, AsArray, StringArray, UInt32Array};
use arrow::compute::take_record_batch;
use arrow::datatypes::{
    DataType, Date32Type, Int32Type, Int64Type, Schema, TimeUnit, TimestampNanosecondType,
};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, Rows, SortField};
use hashbrown::HashTable;

use crate::Error;
use crate::storage::RowKey;

static HASHER: LazyLock<RandomState> = LazyLock::new(|| {
    RandomState::with_seeds(
        0x243f_6a88_85a3_08d3,
        0x1319_8a2e_0370_7344,
        0xa409_3822_299f_31d0,
        0x082e_fa98_ec4e_6c89,
    )
});

/// The shard of a key hash. Uses the high bits, which hash tables do not use for bucket indices,
/// so keys within one shard still spread over that shard's table.
pub(crate) fn shard_of(hash: u64, shards: usize) -> usize {
    (((hash >> 32) * shards as u64) >> 32) as usize
}

/// Encodes key columns as comparable bytes, one key per row. Used by keyed input tables.
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

/// How a group key is read from a batch.
pub(crate) enum KeyKind {
    /// No key columns: every row has the same key.
    None,
    /// One integer, date, or timestamp column, read as `i64`.
    Int(usize),
    /// One string column.
    Str(usize),
    /// Anything else: Arrow's row encoding of the key columns.
    Rows(Grouper),
}

impl KeyKind {
    pub fn new(schema: &Schema, columns: Vec<usize>) -> Result<Self, ArrowError> {
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

    /// The row encoding of `batch`'s keys, for kinds that need one.
    pub fn encode(&self, batch: &RecordBatch) -> Result<Option<Rows>, ArrowError> {
        match self {
            KeyKind::Rows(grouper) => grouper.encode(batch).map(Some),
            _ => Ok(None),
        }
    }

    /// A view of `batch`'s keys; `encoded` is what `encode` returned for the same batch.
    pub fn keys<'a>(&self, batch: &'a RecordBatch, encoded: Option<&'a Rows>) -> Keys<'a> {
        match self {
            KeyKind::None => Keys::None,
            KeyKind::Int(column) => Keys::Int(batch.column(*column).as_ref()),
            KeyKind::Str(column) => Keys::Str(batch.column(*column).as_string::<i32>()),
            KeyKind::Rows(_) => Keys::Rows(encoded.expect("row-encoded keys are encoded first")),
        }
    }
}

/// The keys of one batch.
pub(crate) enum Keys<'a> {
    None,
    Int(&'a dyn Array),
    Str(&'a StringArray),
    Rows(&'a Rows),
}

/// A group's key, kept by the table that owns the group.
pub(crate) enum OwnedKey {
    All,
    Null,
    Int(i64),
    Str(Box<str>),
    Bytes(Box<[u8]>),
}

impl Keys<'_> {
    fn int(array: &dyn Array, row: usize) -> i64 {
        match array.data_type() {
            DataType::Int32 => i64::from(array.as_primitive::<Int32Type>().value(row)),
            DataType::Int64 => array.as_primitive::<Int64Type>().value(row),
            DataType::Date32 => i64::from(array.as_primitive::<Date32Type>().value(row)),
            _ => array.as_primitive::<TimestampNanosecondType>().value(row),
        }
    }

    /// The key of `row`, packed. See [`Packed`].
    #[inline(always)]
    pub fn pack(&self, row: usize) -> Packed {
        match self {
            Keys::None => Packed::default(),
            Keys::Int(array) if array.is_null(row) => Packed::NULL,
            Keys::Int(array) => Packed {
                lo: Self::int(*array, row) as u64,
                ..Packed::default()
            },
            Keys::Str(array) if array.is_null(row) => Packed::NULL,
            Keys::Str(array) => Packed::bytes(array.value(row).as_bytes()),
            Keys::Rows(rows) => Packed::bytes(rows.row(row).data()),
        }
    }

    /// The engine-wide hash of the key of `row`, whose packing is `packed`.
    #[inline(always)]
    pub fn hash_packed(&self, row: usize, packed: Packed) -> u64 {
        if !packed.exact() {
            return match self {
                Keys::Str(array) => HASHER.hash_one(array.value(row).as_bytes()),
                Keys::Rows(rows) => HASHER.hash_one(rows.row(row).data()),
                Keys::None | Keys::Int(_) => unreachable!("fixed-width keys pack exactly"),
            };
        }
        HASHER.hash_one((packed.lo, packed.hi, packed.tag))
    }

    pub fn hash(&self, row: usize) -> u64 {
        self.hash_packed(row, self.pack(row))
    }

    /// Whether rows `a` and `b` have the same key, given that their packings are equal.
    fn same_packed(&self, a: usize, b: usize, packed: Packed) -> bool {
        packed.exact()
            || match self {
                Keys::Str(array) => array.value(a) == array.value(b),
                Keys::Rows(rows) => rows.row(a) == rows.row(b),
                Keys::None | Keys::Int(_) => unreachable!("fixed-width keys pack exactly"),
            }
    }

    pub fn matches(&self, row: usize, key: &OwnedKey) -> bool {
        match (self, key) {
            (Keys::None, OwnedKey::All) => true,
            (Keys::Int(array), OwnedKey::Null) => array.is_null(row),
            (Keys::Str(array), OwnedKey::Null) => array.is_null(row),
            (Keys::Int(array), OwnedKey::Int(value)) => {
                array.is_valid(row) && Self::int(*array, row) == *value
            }
            (Keys::Str(array), OwnedKey::Str(text)) => {
                array.is_valid(row) && array.value(row) == text.as_ref()
            }
            (Keys::Rows(rows), OwnedKey::Bytes(bytes)) => rows.row(row).data() == bytes.as_ref(),
            _ => false,
        }
    }

    pub fn owned(&self, row: usize) -> OwnedKey {
        match self {
            Keys::None => OwnedKey::All,
            Keys::Int(array) if array.is_null(row) => OwnedKey::Null,
            Keys::Int(array) => OwnedKey::Int(Self::int(*array, row)),
            Keys::Str(array) if array.is_null(row) => OwnedKey::Null,
            Keys::Str(array) => OwnedKey::Str(array.value(row).into()),
            Keys::Rows(rows) => OwnedKey::Bytes(rows.row(row).data().into()),
        }
    }
}

/// A key packed into fixed-width words. Equal keys pack equally; keys of at most 16 bytes pack
/// distinctly, so equal packings mean equal keys without reading the keys again. Longer keys
/// keep their first and last 8 bytes, which only rules out most unequal keys.
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Packed {
    lo: u64,
    hi: u64,
    /// Byte length, or `NULL_TAG`.
    tag: u32,
}

const NULL_TAG: u32 = u32::MAX;

impl Packed {
    const NULL: Self = Self {
        lo: 0,
        hi: 0,
        tag: NULL_TAG,
    };

    /// Packs with fixed-size loads only: short ranges overlap rather than branch per byte.
    #[inline(always)]
    fn bytes(b: &[u8]) -> Self {
        let len = b.len();
        let u32_at = |at: usize| u64::from(u32::from_le_bytes(b[at..at + 4].try_into().unwrap()));
        let u64_at = |at: usize| u64::from_le_bytes(b[at..at + 8].try_into().unwrap());
        let (lo, hi) = match len {
            0 => (0, 0),
            1..=3 => (
                u64::from(b[0]) | u64::from(b[len / 2]) << 8 | u64::from(b[len - 1]) << 16,
                0,
            ),
            4..=7 => (u32_at(0) | u32_at(len - 4) << 32, 0),
            _ => (u64_at(0), u64_at(len - 8)),
        };
        Self {
            lo,
            hi,
            tag: u32::try_from(len).map_or(NULL_TAG - 1, |l| l.min(NULL_TAG - 1)),
        }
    }

    /// Whether equal packings imply equal keys.
    fn exact(self) -> bool {
        self.tag == NULL_TAG || self.tag <= 16
    }
}

/// A piece-local table entry: a group's packed key beside its id, so probes compare in place.
pub(crate) struct LocalEntry {
    packed: Packed,
    id: u32,
}

/// The rows of `batch` in each shard, as row indices plus `offset`, in row order.
pub(crate) fn split(
    kind: &KeyKind,
    batch: &RecordBatch,
    offset: u32,
    shards: usize,
) -> Result<Vec<Vec<u32>>, ArrowError> {
    let encoded = kind.encode(batch)?;
    let keys = kind.keys(batch, encoded.as_ref());
    let mut buckets: Vec<Vec<u32>> = vec![Vec::new(); shards];
    for row in 0..batch.num_rows() {
        buckets[shard_of(keys.hash(row), shards)].push(offset + row as u32);
    }
    Ok(buckets)
}

/// Groups of contiguous rows within one batch, for piece-local aggregation: a group id per row,
/// and per group its hash and the first row holding its key.
pub(crate) struct LocalGroups {
    pub ids: Vec<u32>,
    pub hash: Vec<u64>,
    pub first_row: Vec<u32>,
}

impl LocalGroups {
    /// Groups the rows, using `table` as scratch: it is cleared but keeps its capacity.
    pub fn build(keys: &Keys<'_>, rows: usize, table: &mut HashTable<LocalEntry>) -> Self {
        table.clear();
        let mut groups = Self {
            ids: Vec::with_capacity(rows),
            hash: Vec::new(),
            first_row: Vec::new(),
        };
        for row in 0..rows {
            let packed = keys.pack(row);
            let hash = keys.hash_packed(row, packed);
            let first_row = &groups.first_row;
            let found = table.find(hash, |entry| {
                entry.packed == packed
                    && keys.same_packed(first_row[entry.id as usize] as usize, row, packed)
            });
            let id = match found {
                Some(entry) => entry.id,
                None => {
                    let id = groups.hash.len() as u32;
                    groups.hash.push(hash);
                    groups.first_row.push(row as u32);
                    let hashes = &groups.hash;
                    table.insert_unique(hash, LocalEntry { packed, id }, |entry| {
                        hashes[entry.id as usize]
                    });
                    id
                }
            };
            groups.ids.push(id);
        }
        groups
    }

    pub fn len(&self) -> usize {
        self.hash.len()
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

/// Groups of an input's rows: ids, membership, output slots, and which groups a cycle touched.
/// Ids of emptied groups are reused; output slots never are.
pub(crate) struct Membership {
    kind: KeyKind,
    table: HashTable<u32>,
    hashes: Vec<u64>,
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
        Ok(Self {
            kind: KeyKind::new(schema, columns)?,
            table: HashTable::new(),
            hashes: Vec::new(),
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
        let selected = match rows {
            Some(rows) if matches!(self.kind, KeyKind::Rows(_)) => {
                Some(take_record_batch(batch, &UInt32Array::from(rows.to_vec()))?)
            }
            _ => None,
        };
        let (batch, rows) = match &selected {
            Some(taken) => (taken, None),
            None => (batch, rows),
        };
        let kind = std::mem::replace(&mut self.kind, KeyKind::None);
        let result = kind.encode(batch).map(|encoded| {
            let keys = kind.keys(batch, encoded.as_ref());
            let mut ids = Vec::with_capacity(rows.map_or(batch.num_rows(), <[u32]>::len));
            let mut each = |row: usize| {
                let id = self.group(
                    keys.hash(row),
                    |key| keys.matches(row, key),
                    || keys.owned(row),
                );
                ids.push(id);
            };
            match rows {
                None => (0..batch.num_rows()).for_each(&mut each),
                Some(rows) => rows.iter().for_each(|row| each(*row as usize)),
            }
            ids
        });
        self.kind = kind;
        result
    }

    /// The id of the group whose key `matches`, created with key `make()` if none does, marked
    /// touched. `hash` is the key's engine-wide hash.
    pub fn group(
        &mut self,
        hash: u64,
        matches: impl Fn(&OwnedKey) -> bool,
        make: impl FnOnce() -> OwnedKey,
    ) -> u32 {
        let owned = &self.owned;
        let id = match self.table.find(hash, |id| matches(&owned[*id as usize])) {
            Some(id) => *id,
            None => {
                let id = self.allocate(hash, make());
                let hashes = &self.hashes;
                self.table
                    .insert_unique(hash, id, |id| hashes[*id as usize]);
                id
            }
        };
        if id as usize >= self.touched.len() {
            self.touched.resize(self.slot.len(), false);
        }
        let seen = &mut self.touched[id as usize];
        if !*seen {
            *seen = true;
            self.touched_list.push(id);
        }
        id
    }

    fn allocate(&mut self, hash: u64, key: OwnedKey) -> u32 {
        let slot = self.next_slot;
        self.next_slot += self.slot_stride;
        let id = match self.free.pop() {
            Some(id) => {
                let i = id as usize;
                self.hashes[i] = hash;
                self.owned[i] = key;
                self.slot[i] = slot;
                self.members[i] = Members::new(self.add_only);
                id
            }
            None => {
                self.hashes.push(hash);
                self.owned.push(key);
                self.slot.push(slot);
                self.members.push(Members::new(self.add_only));
                (self.slot.len() - 1) as u32
            }
        };
        self.created.push(id);
        id
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
        if let Ok(entry) = self
            .table
            .find_entry(self.hashes[id as usize], |other| *other == id)
        {
            entry.remove();
        }
        self.owned[id as usize] = OwnedKey::Null;
        self.free.push(id);
    }
}
