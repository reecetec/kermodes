//! Columns of mutable slots: each live row of a table owns a slot, a new version of the row
//! overwrites it, and a removed row's slot is reused. Memory follows the live rows.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Float64Array, PrimitiveArray, RecordBatch, StringArray,
};
use arrow::buffer::{BooleanBuffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{
    ArrowNativeType, DataType, Date32Type, Float64Type, Int32Type, Int64Type, Schema,
    TimestampNanosecondType,
};
use arrow::error::ArrowError;

/// A growable bitmap.
#[derive(Default)]
struct Bits(Vec<u64>);

impl Bits {
    fn resize(&mut self, len: usize) {
        self.0.resize(len.div_ceil(64), 0);
    }

    fn get(&self, at: usize) -> bool {
        self.0[at / 64] >> (at % 64) & 1 == 1
    }

    fn set(&mut self, at: usize, on: bool) {
        let (word, bit) = (at / 64, 1u64 << (at % 64));
        if on {
            self.0[word] |= bit;
        } else {
            self.0[word] &= !bit;
        }
    }
}

/// Up to this many bytes of a string live in its slot's view.
const INLINE: usize = 12;

/// A string slot, in Arrow's string-view layout: the length, then the bytes when they fit, or
/// else the offset of the bytes in the column's heap.
#[derive(Clone, Copy, Default)]
struct View {
    len: u32,
    bytes: [u8; INLINE],
}

impl View {
    fn offset(&self) -> usize {
        u64::from_le_bytes(self.bytes[..8].try_into().expect("8 bytes")) as usize
    }

    fn set_offset(&mut self, offset: usize) {
        self.bytes[..8].copy_from_slice(&(offset as u64).to_le_bytes());
    }
}

/// Strings by slot. Short ones live inline; longer ones in a shared heap whose superseded bytes
/// are reclaimed once they outweigh the live ones.
#[derive(Default)]
struct Strings {
    views: Vec<View>,
    heap: Vec<u8>,
    live: usize,
    dead: usize,
}

impl Strings {
    /// Below this many superseded bytes, the heap is not worth rewriting.
    const SLACK: usize = 1 << 20;

    fn get(&self, slot: usize) -> &[u8] {
        let view = &self.views[slot];
        let len = view.len as usize;
        if len <= INLINE {
            &view.bytes[..len]
        } else {
            &self.heap[view.offset()..view.offset() + len]
        }
    }

    fn forget(&mut self, slot: usize) {
        let len = self.views[slot].len as usize;
        if len > INLINE {
            self.live -= len;
            self.dead += len;
        }
        self.views[slot] = View::default();
    }

    fn set(&mut self, slot: usize, value: &[u8]) {
        self.forget(slot);
        // Arrow string offsets are i32, so a value's length fits u32.
        let mut view = View {
            len: value.len() as u32,
            bytes: [0; INLINE],
        };
        if value.len() <= INLINE {
            view.bytes[..value.len()].copy_from_slice(value);
        } else {
            view.set_offset(self.heap.len());
            self.heap.extend_from_slice(value);
            self.live += value.len();
        }
        self.views[slot] = view;
    }

    /// Rewrites the heap with only live bytes once superseded ones outweigh them: the copy is
    /// paid for by the writes that superseded at least as many bytes.
    fn compact(&mut self) {
        if self.dead <= self.live.max(Self::SLACK) {
            return;
        }
        let mut heap = Vec::with_capacity(self.live);
        for view in &mut self.views {
            let len = view.len as usize;
            if len > INLINE {
                let at = view.offset();
                view.set_offset(heap.len());
                heap.extend_from_slice(&self.heap[at..at + len]);
            }
        }
        self.heap = heap;
        self.dead = 0;
    }

    fn read(&self, slots: &[u32], nulls: Option<NullBuffer>) -> Result<StringArray, ArrowError> {
        let mut offsets = Vec::with_capacity(slots.len() + 1);
        let mut data = Vec::new();
        offsets.push(0i32);
        for slot in slots {
            data.extend_from_slice(self.get(*slot as usize));
            let end = i32::try_from(data.len())
                .map_err(|_| ArrowError::ComputeError("strings over 2 GiB in one batch".into()))?;
            offsets.push(end);
        }
        StringArray::try_new(
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            data.into(),
            nulls,
        )
    }
}

enum Values {
    Bool(Bits),
    /// `int32` and `date32`.
    I32(Vec<i32>),
    /// `int64` and `timestamp`.
    I64(Vec<i64>),
    F64(Vec<f64>),
    Utf8(Strings),
}

struct Column {
    data_type: DataType,
    valid: Bits,
    values: Values,
}

fn i32_values(array: &dyn Array) -> &[i32] {
    match array.data_type() {
        DataType::Date32 => array.as_primitive::<Date32Type>().values(),
        _ => array.as_primitive::<Int32Type>().values(),
    }
}

fn i64_values(array: &dyn Array) -> &[i64] {
    match array.data_type() {
        DataType::Timestamp(_, _) => array.as_primitive::<TimestampNanosecondType>().values(),
        _ => array.as_primitive::<Int64Type>().values(),
    }
}

fn scatter<T: Copy>(values: &mut [T], from: &[T], slots: &[u32]) {
    for (value, slot) in from.iter().zip(slots) {
        values[*slot as usize] = *value;
    }
}

fn gather<T: ArrowNativeType>(values: &[T], slots: &[u32]) -> ScalarBuffer<T> {
    ScalarBuffer::from(
        slots
            .iter()
            .map(|slot| values[*slot as usize])
            .collect::<Vec<_>>(),
    )
}

impl Column {
    fn new(data_type: &DataType) -> Self {
        let values = match data_type {
            DataType::Boolean => Values::Bool(Bits::default()),
            DataType::Int32 | DataType::Date32 => Values::I32(Vec::new()),
            DataType::Int64 | DataType::Timestamp(_, _) => Values::I64(Vec::new()),
            DataType::Float64 => Values::F64(Vec::new()),
            DataType::Utf8 => Values::Utf8(Strings::default()),
            other => unreachable!("column types are checked when tables are created: {other}"),
        };
        Self {
            data_type: data_type.clone(),
            valid: Bits::default(),
            values,
        }
    }

    fn resize(&mut self, len: usize) {
        self.valid.resize(len);
        match &mut self.values {
            Values::Bool(bits) => bits.resize(len),
            Values::I32(values) => values.resize(len, 0),
            Values::I64(values) => values.resize(len, 0),
            Values::F64(values) => values.resize(len, 0.0),
            Values::Utf8(strings) => strings.views.resize(len, View::default()),
        }
    }

    fn write(&mut self, array: &dyn Array, slots: &[u32]) {
        for (row, slot) in slots.iter().enumerate() {
            self.valid.set(*slot as usize, array.is_valid(row));
        }
        match &mut self.values {
            Values::Bool(bits) => {
                let array = array.as_boolean();
                for (row, slot) in slots.iter().enumerate() {
                    bits.set(*slot as usize, array.value(row));
                }
            }
            Values::I32(values) => scatter(values, i32_values(array), slots),
            Values::I64(values) => scatter(values, i64_values(array), slots),
            Values::F64(values) => {
                scatter(values, array.as_primitive::<Float64Type>().values(), slots);
            }
            Values::Utf8(strings) => {
                let array = array.as_string::<i32>();
                for (row, slot) in slots.iter().enumerate() {
                    let value = if array.is_valid(row) {
                        array.value(row).as_bytes()
                    } else {
                        &[]
                    };
                    strings.set(*slot as usize, value);
                }
            }
        }
    }

    fn read(&self, slots: &[u32]) -> Result<ArrayRef, ArrowError> {
        let valid = BooleanBuffer::from_iter(slots.iter().map(|s| self.valid.get(*s as usize)));
        let nulls = Some(NullBuffer::new(valid)).filter(|n| n.null_count() > 0);
        Ok(match (&self.values, &self.data_type) {
            (Values::Bool(bits), _) => Arc::new(BooleanArray::new(
                BooleanBuffer::from_iter(slots.iter().map(|s| bits.get(*s as usize))),
                nulls,
            )),
            (Values::I32(values), DataType::Date32) => Arc::new(PrimitiveArray::<Date32Type>::new(
                gather(values, slots),
                nulls,
            )),
            (Values::I32(values), _) => Arc::new(PrimitiveArray::<Int32Type>::new(
                gather(values, slots),
                nulls,
            )),
            (Values::I64(values), DataType::Timestamp(_, _)) => Arc::new(
                PrimitiveArray::<TimestampNanosecondType>::new(gather(values, slots), nulls)
                    .with_data_type(self.data_type.clone()),
            ),
            (Values::I64(values), _) => Arc::new(PrimitiveArray::<Int64Type>::new(
                gather(values, slots),
                nulls,
            )),
            (Values::F64(values), _) => Arc::new(Float64Array::new(gather(values, slots), nulls)),
            (Values::Utf8(strings), _) => Arc::new(strings.read(slots, nulls)?),
        })
    }
}

/// A table's columns, one value per slot.
pub(crate) struct Slots {
    columns: Vec<Column>,
    len: usize,
}

impl Slots {
    pub fn new(schema: &Schema) -> Self {
        Self {
            columns: schema
                .fields()
                .iter()
                .map(|f| Column::new(f.data_type()))
                .collect(),
            len: 0,
        }
    }

    /// Number of slots, live or free.
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn resize(&mut self, len: usize) {
        for column in &mut self.columns {
            column.resize(len);
        }
        self.len = len;
    }

    /// Writes `batch`'s rows into `slots`, row `i` into `slots[i]`.
    pub fn write(&mut self, batch: &RecordBatch, slots: &[u32]) {
        for (column, array) in self.columns.iter_mut().zip(batch.columns()) {
            column.write(array.as_ref(), slots);
        }
    }

    /// The values in `slots`, one array per column.
    pub fn read(&self, slots: &[u32]) -> Result<Vec<ArrayRef>, ArrowError> {
        self.columns.iter().map(|c| c.read(slots)).collect()
    }

    /// Empties a slot whose row was removed.
    pub fn clear(&mut self, slot: u32) {
        for column in &mut self.columns {
            column.valid.set(slot as usize, false);
            if let Values::Utf8(strings) = &mut column.values {
                strings.forget(slot as usize);
            }
        }
    }

    /// Reclaims superseded string bytes where they outweigh live ones.
    pub fn compact(&mut self) {
        for column in &mut self.columns {
            if let Values::Utf8(strings) = &mut column.values {
                strings.compact();
            }
        }
    }
}
