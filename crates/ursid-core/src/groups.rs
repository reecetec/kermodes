use std::collections::HashMap;

use ahash::RandomState;
use arrow::array::ArrayRef;
use arrow::datatypes::Schema;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, Rows, SortField};

use crate::Error;

/// Encodes the values of some columns as comparable bytes, one key per row: equal values give
/// equal bytes, so the bytes serve as hash-map keys for groups and keyed rows.
pub(crate) struct Grouper {
    columns: Vec<usize>,
    /// `None` when there are no columns: every row is in the one group.
    converter: Option<RowConverter>,
}

/// One batch's encoded keys.
pub(crate) enum Encoded {
    Rows(Rows),
    /// No key columns: `n` rows, all with the empty key.
    Empty(usize),
}

impl Encoded {
    pub fn len(&self) -> usize {
        match self {
            Encoded::Rows(rows) => rows.num_rows(),
            Encoded::Empty(n) => *n,
        }
    }

    pub fn get(&self, row: usize) -> &[u8] {
        match self {
            Encoded::Rows(rows) => rows.row(row).data(),
            Encoded::Empty(_) => &[],
        }
    }
}

impl Grouper {
    pub fn new(schema: &Schema, names: &[&str]) -> Result<Self, Error> {
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
        Self::by_position(schema, columns).map_err(|e| Error::Schema(e.to_string()))
    }

    pub fn by_position(schema: &Schema, columns: Vec<usize>) -> Result<Self, ArrowError> {
        let converter = if columns.is_empty() {
            None
        } else {
            let fields = columns
                .iter()
                .map(|i| SortField::new(schema.field(*i).data_type().clone()))
                .collect();
            Some(RowConverter::new(fields)?)
        };
        Ok(Self { columns, converter })
    }

    pub fn columns(&self) -> &[usize] {
        &self.columns
    }

    pub fn encode(&self, batch: &RecordBatch) -> Result<Encoded, ArrowError> {
        let Some(converter) = &self.converter else {
            return Ok(Encoded::Empty(batch.num_rows()));
        };
        let columns: Vec<ArrayRef> = self
            .columns
            .iter()
            .map(|i| batch.column(*i).clone())
            .collect();
        Ok(Encoded::Rows(converter.convert_columns(&columns)?))
    }

    /// The column values behind `keys`, one array per column.
    pub fn decode(&self, keys: &[&[u8]]) -> Result<Vec<ArrayRef>, ArrowError> {
        let Some(converter) = &self.converter else {
            return Ok(Vec::new());
        };
        let parser = converter.parser();
        converter.convert_rows(keys.iter().map(|key| parser.parse(key)))
    }
}

/// Groups by encoded key. Each row costs one hash lookup; groups are then addressed by index.
/// Emptied groups free their index for reuse.
pub(crate) struct Arena<G> {
    index: HashMap<Box<[u8]>, usize, RandomState>,
    entries: Vec<Option<Entry<G>>>,
    free: Vec<usize>,
    touched: Vec<usize>,
}

pub(crate) struct Entry<G> {
    pub key: Box<[u8]>,
    touched: bool,
    pub group: G,
}

impl<G> Default for Arena<G> {
    fn default() -> Self {
        Self {
            index: HashMap::default(),
            entries: Vec::new(),
            free: Vec::new(),
            touched: Vec::new(),
        }
    }
}

impl<G> Arena<G> {
    /// The group for `key`, created with `make` if it does not exist, marked touched.
    pub fn touch(&mut self, key: &[u8], make: impl FnOnce() -> G) -> usize {
        let index = match self.index.get(key) {
            Some(index) => *index,
            None => {
                let entry = Entry {
                    key: key.into(),
                    touched: false,
                    group: make(),
                };
                let index = match self.free.pop() {
                    Some(index) => {
                        self.entries[index] = Some(entry);
                        index
                    }
                    None => {
                        self.entries.push(Some(entry));
                        self.entries.len() - 1
                    }
                };
                self.index.insert(key.into(), index);
                index
            }
        };
        let entry = self.entry(index);
        let first = !entry.touched;
        entry.touched = true;
        if first {
            self.touched.push(index);
        }
        index
    }

    pub fn entry(&mut self, index: usize) -> &mut Entry<G> {
        self.entries[index]
            .as_mut()
            .expect("group indices stay live until removed")
    }

    pub fn get(&mut self, index: usize) -> &mut G {
        &mut self.entry(index).group
    }

    pub fn entry_ref(&self, index: usize) -> &Entry<G> {
        self.entries[index]
            .as_ref()
            .expect("group indices stay live until removed")
    }

    /// Groups touched since the last call, in first-touch order.
    pub fn take_touched(&mut self) -> Vec<usize> {
        let touched = std::mem::take(&mut self.touched);
        for index in &touched {
            if let Some(entry) = self.entries[*index].as_mut() {
                entry.touched = false;
            }
        }
        touched
    }

    pub fn remove(&mut self, index: usize) {
        if let Some(entry) = self.entries[index].take() {
            self.index.remove(&entry.key);
            self.free.push(index);
        }
    }
}
