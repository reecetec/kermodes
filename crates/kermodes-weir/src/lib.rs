//! weir (architecture.md §7): the ingest log. Each table's rows are kept per internal partition
//! `(date, source)` as Arrow IPC segments, durably and in order; readers get every committed
//! batch, then each new one. This crate is the storage, embedded in a process by
//! `km.local(path)` through `Store`; the Flight server that shares it between processes wraps the
//! same store.

#![forbid(unsafe_code)]

mod catalog;
mod keyed;
mod names;
mod partition;
mod shape;
mod store;
mod wal;

use std::fmt;
use std::path::Path;

pub use catalog::{Garbage, HistoryCommit, HistoryDate, HistoryFile, TableEntry};
pub use names::{Date, SourceId, TableName};
pub use partition::Stamp;
pub use shape::{ColumnId, Shape, column, spec, widens};
pub use store::{BRANCH, Failed, Pace, Store, Subscriber, Writer, conform, keys_only};

/// What a batch is (§7.1): an append table's rows, or a keyed table's upserts, deletes, or the
/// checkpoint of its state a day begins with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Change {
    Rows,
    Upsert,
    Delete,
    Checkpoint,
}

impl Change {
    /// Its code in weir's index records and in Flight messages.
    pub fn code(self) -> u8 {
        match self {
            Change::Rows => 0,
            Change::Upsert => 1,
            Change::Delete => 2,
            Change::Checkpoint => 3,
        }
    }

    /// `None` for a code this version does not know.
    pub fn from_code(code: u32) -> Option<Self> {
        match code {
            0 => Some(Change::Rows),
            1 => Some(Change::Upsert),
            2 => Some(Change::Delete),
            3 => Some(Change::Checkpoint),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub enum Error {
    /// A table or source name that is not one.
    Name(String),
    /// Columns that do not match the table's, or a definition that conflicts with it.
    Schema(String),
    NotFound(String),
    /// Held by someone else: the store by another process, a source by another writer.
    Busy(String),
    /// Files or catalog contents this version cannot read.
    Corrupt(String),
    Io(String),
    Arrow(String),
    Catalog(String),
    /// A write over the limit for one batch: split it, it cannot succeed as it is.
    TooLarge(String),
    /// A subscription asked to resume after rows no longer all held: it must start again.
    Gone(String),
}

impl Error {
    fn io(path: &Path, error: std::io::Error) -> Self {
        Self::Io(format!("{}: {error}", path.display()))
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Name(m)
            | Error::Schema(m)
            | Error::NotFound(m)
            | Error::Busy(m)
            | Error::Corrupt(m)
            | Error::Io(m)
            | Error::Arrow(m)
            | Error::Catalog(m)
            | Error::TooLarge(m)
            | Error::Gone(m) => f.write_str(m),
        }
    }
}

impl std::error::Error for Error {}
