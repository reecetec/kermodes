//! weir (architecture.md §7): the ingest log. Each table's rows are kept per internal partition
//! `(date, source)` as Arrow IPC segments, durably and in order; readers get every committed
//! batch, then each new one. This crate is the storage, embedded in a process by
//! `km.local(path)` through `Store`; the Flight server that shares it between processes wraps the
//! same store.

#![forbid(unsafe_code)]

mod catalog;
mod names;
mod partition;
mod store;

use std::fmt;
use std::path::Path;

pub use catalog::TableEntry;
pub use names::{SourceId, TableName};
pub use store::{BRANCH, Store, Subscriber, Writer, conform};

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
            | Error::Catalog(m) => f.write_str(m),
        }
    }
}

impl std::error::Error for Error {}
