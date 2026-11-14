//! The historical store (architecture.md §8): Parquet files of whole dates in object storage,
//! the catalog the only record of which are current.

#![forbid(unsafe_code)]

mod files;
mod merge;
mod plan;

pub use files::{Batches, DATE, Lake, TARGET_FILE_BYTES, Written, with_date_column};
pub use merge::{GARBAGE_GRACE, Merged, collect_garbage, merge};
pub use plan::{Held, Incoming, Plan, plan};
