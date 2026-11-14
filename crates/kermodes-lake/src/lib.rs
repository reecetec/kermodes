//! The historical store (architecture.md §8): Parquet files of whole dates in object storage,
//! the catalog the only record of which are current.

#![forbid(unsafe_code)]

mod plan;

pub use plan::{Held, Incoming, Plan, plan};
