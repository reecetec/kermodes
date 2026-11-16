//! The lake (architecture.md §8): Parquet files of whole dates in object storage,
//! the catalog the only record of which are current.

#![forbid(unsafe_code)]

mod bounds;
mod files;
mod merge;
mod plan;
mod read;
mod weir;

pub use bounds::{Bound, Literal, column_bounds, date_bounds};
pub use files::{
    Batches, DATE, Lake, OP, TARGET_FILE_BYTES, Written, dated, history_columns, with_date_column,
    with_op,
};
pub use merge::{GARBAGE_GRACE, Merged, collect_garbage, merge};
pub use plan::{Held, Incoming, Plan, plan};
pub use read::{Explain, History, explain_history, read_history};
pub use weir::Weir;
