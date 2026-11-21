//! The Kermodes engine: tables that tick, built from expressions over Arrow data.

#![forbid(unsafe_code)]

mod agg;
mod error;
mod exact;
pub mod exec;
mod expr;
mod groups;
mod join;
mod ops;
mod order;
mod parallel;
pub mod plan;
pub mod replay;
mod rolling;
pub mod series;
mod session;
mod slots;
mod sort;
pub mod sql;
mod storage;
mod types;
pub mod viewer;

pub use error::Error;
pub use expr::Expression;
pub use rolling::Window;
pub use session::{
    Changes, Feed, Listener, RowsAt, Session, SortKey, Table, WeakTable, set_idle_hook,
};
pub use types::{parse_type, type_name};
