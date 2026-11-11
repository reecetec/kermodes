//! The Ursid engine: tables that tick, built from expressions over Arrow data.

#![forbid(unsafe_code)]

mod agg;
mod error;
mod expr;
mod groups;
mod join;
mod ops;
mod order;
mod parallel;
mod rolling;
mod session;
mod slots;
mod sort;
mod storage;
mod types;

pub use error::Error;
pub use expr::Expression;
pub use rolling::Window;
pub use session::{RowsAt, Session, Table, set_idle_hook};
pub use types::{parse_type, type_name};
