//! The Ursid engine: tables that tick, built from expressions over Arrow data.

#![forbid(unsafe_code)]

mod agg;
mod error;
mod expr;
mod groups;
mod join;
mod ops;
mod parallel;
mod session;
mod slots;
mod storage;
mod types;

pub use error::Error;
pub use expr::Expression;
pub use session::{Session, Table};
pub use types::{parse_type, type_name};
