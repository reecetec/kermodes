//! A store served over Arrow Flight (architecture.md §6.1, §7.2), so many processes share one
//! weir: `Server` wraps a `kermodes_weir::Store`; `Client` gives a process the same operations.
//!
//! Protocol:
//! - `DoPut` writes: the first message's descriptor is a command `{"table", "source"}`; each
//!   record batch's `app_metadata` is its sequence number (u64, little-endian). Each batch is
//!   acknowledged once durable (or dropped as a resend) with a `PutResult` whose `app_metadata` is
//!   the source's last sequence number (u64) and whether the batch was written (one byte).
//! - `DoGet` reads: the ticket `{"table"}` streams every committed batch, then each new one.
//! - Actions, JSON in and out: `create_table` `{"table", "columns": [[name, type]]}`; `tables`;
//!   `last_seq` `{"table", "source"}`.

#![forbid(unsafe_code)]

mod client;
mod server;

use std::sync::Arc;

use arrow::datatypes::{Field, Schema, SchemaRef};
use kermodes_core::{parse_type, type_name};
use kermodes_weir::Error;
use serde_json::{Value, json};
use tonic::{Code, Status};

pub use client::{Client, RemoteWriter};
pub use server::{Server, serve};

fn columns_json(schema: &Schema) -> Value {
    Value::Array(
        schema
            .fields()
            .iter()
            .map(|f| json!([f.name(), type_name(f.data_type())]))
            .collect(),
    )
}

fn schema_from_json(value: &Value) -> Result<SchemaRef, Error> {
    let bad = || Error::Schema(format!("columns must be [[name, type], ...], not {value}"));
    let fields = value
        .as_array()
        .ok_or_else(bad)?
        .iter()
        .map(|column| {
            let name = column[0].as_str().ok_or_else(bad)?;
            let kind = column[1].as_str().ok_or_else(bad)?;
            let data_type = parse_type(kind).map_err(|e| Error::Schema(e.to_string()))?;
            Ok(Field::new(name, data_type, true))
        })
        .collect::<Result<Vec<_>, Error>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

/// A store error as a gRPC status; `error_from_status` reverses it.
fn status(error: Error) -> Status {
    let code = match &error {
        Error::Name(_) | Error::Schema(_) => Code::InvalidArgument,
        Error::NotFound(_) => Code::NotFound,
        Error::Busy(_) => Code::AlreadyExists,
        Error::Corrupt(_) => Code::DataLoss,
        Error::Io(_) | Error::Arrow(_) | Error::Catalog(_) => Code::Internal,
    };
    Status::new(code, error.to_string())
}

fn error_from_status(status: &Status) -> Error {
    let message = status.message().to_owned();
    match status.code() {
        Code::InvalidArgument => Error::Schema(message),
        Code::NotFound => Error::NotFound(message),
        Code::AlreadyExists => Error::Busy(message),
        Code::DataLoss => Error::Corrupt(message),
        _ => Error::Io(format!("kermodes server: {message}")),
    }
}
