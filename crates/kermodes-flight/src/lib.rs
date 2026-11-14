//! A store served over Arrow Flight (architecture.md §6.1, §7.2), so many processes share one
//! weir: `Server` wraps a `kermodes_weir::Store`; `Client` gives a process the same operations.
//!
//! Protocol (additions since 0.1.1 are optional fields, so either side may be older):
//! - `DoPut` writes: the first message's descriptor is a command `{"table", "source", "wait"}`.
//!   The server answers once it holds the source's lease (§7.4): at once, or, with `"wait":
//!   true`, when the current writer goes; without, a taken source fails with `Busy`. Each record
//!   batch's `app_metadata` is its sequence number (u64, little-endian). Each batch is
//!   acknowledged once durable (or dropped as a resend) with a `PutResult` whose `app_metadata` is
//!   the source's last sequence number (u64) and whether the batch was written (one byte). A
//!   batch's message is at most `MAX_WRITE_BYTES`: the client refuses a larger one as `TooLarge`.
//! - `DoGet` reads: the ticket `{"table"}` streams every committed batch, then each new one;
//!   `{"table", "date"}` streams a past date's batches and ends (a merge's read, §8.3).
//! - Actions, JSON in and out: `create_table` `{"table", "columns": [[name, type]]}`; `tables`;
//!   `last_seq` `{"table", "source"}` → `{"last_seq", "epoch"}`. A writer asks for it once its
//!   `DoPut` holds the lease, so nothing can write between the answer and its first batch.
//! - History (§8), dates as `YYYY-MM-DD`: `date_rows` `{"table", "date"}` → `{"rows", "bytes"}`;
//!   `past_dates` `{"table"}` → dates; `history` `{"table"}` → dates in files;
//!   `history_files` `{"table", "first", "last"}` → files; `commit_history`
//!   `{"table", "written", "replaced", "files", "dates"}`; `drop_date` `{"table", "date"}`;
//!   `history_garbage` `{"grace_secs"}` → garbage; `forget_garbage` garbage. Each mirrors the
//!   `Store` method of its name.

#![forbid(unsafe_code)]

mod client;
mod server;

use std::sync::Arc;

use arrow::datatypes::{Field, Schema, SchemaRef};
use kermodes_core::{parse_type, type_name};
use kermodes_weir::{Date, Error, Garbage, HistoryCommit, HistoryDate, HistoryFile};
use serde_json::{Value, json};
use tonic::{Code, Status};

pub use client::{Client, RemoteWriter};
pub use server::{Server, serve};

/// The most one write's batch may take over Flight, encoded. A write is one atomic batch (one
/// sequence number), so it is never split; at weir's segment size, one fits under a live
/// reader's queue (128 MiB), and the server holds at most this per writer while decoding.
pub const MAX_WRITE_BYTES: usize = 64 << 20;

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

fn date_from_json(value: &Value) -> Result<Date, Error> {
    Date::parse(value.as_str().unwrap_or_default())
}

fn u64_from_json(value: &Value, field: &str) -> Result<u64, Error> {
    value[field]
        .as_u64()
        .ok_or_else(|| Error::Schema(format!("`{field}` must be a count, not {}", value[field])))
}

fn str_from_json<'a>(value: &'a Value, field: &str) -> Result<&'a str, Error> {
    value[field]
        .as_str()
        .ok_or_else(|| Error::Schema(format!("`{field}` must be text, not {}", value[field])))
}

fn list_from_json<T>(
    value: &Value,
    each: impl Fn(&Value) -> Result<T, Error>,
) -> Result<Vec<T>, Error> {
    value
        .as_array()
        .ok_or_else(|| Error::Schema(format!("expected a list, not {value}")))?
        .iter()
        .map(each)
        .collect()
}

/// A table's key columns; absent (an older peer, or an append table) is none.
fn key_from_json(value: &Value) -> Result<Vec<String>, Error> {
    if value.is_null() {
        return Ok(Vec::new());
    }
    list_from_json(value, |column| {
        column
            .as_str()
            .map(str::to_owned)
            .ok_or_else(|| Error::Schema(format!("a key column is named, not {column}")))
    })
}

fn history_date_json(held: &HistoryDate) -> Value {
    json!({"date": held.date.to_string(), "file": held.file, "rows": held.rows, "bytes": held.bytes})
}

fn history_date_from_json(value: &Value) -> Result<HistoryDate, Error> {
    Ok(HistoryDate {
        date: date_from_json(&value["date"])?,
        file: str_from_json(value, "file")?.to_owned(),
        rows: u64_from_json(value, "rows")?,
        bytes: u64_from_json(value, "bytes")?,
    })
}

fn history_file_json(file: &HistoryFile) -> Value {
    json!({
        "name": file.name,
        "first": file.first.to_string(),
        "last": file.last.to_string(),
        "rows": file.rows,
        "bytes": file.bytes,
    })
}

fn history_file_from_json(value: &Value) -> Result<HistoryFile, Error> {
    Ok(HistoryFile {
        name: str_from_json(value, "name")?.to_owned(),
        first: date_from_json(&value["first"])?,
        last: date_from_json(&value["last"])?,
        rows: u64_from_json(value, "rows")?,
        bytes: u64_from_json(value, "bytes")?,
    })
}

fn commit_json(commit: &HistoryCommit) -> Value {
    json!({
        "written": commit.written.iter().map(Date::to_string).collect::<Vec<_>>(),
        "replaced": commit.replaced,
        "files": commit.files.iter().map(history_file_json).collect::<Vec<_>>(),
        "dates": commit.dates.iter().map(history_date_json).collect::<Vec<_>>(),
    })
}

fn commit_from_json(value: &Value) -> Result<HistoryCommit, Error> {
    Ok(HistoryCommit {
        written: list_from_json(&value["written"], date_from_json)?,
        replaced: list_from_json(&value["replaced"], |file| {
            file.as_str()
                .map(str::to_owned)
                .ok_or_else(|| Error::Schema(format!("a replaced file is named, not {file}")))
        })?,
        files: list_from_json(&value["files"], history_file_from_json)?,
        dates: list_from_json(&value["dates"], history_date_from_json)?,
    })
}

fn garbage_json(garbage: &Garbage) -> Value {
    json!({"table_id": garbage.table_id, "location": garbage.location, "file": garbage.file})
}

fn garbage_from_json(value: &Value) -> Result<Garbage, Error> {
    Ok(Garbage {
        table_id: value["table_id"]
            .as_i64()
            .ok_or_else(|| Error::Schema(format!("garbage needs its table_id, not {value}")))?,
        location: str_from_json(value, "location")?.to_owned(),
        file: str_from_json(value, "file")?.to_owned(),
    })
}

/// A store error as a gRPC status; `error_from_status` reverses it.
fn status(error: Error) -> Status {
    let code = match &error {
        Error::Name(_) | Error::Schema(_) => Code::InvalidArgument,
        Error::NotFound(_) => Code::NotFound,
        Error::Busy(_) => Code::AlreadyExists,
        Error::Corrupt(_) => Code::DataLoss,
        Error::TooLarge(_) => Code::OutOfRange,
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
        // Also how tonic refuses a message over the server's limit.
        Code::OutOfRange => Error::TooLarge(message),
        _ => Error::Io(format!("kermodes server: {message}")),
    }
}
