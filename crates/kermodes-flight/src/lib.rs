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
//!   `{"table", "date"}` streams a past date's batches and ends (a merge's read, §8.3). A
//!   subscription may ask `"lsn": true` for each batch's log position in its stamp, and
//!   `"after": n` to resume after a position it had (§7.6): the batches after it, then each new
//!   one. If they may no longer all be held, the stream fails `Aborted` (`Gone`) and the reader
//!   must start again.
//! - Actions, JSON in and out: `create_table` `{"table", "columns": [[name, type]]}`; `tables`;
//!   `last_seq` `{"table", "source"}` → `{"last_seq", "epoch"}`. A writer asks for it once its
//!   `DoPut` holds the lease, so nothing can write between the answer and its first batch.
//!   `stats` → what each table takes in weir and in history, and weir's volume
//!   (`Store::stats_json`), for the web UI's catalog.
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

use arrow::datatypes::{Schema, SchemaRef};
use kermodes_weir::{
    Change, Date, Error, Garbage, HistoryCommit, HistoryDate, HistoryFile, Stamp, column, spec,
};
use serde_json::{Value, json};
use tonic::{Code, Status};

pub use client::{Client, RemoteWriter};
pub use server::{Server, serve};

/// The most one write's batch may take over Flight, encoded. A write is one atomic batch (one
/// sequence number), so it is never split; at weir's segment size, one fits under a live
/// reader's queue (128 MiB), and the server holds at most this per writer while decoding.
pub const MAX_WRITE_BYTES: usize = 64 << 20;

/// `[[name, spec], ...]`: each column's type, and in a declaration the names it was
/// (`kermodes_weir::spec`). A server older than column IDs refuses a spec with `was` as a
/// type it does not know.
fn columns_json(schema: &Schema) -> Value {
    Value::Array(
        schema
            .fields()
            .iter()
            .map(|f| json!([f.name(), spec(f)]))
            .collect(),
    )
}

fn schema_from_json(value: &Value) -> Result<SchemaRef, Error> {
    let bad = || Error::Schema(format!("columns must be [[name, type], ...], not {value}"));
    let fields = value
        .as_array()
        .ok_or_else(bad)?
        .iter()
        .map(|c| {
            column(
                c[0].as_str().ok_or_else(bad)?,
                c[1].as_str().ok_or_else(bad)?,
            )
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

/// A table's key or sort columns; absent (an older peer, or a table without) is none.
fn names_from_json(value: &Value) -> Result<Vec<String>, Error> {
    if value.is_null() {
        return Ok(Vec::new());
    }
    list_from_json(value, |column| {
        column
            .as_str()
            .map(str::to_owned)
            .ok_or_else(|| Error::Schema(format!("a column is named, not {column}")))
    })
}

/// A subscription that asks (`"caught_up": true` in its ticket) gets this on its schema
/// message: the server marks the end of its replay (`CAUGHT_UP`). A client that does not see it
/// (a server before the marker) treats the table as caught up at once.
const MARKS_CAUGHT_UP: &[u8] = b"marks-caught-up";

/// The `app_metadata` of the empty batch that ends a subscription's replay of what the table
/// held when it subscribed; what follows is live. Twelve bytes, never a stamp's nine.
const CAUGHT_UP: &[u8] = b"caught-up-v1";

/// A read batch's `app_metadata`: its change kind (one byte, `Change::code`), then when it
/// arrived (ns, i64 little-endian), then, for a reader that asked (`"lsn": true` in its ticket),
/// its place in the shard log (u64 little-endian), which it may resume after.
fn stamp_metadata(stamp: Stamp, lsn: bool) -> Vec<u8> {
    let mut metadata = vec![stamp.change.code()];
    metadata.extend_from_slice(&stamp.ingest_ns.to_le_bytes());
    if lsn {
        metadata.extend_from_slice(&stamp.lsn.to_le_bytes());
    }
    metadata
}

/// A read batch's stamp (`stamp_metadata`). A server before keyed tables sends none (rows), one
/// before ingest times only the change kind: the batch is then dated by when it was received.
/// One before the log sends no position: 0, which nothing resumes after.
fn stamp_of(metadata: &[u8], received_ns: impl FnOnce() -> i64) -> Result<Stamp, Error> {
    let change = |code: u8| {
        Change::from_code(u32::from(code))
            .ok_or_else(|| Error::Io(format!("a change kind ({code}) this client cannot read")))
    };
    let word = |bytes: &[u8]| -> Result<[u8; 8], Error> {
        bytes
            .try_into()
            .map_err(|_| Error::Io("batch metadata this client cannot read".into()))
    };
    match metadata {
        [] => Ok(Stamp {
            change: Change::Rows,
            ingest_ns: received_ns(),
            lsn: 0,
        }),
        [code] => Ok(Stamp {
            change: change(*code)?,
            ingest_ns: received_ns(),
            lsn: 0,
        }),
        [code, rest @ ..] if rest.len() == 16 => Ok(Stamp {
            change: change(*code)?,
            ingest_ns: i64::from_le_bytes(word(&rest[..8])?),
            lsn: u64::from_le_bytes(word(&rest[8..])?),
        }),
        [code, ingest @ ..] => Ok(Stamp {
            change: change(*code)?,
            ingest_ns: i64::from_le_bytes(word(ingest)?),
            lsn: 0,
        }),
    }
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
        Error::Gone(_) => Code::Aborted,
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
        Code::Aborted => Error::Gone(message),
        _ => Error::Io(format!("kermodes server: {message}")),
    }
}
