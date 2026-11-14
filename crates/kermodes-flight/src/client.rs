//! A process's handle on a served store: the store's operations, over Flight.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow_flight::decode::{DecodedPayload, FlightDataDecoder};
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::utils::batches_to_flight_data;
use arrow_flight::{Action, FlightData, FlightDescriptor, PutResult, Ticket};
use futures::{StreamExt, TryStreamExt};
use kermodes_weir::{
    Change, Date, Error, Garbage, HistoryCommit, HistoryDate, HistoryFile, Pace, SourceId,
    Subscriber, TableEntry, TableName, conform, keys_only,
};
use serde_json::{Value, json};
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tonic::Streaming;
use tonic::transport::Channel;

use crate::{
    MAX_WRITE_BYTES, columns_json, commit_json, date_from_json, error_from_status,
    garbage_from_json, garbage_json, history_date_from_json, history_file_from_json,
    list_from_json, names_from_json, schema_from_json,
};

/// Connected to a store's Flight server. Calls block until the server answers. Clones share the
/// connection.
#[derive(Clone)]
pub struct Client {
    runtime: Arc<Runtime>,
    flight: FlightServiceClient<Channel>,
}

impl Client {
    /// Connects to `url` (`grpc://host:port` or `http://host:port`).
    pub fn connect(url: &str) -> Result<Self, Error> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("kermodes-client")
            .enable_all()
            .build()
            .map_err(|e| Error::Io(format!("cannot start the client: {e}")))?;
        let endpoint = url
            .strip_prefix("grpc://")
            .map_or_else(|| url.to_owned(), |rest| format!("http://{rest}"));
        let channel = runtime
            .block_on(async {
                Channel::from_shared(endpoint.clone())
                    .map_err(|e| e.to_string())?
                    .connect()
                    .await
                    .map_err(|e| chain(&e))
            })
            .map_err(|e| Error::Io(format!("cannot connect to {url}: {e}")))?;
        Ok(Self {
            runtime: Arc::new(runtime),
            flight: FlightServiceClient::new(channel),
        })
    }

    fn action(&self, kind: &str, body: Value) -> Result<Value, Error> {
        let mut flight = self.flight.clone();
        self.runtime.block_on(async move {
            let action = Action {
                r#type: kind.into(),
                body: body.to_string().into(),
            };
            let mut results = flight
                .do_action(action)
                .await
                .map_err(|s| error_from_status(&s))?
                .into_inner();
            let result = results
                .message()
                .await
                .map_err(|s| error_from_status(&s))?
                .ok_or_else(|| Error::Io(format!("no answer to `{kind}`")))?;
            serde_json::from_slice(&result.body)
                .map_err(|e| Error::Io(format!("unreadable answer to `{kind}`: {e}")))
        })
    }

    /// Creates `name`, or checks it exists with the same columns; its schema.
    pub fn create_table(&self, name: &TableName, schema: &Schema) -> Result<SchemaRef, Error> {
        let created = self.action(
            "create_table",
            json!({"table": name.to_string(), "columns": columns_json(schema)}),
        )?;
        schema_from_json(&created["columns"])
    }

    /// Creates `name` as a keyed table, or checks it exists with the same columns and key.
    pub fn create_keyed_table(
        &self,
        name: &TableName,
        schema: &Schema,
        key: &[String],
    ) -> Result<SchemaRef, Error> {
        let created = self.action(
            "create_table",
            json!({"table": name.to_string(), "columns": columns_json(schema), "key": key}),
        )?;
        schema_from_json(&created["columns"])
    }

    /// Creates `name` as an append table sorted in history by `sort`, or checks it exists with
    /// the same columns and sort.
    pub fn create_sorted_table(
        &self,
        name: &TableName,
        schema: &Schema,
        sort: &[String],
    ) -> Result<SchemaRef, Error> {
        let created = self.action(
            "create_table",
            json!({"table": name.to_string(), "columns": columns_json(schema), "sort": sort}),
        )?;
        schema_from_json(&created["columns"])
    }

    /// Every table, by name, with its schema.
    pub fn tables(&self) -> Result<Vec<(TableName, SchemaRef)>, Error> {
        let tables = self.action("tables", json!({}))?;
        tables
            .as_array()
            .into_iter()
            .flatten()
            .map(|table| {
                let name = TableName::parse(table["table"].as_str().unwrap_or_default())?;
                Ok((name, schema_from_json(&table["columns"])?))
            })
            .collect()
    }

    /// The schema of `name`.
    pub fn table(&self, name: &TableName) -> Result<SchemaRef, Error> {
        self.tables()?
            .into_iter()
            .find(|(n, _)| n == name)
            .map(|(_, schema)| schema)
            .ok_or_else(|| Error::NotFound(format!("no table {name}; create it first")))
    }

    /// The catalog's entry for `name`: its ID, location, and schema.
    pub fn entry(&self, name: &TableName) -> Result<TableEntry, Error> {
        let tables = self.action("tables", json!({}))?;
        let table = tables
            .as_array()
            .into_iter()
            .flatten()
            .find(|t| t["table"].as_str() == Some(&name.to_string()))
            .ok_or_else(|| Error::NotFound(format!("no table {name}; create it first")))?;
        let older = || {
            Error::Io(format!(
                "the server is older than history: {name} has no location"
            ))
        };
        let key = names_from_json(&table["key"])?;
        // Absent from a server older than sorted tables: unsorted.
        let sort = names_from_json(&table["sort"])?;
        Ok(TableEntry {
            id: table["id"].as_i64().ok_or_else(older)?,
            name: name.clone(),
            location: table["location"].as_str().ok_or_else(older)?.to_owned(),
            schema: schema_from_json(&table["columns"])?,
            key,
            sort,
        })
    }

    /// The rows and stream bytes weir holds of a past `date` of `name`.
    pub fn date_rows(&self, name: &TableName, date: Date) -> Result<(u64, u64), Error> {
        let held = self.action(
            "date_rows",
            json!({"table": name.to_string(), "date": date.to_string()}),
        )?;
        Ok((
            held["rows"].as_u64().unwrap_or(0),
            held["bytes"].as_u64().unwrap_or(0),
        ))
    }

    /// Every batch of a past `date` of `name` with its change kind, to `each`, as the server
    /// reads them.
    pub fn read_date(
        &self,
        name: &TableName,
        date: Date,
        each: &mut dyn FnMut(RecordBatch, Change) -> Result<(), Error>,
    ) -> Result<(), Error> {
        let mut flight = self.flight.clone();
        let ticket = Ticket {
            ticket: json!({"table": name.to_string(), "date": date.to_string()})
                .to_string()
                .into(),
        };
        let mut decoded = self.runtime.block_on(async move {
            let data = flight
                .do_get(ticket)
                .await
                .map_err(|s| error_from_status(&s))?
                .into_inner()
                .map_err(FlightError::from);
            Ok::<_, Error>(FlightDataDecoder::new(data))
        })?;
        // `each` runs outside the client's runtime: it may block on another (a history write).
        while let Some(message) = self.runtime.block_on(decoded.next()) {
            match message {
                Ok(message) => {
                    if let DecodedPayload::RecordBatch(batch) = message.payload {
                        each(batch, change_of(&message.inner.app_metadata)?)?;
                    }
                }
                Err(FlightError::Tonic(status)) => return Err(error_from_status(&status)),
                Err(e) => return Err(Error::Io(format!("reading {name} {date}: {e}"))),
            }
        }
        Ok(())
    }

    /// The past dates weir still holds rows of for `name`.
    pub fn past_dates(&self, name: &TableName) -> Result<Vec<Date>, Error> {
        let dates = self.action("past_dates", json!({"table": name.to_string()}))?;
        list_from_json(&dates, date_from_json)
    }

    /// Every date of `name`'s history, by date then file.
    pub fn history(&self, name: &TableName) -> Result<Vec<HistoryDate>, Error> {
        let held = self.action("history", json!({"table": name.to_string()}))?;
        list_from_json(&held, history_date_from_json)
    }

    /// The files of `name`'s history holding any date from `first` to `last`.
    pub fn history_files(
        &self,
        name: &TableName,
        first: Date,
        last: Date,
    ) -> Result<Vec<HistoryFile>, Error> {
        let files = self.action(
            "history_files",
            json!({"table": name.to_string(), "first": first.to_string(), "last": last.to_string()}),
        )?;
        list_from_json(&files, history_file_from_json)
    }

    /// Commits a write of `name`'s history; `Busy` when it changed since the commit was planned.
    pub fn commit_history(&self, name: &TableName, commit: &HistoryCommit) -> Result<(), Error> {
        let mut body = commit_json(commit);
        body["table"] = json!(name.to_string());
        self.action("commit_history", body).map(|_| ())
    }

    /// Drops a past `date` of `name` from weir once its history holds it whole.
    pub fn drop_date(&self, name: &TableName, date: Date) -> Result<(), Error> {
        self.action(
            "drop_date",
            json!({"table": name.to_string(), "date": date.to_string()}),
        )
        .map(|_| ())
    }

    /// History files replaced at least `grace` ago.
    pub fn history_garbage(&self, grace: Duration) -> Result<Vec<Garbage>, Error> {
        let garbage = self.action("history_garbage", json!({"grace_secs": grace.as_secs()}))?;
        list_from_json(&garbage, garbage_from_json)
    }

    /// Forgets a garbage file once it is deleted.
    pub fn forget_garbage(&self, garbage: &Garbage) -> Result<(), Error> {
        self.action("forget_garbage", garbage_json(garbage))
            .map(|_| ())
    }

    /// A writer for `source`'s rows of `name`, holding the source's lease server-wide until it
    /// is dropped: with `wait`, once the current writer goes; without, failing with `Busy` if
    /// there is one.
    pub fn writer(
        &self,
        name: &TableName,
        source: &SourceId,
        wait: bool,
    ) -> Result<RemoteWriter, Error> {
        let TableEntry { schema, key, .. } = self.entry(name)?;
        let mut first: FlightData = batches_to_flight_data(&schema, [])
            .map_err(|e| Error::Arrow(e.to_string()))?
            .remove(0);
        let target = json!({"table": name.to_string(), "source": source.as_str(), "wait": wait});
        first.flight_descriptor = Some(FlightDescriptor::new_cmd(target.to_string()));
        let (sender, outgoing) = mpsc::unbounded_channel::<FlightData>();
        sender
            .send(first)
            .map_err(|_| Error::Io("the write stream closed".into()))?;
        let outgoing = futures::stream::unfold(outgoing, |mut outgoing| async move {
            outgoing.recv().await.map(|data| (data, outgoing))
        });
        let mut flight = self.flight.clone();
        // Returns once the server holds the lease for us.
        let acks = self
            .runtime
            .block_on(async move { flight.do_put(outgoing).await })
            .map_err(|s| error_from_status(&s))?
            .into_inner();
        // Only now: a writer before us may have written until it let go.
        let state = self.action(
            "last_seq",
            json!({"table": name.to_string(), "source": source.as_str()}),
        )?;
        Ok(RemoteWriter {
            runtime: self.runtime.clone(),
            table: name.clone(),
            schema,
            key,
            sender,
            epoch: state["epoch"].as_u64(),
            state: Mutex::new(WriterState {
                acks,
                last: state["last_seq"].as_u64(),
            }),
        })
    }

    /// Feeds `subscriber` every committed batch of `name`, then each new one, from a background
    /// task until it returns `false`; returns the table's schema. If the stream ends first (the
    /// server stopped, the connection broke), `lost` gets the reason: the subscriber would
    /// otherwise wait forever on rows that cannot come.
    pub fn subscribe(
        &self,
        name: &TableName,
        mut subscriber: Subscriber,
        lost: Box<dyn FnOnce(String) + Send>,
    ) -> Result<SchemaRef, Error> {
        let mut flight = self.flight.clone();
        let ticket = Ticket {
            ticket: json!({"table": name.to_string()}).to_string().into(),
        };
        let (schema, mut decoded) = self.runtime.block_on(async move {
            let data = flight
                .do_get(ticket)
                .await
                .map_err(|s| error_from_status(&s))?
                .into_inner()
                .map_err(FlightError::from);
            let mut decoded = FlightDataDecoder::new(data);
            // The server sends the schema first.
            loop {
                match decoded.next().await {
                    Some(Ok(message)) => {
                        if let DecodedPayload::Schema(schema) = message.payload {
                            return Ok((schema, decoded));
                        }
                    }
                    Some(Err(e)) => return Err(Error::Io(e.to_string())),
                    None => return Err(Error::Io("the server closed the stream".into())),
                }
            }
        })?;
        let name = name.to_string();
        self.runtime.spawn(async move {
            let reason = loop {
                match decoded.next().await {
                    Some(Ok(message)) => {
                        if let DecodedPayload::RecordBatch(batch) = message.payload {
                            let change = match change_of(&message.inner.app_metadata) {
                                Ok(change) => change,
                                Err(e) => break e.to_string(),
                            };
                            if !subscriber(&batch, change, Pace::MayWait) {
                                return;
                            }
                        }
                    }
                    Some(Err(e)) => break format!("the stream of {name} broke: {e}"),
                    None => break format!("the server ended the stream of {name}"),
                }
            };
            lost(reason);
        });
        Ok(schema)
    }
}

/// Writes one source's rows of a served table (`Client::writer`).
pub struct RemoteWriter {
    runtime: Arc<Runtime>,
    table: TableName,
    schema: SchemaRef,
    /// A keyed table's key columns; empty for an append table.
    key: Vec<String>,
    sender: mpsc::UnboundedSender<FlightData>,
    epoch: Option<u64>,
    state: Mutex<WriterState>,
}

struct WriterState {
    acks: Streaming<PutResult>,
    last: Option<u64>,
}

impl RemoteWriter {
    pub fn last_seq(&self) -> Option<u64> {
        self.state
            .lock()
            .expect("kermodes writer poisoned by an earlier panic")
            .last
    }

    /// This writer's lease epoch (§7.4); `None` from a server older than leases (0.1.1).
    pub fn epoch(&self) -> Option<u64> {
        self.epoch
    }

    /// As `kermodes_weir::Writer::write`: returns once the server has the batch durably, or
    /// dropped it as a resend (`false`).
    pub fn write(&self, batch: &RecordBatch, seq: Option<u64>) -> Result<bool, Error> {
        self.send(&conform(&self.schema, batch)?, seq, Change::Rows)
    }

    /// As `kermodes_weir::Writer::upsert`.
    pub fn upsert(&self, batch: &RecordBatch, seq: Option<u64>) -> Result<bool, Error> {
        self.send(&conform(&self.schema, batch)?, seq, Change::Upsert)
    }

    /// As `kermodes_weir::Writer::delete`: `batch` needs only the key columns.
    pub fn delete(&self, batch: &RecordBatch, seq: Option<u64>) -> Result<bool, Error> {
        self.send(
            &keys_only(&self.schema, &self.key, batch)?,
            seq,
            Change::Delete,
        )
    }

    /// As `kermodes_weir::Writer::replace`.
    pub fn replace(&self, batch: &RecordBatch, seq: Option<u64>) -> Result<bool, Error> {
        self.send(&conform(&self.schema, batch)?, seq, Change::Checkpoint)
    }

    fn send(&self, batch: &RecordBatch, seq: Option<u64>, change: Change) -> Result<bool, Error> {
        let mut state = self
            .state
            .lock()
            .expect("kermodes writer poisoned by an earlier panic");
        let seq = seq.unwrap_or_else(|| state.last.map_or(1, |last| last + 1));
        let mut data = batches_to_flight_data(&self.schema, [batch])
            .map_err(|e| Error::Arrow(e.to_string()))?
            .pop()
            .expect("a batch message");
        // A plain write's metadata is its sequence number alone, as servers before keyed tables
        // read it.
        let mut metadata = seq.to_le_bytes().to_vec();
        if change != Change::Rows {
            metadata.push(change.code());
        }
        data.app_metadata = metadata.into();
        let bytes = data.data_header.len() + data.data_body.len();
        if bytes > MAX_WRITE_BYTES {
            return Err(Error::TooLarge(format!(
                "a write to {} of {:.1} MiB is over the {} MiB limit for one batch; write it as \
                 smaller batches",
                self.table,
                bytes as f64 / (1 << 20) as f64,
                MAX_WRITE_BYTES >> 20
            )));
        }
        self.sender
            .send(data)
            .map_err(|_| Error::Io("the write stream closed".into()))?;
        let ack = self
            .runtime
            .block_on(state.acks.message())
            .map_err(|s| error_from_status(&s))?
            .ok_or_else(|| Error::Io("the server closed the write stream".into()))?;
        let bytes = ack.app_metadata.as_ref();
        if bytes.len() != 9 {
            return Err(Error::Io(
                "an acknowledgement this client cannot read".into(),
            ));
        }
        let last = u64::from_le_bytes(bytes[..8].try_into().expect("8 bytes"));
        state.last = Some(last);
        Ok(bytes[8] == 1)
    }
}

/// A read batch's change kind: one byte, or none from a server before keyed tables (rows).
fn change_of(metadata: &[u8]) -> Result<Change, Error> {
    match metadata {
        [] => Ok(Change::Rows),
        [code] => Change::from_code(u32::from(*code))
            .ok_or_else(|| Error::Io(format!("a change kind ({code}) this client cannot read"))),
        _ => Err(Error::Io("batch metadata this client cannot read".into())),
    }
}

/// An error and its causes: tonic's own message ("transport error") says nothing on its own.
fn chain(error: &dyn std::error::Error) -> String {
    let mut text = error.to_string();
    let mut cause = error.source();
    while let Some(next) = cause {
        text.push_str(": ");
        text.push_str(&next.to_string());
        cause = next.source();
    }
    text
}
