//! A process's handle on a served store: the store's operations, over Flight.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow::datatypes::{Schema, SchemaRef};
use arrow::ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteContext, IpcWriteOptions};
use arrow::record_batch::RecordBatch;
use arrow_flight::decode::{DecodedPayload, FlightDataDecoder};
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::utils::batches_to_flight_data;
use arrow_flight::{Action, FlightData, FlightDescriptor, PutResult, Ticket};
use futures::{StreamExt, TryStreamExt};
use kermodes_weir::{
    Answer, Change, ColumnId, Date, Error, Garbage, HistoryCommit, HistoryDate, HistoryFile,
    InFlight, Pace, Pending, SourceId, Stamp, Subscriber, TableEntry, TableName, conform,
    keys_only,
};
use serde_json::{Value, json};
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tonic::transport::Channel;
use tonic::{Code, Streaming};

use crate::{
    CAUGHT_UP, MARKS_CAUGHT_UP, MAX_WRITE_BYTES, columns_json, commit_json, date_from_json,
    error_from_status, garbage_from_json, garbage_json, history_date_from_json,
    history_file_from_json, list_from_json, names_from_json, schema_from_json, stamp_of,
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
        // Pings on the HTTP/2 connection, idle or not, find one that died silently (a lost peer,
        // a dropped route) within about 15 s, so no subscription waits on a dead one.
        let channel = runtime
            .block_on(async {
                Channel::from_shared(endpoint.clone())
                    .map_err(|e| e.to_string())?
                    .connect_timeout(Duration::from_secs(5))
                    .http2_keep_alive_interval(Duration::from_secs(5))
                    .keep_alive_timeout(Duration::from_secs(10))
                    .keep_alive_while_idle(true)
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
        self.runtime.block_on(call(self.flight.clone(), kind, body))
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

    /// What each table takes in weir and in history, and the server's volume, as
    /// `Store::stats_json` gives it.
    pub fn stats(&self) -> Result<Value, Error> {
        self.action("stats", json!({}))
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
        let schema = schema_from_json(&table["columns"])?;
        let columns = schema.fields().len();
        // Absent from a server older than column IDs: each column's position, as its catalog
        // has them, and no former names.
        let ids = match table["ids"].as_array() {
            None => (1..=columns as u32).map(ColumnId).collect(),
            Some(ids) => ids
                .iter()
                .map(|id| {
                    id.as_u64()
                        .and_then(|id| u32::try_from(id).ok())
                        .map(ColumnId)
                })
                .collect::<Option<Vec<_>>>()
                .filter(|ids| ids.len() == columns)
                .ok_or_else(|| {
                    Error::Io(format!("the server's column IDs of {name} are unreadable"))
                })?,
        };
        let was = match &table["was"] {
            Value::Null => vec![Vec::new(); columns],
            was => serde_json::from_value(was.clone())
                .map_err(|e| Error::Io(format!("the server's former names of {name}: {e}")))?,
        };
        Ok(TableEntry {
            id: table["id"].as_i64().ok_or_else(older)?,
            name: name.clone(),
            location: table["location"].as_str().ok_or_else(older)?.to_owned(),
            schema,
            ids: Arc::<[ColumnId]>::from(ids),
            was,
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
        each: &mut dyn FnMut(RecordBatch, Stamp) -> Result<(), Error>,
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
                        each(batch, stamp_of(&message.inner.app_metadata, now_ns)?)?;
                    }
                }
                Err(FlightError::Tonic(status)) => return Err(error_from_status(&status)),
                Err(e) => return Err(Error::Io(format!("reading {name} {date}: {e}"))),
            }
        }
        Ok(())
    }

    /// How many rows `name` holds today, and its rows `[start, start + count)` in commit order,
    /// with `columns` only (every column when `None`), as `Store::rows_at` gives them.
    pub fn rows_at(
        &self,
        name: &TableName,
        start: u64,
        count: u64,
        columns: Option<&[String]>,
    ) -> Result<(u64, Vec<RecordBatch>), Error> {
        let mut flight = self.flight.clone();
        let mut ticket = json!({"table": name.to_string(), "rows": [start, count]});
        if let Some(columns) = columns {
            ticket["columns"] = json!(columns);
        }
        let ticket = Ticket {
            ticket: ticket.to_string().into(),
        };
        self.runtime.block_on(async move {
            let data = flight
                .do_get(ticket)
                .await
                .map_err(|s| error_from_status(&s))?
                .into_inner()
                .map_err(FlightError::from);
            let mut decoded = FlightDataDecoder::new(data);
            let (mut total, mut rows) = (None, Vec::new());
            while let Some(message) = decoded.next().await {
                let message = match message {
                    Ok(message) => message,
                    Err(FlightError::Tonic(status)) => return Err(error_from_status(&status)),
                    Err(e) => return Err(Error::Io(format!("reading {name}'s rows: {e}"))),
                };
                match message.payload {
                    DecodedPayload::Schema(_) => {
                        let bytes: [u8; 8] = message
                            .inner
                            .app_metadata
                            .as_ref()
                            .try_into()
                            .map_err(|_| {
                                Error::Io(format!("{name}'s rows came without the day's row count"))
                            })?;
                        total = Some(u64::from_le_bytes(bytes));
                    }
                    DecodedPayload::RecordBatch(batch) => rows.push(batch),
                    DecodedPayload::None => {}
                }
            }
            let total =
                total.ok_or_else(|| Error::Io(format!("{name}'s rows came without a schema")))?;
            Ok((total, rows))
        })
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
    ///
    /// A stream that breaks (the server restarted, the connection dropped) is opened again,
    /// retrying with backoff, and the writes it had not acknowledged are resent after the
    /// source's last seq, so each is written once; writes sent meanwhile wait in the writer's
    /// in-flight budget. Every break and reconnect is reported on stderr: why, how long writes
    /// waited, how many were resent. A write the server refuses fails, and so does every write
    /// after it.
    pub fn writer(
        &self,
        name: &TableName,
        source: &SourceId,
        wait: bool,
    ) -> Result<RemoteWriter, Error> {
        let TableEntry { schema, key, .. } = self.entry(name)?;
        let target = WriteTarget {
            flight: self.flight.clone(),
            name: name.clone(),
            source: source.clone(),
            schema: schema.clone(),
        };
        // Returns once the server holds the lease for us.
        let opened = self.runtime.block_on(target.open(wait))?;
        let stream = Arc::new(Mutex::new(Stream {
            sender: Some(opened.sender),
            unacked: VecDeque::new(),
            sent: opened.last_seq,
            refused: None,
            dropped: false,
        }));
        let acked = Arc::new(Mutex::new(opened.last_seq));
        let epoch = Arc::new(Mutex::new(opened.epoch));
        self.runtime.spawn(keep_writing(
            target,
            opened.acks,
            stream.clone(),
            acked.clone(),
            epoch.clone(),
        ));
        Ok(RemoteWriter {
            _runtime: self.runtime.clone(),
            table: name.clone(),
            schema,
            key,
            stream,
            acked,
            epoch,
            in_flight: Arc::default(),
            gone: format!("the write stream of {name} closed").into(),
        })
    }

    /// Feeds `subscriber` every committed batch of `name`, then each new one, from a background
    /// task until it returns `false`; returns the table's schema. `caught_up` runs once the
    /// batches the table held when it subscribed are all given (at once, from a server too old to
    /// say).
    ///
    /// A stream that breaks (the server restarted, the connection dropped) is resumed after the
    /// last batch the subscriber was given, retrying with backoff, so it sees every batch once.
    /// Every break and resume is reported on stderr: why, how long the table was not current,
    /// and how much was replayed. `lost` gets the reason when it cannot go on: the table's
    /// columns changed beyond the subscriber's, a server too old to give positions, or the
    /// batches after its position no longer all held. In that last case, with `rebuilds`, the
    /// subscription starts again from the table's start instead: for a subscriber whose replay
    /// begins by replacing what it holds (a keyed table's checkpoint).
    pub fn subscribe(
        &self,
        name: &TableName,
        mut subscriber: Subscriber,
        caught_up: Box<dyn FnOnce() + Send>,
        lost: Box<dyn FnOnce(String) + Send>,
        rebuilds: bool,
    ) -> Result<SchemaRef, Error> {
        let (schema, marks, decoded) =
            self.runtime
                .block_on(open_subscription(self.flight.clone(), name, None))?;
        let mut caught_up = Some(caught_up);
        if !marks && let Some(ready) = caught_up.take() {
            ready();
        }
        let (flight, name) = (self.flight.clone(), name.clone());
        self.runtime.spawn(async move {
            let mut decoded = decoded;
            // The position of the last batch given: what a resumed stream starts after.
            let mut last = 0;
            // A resumed stream: when the old one ended, how many tries it took, batches since.
            let mut resumed: Option<(Instant, u32, u64)> = None;
            loop {
                let ended = loop {
                    match decoded.next().await {
                        Some(Ok(message)) => match message.payload {
                            DecodedPayload::RecordBatch(_)
                                if message.inner.app_metadata.as_ref() == CAUGHT_UP =>
                            {
                                if let Some(ready) = caught_up.take() {
                                    ready();
                                }
                                if let Some((down, tries, batches)) = resumed.take() {
                                    eprintln!(
                                        "kermodes: {name} resumed: not current for {:.1} s, \
                                         {tries} attempt(s), {batches} batch(es) replayed",
                                        down.elapsed().as_secs_f64()
                                    );
                                }
                            }
                            DecodedPayload::RecordBatch(batch) => {
                                let stamp = match stamp_of(&message.inner.app_metadata, now_ns) {
                                    Ok(stamp) => stamp,
                                    Err(e) => break Ended::Refused(e.to_string()),
                                };
                                last = last.max(stamp.lsn);
                                if let Some((_, _, batches)) = resumed.as_mut() {
                                    *batches += 1;
                                }
                                if !subscriber(&batch, stamp, Pace::MayWait) {
                                    return;
                                }
                            }
                            _ => {}
                        },
                        Some(Err(e)) => break Ended::from(e),
                        None => break Ended::Broke("the server ended it".into()),
                    }
                };
                let down = Instant::now();
                match ended {
                    Ended::Refused(why) => return lost(why),
                    Ended::Gone(why) if rebuilds => {
                        eprintln!("kermodes: {name}: {why}; reading it again from its start");
                        last = 0;
                    }
                    Ended::Gone(why) => return lost(why),
                    Ended::Broke(why) if last == 0 && !rebuilds => {
                        return lost(format!(
                            "the stream of {name} broke ({why}) before giving a position to \
                             resume after"
                        ));
                    }
                    Ended::Broke(why) => {
                        eprintln!(
                            "kermodes: the stream of {name} broke ({why}); resuming after \
                             position {last}"
                        );
                    }
                }
                let mut backoff = Duration::from_millis(250);
                let mut tries = 0;
                decoded = loop {
                    tries += 1;
                    let after = (last > 0).then_some(last);
                    match open_subscription(flight.clone(), &name, after).await {
                        Ok((_, _, decoded)) => break decoded,
                        Err(Error::Gone(why)) if rebuilds => {
                            eprintln!("kermodes: {name}: {why}; reading it again from its start");
                            last = 0;
                        }
                        Err(Error::Gone(why)) => return lost(why),
                        Err(e) => {
                            eprintln!(
                                "kermodes: {name}: cannot resume yet ({e}); trying again in \
                                 {backoff:?}"
                            );
                            tokio::time::sleep(backoff).await;
                            backoff = (backoff * 2).min(MAX_BACKOFF);
                        }
                    }
                };
                resumed = Some((down, tries, 0));
            }
        });
        Ok(schema)
    }
}

/// The longest a broken subscription waits between attempts to resume.
const MAX_BACKOFF: Duration = Duration::from_secs(5);

/// How a subscription's stream ended.
enum Ended {
    /// It broke (the server went, the connection dropped): resume after the last position.
    Broke(String),
    /// The server no longer holds everything after the position asked for.
    Gone(String),
    /// Nothing resuming helps: the table's columns changed beyond the subscriber's, or the
    /// stream is one this client cannot read.
    Refused(String),
}

impl From<FlightError> for Ended {
    fn from(error: FlightError) -> Self {
        match &error {
            FlightError::Tonic(status) if status.code() == Code::Aborted => {
                Ended::Gone(status.message().to_owned())
            }
            FlightError::Tonic(status) if status.code() == Code::FailedPrecondition => {
                Ended::Refused(status.message().to_owned())
            }
            _ => Ended::Broke(error.to_string()),
        }
    }
}

/// Opens a subscription to `name`, after the position `after` if given: its schema, whether the
/// server marks the end of its replay, and the rest of the stream.
async fn open_subscription(
    mut flight: FlightServiceClient<Channel>,
    name: &TableName,
    after: Option<u64>,
) -> Result<(SchemaRef, bool, FlightDataDecoder), Error> {
    let mut ticket = json!({"table": name.to_string(), "caught_up": true, "lsn": true});
    if let Some(after) = after {
        ticket["after"] = json!(after);
    }
    let data = flight
        .do_get(Ticket {
            ticket: ticket.to_string().into(),
        })
        .await
        .map_err(|s| error_from_status(&s))?
        .into_inner()
        .map_err(FlightError::from);
    let mut decoded = FlightDataDecoder::new(data);
    // The server sends the schema first.
    loop {
        match decoded.next().await {
            Some(Ok(message)) => {
                let marks = message.inner.app_metadata.as_ref() == MARKS_CAUGHT_UP;
                if let DecodedPayload::Schema(schema) = message.payload {
                    return Ok((schema, marks, decoded));
                }
            }
            Some(Err(FlightError::Tonic(status))) => return Err(error_from_status(&status)),
            Some(Err(e)) => return Err(Error::Io(e.to_string())),
            None => return Err(Error::Io("the server closed the stream".into())),
        }
    }
}

/// What a remote writer writes to: enough to open its stream again after a break.
struct WriteTarget {
    flight: FlightServiceClient<Channel>,
    name: TableName,
    source: SourceId,
    schema: SchemaRef,
}

/// A write stream just opened, the source's lease held.
struct Opened {
    sender: mpsc::UnboundedSender<FlightData>,
    acks: Streaming<PutResult>,
    /// The source's last seq and lease epoch once the lease is ours.
    last_seq: Option<u64>,
    epoch: Option<u64>,
}

impl WriteTarget {
    /// Opens a write stream: returns once the server holds the source's lease for it, with
    /// `wait` once the writer holding it goes, without failing with `Busy` if one does.
    async fn open(&self, wait: bool) -> Result<Opened, Error> {
        let mut first: FlightData = batches_to_flight_data(&self.schema, [])
            .map_err(|e| Error::Arrow(e.to_string()))?
            .remove(0);
        let target = json!({
            "table": self.name.to_string(),
            "source": self.source.as_str(),
            "wait": wait,
        });
        first.flight_descriptor = Some(FlightDescriptor::new_cmd(target.to_string()));
        let (sender, outgoing) = mpsc::unbounded_channel::<FlightData>();
        sender
            .send(first)
            .map_err(|_| Error::Io("the write stream closed".into()))?;
        let outgoing = futures::stream::unfold(outgoing, |mut outgoing| async move {
            outgoing.recv().await.map(|data| (data, outgoing))
        });
        let acks = self
            .flight
            .clone()
            .do_put(outgoing)
            .await
            .map_err(|s| error_from_status(&s))?
            .into_inner();
        // Only now: a writer before us may have written until it let go.
        let state = call(
            self.flight.clone(),
            "last_seq",
            json!({"table": self.name.to_string(), "source": self.source.as_str()}),
        )
        .await?;
        Ok(Opened {
            sender,
            acks,
            last_seq: state["last_seq"].as_u64(),
            epoch: state["epoch"].as_u64(),
        })
    }
}

/// A remote writer's stream and the writes it has not acknowledged. Its lock keeps writes in
/// the order sent, across a reconnect's resending too.
struct Stream {
    /// Where writes go; none while the stream is being opened again.
    sender: Option<mpsc::UnboundedSender<FlightData>>,
    /// Writes not yet acknowledged, oldest first: each one's seq, message (its buffers shared,
    /// not copied), and answer. Resent after a break.
    unacked: VecDeque<(u64, FlightData, Answer)>,
    /// The highest seq sent: the next unnumbered write's is one above.
    sent: Option<u64>,
    /// Why writing stopped for good: the server refused a write.
    refused: Option<String>,
    /// The writer was dropped: its stream is closed, and is not opened again.
    dropped: bool,
}

/// How a write stream stopped.
enum Stopped {
    /// It broke: open it again and resend.
    Broke(String),
    /// The server refused a write, or the stream: nothing resending would change.
    Refused(Error),
}

/// A remote writer's background task: answers each write as the server acknowledges it (in the
/// order sent); on a break, opens the stream again and resends what was not acknowledged; on a
/// refusal, fails the refused write and every one after it.
async fn keep_writing(
    target: WriteTarget,
    mut acks: Streaming<PutResult>,
    stream: Arc<Mutex<Stream>>,
    acked: Arc<Mutex<Option<u64>>>,
    epoch: Arc<Mutex<Option<u64>>>,
) {
    let who = format!("{} ({})", target.name, target.source.as_str());
    loop {
        let stopped = loop {
            match acks.message().await {
                Ok(Some(ack)) => {
                    let Some((written, last)) = ack.app_metadata.split_last() else {
                        break Stopped::Refused(Error::Io(
                            "an acknowledgement this client cannot read".into(),
                        ));
                    };
                    let Ok(last) = <[u8; 8]>::try_from(last) else {
                        break Stopped::Refused(Error::Io(
                            "an acknowledgement this client cannot read".into(),
                        ));
                    };
                    *lock(&acked) = Some(u64::from_le_bytes(last));
                    if let Some((_, _, answer)) = lock(&stream).unacked.pop_front() {
                        answer.send(Ok(*written == 1));
                    }
                }
                Ok(None) => break Stopped::Broke("the server closed the stream".into()),
                Err(status) => {
                    break match status.code() {
                        Code::InvalidArgument
                        | Code::NotFound
                        | Code::OutOfRange
                        | Code::DataLoss
                        | Code::AlreadyExists
                        | Code::FailedPrecondition => Stopped::Refused(error_from_status(&status)),
                        _ => Stopped::Broke(error_from_status(&status).to_string()),
                    };
                }
            }
        };
        let why = match stopped {
            Stopped::Refused(error) => return refuse(&stream, error),
            Stopped::Broke(why) => why,
        };
        let down = Instant::now();
        {
            let mut stream = lock(&stream);
            stream.sender = None;
            if stream.dropped {
                // Dropped and done: the server answers every write before closing.
                for (_, _, answer) in stream.unacked.drain(..) {
                    answer.send(Err(Error::Io(format!(
                        "the write stream of {who} broke ({why}) after its writer was dropped"
                    ))));
                }
                return;
            }
            eprintln!(
                "kermodes: the write stream of {who} broke ({why}); {} write(s) not yet \
                 acknowledged; reconnecting",
                stream.unacked.len()
            );
        }
        let mut backoff = Duration::from_millis(250);
        let mut tries = 0;
        let opened = loop {
            tries += 1;
            // Waiting for the lease: after a connection drops, the server holds the old
            // stream's lease until it notices, up to its pings' timeout.
            match target.open(true).await {
                Ok(opened) => break opened,
                Err(error @ (Error::Schema(_) | Error::NotFound(_) | Error::Corrupt(_))) => {
                    eprintln!("kermodes: the write stream of {who} cannot reconnect: {error}");
                    return refuse(&stream, error);
                }
                Err(error) => {
                    eprintln!(
                        "kermodes: {who}: cannot reconnect yet ({error}); trying again in \
                         {backoff:?}"
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                }
            }
        };
        let mut stream = lock(&stream);
        *lock(&acked) = opened.last_seq;
        *lock(&epoch) = opened.epoch;
        // Written before the break, their acknowledgements lost with it.
        let mut found = 0;
        while let Some((seq, _, _)) = stream.unacked.front()
            && opened.last_seq.is_some_and(|last| *seq <= last)
        {
            let (_, _, answer) = stream.unacked.pop_front().expect("a front");
            answer.send(Ok(true));
            found += 1;
        }
        for (_, data, _) in &stream.unacked {
            let _ = opened.sender.send(data.clone());
        }
        eprintln!(
            "kermodes: the write stream of {who} reconnected: writes waited {:.1} s, {tries} \
             attempt(s), {} write(s) resent, {found} found already written",
            down.elapsed().as_secs_f64(),
            stream.unacked.len()
        );
        // A writer dropped meanwhile closes its new stream at once: the server answers what was
        // resent, then ends it.
        if !stream.dropped {
            stream.sender = Some(opened.sender);
        }
        drop(stream);
        acks = opened.acks;
    }
}

/// Fails the oldest unacknowledged write with `error`, every other one as following it, and
/// every later write too.
fn refuse(stream: &Mutex<Stream>, error: Error) {
    let mut stream = lock(stream);
    let why = error.to_string();
    stream.sender = None;
    stream.refused = Some(why.clone());
    let mut unacked = stream.unacked.drain(..);
    if let Some((_, _, answer)) = unacked.next() {
        answer.send(Err(error));
    }
    for (_, _, answer) in unacked {
        answer.send(Err(Error::Io(format!(
            "not written: an earlier write was refused ({why})"
        ))));
    }
}

/// Runs the action `kind` with `body`: the server's answer.
async fn call(
    mut flight: FlightServiceClient<Channel>,
    kind: &str,
    body: Value,
) -> Result<Value, Error> {
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
}

/// Writes one source's rows of a served table (`Client::writer`), many in flight at once.
pub struct RemoteWriter {
    /// Runs the write stream and its acknowledgements, outliving the `Client` it came from.
    _runtime: Arc<Runtime>,
    table: TableName,
    schema: SchemaRef,
    /// A keyed table's key columns; empty for an append table.
    key: Vec<String>,
    stream: Arc<Mutex<Stream>>,
    /// The source's last seq the server acknowledged.
    acked: Arc<Mutex<Option<u64>>>,
    /// The lease epoch, new with each stream opened.
    epoch: Arc<Mutex<Option<u64>>>,
    in_flight: Arc<InFlight>,
    /// What a write says if the stream closes before it is acknowledged.
    gone: Arc<str>,
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .expect("kermodes writer poisoned by an earlier panic")
}

impl RemoteWriter {
    /// The source's last sequence number the server acknowledged.
    pub fn last_seq(&self) -> Option<u64> {
        *lock(&self.acked)
    }

    /// This writer's lease epoch (§7.4), new each time its stream is opened again; `None` from
    /// a server older than leases (0.1.1).
    pub fn epoch(&self) -> Option<u64> {
        *lock(&self.epoch)
    }

    /// As `kermodes_weir::Writer::write`: returns once the server has the batch durably, or
    /// dropped it as a resend (`false`).
    pub fn write(&self, batch: &RecordBatch, seq: Option<u64>) -> Result<bool, Error> {
        self.send(Change::Rows, batch, seq)?.wait()
    }

    /// As `kermodes_weir::Writer::upsert`.
    pub fn upsert(&self, batch: &RecordBatch, seq: Option<u64>) -> Result<bool, Error> {
        self.send(Change::Upsert, batch, seq)?.wait()
    }

    /// As `kermodes_weir::Writer::delete`: `batch` needs only the key columns.
    pub fn delete(&self, batch: &RecordBatch, seq: Option<u64>) -> Result<bool, Error> {
        self.send(Change::Delete, batch, seq)?.wait()
    }

    /// As `kermodes_weir::Writer::replace`.
    pub fn replace(&self, batch: &RecordBatch, seq: Option<u64>) -> Result<bool, Error> {
        self.send(Change::Checkpoint, batch, seq)?.wait()
    }

    /// As `kermodes_weir::Writer::send`: sends without waiting for the acknowledgement, so
    /// many writes are in flight on the stream and the server groups them.
    pub fn send(
        &self,
        change: Change,
        batch: &RecordBatch,
        seq: Option<u64>,
    ) -> Result<Pending, Error> {
        let batch = match change {
            Change::Delete => keys_only(&self.schema, &self.key, batch)?,
            _ => conform(&self.schema, batch)?,
        };
        // The batch message alone: the stream's schema went first, when it opened.
        let (dictionaries, encoded) = IpcDataGenerator::default()
            .encode(
                &batch,
                &mut DictionaryTracker::new(false),
                &IpcWriteOptions::default(),
                &mut IpcWriteContext::default(),
            )
            .map_err(|e| Error::Arrow(e.to_string()))?;
        // The column types tables store (kermodes_core::parse_type) have no dictionaries.
        debug_assert!(dictionaries.is_empty());
        let mut data = FlightData::from(encoded);
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
        // Waits while the budget is full: a stream long broken slows its producer this way.
        let (pending, answer) = self.in_flight.send(bytes, &self.gone);
        let mut stream = lock(&self.stream);
        if let Some(why) = &stream.refused {
            answer.send(Err(Error::Io(format!(
                "{} cannot be written: {why}",
                self.table
            ))));
            return Ok(pending);
        }
        let seq = seq.unwrap_or_else(|| stream.sent.map_or(1, |last| last + 1));
        // A plain write's metadata is its sequence number alone, as servers before keyed tables
        // read it.
        let mut metadata = seq.to_le_bytes().to_vec();
        if change != Change::Rows {
            metadata.push(change.code());
        }
        data.app_metadata = metadata.into();
        // While the stream is being opened again, the write waits with those unacknowledged,
        // to be sent with them.
        if let Some(sender) = &stream.sender
            && sender.send(data.clone()).is_err()
        {
            stream.sender = None;
        }
        stream.unacked.push_back((seq, data, answer));
        stream.sent = Some(stream.sent.map_or(seq, |last| last.max(seq)));
        Ok(pending)
    }

    /// As `kermodes_weir::Writer::flush`.
    pub fn flush(&self) -> Result<(), Error> {
        self.in_flight.flush()
    }
}

impl Drop for RemoteWriter {
    /// Closes the stream: the server answers the writes on it, then ends it and lets the
    /// source's lease go.
    fn drop(&mut self) {
        let mut stream = lock(&self.stream);
        stream.dropped = true;
        stream.sender = None;
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

/// Now, for a batch from a server that sends no ingest time.
fn now_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as i64)
}
