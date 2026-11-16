//! The Flight service over a store.

use std::net::{SocketAddr, TcpListener};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::{self, JoinHandle};

use arrow::record_batch::RecordBatch;
use arrow_flight::decode::{DecodedPayload, FlightDataDecoder};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use futures::stream::{self, StreamExt, TryStreamExt};
use kermodes_weir::{Change, Date, Error, Pace, SourceId, Stamp, Store, TableName};
use serde_json::{Value, json};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tonic::{Request, Response, Status, Streaming};

use crate::{
    CAUGHT_UP, MARKS_CAUGHT_UP, columns_json, commit_from_json, garbage_from_json, garbage_json,
    history_date_json, history_file_json, names_from_json, schema_from_json, stamp_metadata,
    status,
};

/// Bytes queued for one reader while it catches up: the replay waits for the reader past this,
/// so a day of any size costs about this much of the server's memory per reader.
const CATCH_UP_BYTES: usize = 16 << 20;
/// Bytes a reader may fall behind live writes before it is dropped (its stream ends, and it
/// subscribes again) rather than held in the server's memory. Above `CATCH_UP_BYTES`, so the
/// last batches of a replay and the first live ones always fit.
const LIVE_BYTES: usize = 128 << 20;

/// A running Flight server; stops when dropped.
pub struct Server {
    addr: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

/// Serves `store` on `addr` (port 0 picks a free one).
pub fn serve(store: Arc<Store>, addr: SocketAddr) -> std::io::Result<Server> {
    let listener = TcpListener::bind(addr)?;
    listener.set_nonblocking(true)?;
    let addr = listener.local_addr()?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("kermodes-flight")
        .enable_all()
        .build()?;
    let (shutdown, stopped) = oneshot::channel::<()>();
    // A write's batch is one message; the rest of the message (its descriptor, sequence number,
    // and protobuf framing) takes far less than the headroom.
    let service = FlightServiceServer::new(Service { store })
        .max_decoding_message_size(crate::MAX_WRITE_BYTES + (64 << 10));
    let thread = thread::Builder::new()
        .name("kermodes-flight".into())
        .spawn(move || {
            runtime.block_on(async move {
                let listener =
                    tokio::net::TcpListener::from_std(listener).expect("a bound listener converts");
                let incoming = tonic::transport::server::TcpIncoming::from(listener);
                // HTTP/2 pings find a vanished client (a crashed or partitioned node) within
                // about half a minute, and closing its connection releases its writer's lease.
                let serving = tonic::transport::Server::builder()
                    .http2_keepalive_interval(Some(Duration::from_secs(10)))
                    .http2_keepalive_timeout(Some(Duration::from_secs(20)))
                    .add_service(service)
                    .serve_with_incoming(incoming);
                tokio::select! {
                    _ = serving => {}
                    _ = stopped => {}
                }
            });
            // Not a graceful shutdown: that waits for open streams, and tails and writers' streams
            // stay open as long as their clients do. Dropping the connections tells readers the
            // server went, and a write cut short is recovered as after a crash; acknowledged
            // writes are already durable.
            runtime.shutdown_timeout(Duration::from_secs(5));
        })?;
    Ok(Server {
        addr,
        shutdown: Some(shutdown),
        thread: Some(thread),
    })
}

impl Server {
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Stops serving: open streams end.
    pub fn stop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.stop();
    }
}

struct Service {
    store: Arc<Store>,
}

type Stream<T> = Pin<Box<dyn futures::Stream<Item = Result<T, Status>> + Send>>;

/// Runs store work (file and catalog I/O) off the async workers.
async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> Result<T, Error> + Send + 'static,
) -> Result<T, Status> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|e| Status::internal(format!("store task failed: {e}")))?
        .map_err(status)
}

fn command(bytes: &[u8]) -> Result<Value, Status> {
    serde_json::from_slice(bytes)
        .map_err(|e| Status::invalid_argument(format!("expected a JSON command: {e}")))
}

fn table_name(value: &Value) -> Result<TableName, Status> {
    let name = value["table"]
        .as_str()
        .ok_or_else(|| Status::invalid_argument("missing `table`"))?;
    TableName::parse(name).map_err(status)
}

fn source_id(value: &Value) -> Result<SourceId, Status> {
    let source = value["source"]
        .as_str()
        .ok_or_else(|| Status::invalid_argument("missing `source`"))?;
    SourceId::parse(source).map_err(status)
}

fn date(value: &Value, field: &str) -> Result<Date, Status> {
    let text = value[field]
        .as_str()
        .ok_or_else(|| Status::invalid_argument(format!("missing `{field}`")))?;
    Date::parse(text).map_err(status)
}

fn ack(last_seq: Option<u64>, written: bool) -> PutResult {
    let mut bytes = last_seq.unwrap_or(0).to_le_bytes().to_vec();
    bytes.push(u8::from(written));
    PutResult {
        app_metadata: bytes.into(),
    }
}

/// What a reader's stream carries after the schema: a batch with its stamp, or (a subscription)
/// the end of the replay of what the table held when it subscribed.
enum Sent {
    Batch(RecordBatch, Stamp),
    CaughtUp,
}

/// The schema, then each batch in messages of its own, tagged with its stamp (`app_metadata`,
/// `stamp_metadata`). Large batches still split into several messages, each tagged; an empty
/// one, which the encoder would drop, is one message (an empty checkpoint clears a keyed table).
/// With `marks` (a subscription that asked), the schema message says `MARKS_CAUGHT_UP`, and the
/// end of its replay is an empty batch tagged `CAUGHT_UP`.
fn tagged(
    schema: arrow::datatypes::SchemaRef,
    rows: impl futures::Stream<Item = Result<Sent, FlightError>> + Send + 'static,
    marks: bool,
) -> Stream<FlightData> {
    let opening = FlightDataEncoderBuilder::new()
        .with_schema(schema.clone())
        .build(stream::empty())
        .map_ok(move |mut message| {
            if marks {
                message.app_metadata = MARKS_CAUGHT_UP.into();
            }
            message
        });
    let batches = rows
        .then(move |row| {
            let schema = schema.clone();
            async move {
                let (batch, metadata) = match row? {
                    Sent::Batch(batch, stamp) => (batch, stamp_metadata(stamp)),
                    // An empty batch: every reader decodes one.
                    Sent::CaughtUp => (RecordBatch::new_empty(schema.clone()), CAUGHT_UP.to_vec()),
                };
                let messages: Vec<FlightData> = if batch.num_rows() == 0 {
                    let mut encoded =
                        arrow_flight::utils::batches_to_flight_data(&schema, [&batch])?;
                    // The schema went first, once.
                    encoded.split_off(1)
                } else {
                    FlightDataEncoderBuilder::new()
                        .with_schema(schema)
                        .build(stream::once(async move { Ok(batch) }))
                        .skip(1)
                        .try_collect()
                        .await?
                };
                let messages = messages
                    .into_iter()
                    .map(|mut message| {
                        message.app_metadata = metadata.clone().into();
                        Ok(message)
                    })
                    .collect::<Vec<_>>();
                Ok::<_, FlightError>(stream::iter(messages))
            }
        })
        .try_flatten();
    opening
        .chain(batches)
        .map_err(|e| Status::internal(e.to_string()))
        .boxed()
}

impl Service {
    /// Rows `[start, start + count)` of a table's day by position (`Store::rows_at`): the schema
    /// of the columns asked for, its `app_metadata` the day's row count (u64, little-endian),
    /// then the rows.
    async fn rows_at(
        &self,
        name: TableName,
        schema: arrow::datatypes::SchemaRef,
        ticket: &Value,
    ) -> Result<Response<Stream<FlightData>>, Status> {
        let range = |at: usize| {
            ticket["rows"][at]
                .as_u64()
                .ok_or_else(|| Status::invalid_argument("`rows` is [start, count]"))
        };
        let (start, count) = (range(0)?, range(1)?);
        let columns = match &ticket["columns"] {
            Value::Null => None,
            columns => Some(names_from_json(columns).map_err(status)?),
        };
        let schema = match &columns {
            None => schema,
            Some(columns) => {
                let indices = columns
                    .iter()
                    .map(|c| schema.index_of(c))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| Status::invalid_argument(e.to_string()))?;
                Arc::new(
                    schema
                        .project(&indices)
                        .map_err(|e| Status::internal(e.to_string()))?,
                )
            }
        };
        let store = self.store.clone();
        let (total, rows) =
            blocking(move || store.rows_at(&name, start, count, columns.as_deref())).await?;
        let messages = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(stream::iter(rows.into_iter().map(Ok)))
            .enumerate()
            .map(move |(at, message)| {
                message.map(|mut message| {
                    if at == 0 {
                        message.app_metadata = total.to_le_bytes().to_vec().into();
                    }
                    message
                })
            })
            .map_err(|e| Status::internal(e.to_string()))
            .boxed();
        Ok(Response::new(messages))
    }
}

#[tonic::async_trait]
impl FlightService for Service {
    type HandshakeStream = Stream<HandshakeResponse>;
    type ListFlightsStream = Stream<FlightInfo>;
    type DoGetStream = Stream<FlightData>;
    type DoPutStream = Stream<PutResult>;
    type DoExchangeStream = Stream<FlightData>;
    type DoActionStream = Stream<arrow_flight::Result>;
    type ListActionsStream = Stream<ActionType>;

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let mut incoming = request.into_inner();
        let first = incoming
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("an empty write"))?;
        let descriptor = first
            .flight_descriptor
            .clone()
            .ok_or_else(|| Status::invalid_argument("the first message needs a descriptor"))?;
        let target = command(&descriptor.cmd)?;
        let (name, source) = (table_name(&target)?, source_id(&target)?);
        // Absent in 0.1.1 clients' descriptors: they fail at once if the source is taken.
        let wait = target["wait"].as_bool().unwrap_or(false);
        let store = self.store.clone();
        // The response (and so the client's `do_put`) waits until the lease is held.
        let writer = Arc::new(blocking(move || store.writer(&name, &source, wait)).await?);
        let messages = stream::once(async { Ok(first) })
            .chain(incoming.map_err(FlightError::from))
            .boxed();
        let mut decoded = FlightDataDecoder::new(messages);
        let (acks, out) = mpsc::unbounded_channel::<Result<PutResult, Status>>();
        tokio::spawn(async move {
            while let Some(message) = decoded.next().await {
                let message = match message {
                    Ok(message) => message,
                    Err(e) => {
                        // A message over the size limit keeps tonic's status (out of range), which
                        // the client reads as too large rather than as a bad batch.
                        let status = match e {
                            FlightError::Tonic(status) => *status,
                            e => Status::invalid_argument(e.to_string()),
                        };
                        let _ = acks.send(Err(status));
                        return;
                    }
                };
                let DecodedPayload::RecordBatch(batch) = message.payload else {
                    continue;
                };
                // The sequence number (8 bytes), then since keyed tables the change kind (one
                // byte; absent, a plain write).
                let metadata = message.inner.app_metadata.as_ref();
                let change = match metadata.get(8) {
                    None => Some(Change::Rows),
                    Some(&code) => Change::from_code(u32::from(code)),
                };
                let (Ok(seq), Some(change)) = (
                    <[u8; 8]>::try_from(&metadata[..metadata.len().min(8)]),
                    change,
                ) else {
                    let _ = acks.send(Err(Status::invalid_argument(
                        "each batch's app_metadata is its sequence number (8 bytes), then its \
                         change kind (one byte)",
                    )));
                    return;
                };
                let seq = u64::from_le_bytes(seq);
                let writer = writer.clone();
                let result = blocking(move || {
                    let written = match change {
                        Change::Rows => writer.write(&batch, Some(seq))?,
                        Change::Upsert => writer.upsert(&batch, Some(seq))?,
                        Change::Delete => writer.delete(&batch, Some(seq))?,
                        Change::Checkpoint => writer.replace(&batch, Some(seq))?,
                    };
                    Ok(ack(writer.last_seq(), written))
                })
                .await;
                let failed = result.is_err();
                if acks.send(result).is_err() || failed {
                    return;
                }
            }
        });
        let out = tokio_stream(out);
        Ok(Response::new(out))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = command(&request.into_inner().ticket)?;
        let name = table_name(&ticket)?;
        let schema = {
            let (store, name) = (self.store.clone(), name.clone());
            blocking(move || store.table(&name).map(|entry| entry.schema)).await?
        };
        // Asked for since the marker; a reader before it would not know one.
        let marks = ticket["caught_up"].as_bool() == Some(true);
        if !ticket["rows"].is_null() {
            return self.rows_at(name, schema, &ticket).await;
        }
        if !ticket["date"].is_null() {
            let date = date(&ticket, "date")?;
            // A past date's batches, then the end. The queue holds a few batches, so the read
            // keeps to the reader's pace and weir's memory stays bounded.
            let (batches, mut receiver) = mpsc::channel::<Result<Sent, FlightError>>(4);
            let store = self.store.clone();
            tokio::task::spawn_blocking(move || {
                let read = store.read_date(&name, date, &mut |batch, stamp| {
                    batches
                        .blocking_send(Ok(Sent::Batch(batch, stamp)))
                        .map_err(|_| Error::Io("the reader went".into()))
                });
                if let Err(e) = read {
                    let _ = batches.blocking_send(Err(FlightError::Tonic(Box::new(status(e)))));
                }
            });
            let rows = futures::stream::poll_fn(move |cx| receiver.poll_recv(cx));
            return Ok(Response::new(tagged(schema, rows, false)));
        }
        // Batches sent to the reader's stream, and the bytes of them it has not taken yet.
        let (batches, receiver) = mpsc::unbounded_channel::<(Result<Sent, FlightError>, usize)>();
        let queued = Arc::new(AtomicUsize::new(0));
        let store = self.store.clone();
        let sent = queued.clone();
        // The replay runs beside the stream: it waits for the reader while catching up, which
        // would never end if the stream started only after it.
        tokio::task::spawn_blocking(move || {
            let subscriber = batches.clone();
            let failing = batches.clone();
            let subscribed = store.subscribe(
                &name,
                Box::new(move |batch, stamp, pace| {
                    let size = batch.get_array_memory_size();
                    match pace {
                        Pace::MayWait => {
                            while sent.load(Ordering::Acquire) > CATCH_UP_BYTES {
                                if subscriber.is_closed() {
                                    return false;
                                }
                                thread::sleep(Duration::from_millis(2));
                            }
                        }
                        // Too far behind to keep up.
                        Pace::MustNotWait => {
                            if sent.load(Ordering::Acquire) + size > LIVE_BYTES {
                                return false;
                            }
                        }
                    }
                    sent.fetch_add(size, Ordering::AcqRel);
                    // Fails once the reader went.
                    subscriber
                        .send((Ok(Sent::Batch(batch.clone(), stamp)), size))
                        .is_ok()
                }),
                // The table's columns changed beyond the reader's: its stream ends with why.
                Box::new(move |reason| {
                    let error = FlightError::Tonic(Box::new(Status::failed_precondition(reason)));
                    let _ = failing.send((Err(error), 0));
                }),
            );
            // Everything the table held is sent; what follows is live.
            let sent = match subscribed {
                Ok(_) if marks => Ok(Sent::CaughtUp),
                Ok(_) => return,
                Err(e) => Err(FlightError::Tonic(Box::new(status(e)))),
            };
            let _ = batches.send((sent, 0));
        });
        let rows = futures::stream::unfold(receiver, move |mut receiver| {
            let queued = queued.clone();
            async move {
                let (batch, size) = receiver.recv().await?;
                queued.fetch_sub(size, Ordering::AcqRel);
                Some((batch, receiver))
            }
        });
        Ok(Response::new(tagged(schema, rows, marks)))
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();
        let store = self.store.clone();
        let body: Value = match action.r#type.as_str() {
            "create_table" => {
                let command = command(&action.body)?;
                let name = table_name(&command)?;
                let schema = schema_from_json(&command["columns"]).map_err(status)?;
                // Absent before keyed tables: an append table; `sort` absent before sorted ones.
                let key = names_from_json(&command["key"]).map_err(status)?;
                let sort = names_from_json(&command["sort"]).map_err(status)?;
                let entry = blocking(move || match (key.as_slice(), sort.as_slice()) {
                    ([], []) => store.create_table(&name, &schema),
                    (key, []) => store.create_keyed_table(&name, &schema, key),
                    ([], sort) => store.create_sorted_table(&name, &schema, sort),
                    _ => Err(Error::Schema(format!(
                        "{name} is keyed: its history is its log, kept in commit order"
                    ))),
                })
                .await?;
                json!({"table": entry.name.to_string(), "columns": columns_json(&entry.schema)})
            }
            "tables" => {
                let tables = blocking(move || store.tables()).await?;
                // `id` and `location` since history (a merge Job writes under the location),
                // `key` since keyed tables (empty for an append table), `sort` since sorted ones,
                // `ids` and `was` since column IDs (§5.4).
                Value::Array(
                    tables
                        .iter()
                        .map(|t| {
                            json!({
                                "table": t.name.to_string(),
                                "columns": columns_json(&t.schema),
                                "id": t.id,
                                "location": t.location,
                                "key": t.key,
                                "sort": t.sort,
                                "ids": t.ids.iter().map(|id| id.0).collect::<Vec<_>>(),
                                "was": t.was,
                            })
                        })
                        .collect(),
                )
            }
            // What each table takes in weir and in history, and weir's volume (§15.2).
            "stats" => blocking(move || store.stats_json()).await?,
            "last_seq" => {
                let command = command(&action.body)?;
                let (name, source) = (table_name(&command)?, source_id(&command)?);
                let (last, epoch) = blocking(move || store.source_state(&name, &source)).await?;
                json!({ "last_seq": last, "epoch": epoch })
            }
            "date_rows" => {
                let command = command(&action.body)?;
                let (name, date) = (table_name(&command)?, date(&command, "date")?);
                let (rows, bytes) = blocking(move || store.date_rows(&name, date)).await?;
                json!({"rows": rows, "bytes": bytes})
            }
            "past_dates" => {
                let name = table_name(&command(&action.body)?)?;
                let dates = blocking(move || store.past_dates(&name)).await?;
                json!(dates.iter().map(Date::to_string).collect::<Vec<_>>())
            }
            "history" => {
                let name = table_name(&command(&action.body)?)?;
                let held = blocking(move || store.history(&name)).await?;
                Value::Array(held.iter().map(history_date_json).collect())
            }
            "history_files" => {
                let command = command(&action.body)?;
                let name = table_name(&command)?;
                let (first, last) = (date(&command, "first")?, date(&command, "last")?);
                let files = blocking(move || store.history_files(&name, first, last)).await?;
                Value::Array(files.iter().map(history_file_json).collect())
            }
            "commit_history" => {
                let command = command(&action.body)?;
                let name = table_name(&command)?;
                let commit = commit_from_json(&command).map_err(status)?;
                blocking(move || store.commit_history(&name, &commit)).await?;
                json!({})
            }
            "drop_date" => {
                let command = command(&action.body)?;
                let (name, date) = (table_name(&command)?, date(&command, "date")?);
                blocking(move || store.drop_date(&name, date)).await?;
                json!({})
            }
            "history_garbage" => {
                let command = command(&action.body)?;
                let grace = command["grace_secs"]
                    .as_u64()
                    .ok_or_else(|| Status::invalid_argument("missing `grace_secs`"))?;
                let garbage =
                    blocking(move || store.history_garbage(Duration::from_secs(grace))).await?;
                Value::Array(garbage.iter().map(garbage_json).collect())
            }
            "forget_garbage" => {
                let garbage = garbage_from_json(&command(&action.body)?).map_err(status)?;
                blocking(move || store.forget_garbage(&garbage)).await?;
                json!({})
            }
            other => return Err(Status::invalid_argument(format!("no action `{other}`"))),
        };
        let result = arrow_flight::Result {
            body: body.to_string().into(),
        };
        Ok(Response::new(stream::iter([Ok(result)]).boxed()))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let actions = [
            (
                "create_table",
                "{table, columns: [[name, type]]}: create, or check it exists",
            ),
            ("tables", "every table with its columns"),
            (
                "last_seq",
                "{table, source}: the source's last sequence number and latest lease epoch",
            ),
            (
                "date_rows",
                "{table, date}: rows and bytes weir holds of a past date",
            ),
            ("past_dates", "{table}: past dates weir still holds rows of"),
            (
                "history",
                "{table}: every date of its history, in its files",
            ),
            (
                "history_files",
                "{table, first, last}: its history files holding those dates",
            ),
            (
                "commit_history",
                "{table, written, replaced, files, dates}: commit a write of history",
            ),
            (
                "drop_date",
                "{table, date}: drop a past date its history holds whole",
            ),
            (
                "history_garbage",
                "{grace_secs}: history files replaced at least that long ago",
            ),
            (
                "forget_garbage",
                "{table_id, location, file}: forget a deleted file",
            ),
        ];
        let actions = actions.map(|(kind, description)| {
            Ok(ActionType {
                r#type: kind.into(),
                description: description.into(),
            })
        });
        Ok(Response::new(stream::iter(actions).boxed()))
    }

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("no authentication in local mode"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("use the `tables` action"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "one server holds every partition here",
        ))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented(
            "one server holds every partition here",
        ))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("use the `tables` action"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("reads use DoGet, writes DoPut"))
    }
}

fn tokio_stream<T: Send + 'static>(
    receiver: mpsc::UnboundedReceiver<Result<T, Status>>,
) -> Stream<T> {
    futures::stream::unfold(receiver, |mut receiver| async move {
        receiver.recv().await.map(|item| (item, receiver))
    })
    .boxed()
}
