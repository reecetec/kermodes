//! The Flight service over a store.

use std::net::{SocketAddr, TcpListener};
use std::pin::Pin;
use std::sync::Arc;
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
use kermodes_weir::{Error, SourceId, Store, TableName};
use serde_json::{Value, json};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tonic::{Request, Response, Status, Streaming};

use crate::{columns_json, schema_from_json, status};

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
    let service = FlightServiceServer::new(Service { store });
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

fn ack(last_seq: Option<u64>, written: bool) -> PutResult {
    let mut bytes = last_seq.unwrap_or(0).to_le_bytes().to_vec();
    bytes.push(u8::from(written));
    PutResult {
        app_metadata: bytes.into(),
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
                        let _ = acks.send(Err(Status::invalid_argument(e.to_string())));
                        return;
                    }
                };
                let DecodedPayload::RecordBatch(batch) = message.payload else {
                    continue;
                };
                let Ok(seq) = <[u8; 8]>::try_from(message.inner.app_metadata.as_ref()) else {
                    let _ = acks.send(Err(Status::invalid_argument(
                        "each batch's app_metadata is its sequence number (8 bytes)",
                    )));
                    return;
                };
                let seq = u64::from_le_bytes(seq);
                let writer = writer.clone();
                let result = blocking(move || {
                    let written = writer.write(&batch, Some(seq))?;
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
        let (batches, receiver) = mpsc::unbounded_channel::<RecordBatch>();
        let store = self.store.clone();
        // A slow reader buffers here rather than holding up writers; a reader that has gone is
        // dropped at the next batch.
        let schema = blocking(move || {
            store.subscribe(
                &name,
                Box::new(move |batch| batches.send(batch.clone()).is_ok()),
            )
        })
        .await?;
        let rows = futures::stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|batch| (Ok(batch), receiver))
        });
        let data = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(rows)
            .map_err(|e| Status::internal(e.to_string()));
        Ok(Response::new(data.boxed()))
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
                let entry = blocking(move || store.create_table(&name, &schema)).await?;
                json!({"table": entry.name.to_string(), "columns": columns_json(&entry.schema)})
            }
            "tables" => {
                let tables = blocking(move || store.tables()).await?;
                Value::Array(
                    tables
                        .iter()
                        .map(|t| json!({"table": t.name.to_string(), "columns": columns_json(&t.schema)}))
                        .collect(),
                )
            }
            "last_seq" => {
                let command = command(&action.body)?;
                let (name, source) = (table_name(&command)?, source_id(&command)?);
                let (last, epoch) = blocking(move || store.source_state(&name, &source)).await?;
                json!({ "last_seq": last, "epoch": epoch })
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
