//! A process's handle on a served store: the store's operations, over Flight.

use std::sync::{Arc, Mutex};

use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow_flight::decode::{DecodedPayload, FlightDataDecoder};
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::utils::batches_to_flight_data;
use arrow_flight::{Action, FlightData, FlightDescriptor, PutResult, Ticket};
use futures::{StreamExt, TryStreamExt};
use kermodes_weir::{Error, SourceId, Subscriber, TableName, conform};
use serde_json::{Value, json};
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tonic::Streaming;
use tonic::transport::Channel;

use crate::{columns_json, error_from_status, schema_from_json};

/// Connected to a store's Flight server. Calls block until the server answers.
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

    /// A writer for `source`'s rows of `name`, holding the source's lease server-wide until it
    /// is dropped: with `wait`, once the current writer goes; without, failing with `Busy` if
    /// there is one.
    pub fn writer(
        &self,
        name: &TableName,
        source: &SourceId,
        wait: bool,
    ) -> Result<RemoteWriter, Error> {
        let schema = self.table(name)?;
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
            schema,
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
                        if let DecodedPayload::RecordBatch(batch) = message.payload
                            && !subscriber(&batch)
                        {
                            return;
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
    schema: SchemaRef,
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
        let batch = conform(&self.schema, batch)?;
        let mut state = self
            .state
            .lock()
            .expect("kermodes writer poisoned by an earlier panic");
        let seq = seq.unwrap_or_else(|| state.last.map_or(1, |last| last + 1));
        let mut data = batches_to_flight_data(&self.schema, [&batch])
            .map_err(|e| Error::Arrow(e.to_string()))?
            .pop()
            .expect("a batch message");
        data.app_metadata = seq.to_le_bytes().to_vec().into();
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
