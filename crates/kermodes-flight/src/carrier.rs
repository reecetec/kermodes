//! Carries a replica's Raft messages to the other replicas of its shard (architecture.md §7.6):
//! the `raft` action, one call per peer at a time, each carrying whatever waited for it.

use std::collections::HashMap;
use std::sync::mpsc::Receiver;
use std::thread;
use std::time::Duration;

use arrow_flight::Action;
use arrow_flight::flight_service_client::FlightServiceClient;
use tokio::sync::mpsc;
use tonic::transport::{Channel, Endpoint};

use crate::{frame_messages, http_url};

/// The most one call carries: whatever waits beyond goes in the next.
const BATCH_BYTES: usize = 16 << 20;

/// Carries `outgoing` (each message with the ID of the replica it is for) to `peers`, each
/// replica's ID and URL, until the store stops sending (it closed), on a thread of its own. A
/// message for a replica not among them is dropped.
pub(crate) fn carry(
    outgoing: Receiver<(u64, Vec<u8>)>,
    peers: &[(u64, String)],
) -> std::io::Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .thread_name("kermodes-carrier")
        .enable_all()
        .build()?;
    let mut queues = HashMap::new();
    for (id, url) in peers {
        let endpoint = Endpoint::from_shared(http_url(url))
            .map_err(|e| std::io::Error::other(format!("replica {id} at {url}: {e}")))?
            .connect_timeout(Duration::from_secs(1))
            .timeout(Duration::from_secs(5))
            .tcp_nodelay(true);
        // Connects when first used, and again after it breaks: a peer may start later.
        let channel = {
            let _inside = runtime.enter();
            endpoint.connect_lazy()
        };
        let (queue, queued) = mpsc::unbounded_channel();
        runtime.spawn(deliver(*id, url.clone(), channel, queued));
        queues.insert(*id, queue);
    }
    thread::Builder::new()
        .name("kermodes-carrier".into())
        .spawn(move || {
            for (to, message) in outgoing {
                if let Some(queue) = queues.get(&to) {
                    let _ = queue.send(message);
                }
            }
            // The store closed: the queues end, and each delivery with them.
            drop(queues);
            runtime.shutdown_timeout(Duration::from_secs(1));
        })?;
    Ok(())
}

/// Sends replica `to` its messages as they come, each call all that waited meanwhile. A call
/// that fails loses its messages, as a network would; Raft sends again what matters. That the
/// replica cannot be reached is said once, and again once it can.
async fn deliver(
    to: u64,
    url: String,
    channel: Channel,
    mut queued: mpsc::UnboundedReceiver<Vec<u8>>,
) {
    let mut flight = FlightServiceClient::new(channel);
    let mut unreachable = false;
    while let Some(first) = queued.recv().await {
        let mut bytes = first.len();
        let mut batch = vec![first];
        while bytes < BATCH_BYTES
            && let Ok(more) = queued.try_recv()
        {
            bytes += more.len();
            batch.push(more);
        }
        let action = Action {
            r#type: "raft".into(),
            body: frame_messages(&batch).into(),
        };
        match flight.do_action(action).await {
            Ok(_) if unreachable => {
                eprintln!("kermodes weir: replica {to} at {url} is reachable again");
                unreachable = false;
            }
            Ok(_) => {}
            Err(status) if !unreachable => {
                eprintln!(
                    "kermodes weir: replica {to} at {url} cannot be reached ({}); its messages \
                     are dropped until it can",
                    status.message()
                );
                unreachable = true;
            }
            Err(_) => {}
        }
    }
}
