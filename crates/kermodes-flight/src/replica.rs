//! A replica of a shard as its process runs it (`kermodes weir --replica`, architecture.md
//! §7.6): its store served over Flight, its Raft messages carried to the other replicas, and,
//! when it is too far behind its shard's log, a peer's partition files copied before it goes on.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use kermodes_weir::{Error, Replica, Store, machine_disk, replica_name, slot};

use crate::carrier::carry;
use crate::client::Client;
use crate::server::serve;

/// How often the replica looks whether it was stopped or wants a snapshot.
const WATCH: Duration = Duration::from_millis(100);

/// Runs `replica` of the shard whose replicas serve at `peers` (replica `n` at `peers[n - 1]`),
/// its store in `root` with the catalog every replica shares in Postgres at `catalog`, the
/// tables of `branch`; serving on `addr` until `stop` is set.
pub fn serve_replica(
    root: &Path,
    catalog: &str,
    branch: &str,
    replica: Replica,
    peers: &[String],
    addr: SocketAddr,
    stop: &AtomicBool,
) -> Result<(), Error> {
    let (id, count) = (replica.id, peers.len() as u64);
    if id == 0 || id > count || replica.voters != (1..=count).collect::<Vec<_>>() {
        return Err(Error::Name(format!(
            "replica {id} of a shard of {count}: replicas are numbered 1 to {count}, each at its \
             place in the peers"
        )));
    }
    let others: Vec<(u64, String)> = (1..=count)
        .zip(peers)
        .filter(|(peer, _)| *peer != id)
        .map(|(peer, url)| (peer, url.clone()))
        .collect();
    // The other replicas, as a client knows them: one asks whichever leads.
    let hosts: Vec<&str> = others
        .iter()
        .map(|(_, url)| url.strip_prefix("grpc://").unwrap_or(url))
        .collect();
    let shard_url = format!("grpc://{}", hosts.join(","));
    loop {
        let store = Arc::new(Store::open_shard(root, catalog, branch, replica.clone())?);
        let me = replica_name(store.replica_id());
        let outgoing = store
            .take_outgoing()
            .expect("a store just opened has its messages to carry");
        carry(outgoing, &others)
            .map_err(|e| Error::Io(format!("cannot carry {me}'s messages: {e}")))?;
        let mut server = serve(store.clone(), addr)
            .map_err(|e| Error::Io(format!("cannot listen on {addr}: {e}")))?;
        eprintln!("kermodes weir: {me} of {count}, serving {}", server.addr());
        if !store.is_member() {
            join(Arc::downgrade(&store), shard_url.clone());
        }
        let wanted = loop {
            if stop.load(Ordering::Acquire) {
                hand_over(&store, &me);
                server.stop();
                close(store)?;
                return Ok(());
            }
            if let Some(wanted) = store.snapshot_wanted() {
                break wanted;
            }
            thread::sleep(WATCH);
        };
        server.stop();
        close(store)?;
        let from = &peers[slot(wanted.from) as usize - 1];
        let donor = replica_name(wanted.from);
        eprintln!(
            "kermodes weir: {me} is too far behind its shard's log; copying {donor}'s partition \
             files from {from}"
        );
        let started = Instant::now();
        let installed = Client::connect(from).and_then(|source| {
            Store::install_snapshot(root, machine_disk(root), replica.limits, &wanted, &source)
        });
        match installed {
            Ok(()) => eprintln!(
                "kermodes weir: {me} copied {donor}'s files in {:.1} s",
                started.elapsed().as_secs_f64()
            ),
            // Opened again as it was, it is handed a snapshot again.
            Err(e) => {
                eprintln!("kermodes weir: {me} could not copy its files: {e}");
                thread::sleep(Duration::from_secs(1));
            }
        }
    }
}

/// A new incarnation of a replica whose disk was lost asks its shard's leader, at `shard_url`,
/// to take it in, every second until it is a member (`Store::join`), on a thread of its own.
/// It holds the store only while it asks, so closing the store is never kept waiting.
fn join(store: std::sync::Weak<Store>, shard_url: String) {
    let asking = move || {
        let mut said = false;
        while let Some(store) = store.upgrade() {
            if store.is_member() {
                eprintln!(
                    "kermodes weir: {} is a member of its shard",
                    replica_name(store.replica_id())
                );
                return;
            }
            let id = store.replica_id();
            drop(store);
            if !said {
                eprintln!(
                    "kermodes weir: {} is new to its shard (its disk replaced a lost one); asking \
                     its leader to take it in",
                    replica_name(id)
                );
                said = true;
            }
            if let Err(e) = Client::connect(&shard_url).and_then(|shard| shard.join(id)) {
                eprintln!("kermodes weir: {} cannot join yet: {e}", replica_name(id));
            }
            thread::sleep(Duration::from_secs(1));
        }
    };
    let _ = thread::Builder::new()
        .name("kermodes-join".into())
        .spawn(asking);
}

/// The longest a stopping leader waits for another replica to take its lead (`Store::hand_over`)
/// before it stops anyway, leaving the others to elect one.
const HAND_OVER: Duration = Duration::from_secs(2);

/// Before a leader stops (a rollout, a drain), it hands its lead to the replica furthest along,
/// so writers wait for that instead of an election. Asked again every half second, it tries
/// another replica, in case the first went.
fn hand_over(store: &Store, me: &str) {
    if !store.is_leader() {
        return;
    }
    let started = Instant::now();
    let mut asked = Duration::ZERO;
    while store.is_leader() && started.elapsed() < HAND_OVER {
        if started.elapsed() >= asked {
            store.hand_over();
            asked += Duration::from_millis(500);
        }
        thread::sleep(Duration::from_millis(5));
    }
    if store.is_leader() {
        eprintln!(
            "kermodes weir: {me} could not hand over its lead in {} s; stopping, so the others \
             elect a leader",
            HAND_OVER.as_secs()
        );
    } else {
        eprintln!(
            "kermodes weir: {me} handed over its lead in {} ms",
            started.elapsed().as_millis()
        );
    }
}

/// Closes `store` once nothing else holds it: the server's last requests let go of it as the
/// server stops. One still held is not closed, and nothing may be copied into its files: the
/// process ends, to start again clean.
fn close(mut store: Arc<Store>) -> Result<(), Error> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match Arc::try_unwrap(store) {
            Ok(store) => {
                drop(store);
                return Ok(());
            }
            Err(shared) if Instant::now() < deadline => {
                store = shared;
                thread::sleep(WATCH);
            }
            Err(_) => {
                return Err(Error::Io(
                    "the store is still in use 10 s after its server stopped".into(),
                ));
            }
        }
    }
}
