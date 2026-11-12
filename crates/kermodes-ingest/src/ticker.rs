//! A synthetic market-data feed over WebSocket, for demos and tests: random-walk prices for a set
//! of symbols, as JSON arrays of `{"seq", "sym", "price", "size", "ts"}` (`ts` an RFC 3339 UTC
//! time) every 10 ms. Every client gets the same ticks, numbered by `seq` in the order sent, as
//! from a real feed, so two runs reading it during a handover see the same records.

use std::net::{SocketAddr, TcpListener};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::extract::State;
use axum::extract::ws::{Message, Utf8Bytes, WebSocket, WebSocketUpgrade};
use axum::response::Response;
use axum::routing::get;
use serde_json::json;
use tokio::sync::{broadcast, oneshot};

/// A running ticker feed; stops when dropped.
pub struct Ticker {
    addr: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl Ticker {
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn stop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for Ticker {
    fn drop(&mut self) {
        self.stop();
    }
}

/// The feed's messages, as they are made.
#[derive(Clone)]
struct Feed {
    ticks: broadcast::Sender<Utf8Bytes>,
}

/// Serves a feed of about `rate` ticks a second over `symbols` symbols on `addr`.
pub fn ticker(addr: SocketAddr, rate: u32, symbols: u32) -> std::io::Result<Ticker> {
    let listener = TcpListener::bind(addr)?;
    listener.set_nonblocking(true)?;
    let addr = listener.local_addr()?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()?;
    let (shutdown, stopped) = oneshot::channel::<()>();
    // About 10 s of messages: a client further behind than that has lost its place.
    let (ticks, _) = broadcast::channel(1024);
    let router = Router::new().route("/", get(upgrade)).with_state(Feed {
        ticks: ticks.clone(),
    });
    let thread = thread::Builder::new()
        .name("kermodes-ticker".into())
        .spawn(move || {
            runtime.block_on(async move {
                let generator = tokio::spawn(generate(ticks, rate, symbols.max(1)));
                let listener =
                    tokio::net::TcpListener::from_std(listener).expect("a bound listener converts");
                let _ = axum::serve(listener, router)
                    .with_graceful_shutdown(async {
                        let _ = stopped.await;
                    })
                    .await;
                generator.abort();
            });
        })?;
    Ok(Ticker {
        addr,
        shutdown: Some(shutdown),
        thread: Some(thread),
    })
}

async fn upgrade(ws: WebSocketUpgrade, State(feed): State<Feed>) -> Response {
    ws.on_upgrade(move |socket| stream(socket, feed))
}

/// Makes the ticks every 10 ms, whether or not anyone is connected, as a market moves on.
async fn generate(ticks: broadcast::Sender<Utf8Bytes>, rate: u32, symbols: u32) {
    let mut random = Random(0x9E37_79B9_7F4A_7C15 ^ u64::from(rate));
    let mut prices: Vec<f64> = (0..symbols).map(|i| 50.0 + f64::from(i % 200)).collect();
    let per_tick = (rate / 100).max(1);
    let mut interval = tokio::time::interval(Duration::from_millis(10));
    let mut seq = 0u64;
    loop {
        interval.tick().await;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let ts = rfc3339(now);
        let rows: Vec<serde_json::Value> = (0..per_tick)
            .map(|_| {
                seq += 1;
                let i = (random.next() % u64::from(symbols)) as usize;
                let step = (random.next() % 2001) as f64 / 1000.0 - 1.0;
                prices[i] = (prices[i] * (1.0 + step / 1000.0)).max(0.01);
                json!({
                    "seq": seq,
                    "sym": format!("S{i:04}"),
                    "price": (prices[i] * 100.0).round() / 100.0,
                    "size": 1 + random.next() % 1000,
                    "ts": ts,
                })
            })
            .collect();
        // Sending fails only while nobody is connected; those ticks go unheard.
        let _ = ticks.send(serde_json::Value::Array(rows).to_string().into());
    }
}

async fn stream(mut socket: WebSocket, feed: Feed) {
    let mut ticks = feed.ticks.subscribe();
    loop {
        match ticks.recv().await {
            Ok(text) => {
                if socket.send(Message::Text(text)).await.is_err() {
                    return;
                }
            }
            // Too slow for the feed: closing tells the client it lost its place; it reconnects
            // and finds the hole in `seq`.
            Err(_) => return,
        }
    }
}

/// xorshift64*: deterministic and dependency-free; quality is irrelevant for a demo feed.
struct Random(u64);

impl Random {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
}

/// `YYYY-MM-DDTHH:MM:SS.fffffffffZ`.
fn rfc3339(since_epoch: Duration) -> String {
    let secs = since_epoch.as_secs() as i64;
    let days = secs.div_euclid(86_400);
    let rest = secs.rem_euclid(86_400);
    // Days to civil date (Howard Hinnant's `civil_from_days`).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:09}Z",
        rest / 3600,
        rest / 60 % 60,
        rest % 60,
        since_epoch.subsec_nanos()
    )
}
