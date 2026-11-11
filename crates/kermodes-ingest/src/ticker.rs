//! A synthetic market-data feed over WebSocket, for demos and the M1 acceptance test: random-walk
//! prices for a set of symbols, as JSON arrays of `{"sym", "price", "size", "ts"}` (`ts` an
//! RFC 3339 UTC time), sent every 10 ms to each client.

use std::net::{SocketAddr, TcpListener};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::Response;
use axum::routing::get;
use serde_json::json;
use tokio::sync::oneshot;

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

#[derive(Clone, Copy)]
struct Feed {
    rate: u32,
    symbols: u32,
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
    let router = Router::new().route("/", get(upgrade)).with_state(Feed {
        rate,
        symbols: symbols.max(1),
    });
    let thread = thread::Builder::new()
        .name("kermodes-ticker".into())
        .spawn(move || {
            runtime.block_on(async move {
                let listener =
                    tokio::net::TcpListener::from_std(listener).expect("a bound listener converts");
                let _ = axum::serve(listener, router)
                    .with_graceful_shutdown(async {
                        let _ = stopped.await;
                    })
                    .await;
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

async fn stream(mut socket: WebSocket, feed: Feed) {
    let mut random = Random(0x9E37_79B9_7F4A_7C15 ^ u64::from(feed.rate));
    let mut prices: Vec<f64> = (0..feed.symbols)
        .map(|i| 50.0 + f64::from(i % 200))
        .collect();
    let per_tick = (feed.rate / 100).max(1);
    let mut ticks = tokio::time::interval(Duration::from_millis(10));
    loop {
        ticks.tick().await;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let ts = rfc3339(now);
        let rows: Vec<serde_json::Value> = (0..per_tick)
            .map(|_| {
                let i = (random.next() % u64::from(feed.symbols)) as usize;
                let step = (random.next() % 2001) as f64 / 1000.0 - 1.0;
                prices[i] = (prices[i] * (1.0 + step / 1000.0)).max(0.01);
                json!({
                    "sym": format!("S{i:04}"),
                    "price": (prices[i] * 100.0).round() / 100.0,
                    "size": 1 + random.next() % 1000,
                    "ts": ts,
                })
            })
            .collect();
        let text = serde_json::Value::Array(rows).to_string();
        if socket.send(Message::Text(text.into())).await.is_err() {
            return;
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
