//! Which replica acts (architecture.md §16.2): every replica serves the HTTP API, and one, the
//! leader, syncs, reconciles, and moves runs. The leader holds a Postgres advisory lock on a
//! connection of its own; Postgres frees the lock when that connection ends, so a replica that
//! stops or loses its node hands over without a lease to expire. The same connection hears the
//! sync requests any replica's webhook sends (`NOTIFY`).

use std::future::poll_fn;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio_postgres::{AsyncMessage, Client, NoTls};

/// The leader lock's key, the same in every replica of an installation (one database each).
const LEADER_LOCK: i64 = 0x6b6d_5f6c_6561_6472; // "km_leadr"
/// What the webhook notifies, and the leader listens to.
pub const SYNC_CHANNEL: &str = "kermodes_control_sync";
/// How often a standby tries for the lock: the longest a handover waits once the lock is free.
const CONTEND_EVERY: Duration = Duration::from_secs(1);
/// The leader checks its connection this often, and stops leading when a check fails. Postgres
/// frees the lock of a connection it finds dead within about 25 s (the keepalives below), so a
/// leader cut off from Postgres stops long before another replica can take over.
const PING_EVERY: Duration = Duration::from_secs(2);
const PING_TIMEOUT: Duration = Duration::from_secs(2);

/// This replica holds the leader lock until this is dropped or `lost` resolves.
pub struct Leadership {
    client: Client,
    closed: oneshot::Receiver<String>,
}

/// Sync requests, from the webhook on any replica.
pub type Wake = mpsc::UnboundedReceiver<()>;

impl Leadership {
    /// Waits until this replica holds the leader lock; an error means the connection failed.
    pub async fn acquire(url: &str) -> Result<(Self, Wake), String> {
        let config: tokio_postgres::Config = url
            .parse()
            .map_err(|e| format!("not a Postgres URL: {e}"))?;
        let (client, mut connection) = config
            .connect(NoTls)
            .await
            .map_err(|e| format!("cannot open the controller's database: {e}"))?;
        let (wake_tx, wake) = mpsc::unbounded_channel();
        let (closed_tx, closed) = oneshot::channel();
        tokio::spawn(async move {
            let reason = loop {
                match poll_fn(|cx| connection.poll_message(cx)).await {
                    Some(Ok(AsyncMessage::Notification(_))) => {
                        let _ = wake_tx.send(());
                    }
                    Some(Ok(_)) => {}
                    Some(Err(e)) => break e.to_string(),
                    None => break "the connection closed".to_owned(),
                }
            };
            let _ = closed_tx.send(reason);
        });
        // Postgres finds a vanished peer only by TCP keepalives, by default after hours; on
        // this session, after about 25 s.
        client
            .batch_execute(
                "SET tcp_keepalives_idle = 10; SET tcp_keepalives_interval = 5;
                 SET tcp_keepalives_count = 3",
            )
            .await
            .map_err(|e| format!("cannot set keepalives: {e}"))?;
        loop {
            let held: bool = client
                .query_one("SELECT pg_try_advisory_lock($1)", &[&LEADER_LOCK])
                .await
                .map_err(|e| format!("cannot try the leader lock: {e}"))?
                .get(0);
            if held {
                break;
            }
            tokio::time::sleep(CONTEND_EVERY).await;
        }
        client
            .batch_execute(&format!("LISTEN {SYNC_CHANNEL}"))
            .await
            .map_err(|e| format!("cannot listen for sync requests: {e}"))?;
        Ok((Self { client, closed }, wake))
    }

    /// Resolves, with why, when this replica must stop acting as leader: its connection failed,
    /// or Postgres did not answer a check in time.
    pub async fn lost(&mut self) -> String {
        loop {
            tokio::select! {
                reason = &mut self.closed => {
                    return reason.unwrap_or_else(|_| "the connection closed".to_owned());
                }
                () = tokio::time::sleep(PING_EVERY) => {
                    match tokio::time::timeout(PING_TIMEOUT, self.client.simple_query("SELECT 1"))
                        .await
                    {
                        Ok(Ok(_)) => {}
                        Ok(Err(e)) => return e.to_string(),
                        Err(_) => return "Postgres did not answer within 2 s".to_owned(),
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{LEADER_LOCK, Leadership};
    use crate::state::{State, test_database};

    #[tokio::test]
    async fn one_replica_leads_hears_every_sync_request_and_hands_over_when_it_goes() {
        let Some(url) = test_database("leader").await else {
            return;
        };
        let (mut first, mut wake) = Leadership::acquire(&url).await.unwrap();

        // The other replica waits while the first leads.
        let second = tokio::spawn({
            let url = url.clone();
            async move { Leadership::acquire(&url).await.unwrap() }
        });
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(!second.is_finished());

        // A webhook on any replica reaches the leader.
        State::open(&url)
            .await
            .unwrap()
            .request_sync()
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), wake.recv())
            .await
            .expect("the leader hears the sync request")
            .unwrap();

        // Postgres ending the leader's session (a lost node, a failover) ends its leadership,
        // and the other replica takes over.
        let (admin, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(connection);
        admin
            .execute(
                "SELECT pg_terminate_backend(pid) FROM pg_locks
                 WHERE locktype = 'advisory' AND granted
                   AND classid::bigint = ($1::bigint >> 32)
                   AND objid::bigint = ($1::bigint & 4294967295)",
                &[&LEADER_LOCK],
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), first.lost())
            .await
            .expect("the old leader learns it no longer leads");
        let (mut second, _) = tokio::time::timeout(Duration::from_secs(5), second)
            .await
            .expect("the other replica takes over")
            .unwrap();

        // A leader that stops (dropped, as on SIGTERM) frees the lock for the next at once.
        let third = tokio::spawn({
            let url = url.clone();
            async move { Leadership::acquire(&url).await.unwrap() }
        });
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert!(!third.is_finished());
        tokio::select! {
            reason = second.lost() => panic!("the leader lost its lock: {reason}"),
            () = tokio::time::sleep(Duration::from_millis(100)) => {}
        }
        drop(second);
        tokio::time::timeout(Duration::from_secs(5), third)
            .await
            .expect("the lock passes on when its leader stops")
            .unwrap();
    }
}
