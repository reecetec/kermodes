//! Writes sent and not yet answered (architecture.md §7.6): a writer may have many in flight,
//! so that writes arriving during an fsync share the next one, and batching follows the load
//! with no batch size or linger to choose. In flight is bounded in bytes, so a slow store slows
//! its producer rather than filling the producer's memory.

use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

use crate::Error;

/// The most a writer has unanswered before sending another waits; a single write larger than
/// this goes alone.
pub const IN_FLIGHT_BYTES: usize = 64 << 20;

/// A write sent and not yet necessarily durable.
pub struct Pending {
    answer: Receiver<Result<bool, Error>>,
    /// What to say if the store went before answering.
    gone: String,
}

impl Pending {
    /// Waits until the write is durable (`true`) or dropped as a resend (`false`).
    pub fn wait(self) -> Result<bool, Error> {
        self.answer.recv().map_err(|_| Error::Io(self.gone))?
    }
}

/// A writer's writes in flight: the bytes not yet answered, and the first failure since the
/// writer last flushed, waited for or not.
#[derive(Default)]
pub struct InFlight {
    state: Mutex<(usize, Option<String>)>,
    answered: Condvar,
}

impl InFlight {
    /// Takes `bytes` for a write about to be sent, waiting while `IN_FLIGHT_BYTES` are
    /// unanswered: the write's `Pending`, and the `Answer` the store gives its outcome to.
    /// `gone` is what the write says if the store goes before answering it.
    pub fn send(self: &Arc<Self>, bytes: usize, gone: String) -> (Pending, Answer) {
        let mut state = self.lock();
        while state.0 > 0 && state.0 + bytes > IN_FLIGHT_BYTES {
            state = self
                .answered
                .wait(state)
                .expect("kermodes writer poisoned by an earlier panic");
        }
        state.0 += bytes;
        let (reply, answer) = sync_channel(1);
        let pending = Pending {
            answer,
            gone: gone.clone(),
        };
        let answer = Answer {
            reply,
            in_flight: self.clone(),
            bytes,
            failed: None,
            gone,
            answered: false,
        };
        (pending, answer)
    }

    /// Waits until every write sent is answered; the first that failed since the last flush,
    /// as an error.
    pub fn flush(&self) -> Result<(), Error> {
        let mut state = self.lock();
        while state.0 > 0 {
            state = self
                .answered
                .wait(state)
                .expect("kermodes writer poisoned by an earlier panic");
        }
        match state.1.take() {
            Some(failure) => Err(Error::Io(format!("a write failed: {failure}"))),
            None => Ok(()),
        }
    }

    fn lock(&self) -> MutexGuard<'_, (usize, Option<String>)> {
        self.state
            .lock()
            .expect("kermodes writer poisoned by an earlier panic")
    }
}

/// Where a write's outcome goes: to its `Pending`, and its bytes back to its writer's
/// `InFlight`. Dropped unanswered (the store went), the write fails.
pub struct Answer {
    reply: SyncSender<Result<bool, Error>>,
    in_flight: Arc<InFlight>,
    bytes: usize,
    failed: Option<String>,
    gone: String,
    answered: bool,
}

impl Answer {
    pub fn send(mut self, outcome: Result<bool, Error>) {
        if let Err(e) = &outcome {
            self.failed = Some(e.to_string());
        }
        self.answered = true;
        let _ = self.reply.send(outcome);
    }
}

impl Drop for Answer {
    fn drop(&mut self) {
        let failed = match self.failed.take() {
            Some(failed) => Some(failed),
            None if !self.answered => Some(self.gone.clone()),
            None => None,
        };
        let mut state = self.in_flight.lock();
        state.0 -= self.bytes;
        if state.1.is_none() {
            state.1 = failed;
        }
        drop(state);
        self.in_flight.answered.notify_all();
    }
}
