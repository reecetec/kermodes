//! Writes sent and not yet answered (architecture.md §7.6): a writer may have many in flight,
//! so that writes arriving during an fsync share the next one, and batching follows the load
//! with no batch size or linger to choose. In flight is bounded in bytes, so a slow store slows
//! its producer rather than filling the producer's memory.
//!
//! Answering is on the committer's path, once per write, so it is kept to filling a slot: a
//! unit's writes give their bytes back under one lock and wake the writer's waiters once.

use std::sync::{Arc, Condvar, Mutex, MutexGuard};

use crate::Error;

/// The most a writer has unanswered before sending another waits; a single write larger than
/// this goes alone.
pub const IN_FLIGHT_BYTES: usize = 64 << 20;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .expect("kermodes writer poisoned by an earlier panic")
}

/// Where one write's outcome is put, and its `Pending` waits.
#[derive(Default)]
struct Slot {
    outcome: Mutex<Option<Result<bool, Error>>>,
    filled: Condvar,
}

impl Slot {
    fn fill(&self, outcome: Result<bool, Error>) {
        *lock(&self.outcome) = Some(outcome);
        self.filled.notify_one();
    }
}

/// A write sent and not yet necessarily durable.
pub struct Pending(Arc<Slot>);

impl Pending {
    /// Waits until the write is durable (`true`) or dropped as a resend (`false`).
    pub fn wait(self) -> Result<bool, Error> {
        let mut outcome = lock(&self.0.outcome);
        loop {
            if let Some(outcome) = outcome.take() {
                return outcome;
            }
            outcome = self
                .0
                .filled
                .wait(outcome)
                .expect("kermodes writer poisoned by an earlier panic");
        }
    }

    /// Whether the write has its answer: `wait` returns at once.
    pub fn is_answered(&self) -> bool {
        lock(&self.0.outcome).is_some()
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
    pub fn send(self: &Arc<Self>, bytes: usize, gone: &Arc<str>) -> (Pending, Answer) {
        let mut state = lock(&self.state);
        while state.0 > 0 && state.0 + bytes > IN_FLIGHT_BYTES {
            state = self
                .answered
                .wait(state)
                .expect("kermodes writer poisoned by an earlier panic");
        }
        state.0 += bytes;
        drop(state);
        let slot = Arc::new(Slot::default());
        let answer = Answer {
            slot: slot.clone(),
            in_flight: Some(self.clone()),
            bytes,
            gone: gone.clone(),
        };
        (Pending(slot), answer)
    }

    /// Waits until every write sent is answered; the first that failed since the last flush,
    /// as an error.
    pub fn flush(&self) -> Result<(), Error> {
        let mut state = lock(&self.state);
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

    /// Gives back `bytes` answered, noting `failed` if it is the first failure since a flush.
    fn release(&self, bytes: usize, failed: Option<String>) {
        let mut state = lock(&self.state);
        state.0 -= bytes;
        if state.1.is_none() {
            state.1 = failed;
        }
        drop(state);
        self.answered.notify_all();
    }
}

/// Where a write's outcome goes: to its `Pending`, and its bytes back to its writer's
/// `InFlight`. Dropped unanswered (the store went), the write fails.
pub struct Answer {
    slot: Arc<Slot>,
    /// Taken once the bytes are given back.
    in_flight: Option<Arc<InFlight>>,
    bytes: usize,
    gone: Arc<str>,
}

impl Answer {
    pub fn send(self, outcome: Result<bool, Error>) {
        Self::send_all(vec![self], outcome);
    }

    /// Gives every write of `answers` the same outcome; writes of one writer next to each
    /// other give their bytes back together.
    pub fn send_all(mut answers: Vec<Answer>, outcome: Result<bool, Error>) {
        let failed = outcome.as_ref().err().map(ToString::to_string);
        let mut outcome = Some(outcome);
        let count = answers.len();
        for (i, answer) in answers.iter().enumerate() {
            let this = match (&outcome, &failed) {
                (Some(Ok(written)), _) => Ok(*written),
                (_, Some(why)) if i + 1 < count => Err(Error::Io(why.clone())),
                // The last is given the error itself.
                _ => outcome.take().expect("the outcome, once"),
            };
            answer.slot.fill(this);
        }
        let mut run: Option<(Arc<InFlight>, usize)> = None;
        for answer in &mut answers {
            let in_flight = answer.in_flight.take().expect("bytes given back once");
            match &mut run {
                Some((current, bytes)) if Arc::ptr_eq(current, &in_flight) => {
                    *bytes += answer.bytes;
                }
                _ => {
                    if let Some((current, bytes)) = run.replace((in_flight, answer.bytes)) {
                        current.release(bytes, failed.clone());
                    }
                }
            }
        }
        if let Some((current, bytes)) = run {
            current.release(bytes, failed);
        }
    }
}

impl Drop for Answer {
    fn drop(&mut self) {
        if let Some(in_flight) = self.in_flight.take() {
            let gone = self.gone.to_string();
            self.slot.fill(Err(Error::Io(gone.clone())));
            in_flight.release(self.bytes, Some(gone));
        }
    }
}
