//! What Raft reads of the shard log (`wal`): the entries it may still need, kept in memory as
//! they were appended (their data shared, not copied), with the term and vote it last
//! persisted and the replicas of the shard.

use raft::eraftpb::{ConfState, Entry, HardState, Snapshot};
use raft::{GetEntriesContext, RaftState, Storage, StorageError};

use crate::wal::Recovered;

/// A replica of the shard: its Raft ID among `voters`, the IDs of every replica (fixed:
/// changing them is not yet supported), and the seed of its election timeouts.
#[derive(Clone, Debug)]
pub struct Replica {
    pub id: u64,
    pub voters: Vec<u64>,
    pub seed: Option<u64>,
}

impl Replica {
    /// The shard's only replica: weir on one machine.
    pub fn alone() -> Self {
        Self {
            id: 1,
            voters: vec![1],
            seed: None,
        }
    }

    /// Whether it replicates to no one: then no replica can need an entry once it is applied.
    pub(crate) fn is_alone(&self) -> bool {
        self.voters == [self.id]
    }
}

/// Raft's log as this replica holds it.
pub(crate) struct LogStore {
    /// The index and term of the entry before `entries`.
    start: (u64, u64),
    entries: Vec<Entry>,
    state: HardState,
    voters: Vec<u64>,
}

impl LogStore {
    /// The log `recovered` on open, whose entries up to `applied` the partition files hold.
    pub(crate) fn new(recovered: Recovered, applied: u64, voters: Vec<u64>) -> Self {
        let mut state = recovered.state;
        // Whatever was applied was committed; the commit index persisted may be older, since
        // Raft does not wait for it to be durable.
        state.commit = state.commit.max(applied);
        Self {
            start: recovered.start,
            entries: recovered.entries,
            state,
            voters,
        }
    }

    /// Takes entries just appended to the log, replacing any at or after the first one's index.
    pub(crate) fn append(&mut self, entries: &[Entry]) {
        let Some(first) = entries.first() else {
            return;
        };
        let keep = self
            .entries
            .partition_point(|kept| kept.index < first.index);
        self.entries.truncate(keep);
        self.entries.extend_from_slice(entries);
    }

    /// Forgets the entries up to `index`, which no replica will ask for again.
    pub(crate) fn compact(&mut self, index: u64) {
        let gone = self.entries.partition_point(|kept| kept.index <= index);
        if let Some(last) = gone.checked_sub(1).map(|at| &self.entries[at]) {
            self.start = (last.index, last.term);
            self.entries.drain(..gone);
        }
    }

    pub(crate) fn last_index(&self) -> u64 {
        self.entries.last().map_or(self.start.0, |last| last.index)
    }
}

impl Storage for LogStore {
    fn initial_state(&self) -> raft::Result<RaftState> {
        Ok(RaftState::new(
            self.state.clone(),
            ConfState {
                voters: self.voters.clone(),
                ..ConfState::default()
            },
        ))
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        _: GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        if low <= self.start.0 {
            return Err(raft::Error::Store(StorageError::Compacted));
        }
        assert!(
            high <= self.last_index() + 1,
            "Raft asked for entries up to {high}, past the log's last ({})",
            self.last_index()
        );
        let offset = self.start.0 + 1;
        let mut entries = self.entries[(low - offset) as usize..(high - offset) as usize].to_vec();
        raft::util::limit_size(&mut entries, max_size.into());
        Ok(entries)
    }

    fn term(&self, index: u64) -> raft::Result<u64> {
        if index == self.start.0 {
            return Ok(self.start.1);
        }
        if index < self.start.0 {
            return Err(raft::Error::Store(StorageError::Compacted));
        }
        match self.entries.get((index - self.start.0 - 1) as usize) {
            Some(entry) => Ok(entry.term),
            None => Err(raft::Error::Store(StorageError::Unavailable)),
        }
    }

    fn first_index(&self) -> raft::Result<u64> {
        Ok(self.start.0 + 1)
    }

    fn last_index(&self) -> raft::Result<u64> {
        Ok(LogStore::last_index(self))
    }

    /// None to give: a replica keeps every entry another may need (a shard of one forgets them
    /// once applied, and has no one to give them to). Catching up a replica from a peer's
    /// partition files instead (architecture.md §7.6) is the next step.
    fn snapshot(&self, _: u64, _: u64) -> raft::Result<Snapshot> {
        Err(raft::Error::Store(
            StorageError::SnapshotTemporarilyUnavailable,
        ))
    }
}
