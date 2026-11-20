//! What Raft reads of the shard log (`wal`): the entries it may still need, the latest kept in
//! memory as they were appended (their data shared, not copied) and the rest read back from the
//! log's segments, with the term and vote it last persisted and the replicas of the shard.

use std::path::PathBuf;

use bytes::Bytes;
use raft::eraftpb::{ConfState, Entry, HardState, Snapshot};
use raft::{GetEntriesContext, RaftState, Storage, StorageError};

use crate::Error;
use crate::wal::{self, Recovered, Wal};

/// A replica of the shard: its Raft ID among `voters`, the IDs of every replica (fixed:
/// changing them is not yet supported), the seed of its election timeouts, and how much log
/// it keeps.
#[derive(Clone, Debug)]
pub struct Replica {
    pub id: u64,
    pub voters: Vec<u64>,
    pub seed: Option<u64>,
    pub limits: LogLimits,
}

impl Replica {
    /// The shard's only replica: weir on one machine.
    pub fn alone() -> Self {
        Self {
            id: 1,
            voters: vec![1],
            seed: None,
            limits: LogLimits::default(),
        }
    }

    /// Whether it replicates to no one: then no replica can need an entry once it is applied.
    pub(crate) fn is_alone(&self) -> bool {
        self.voters == [self.id]
    }
}

/// How much of the shard log a replica keeps.
#[derive(Clone, Copy, Debug)]
pub struct LogLimits {
    /// A segment takes entries until it holds this many bytes.
    pub segment_bytes: u64,
    /// Of segments the partition files already hold, as many bytes as this are kept for
    /// replicas behind: one further behind catches up from a peer's partition files instead.
    pub retain_bytes: u64,
}

impl Default for LogLimits {
    fn default() -> Self {
        Self {
            segment_bytes: wal::SEGMENT_BYTES,
            retain_bytes: 4 << 30,
        }
    }
}

/// Raft's log as this replica holds it.
pub(crate) struct LogStore {
    wal: Wal,
    /// The index and term of the entry before the log's first.
    start: (u64, u64),
    /// The first index of each term in the log (`wal::term_at`).
    terms: Vec<(u64, u64)>,
    /// The latest entries, at least every one not yet applied; the rest are read back from
    /// the log.
    cache: Vec<Entry>,
    cache_bytes: u64,
    last: u64,
    state: HardState,
    /// This replica's ID, which a snapshot it gives names (`snapshot`).
    id: u64,
    voters: Vec<u64>,
}

impl LogStore {
    /// The log `recovered` on opening `wal`, whose entries up to `applied` the partition files
    /// hold.
    pub(crate) fn new(wal: Wal, recovered: Recovered, applied: u64, replica: &Replica) -> Self {
        let mut state = recovered.state;
        // Whatever was applied was committed; the commit index persisted may be older, since
        // Raft does not wait for it to be durable.
        state.commit = state.commit.max(applied);
        let last = wal.last_index();
        let cache_bytes = recovered.entries.iter().map(|e| e.data.len() as u64).sum();
        Self {
            wal,
            start: recovered.start,
            terms: recovered.terms,
            cache: recovered.entries,
            cache_bytes,
            last,
            state,
            id: replica.id,
            voters: replica.voters.clone(),
        }
    }

    /// Appends `entries` to the log (replacing any at or after the first one's index) with
    /// Raft's `state`, durably, and keeps them in memory.
    pub(crate) fn persist(
        &mut self,
        entries: &[Entry],
        state: Option<&HardState>,
    ) -> Result<(), Error> {
        self.wal.append(entries, state)?;
        let Some(first) = entries.first() else {
            return Ok(());
        };
        let keep = self.cache.partition_point(|kept| kept.index < first.index);
        for gone in self.cache.drain(keep..) {
            self.cache_bytes -= gone.data.len() as u64;
        }
        for entry in entries {
            wal::note_term(&mut self.terms, entry.index, entry.term);
            self.cache_bytes += entry.data.len() as u64;
        }
        self.cache.extend_from_slice(entries);
        self.last = entries[entries.len() - 1].index;
        Ok(())
    }

    /// Forgets from memory the entries up to `index`: the log still has them.
    pub(crate) fn compact_cache(&mut self, index: u64) {
        let gone = self.cache.partition_point(|kept| kept.index <= index);
        for entry in self.cache.drain(..gone) {
            self.cache_bytes -= entry.data.len() as u64;
        }
    }

    /// The bytes of entries kept in memory.
    pub(crate) fn cache_bytes(&self) -> u64 {
        self.cache_bytes
    }

    /// Deletes what the log no longer needs to keep (`Wal::trim`): the segments' files, for
    /// the caller to delete outside whatever lock it holds.
    pub(crate) fn trim(&mut self, flushed: u64, needed: u64, retain: u64) -> Vec<PathBuf> {
        let (gone, start) = self.wal.trim(flushed, needed, retain);
        if start.0 > self.start.0 {
            // The entry's term as the log holds it: the segment's record of it may be older
            // than an entry that replaced it (`Wal::open`).
            let term = wal::term_at(&self.terms, start.0).unwrap_or(start.1);
            self.start = (start.0, term);
            // The run of terms that covers the new first entry, and those after.
            let covering = self
                .terms
                .partition_point(|&(first, _)| first <= start.0 + 1);
            self.terms.drain(..covering.saturating_sub(1));
            self.compact_cache(start.0);
        }
        gone
    }

    fn cache_first(&self) -> u64 {
        self.cache
            .first()
            .map_or(self.last + 1, |first| first.index)
    }
}

fn unavailable(e: Error) -> raft::Error {
    raft::Error::Store(StorageError::Other(Box::new(e)))
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
            high <= self.last + 1,
            "Raft asked for entries up to {high}, past the log's last ({})",
            self.last
        );
        let max_size = max_size.into();
        let cached = self.cache_first();
        let mut entries = if low < cached {
            // Behind what is in memory: read back from the log, and on into memory if they fit.
            let mut read = self
                .wal
                .read(low, high.min(cached), max_size.unwrap_or(u64::MAX))
                .map_err(unavailable)?;
            if read.last().is_some_and(|last| last.index + 1 == cached) && high > cached {
                read.extend_from_slice(&self.cache[..(high - cached) as usize]);
            }
            read
        } else {
            self.cache[(low - cached) as usize..(high - cached) as usize].to_vec()
        };
        raft::util::limit_size(&mut entries, max_size);
        Ok(entries)
    }

    fn term(&self, index: u64) -> raft::Result<u64> {
        if index == self.start.0 {
            return Ok(self.start.1);
        }
        if index < self.start.0 {
            return Err(raft::Error::Store(StorageError::Compacted));
        }
        if index > self.last {
            return Err(raft::Error::Store(StorageError::Unavailable));
        }
        wal::term_at(&self.terms, index).ok_or(raft::Error::Store(StorageError::Unavailable))
    }

    fn first_index(&self) -> raft::Result<u64> {
        Ok(self.start.0 + 1)
    }

    fn last_index(&self) -> raft::Result<u64> {
        Ok(self.last)
    }

    /// A follower needs entries from before the log's start: it is handed the start, and this
    /// replica's ID, to copy its partition files from (`snapshot`), which hold everything up
    /// to there. Raft asking for one past the start (a follower that asked itself) waits.
    fn snapshot(&self, request_index: u64, _: u64) -> raft::Result<Snapshot> {
        if request_index > self.start.0 {
            return Err(raft::Error::Store(
                StorageError::SnapshotTemporarilyUnavailable,
            ));
        }
        let mut snapshot = Snapshot {
            data: Bytes::copy_from_slice(&self.id.to_le_bytes()),
            ..Snapshot::default()
        };
        let meta = snapshot.mut_metadata();
        meta.index = self.start.0;
        meta.term = self.start.1;
        meta.mut_conf_state().voters = self.voters.clone();
        Ok(snapshot)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use raft::Storage;
    use raft::eraftpb::Entry;

    use super::{LogStore, Replica};
    use crate::Error;
    use crate::disk::real;
    use crate::wal::{Logged, Wal};

    fn entries(indexes: std::ops::RangeInclusive<u64>, term: u64) -> Vec<Entry> {
        indexes
            .map(|index| Entry {
                index,
                term,
                data: Bytes::from(format!("{index}@{term}")),
                ..Entry::default()
            })
            .collect()
    }

    /// The entry before the log's first has the term it was last written with, once the log
    /// is trimmed to it and once it is reopened: a segment records the entry before its first
    /// as it was when the segment began, and a new leader may have replaced that entry since,
    /// in that segment. A wrong term there makes the leader's appends fail forever.
    #[test]
    fn the_logs_start_has_the_term_of_the_entry_that_replaced_it() {
        let dir = std::env::temp_dir().join(format!("kermodes-replica-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let disk = real(&dir.join("fsync-failed"));
        let none = &mut |_: Logged| -> Result<(), Error> { unreachable!() };
        let (wal, recovered) = Wal::open(&disk, &dir, &dir, 4096, 0, 0, none).unwrap();
        let mut store = LogStore::new(wal, recovered, 0, &Replica::alone());
        store.persist(&entries(1..=10, 1), None).unwrap();
        drop(store);
        // Reopened, it writes a new segment, which begins after 10 at term 1; then a new
        // leader replaces 10 on, and they commit, so the first segment goes.
        let (wal, recovered) = Wal::open(&disk, &dir, &dir, 4096, 0, 0, none).unwrap();
        let mut store = LogStore::new(wal, recovered, 0, &Replica::alone());
        store.persist(&entries(10..=15, 2), None).unwrap();
        for gone in store.trim(15, 15, 0) {
            std::fs::remove_file(gone).unwrap();
        }
        assert_eq!(store.first_index().unwrap(), 11);
        assert_eq!(store.term(10).unwrap(), 2);
        drop(store);
        let (wal, recovered) = Wal::open(&disk, &dir, &dir, 4096, 15, 15, none).unwrap();
        let store = LogStore::new(wal, recovered, 15, &Replica::alone());
        assert_eq!(store.first_index().unwrap(), 11);
        assert_eq!(store.term(10).unwrap(), 2);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
