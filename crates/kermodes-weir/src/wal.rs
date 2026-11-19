//! The shard log (architecture.md §7.6): Raft's log of every write of the store's tables, in one
//! total order, fsynced (on a quorum of replicas) before a write is acknowledged. It is the only
//! durable copy of a write until the partition files derived from it are flushed; then the log
//! below that point is deleted.
//!
//! ```text
//! {root}/weir/log/{name, 20 digits}.log    segments, each preallocated
//! {root}/weir/log/applied                  the partition files hold every entry up to this index
//! ```
//!
//! A segment is an 8-byte header, then frames, each: length (u32, of what follows the CRC),
//! CRC-32 (u32) of what follows it, kind (u8), body. Everything little-endian.
//!
//! - START, every segment's first frame: the index and term (u64 each) of the entry before the
//!   segment's first, which is where the segment takes up the log.
//! - STATE: Raft's term, vote, and commit index (u64 each). The last one read is Raft's; each
//!   segment starts with the latest, so deleting older segments loses none.
//! - ENTRY: index (u64), term (u64), Raft entry type (u8), data. An index at or below the last
//!   one read replaces that entry and every one after it: a new leader overwrote entries that
//!   were never committed.
//! - SHAPE: a number (u32, within the segment) and the columns of writes as their data has them
//!   (a schema message), before the first entry in the segment that uses them.
//! - SHAPED: an ENTRY whose data is a write, with the number of its SHAPE where its data has
//!   the schema message: index, term, entry type, shape number (u32), then the data before the
//!   schema message's length and after the message. Read back, it is the ENTRY it stands for. A
//!   group's writes mostly share their columns, and those bytes on every entry would cost each
//!   fsync more: on preallocated files, the first write to each block is a change of the file's
//!   extents, which `fdatasync` must journal.
//!
//! The log ends at the first frame that is empty (the preallocated zeros), fails its CRC, or is
//! an entry past the next index; a crash leaves nothing after that point that was ever
//! acknowledged. Opening the log starts a new segment, so a torn tail is never written over.
//! A segment's name is above every earlier one's and above the index before its first entry,
//! so the segments read in name order, and the one after a segment is named at most one past
//! that segment's last entry.
//!
//! An entry's data is a write, whole in itself so that any replica reads it alone: kind (u8,
//! `WRITE`), table ID (i64), ingest time (i64, ns), seq (u64), epoch (u64), change kind (u8),
//! source (u16 length, then bytes), the columns with their IDs as an Arrow IPC schema message
//! (u32 length, then bytes), then the rows as an Arrow IPC record batch message. A leader's
//! first entry of its term has no data.
//!
//! Format 1 (header version 1), weir's log before Raft, is still read: its segments hold the
//! writes (and shapes) of a single writer, numbered from 1, all committed. They are replayed as
//! entries of term 0 and deleted, like any segment, once the partition files hold them.

use std::collections::HashMap;
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};

use arrow::buffer::Buffer;
use arrow::ipc::reader::StreamDecoder;
use arrow::ipc::writer::{
    DictionaryTracker, IpcDataGenerator, IpcWriteContext, IpcWriteOptions, write_message,
};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use raft::eraftpb::{Entry, EntryType, HardState};

use crate::disk::{self, DiskFile, DiskRef};
use crate::shape::Shape;
use crate::{Change, Error, SourceId};

const HEADER: &[u8; 8] = b"KMLOG\x00\x00\x02";
const HEADER_V1: &[u8; 8] = b"KMLOG\x00\x00\x01";
/// A segment takes entries until it reaches this size; entries larger than what is left go to
/// a new segment, which grows past it if they alone are larger.
const SEGMENT_BYTES: u64 = 256 << 20;
/// Length and CRC.
const FRAME: usize = 8;

const START: u8 = 1;
const STATE: u8 = 2;
const ENTRY: u8 = 3;
const SHAPE: u8 = 4;
const SHAPED: u8 = 5;

/// An entry's data: a write of a table's rows.
const WRITE: u8 = 1;

/// Format 1's entries.
const V1_SHAPE: u8 = 1;
const V1_BATCH: u8 = 2;

/// A write, to encode as an entry's data.
pub(crate) struct Append<'a> {
    pub table: i64,
    /// The columns `batch` has with their IDs, as a schema message (`partition::schema_header`
    /// of `Shape::segment_schema`).
    pub schema: &'a [u8],
    pub source: &'a SourceId,
    pub seq: u64,
    pub epoch: u64,
    pub change: Change,
    pub ingest_ns: i64,
    pub batch: &'a RecordBatch,
}

/// A write read back from the log.
pub(crate) struct Logged {
    pub lsn: u64,
    pub table: i64,
    pub shape: Shape,
    pub source: SourceId,
    pub seq: u64,
    pub epoch: u64,
    pub change: Change,
    pub ingest_ns: i64,
    pub batch: RecordBatch,
}

/// The log as opened: Raft's state and the entries kept.
pub(crate) struct Recovered {
    /// The index and term of the entry before `entries`: Raft's log starts after it.
    pub start: (u64, u64),
    /// In index order, every one after `start`.
    pub entries: Vec<Entry>,
    pub state: HardState,
}

/// The log, open for appending.
pub(crate) struct Wal {
    disk: DiskRef,
    dir: PathBuf,
    /// The segment taking entries, where the next frame goes in it, and where its first entry
    /// went (after its START and STATE).
    file: Box<dyn DiskFile>,
    offset: u64,
    first_entry: u64,
    /// Each segment's name, the one taking entries last.
    segments: Vec<u64>,
    /// The index and term of the last entry appended.
    last: (u64, u64),
    /// Raft's state as last appended: a new segment starts with it.
    state: HardState,
    /// The schema messages the open segment has as SHAPEs, by number.
    shapes: Vec<Bytes>,
    /// Set once a write failed: what is on disk past the last good frame is unknown, so nothing
    /// more is appended until the store is opened again.
    broken: Option<String>,
}

impl Wal {
    /// Opens the log in `dir` (creating it, under the store's directory `root`). Hands each
    /// format 1 write after `after` (what the partition files hold) to `each`, in order; returns
    /// Raft's state and the entries after `keep_after`. Appending continues in a new segment.
    pub(crate) fn open(
        disk: &DiskRef,
        root: &Path,
        dir: &Path,
        after: u64,
        keep_after: u64,
        each: &mut dyn FnMut(Logged) -> Result<(), Error>,
    ) -> Result<(Self, Recovered), Error> {
        // Its directory durable before anything in it is acknowledged: a segment's fsync and
        // its own directory's make nothing durable in a directory a power loss takes away.
        for parent in disk::create_dirs(&**disk, root, dir).map_err(|e| Error::io(dir, e))? {
            disk.sync_dir(&parent).map_err(|e| Error::io(&parent, e))?;
        }
        let mut names: Vec<u64> = disk
            .list(dir)
            .map_err(|e| Error::io(dir, e))?
            .iter()
            .filter_map(|name| name.strip_suffix(".log")?.parse().ok())
            .collect();
        names.sort_unstable();
        let mut reading = Reading {
            keep_after,
            start: (after, 0),
            entries: Vec::new(),
            last: None,
            state: HardState::default(),
        };
        let mut kept = Vec::with_capacity(names.len() + 1);
        for &name in &names {
            let path = segment_path(dir, name);
            if read_segment(disk, &path, name, after, &mut reading, each)? {
                // What is replayed must be durable before anything is built on it: a process
                // that was killed leaves its last entries in the page cache, read here, and lost
                // to a later power loss unless fsynced now.
                disk.open(&path, false)
                    .and_then(|file| file.sync_data())
                    .map_err(|e| Error::io(&path, e))?;
                kept.push(name);
            } else {
                // Created, then the process died before its start was durable: it holds
                // nothing, and its name may be the next segment's.
                disk.remove_file(&path).map_err(|e| Error::io(&path, e))?;
            }
        }
        let last = reading.last.unwrap_or((after, 0));
        if last.0 < after {
            return Err(Error::Corrupt(format!(
                "{}: the log ends at index {}, but the partition files hold up to {after}",
                dir.display(),
                last.0
            )));
        }
        let name = (last.0 + 1).max(kept.last().map_or(0, |newest| newest + 1));
        let (file, first_entry) = start_segment(disk, dir, name, last, &reading.state)?;
        kept.push(name);
        let recovered = Recovered {
            start: reading.start,
            entries: reading.entries,
            state: reading.state.clone(),
        };
        let wal = Self {
            disk: disk.clone(),
            dir: dir.to_owned(),
            file,
            offset: first_entry,
            first_entry,
            segments: kept,
            last,
            state: reading.state,
            shapes: Vec::new(),
            broken: None,
        };
        Ok((wal, recovered))
    }

    /// Appends `entries` (replacing any at or after the first one's index) and Raft's `state`,
    /// in order, then fsyncs them: durable when this returns.
    pub(crate) fn append(
        &mut self,
        entries: &[Entry],
        state: Option<&HardState>,
    ) -> Result<(), Error> {
        if let Some(why) = &self.broken {
            return Err(Error::Io(format!("the log cannot be written: {why}")));
        }
        let needed: u64 = entries
            .iter()
            .map(|entry| entry.data.len() as u64 + 32)
            .sum::<u64>()
            + 64;
        if self.offset > self.first_entry && self.offset + needed > SEGMENT_BYTES {
            self.roll()?;
        }
        let mut buffer = Vec::with_capacity(needed as usize);
        for entry in entries {
            let Some((head, schema, rows)) = split(&entry.data) else {
                entry_frame(&mut buffer, ENTRY, entry, &[&entry.data]);
                continue;
            };
            let number = match self.shapes.iter().position(|known| *known == schema) {
                Some(at) => at as u32,
                None => {
                    let number = self.shapes.len() as u32;
                    frame(&mut buffer, SHAPE, &[&number.to_le_bytes(), &schema]);
                    self.shapes.push(schema);
                    number
                }
            };
            entry_frame(
                &mut buffer,
                SHAPED,
                entry,
                &[&number.to_le_bytes(), &head, &rows],
            );
        }
        if let Some(state) = state {
            frame(&mut buffer, STATE, &[&state_body(state)]);
        }
        let written = self
            .file
            .write_all(&buffer)
            .and_then(|()| self.file.sync_data());
        if let Err(e) = written {
            let path = segment_path(&self.dir, self.segments[self.segments.len() - 1]);
            let why = format!("{}: {e}", path.display());
            self.broken = Some(why.clone());
            return Err(Error::Io(why));
        }
        self.offset += buffer.len() as u64;
        if let Some(entry) = entries.last() {
            self.last = (entry.index, entry.term);
        }
        if let Some(state) = state {
            self.state = state.clone();
        }
        Ok(())
    }

    /// Forgets the segments whose every entry is at or below `applied` (the partition files hold
    /// them now), never the one taking entries; their files, for the caller to delete outside
    /// whatever lock it holds. A segment's entries are at most one below the next's name.
    pub(crate) fn trim(&mut self, applied: u64) -> Vec<PathBuf> {
        let mut gone = Vec::new();
        while self.segments.len() > 1 && self.segments[1] <= applied + 1 {
            gone.push(segment_path(&self.dir, self.segments.remove(0)));
        }
        gone
    }

    fn roll(&mut self) -> Result<(), Error> {
        let name = (self.last.0 + 1).max(self.segments[self.segments.len() - 1] + 1);
        let (file, first_entry) =
            start_segment(&self.disk, &self.dir, name, self.last, &self.state)?;
        self.file = file;
        self.offset = first_entry;
        self.first_entry = first_entry;
        self.segments.push(name);
        self.shapes.clear();
        Ok(())
    }
}

fn segment_path(dir: &Path, name: u64) -> PathBuf {
    dir.join(format!("{name:020}.log"))
}

/// A new segment, taking up the log after `last`: preallocated (so an fsync need not also
/// commit the file's growth), its header, start, and `state` durable, and its directory entry.
/// Where its first entry goes.
fn start_segment(
    disk: &DiskRef,
    dir: &Path,
    name: u64,
    last: (u64, u64),
    state: &HardState,
) -> Result<(Box<dyn DiskFile>, u64), Error> {
    let path = segment_path(dir, name);
    let mut file = disk.create(&path, true).map_err(|e| Error::io(&path, e))?;
    file.allocate(SEGMENT_BYTES)
        .map_err(|e| Error::Io(format!("{}: cannot preallocate: {e}", path.display())))?;
    let mut head = HEADER.to_vec();
    frame(
        &mut head,
        START,
        &[&last.0.to_le_bytes(), &last.1.to_le_bytes()],
    );
    frame(&mut head, STATE, &[&state_body(state)]);
    file.write_all(&head)
        .and_then(|()| file.sync_all())
        .map_err(|e| Error::io(&path, e))?;
    disk.sync_dir(dir).map_err(|e| Error::io(dir, e))?;
    Ok((file, head.len() as u64))
}

/// A frame of `kind` whose body is `parts`, each copied once, into `buffer`.
fn frame(buffer: &mut Vec<u8>, kind: u8, parts: &[&[u8]]) {
    let length: usize = parts.iter().map(|part| part.len()).sum();
    let mut crc = crc32fast::Hasher::new();
    crc.update(&[kind]);
    for part in parts {
        crc.update(part);
    }
    buffer.extend_from_slice(&(1 + length as u32).to_le_bytes());
    buffer.extend_from_slice(&crc.finalize().to_le_bytes());
    buffer.push(kind);
    for part in parts {
        buffer.extend_from_slice(part);
    }
}

/// An ENTRY or SHAPED frame of `entry`, the rest of its body `parts`.
fn entry_frame(buffer: &mut Vec<u8>, kind: u8, entry: &Entry, parts: &[&[u8]]) {
    let mut head = [0; 17];
    head[..8].copy_from_slice(&entry.index.to_le_bytes());
    head[8..16].copy_from_slice(&entry.term.to_le_bytes());
    head[16] = entry.get_entry_type() as u8;
    let mut all = Vec::with_capacity(parts.len() + 1);
    all.push(head.as_slice());
    all.extend_from_slice(parts);
    frame(buffer, kind, &all);
}

/// A write's data (`encode`) in three: up to its schema message's length, the message, and
/// what follows; `None` for data that is not a write.
fn split(data: &Bytes) -> Option<(Bytes, Bytes, Bytes)> {
    let head = head_length(data)?;
    let length = u32::from_le_bytes(data.get(head..head + 4)?.try_into().ok()?) as usize;
    let rows = head + 4 + length;
    (data.len() >= rows).then(|| {
        (
            data.slice(..head),
            data.slice(head + 4..rows),
            data.slice(rows..),
        )
    })
}

/// How much of a write's data comes before its schema message's length: kind, table, ingest
/// time, seq, epoch, change kind, and source (`encode`).
fn head_length(data: &[u8]) -> Option<usize> {
    if data.first() != Some(&WRITE) {
        return None;
    }
    let source = u16::from_le_bytes(data.get(34..36)?.try_into().ok()?) as usize;
    (data.len() >= 36 + source).then_some(36 + source)
}

/// The data `split` cut in three, whole again.
fn join(head: &[u8], schema: &[u8], rest: &[u8]) -> Bytes {
    let mut data = Vec::with_capacity(head.len() + 4 + schema.len() + rest.len());
    data.extend_from_slice(head);
    data.extend_from_slice(&(schema.len() as u32).to_le_bytes());
    data.extend_from_slice(schema);
    data.extend_from_slice(rest);
    Bytes::from(data)
}

fn state_body(state: &HardState) -> [u8; 24] {
    let mut body = [0; 24];
    body[..8].copy_from_slice(&state.term.to_le_bytes());
    body[8..16].copy_from_slice(&state.vote.to_le_bytes());
    body[16..].copy_from_slice(&state.commit.to_le_bytes());
    body
}

fn word(bytes: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(bytes[at..at + 8].try_into().expect("8 bytes"))
}

/// What reading the segments in order has found so far.
struct Reading {
    keep_after: u64,
    start: (u64, u64),
    entries: Vec<Entry>,
    /// The index and term of the last entry read; `None` before the first segment.
    last: Option<(u64, u64)>,
    state: HardState,
}

impl Reading {
    /// Takes the entry at `index`, replacing any at or after it; `false` if it is past the next
    /// index, where the log ends.
    fn entry(&mut self, index: u64, term: u64, entry: impl FnOnce() -> Entry) -> bool {
        let (last, _) = self.last.expect("a segment starts before its entries");
        if index == 0 || index > last + 1 {
            return false;
        }
        let keep = self.entries.partition_point(|kept| kept.index < index);
        self.entries.truncate(keep);
        self.last = Some((index, term));
        if index > self.keep_after {
            self.entries.push(entry());
        } else if index == self.keep_after {
            self.start = (index, term);
        }
        true
    }

    /// Takes up the log at a segment's start: where the first segment begins it, and where each
    /// later one must continue it.
    fn start_at(&mut self, path: &Path, after: u64, at: (u64, u64)) -> Result<(), Error> {
        match self.last {
            None if at.0 > after => Err(Error::Corrupt(format!(
                "{}: the log begins after index {}, but the partition files hold only up to \
                 {after}",
                path.display(),
                at.0
            ))),
            None => {
                self.last = Some(at);
                if at.0 >= self.keep_after {
                    self.start = at;
                }
                Ok(())
            }
            Some(last) if last == at => Ok(()),
            Some(last) => Err(Error::Corrupt(format!(
                "{}: the log jumps from index {} (term {}) to after {} (term {})",
                path.display(),
                last.0,
                last.1,
                at.0,
                at.1
            ))),
        }
    }
}

/// Reads the segment at `path`, named `name`, into `reading`; format 1 writes after `after`
/// go to `each`. Whether it holds anything: `false` for one whose start never became durable.
fn read_segment(
    disk: &DiskRef,
    path: &Path,
    name: u64,
    after: u64,
    reading: &mut Reading,
    each: &mut dyn FnMut(Logged) -> Result<(), Error>,
) -> Result<bool, Error> {
    let file = disk.open(path, false).map_err(|e| Error::io(path, e))?;
    let mut reader = BufReader::with_capacity(1 << 20, file);
    let mut header = Vec::with_capacity(HEADER.len());
    (&mut reader)
        .take(HEADER.len() as u64)
        .read_to_end(&mut header)
        .map_err(|e| Error::io(path, e))?;
    if header == HEADER_V1 {
        reading.start_at(path, after, (name - 1, 0))?;
        return read_v1(&mut reader, path, name, after, reading, each);
    }
    let corrupt = |what: &str| Error::Corrupt(format!("{}: {what}", path.display()));
    let mut frames = Frames {
        reader: &mut reader,
    };
    let start = if header == HEADER {
        frames.next()
    } else {
        // A process that died creating it left its header unwritten, or part of it; anything
        // past it but the preallocation's zeros is corruption.
        let partial = HEADER.starts_with(&header) || HEADER_V1.starts_with(&header);
        if !(partial || header.iter().all(|&b| b == 0)) {
            return Err(corrupt("not a log segment this version can read"));
        }
        let mut rest = Vec::new();
        reader
            .read_to_end(&mut rest)
            .map_err(|e| Error::io(path, e))?;
        if rest.iter().all(|&b| b == 0) {
            return Ok(false);
        }
        return Err(corrupt("not a log segment this version can read"));
    };
    // Written with the header and synced before the segment took anything: without it, the
    // segment was never used.
    let Some((START, start)) = start.filter(|(kind, body)| *kind == START && body.len() == 16)
    else {
        return Ok(false);
    };
    reading.start_at(path, after, (word(&start, 0), word(&start, 8)))?;
    let mut shapes: Vec<Bytes> = Vec::new();
    while let Some((kind, body)) = frames.next() {
        match kind {
            STATE if body.len() == 24 => {
                reading.state = HardState {
                    term: word(&body, 0),
                    vote: word(&body, 8),
                    commit: word(&body, 16),
                };
            }
            SHAPE if body.len() >= 4 => {
                let number = u32::from_le_bytes(body[..4].try_into().expect("4 bytes"));
                if number as usize != shapes.len() {
                    return Err(corrupt(&format!("shape {number} out of order")));
                }
                shapes.push(body.slice(4..));
            }
            ENTRY | SHAPED if body.len() >= 17 => {
                let entry_type = EntryType::from_i32(i32::from(body[16]))
                    .ok_or_else(|| corrupt(&format!("an entry of type {}", body[16])))?;
                // A write's head, its columns, and its rows, for SHAPED; checked here, joined
                // only if the entry is kept.
                let shaped = if kind == SHAPED {
                    let number = body
                        .get(17..21)
                        .map(|n| u32::from_le_bytes(n.try_into().expect("4 bytes")));
                    let schema = number.and_then(|n| shapes.get(n as usize));
                    let head = head_length(&body[21.min(body.len())..]);
                    match (schema, head) {
                        (Some(schema), Some(head)) => Some((schema.clone(), 21 + head)),
                        _ => return Err(corrupt("a write whose shape or head is not there")),
                    }
                } else {
                    None
                };
                let (index, term) = (word(&body, 0), word(&body, 8));
                let taken = reading.entry(index, term, || {
                    let mut entry = Entry {
                        index,
                        term,
                        data: match &shaped {
                            Some((schema, rows)) => join(&body[21..*rows], schema, &body[*rows..]),
                            None => body.slice(17..),
                        },
                        ..Entry::default()
                    };
                    entry.set_entry_type(entry_type);
                    entry
                });
                if !taken {
                    break;
                }
            }
            kind => {
                return Err(corrupt(&format!(
                    "a frame of kind {kind}, or of a length its kind cannot have, which this \
                     version cannot read"
                )));
            }
        }
    }
    Ok(true)
}

/// A segment's frames after its header, each kind and body, to the first that is not one.
struct Frames<'a, R> {
    reader: &'a mut R,
}

impl<R: Read> Frames<'_, R> {
    fn next(&mut self) -> Option<(u8, Bytes)> {
        let mut frame = [0; FRAME];
        self.reader.read_exact(&mut frame).ok()?;
        let length = u32::from_le_bytes(frame[..4].try_into().expect("4 bytes")) as usize;
        let crc = u32::from_le_bytes(frame[4..].try_into().expect("4 bytes"));
        if length == 0 || length > SEGMENT_BYTES as usize * 2 {
            return None;
        }
        let mut body = vec![0; length];
        if self.reader.read_exact(&mut body).is_err() || crc32fast::hash(&body) != crc {
            return None;
        }
        let body = Bytes::from(body);
        Some((body[0], body.slice(1..)))
    }
}

/// Encodes a write as an entry's data.
pub(crate) fn encode(append: &Append) -> Result<Bytes, Error> {
    let source = append.source.as_str().as_bytes();
    // The rows' message is about the size of their buffers: reserved once, so writing it into
    // the entry copies them once.
    let rows = append.batch.get_array_memory_size() + 256;
    let mut data = Vec::with_capacity(48 + source.len() + append.schema.len() + rows);
    data.push(WRITE);
    data.extend_from_slice(&append.table.to_le_bytes());
    data.extend_from_slice(&append.ingest_ns.to_le_bytes());
    data.extend_from_slice(&append.seq.to_le_bytes());
    data.extend_from_slice(&append.epoch.to_le_bytes());
    data.push(append.change.code());
    data.extend_from_slice(&(source.len() as u16).to_le_bytes());
    data.extend_from_slice(source);
    data.extend_from_slice(&(append.schema.len() as u32).to_le_bytes());
    data.extend_from_slice(append.schema);
    write_rows(append.batch, &mut data)?;
    Ok(Bytes::from(data))
}

/// Reads the write an entry at `index` holds (`encode`).
pub(crate) fn decode(index: u64, data: &Bytes) -> Result<Logged, String> {
    let short = || format!("the entry at index {index} is too short to read");
    let get = |at: usize, len: usize| data.get(at..at + len).ok_or_else(short);
    if data.first() != Some(&WRITE) {
        return Err(format!(
            "the entry at index {index} is of a kind this version cannot read"
        ));
    }
    let table = i64::from_le_bytes(get(1, 8)?.try_into().expect("8"));
    let ingest_ns = i64::from_le_bytes(get(9, 8)?.try_into().expect("8"));
    let seq = u64::from_le_bytes(get(17, 8)?.try_into().expect("8"));
    let epoch = u64::from_le_bytes(get(25, 8)?.try_into().expect("8"));
    let code = get(33, 1)?[0];
    let change = Change::from_code(u32::from(code))
        .ok_or_else(|| format!("a change kind ({code}) this version cannot read"))?;
    let length = u16::from_le_bytes(get(34, 2)?.try_into().expect("2")) as usize;
    let source = std::str::from_utf8(get(36, length)?)
        .map_err(|_| "a source that is not text".to_owned())?;
    let source = SourceId::parse(source).map_err(|e| e.to_string())?;
    let at = 36 + length;
    let schema_length = u32::from_le_bytes(get(at, 4)?.try_into().expect("4")) as usize;
    let schema_at = at + 4;
    get(schema_at, schema_length)?;
    let rows_at = schema_at + schema_length;
    let (shape, batch) = decode_rows(data.slice(schema_at..rows_at), data.slice(rows_at..), index)?;
    Ok(Logged {
        lsn: index,
        table,
        shape,
        source,
        seq,
        epoch,
        change,
        ingest_ns,
        batch,
    })
}

/// The rows of an IPC record batch message, read against a schema message (its column IDs as
/// field metadata), and the shape they are in.
fn decode_rows(schema: Bytes, rows: Bytes, index: u64) -> Result<(Shape, RecordBatch), String> {
    let mut decoder = StreamDecoder::new();
    decoder
        .decode(&mut Buffer::from(schema))
        .map_err(|e| e.to_string())?;
    let batch = decoder
        .decode(&mut Buffer::from(rows))
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("the entry at index {index} has no rows"))?;
    let shape = Shape::of_segment(&batch.schema());
    // Read with the log's schema (column IDs as field metadata); delivered in the shape's.
    let batch = shape.convert(batch, &shape).map_err(|e| e.to_string())?;
    Ok((shape, batch))
}

/// Appends `batch` to `into` as an Arrow IPC record batch message, without its schema.
fn write_rows(batch: &RecordBatch, into: &mut Vec<u8>) -> Result<(), Error> {
    let arrow = |e: arrow::error::ArrowError| Error::Arrow(e.to_string());
    let options = IpcWriteOptions::default();
    let (dictionaries, encoded) = IpcDataGenerator::default()
        .encode(
            batch,
            &mut DictionaryTracker::new(false),
            &options,
            &mut IpcWriteContext::default(),
        )
        .map_err(arrow)?;
    // The column types tables store (kermodes_core::parse_type) have no dictionaries.
    debug_assert!(dictionaries.is_empty());
    write_message(into, encoded, &options).map_err(arrow)?;
    Ok(())
}

/// Reads a format 1 segment after its header: entries of LSN, kind, and body, each LSN one past
/// the last; its writes after `after` go to `each`. Shape entries take an LSN of their own.
fn read_v1(
    reader: &mut impl Read,
    path: &Path,
    first: u64,
    after: u64,
    reading: &mut Reading,
    each: &mut dyn FnMut(Logged) -> Result<(), Error>,
) -> Result<bool, Error> {
    let corrupt = |what: &str| Error::Corrupt(format!("{}: {what}", path.display()));
    // Each shape's table and schema message, by number.
    let mut shapes: HashMap<u32, (i64, Bytes)> = HashMap::new();
    let mut frames = Frames { reader };
    let mut next = first;
    // A format 1 entry: LSN (u64), kind (u8), body; its frame's kind byte is the LSN's first.
    while let Some((byte, rest)) = frames.next() {
        if rest.len() < 8 {
            break;
        }
        let mut lsn = [0; 8];
        lsn[0] = byte;
        lsn[1..].copy_from_slice(&rest[..7]);
        let lsn = u64::from_le_bytes(lsn);
        if lsn != next || !reading.entry(lsn, 0, Entry::default) {
            break;
        }
        // Not Raft's: format 1's writes are applied here, and Raft's log starts after them.
        reading.entries.clear();
        reading.start = (lsn, 0);
        next += 1;
        let body = rest.slice(8..);
        match rest[7] {
            V1_SHAPE => {
                if body.len() < 12 {
                    return Err(corrupt("a shape entry too short to read"));
                }
                let table = i64::from_le_bytes(body[..8].try_into().expect("8 bytes"));
                let number = u32::from_le_bytes(body[8..12].try_into().expect("4 bytes"));
                shapes.insert(number, (table, body.slice(12..)));
            }
            V1_BATCH if lsn > after => {
                let logged = decode_v1(&body, lsn, &shapes).map_err(|e| corrupt(&e))?;
                each(logged)?;
            }
            V1_BATCH => {}
            kind => {
                return Err(corrupt(&format!(
                    "an entry of kind {kind}, which this version cannot read"
                )));
            }
        }
    }
    Ok(next > first)
}

/// A format 1 batch entry: shape number (u32), ingest time (i64), seq (u64), epoch (u64),
/// change kind (u8), source (u16 length, then bytes), and the rows.
fn decode_v1(
    body: &Bytes,
    lsn: u64,
    shapes: &HashMap<u32, (i64, Bytes)>,
) -> Result<Logged, String> {
    let short = || "a batch entry too short to read".to_owned();
    let get = |at: usize, len: usize| body.get(at..at + len).ok_or_else(short);
    let number = u32::from_le_bytes(get(0, 4)?.try_into().expect("4"));
    let ingest_ns = i64::from_le_bytes(get(4, 8)?.try_into().expect("8"));
    let seq = u64::from_le_bytes(get(12, 8)?.try_into().expect("8"));
    let epoch = u64::from_le_bytes(get(20, 8)?.try_into().expect("8"));
    let code = get(28, 1)?[0];
    let change = Change::from_code(u32::from(code))
        .ok_or_else(|| format!("a change kind ({code}) this version cannot read"))?;
    let length = u16::from_le_bytes(get(29, 2)?.try_into().expect("2")) as usize;
    let source = std::str::from_utf8(get(31, length)?)
        .map_err(|_| "a source that is not text".to_owned())?;
    let source = SourceId::parse(source).map_err(|e| e.to_string())?;
    let (table, schema) = shapes
        .get(&number)
        .ok_or_else(|| format!("a batch of shape {number}, which the segment does not have"))?;
    let rows = body.slice(31 + length..);
    let (shape, batch) = decode_rows(schema.clone(), rows, lsn)?;
    Ok(Logged {
        lsn,
        table: *table,
        shape,
        source,
        seq,
        epoch,
        change,
        ingest_ns,
        batch,
    })
}

/// The index the partition files hold every entry up to: 0 for a store before the log.
pub(crate) fn read_applied(disk: &DiskRef, dir: &Path) -> Result<u64, Error> {
    let path = dir.join("applied");
    match disk.read(&path) {
        Ok(bytes) => {
            let bytes: [u8; 12] = bytes
                .try_into()
                .map_err(|_| Error::Corrupt(format!("{}: unreadable", path.display())))?;
            let (lsn, crc) = bytes.split_at(8);
            if crc32fast::hash(lsn).to_le_bytes() != crc {
                return Err(Error::Corrupt(format!("{}: unreadable", path.display())));
            }
            Ok(u64::from_le_bytes(lsn.try_into().expect("8 bytes")))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(e) => Err(Error::io(&path, e)),
    }
}

/// Records that the partition files hold every entry up to `lsn`, atomically and durably.
pub(crate) fn write_applied(disk: &DiskRef, dir: &Path, lsn: u64) -> Result<(), Error> {
    let path = dir.join("applied");
    let staged = dir.join("applied.new");
    let mut bytes = lsn.to_le_bytes().to_vec();
    bytes.extend_from_slice(&crc32fast::hash(&lsn.to_le_bytes()).to_le_bytes());
    let mut file = disk
        .create(&staged, false)
        .map_err(|e| Error::io(&staged, e))?;
    file.write_all(&bytes)
        .and_then(|()| file.sync_all())
        .map_err(|e| Error::io(&staged, e))?;
    disk.rename(&staged, &path)
        .map_err(|e| Error::io(&path, e))?;
    disk.sync_dir(dir).map_err(|e| Error::io(dir, e))
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use arrow::array::{Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use bytes::Bytes;
    use raft::eraftpb::{Entry, HardState};

    use super::{
        Append, HEADER_V1, Logged, Recovered, Wal, encode, read_applied, write_applied, write_rows,
    };
    use crate::disk::{DiskRef, real};
    use crate::partition::schema_header;
    use crate::shape::{ColumnId, Shape};
    use crate::{Change, Error, SourceId};

    fn dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("kermodes-wal-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn entry(index: u64, term: u64) -> Entry {
        Entry {
            index,
            term,
            data: Bytes::from(format!("{index}@{term}")),
            ..Entry::default()
        }
    }

    fn open(disk: &DiskRef, dir: &Path, after: u64, keep_after: u64) -> (Wal, Recovered) {
        Wal::open(disk, dir, dir, after, keep_after, &mut |_| {
            unreachable!("no format 1 segments")
        })
        .unwrap()
    }

    /// Each kept entry's index, term, and data.
    fn read(disk: &DiskRef, dir: &Path, keep_after: u64) -> (Recovered, Vec<(u64, u64, String)>) {
        let (_, recovered) = open(disk, dir, keep_after, keep_after);
        let entries = recovered
            .entries
            .iter()
            .map(|e| {
                let data = String::from_utf8(e.data.to_vec()).unwrap();
                (e.index, e.term, data)
            })
            .collect();
        (recovered, entries)
    }

    fn owned(entries: &[(u64, u64, &str)]) -> Vec<(u64, u64, String)> {
        entries
            .iter()
            .map(|&(index, term, data)| (index, term, data.to_owned()))
            .collect()
    }

    /// What is appended reads back after reopening: entries in index order, an entry replacing
    /// the one it overwrote and every one after, and Raft's last state. A torn tail ends the
    /// log there, and appending goes on after it.
    #[test]
    fn entries_read_back_overwritten_ones_replaced_and_a_torn_tail_ends_the_log() {
        let dir = dir("entries");
        let disk = real(&dir.join("fsync-failed"));
        let (mut wal, recovered) = open(&disk, &dir, 0, 0);
        assert_eq!((recovered.start, recovered.entries.len()), ((0, 0), 0));
        let state = HardState {
            term: 1,
            vote: 1,
            commit: 1,
        };
        let first: Vec<Entry> = (1..=3).map(|i| entry(i, 1)).collect();
        wal.append(&first, Some(&state)).unwrap();
        // A new leader overwrites 2 and 3, never committed.
        wal.append(&[entry(2, 2)], None).unwrap();
        let later = HardState {
            term: 2,
            vote: 3,
            commit: 2,
        };
        // A state alone: a new term and vote.
        wal.append(&[], Some(&later)).unwrap();
        drop(wal);
        let (recovered, entries) = read(&disk, &dir, 0);
        assert_eq!(entries, owned(&[(1, 1, "1@1"), (2, 2, "2@2")]));
        assert_eq!((recovered.start, recovered.state), ((0, 0), later));
        // Kept after what the partition files hold: the start is the entry there.
        let (recovered, entries) = read(&disk, &dir, 1);
        assert_eq!(
            (recovered.start, entries),
            ((1, 1), owned(&[(2, 2, "2@2")]))
        );

        // Each open started a segment, named past the entries before it.
        let (mut wal, _) = open(&disk, &dir, 0, 0);
        wal.append(&[entry(3, 2), entry(4, 2)], None).unwrap();
        drop(wal);
        let mut names: Vec<PathBuf> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|e| e == "log"))
            .collect();
        names.sort();
        let newest = names.last().unwrap().clone();
        // Tear the last entry: the log ends before it.
        let mut bytes = std::fs::read(&newest).unwrap();
        let end = bytes.iter().rposition(|&b| b != 0).unwrap();
        bytes[end] ^= 0xff;
        std::fs::write(&newest, &bytes).unwrap();
        let (mut wal, recovered) = open(&disk, &dir, 0, 0);
        assert_eq!(recovered.entries.last().map(|e| e.index), Some(3));
        wal.append(&[entry(4, 3)], None).unwrap();
        drop(wal);
        let (_, entries) = read(&disk, &dir, 0);
        let expected = [(1, 1, "1@1"), (2, 2, "2@2"), (3, 2, "3@2"), (4, 3, "4@3")];
        assert_eq!(entries, owned(&expected));

        // Segments wholly at or below what the partition files hold go; the log still opens
        // from there.
        let (mut wal, _) = open(&disk, &dir, 0, 0);
        for path in wal.trim(4) {
            std::fs::remove_file(path).unwrap();
        }
        drop(wal);
        let (recovered, entries) = read(&disk, &dir, 4);
        assert_eq!((recovered.start, entries), ((4, 3), Vec::new()));

        assert_eq!(read_applied(&disk, &dir).unwrap(), 0);
        write_applied(&disk, &dir, 3).unwrap();
        assert_eq!(read_applied(&disk, &dir).unwrap(), 3);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Writes come back from the log as the entries Raft gave it, byte for byte, though each
    /// segment holds their columns once.
    #[test]
    fn writes_read_back_whole_with_their_columns_once_per_segment() {
        let dir = dir("shapes");
        let disk = real(&dir.join("fsync-failed"));
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));
        let shape = Shape::new(schema.clone(), Arc::new([ColumnId(1)]));
        let message = schema_header(&shape.segment_schema()).unwrap();
        let source = SourceId::parse("feed").unwrap();
        let write = |index: u64, n: i64| {
            let batch =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![n]))])
                    .unwrap();
            let data = encode(&Append {
                table: 7,
                schema: &message,
                source: &source,
                seq: index,
                epoch: 1,
                change: Change::Rows,
                ingest_ns: n,
                batch: &batch,
            })
            .unwrap();
            Entry {
                index,
                term: 1,
                data,
                ..Entry::default()
            }
        };
        let (mut wal, _) = open(&disk, &dir, 0, 0);
        let first = [write(1, 10), entry(2, 1), write(3, 30)];
        wal.append(&first, None).unwrap();
        drop(wal);
        // Reopened: a new segment, which writes the columns again.
        let (mut wal, _) = open(&disk, &dir, 0, 0);
        wal.append(&[write(4, 40)], None).unwrap();
        drop(wal);
        let (_, recovered) = open(&disk, &dir, 0, 0);
        let all: Vec<Entry> = first.into_iter().chain([write(4, 40)]).collect();
        assert_eq!(recovered.entries, all);
        let mut held = 0;
        for path in std::fs::read_dir(&dir).unwrap().map(|e| e.unwrap().path()) {
            if path.extension().is_some_and(|e| e == "log") {
                let bytes = std::fs::read(&path).unwrap();
                let copies = bytes
                    .windows(message.len())
                    .filter(|w| *w == message)
                    .count();
                assert!(
                    copies <= 1,
                    "{}: {copies} copies of the columns",
                    path.display()
                );
                held += copies;
            }
        }
        assert_eq!(
            held, 2,
            "the columns, once in each segment that took writes"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A format 1 frame: LSN, kind, and body, under length and CRC.
    fn v1_frame(segment: &mut Vec<u8>, lsn: u64, kind: u8, body: &[u8]) {
        let mut entry = lsn.to_le_bytes().to_vec();
        entry.push(kind);
        entry.extend_from_slice(body);
        segment.extend_from_slice(&(entry.len() as u32).to_le_bytes());
        segment.extend_from_slice(&crc32fast::hash(&entry).to_le_bytes());
        segment.extend_from_slice(&entry);
    }

    /// A store's log from before Raft (format 1) replays its writes once, after what the
    /// partition files hold; Raft's log takes up after them.
    #[test]
    fn a_format_1_log_replays_its_writes_and_raft_takes_up_after_them() {
        let dir = dir("v1");
        std::fs::create_dir_all(&dir).unwrap();
        let disk = real(&dir.join("fsync-failed"));
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));
        let shape = Shape::new(schema.clone(), Arc::new([ColumnId(1)]));
        let rows = |n: i64| {
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![n]))]).unwrap()
        };
        // LSN 1 the shape; 2 and 3 writes of seq 1 and 2.
        let mut segment = HEADER_V1.to_vec();
        let mut body = 7i64.to_le_bytes().to_vec();
        body.extend_from_slice(&0u32.to_le_bytes());
        body.extend_from_slice(&schema_header(&shape.segment_schema()).unwrap());
        v1_frame(&mut segment, 1, 1, &body);
        for (lsn, seq, n) in [(2, 1u64, 10), (3, 2, 20)] {
            let mut body = 0u32.to_le_bytes().to_vec();
            body.extend_from_slice(&(lsn as i64 * 100).to_le_bytes());
            body.extend_from_slice(&seq.to_le_bytes());
            body.extend_from_slice(&1u64.to_le_bytes());
            body.push(Change::Rows.code());
            body.extend_from_slice(&4u16.to_le_bytes());
            body.extend_from_slice(b"feed");
            write_rows(&rows(n), &mut body).unwrap();
            v1_frame(&mut segment, lsn, 2, &body);
        }
        segment.resize(segment.len() + 4096, 0);
        std::fs::write(dir.join(format!("{:020}.log", 1)), &segment).unwrap();

        let replay = |after: u64| -> (Vec<(u64, i64, u64, i64)>, Recovered) {
            let mut seen = Vec::new();
            let (_, recovered) = Wal::open(&disk, &dir, &dir, after, after, &mut |l: Logged| {
                let n = l.batch.column(0).as_any().downcast_ref::<Int64Array>();
                assert_eq!(
                    (l.table, l.shape.ids.as_ref()),
                    (7, [ColumnId(1)].as_slice())
                );
                seen.push((l.lsn, n.unwrap().value(0), l.seq, l.ingest_ns));
                Ok::<(), Error>(())
            })
            .unwrap();
            (seen, recovered)
        };
        let (seen, recovered) = replay(2);
        assert_eq!(seen, [(3, 20, 2, 300)]);
        assert_eq!((recovered.start, recovered.entries.len()), ((3, 0), 0));
        let (seen, _) = replay(0);
        assert_eq!(seen, [(2, 10, 1, 200), (3, 20, 2, 300)]);

        // Raft's entries continue the log after it; once the partition files hold it, it goes.
        let (mut wal, _) = open_after(&disk, &dir, 3);
        wal.append(&[entry(4, 1)], None).unwrap();
        for path in wal.trim(3) {
            std::fs::remove_file(path).unwrap();
        }
        drop(wal);
        let (recovered, entries) = read(&disk, &dir, 3);
        assert_eq!(
            (recovered.start, entries),
            ((3, 0), owned(&[(4, 1, "4@1")]))
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Opens a log whose format 1 writes up to `after` the partition files hold.
    fn open_after(disk: &DiskRef, dir: &Path, after: u64) -> (Wal, Recovered) {
        Wal::open(disk, dir, dir, after, after, &mut |_| Ok(())).unwrap()
    }
}
