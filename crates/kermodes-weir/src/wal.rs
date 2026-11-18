//! The shard log (architecture.md §7.6): every write of the store's tables, in one total order,
//! appended and fsynced before it is acknowledged. It is the only durable copy of a write until
//! the partition files derived from it are flushed; then the log below that point is deleted.
//!
//! ```text
//! {root}/weir/log/{first lsn, 20 digits}.log    segments, each preallocated
//! {root}/weir/log/applied                       the partition files hold everything up to this LSN
//! ```
//!
//! A segment is an 8-byte header, then entries, each: length (u32, of what follows the CRC),
//! CRC-32 (u32) of what follows it, LSN (u64), kind (u8), body. Everything little-endian. The log
//! ends at the first entry that is empty (the preallocated zeros), fails its CRC, or does not
//! follow the one before; a crash leaves nothing after that point that was ever acknowledged.
//! Writing after opening starts a new segment, so a torn tail is never written over.
//!
//! - A shape entry: table ID (i64), shape number (u32, within the segment), and the columns with
//!   their IDs as an Arrow IPC schema message. Each segment carries the shapes its batches use,
//!   so it reads alone, and deleting older segments loses nothing.
//! - A batch entry: shape number (u32), ingest time (i64, ns), seq (u64), epoch (u64), change
//!   kind (u8), source (u16 length, then bytes), and the rows as an Arrow IPC record batch
//!   message, read against its shape's schema.

use std::collections::HashMap;
use std::io::{BufReader, Cursor, Read, Write};
use std::path::{Path, PathBuf};

use arrow::buffer::Buffer;
use arrow::ipc::reader::{StreamDecoder, StreamReader};
use arrow::ipc::writer::{
    DictionaryTracker, IpcDataGenerator, IpcWriteContext, IpcWriteOptions, write_message,
};
use arrow::record_batch::RecordBatch;

use crate::disk::{self, DiskFile, DiskRef};
use crate::partition::schema_header;
use crate::shape::Shape;
use crate::{Change, Error, SourceId};

const HEADER: &[u8; 8] = b"KMLOG\x00\x00\x01";
/// A segment takes entries until it reaches this size; an entry larger than what is left goes
/// to a new segment, which grows past it if the entry alone is larger.
const SEGMENT_BYTES: u64 = 256 << 20;
/// Length and CRC.
const FRAME: usize = 8;

const SHAPE: u8 = 1;
const BATCH: u8 = 2;

/// A batch to append.
pub(crate) struct Append<'a> {
    pub table: i64,
    /// The columns `batch` has.
    pub shape: &'a Shape,
    pub source: &'a SourceId,
    pub seq: u64,
    pub epoch: u64,
    pub change: Change,
    pub ingest_ns: i64,
    pub batch: &'a RecordBatch,
}

/// A batch read back from the log.
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

/// The log, open for appending.
pub(crate) struct Wal {
    disk: DiskRef,
    dir: PathBuf,
    /// The segment taking entries, and where the next one goes in it.
    file: Box<dyn DiskFile>,
    offset: u64,
    /// The first LSN of each segment, the one taking entries last.
    segments: Vec<u64>,
    next: u64,
    /// The shapes the open segment has written: table, columns, number.
    shapes: Vec<(i64, Shape, u32)>,
    /// Set once a write failed: what is on disk past the last good entry is unknown, so nothing
    /// more is appended until the store is opened again.
    broken: Option<String>,
}

impl Wal {
    /// Opens the log in `dir` (creating it, under the store's directory `root`), handing each
    /// batch logged after `after` to `each`, in order. Appending continues in a new segment.
    pub(crate) fn open(
        disk: &DiskRef,
        root: &Path,
        dir: &Path,
        after: u64,
        each: &mut dyn FnMut(Logged) -> Result<(), Error>,
    ) -> Result<Self, Error> {
        // Its directory durable before anything in it is acknowledged: a segment's fsync and
        // its own directory's make nothing durable in a directory a power loss takes away.
        for parent in disk::create_dirs(&**disk, root, dir).map_err(|e| Error::io(dir, e))? {
            disk.sync_dir(&parent).map_err(|e| Error::io(&parent, e))?;
        }
        let mut firsts: Vec<u64> = disk
            .list(dir)
            .map_err(|e| Error::io(dir, e))?
            .iter()
            .filter_map(|name| name.strip_suffix(".log")?.parse().ok())
            .collect();
        firsts.sort_unstable();
        // The log may have been trimmed to nothing: LSNs go on above what the files hold.
        let mut next = firsts.first().copied().unwrap_or(after + 1);
        if next > after + 1 {
            return Err(Error::Corrupt(format!(
                "{}: the log begins at LSN {next}, but the partition files hold only up to {after}",
                dir.display()
            )));
        }
        let mut kept = Vec::with_capacity(firsts.len() + 1);
        for first in firsts {
            if first != next {
                return Err(Error::Corrupt(format!(
                    "{}: the log jumps from LSN {} to {first}",
                    dir.display(),
                    next - 1
                )));
            }
            let path = segment_path(dir, first);
            next = read_segment(disk, &path, first, after, each)?;
            if next == first {
                // No entry committed in it (opened, then closed or crashed before its first
                // write): nothing to keep, and its name is the next segment's.
                disk.remove_file(&path).map_err(|e| Error::io(&path, e))?;
            } else {
                // What is replayed must be durable before anything is built on it: a process
                // that was killed leaves its last entries in the page cache, read here, and lost
                // to a later power loss unless fsynced now.
                disk.open(&path, false)
                    .and_then(|file| file.sync_data())
                    .map_err(|e| Error::io(&path, e))?;
                kept.push(first);
            }
        }
        kept.push(next);
        Ok(Self {
            disk: disk.clone(),
            dir: dir.to_owned(),
            file: start_segment(disk, dir, next)?,
            offset: HEADER.len() as u64,
            segments: kept,
            next,
            shapes: Vec::new(),
            broken: None,
        })
    }

    /// The LSN the next entry gets.
    pub(crate) fn next_lsn(&self) -> u64 {
        self.next
    }

    /// Appends `batches` in order and fsyncs them once: their LSNs, durable when this returns.
    pub(crate) fn append(&mut self, batches: &[Append]) -> Result<Vec<u64>, Error> {
        if let Some(why) = &self.broken {
            return Err(Error::Io(format!("the log cannot be written: {why}")));
        }
        let encoded = batches
            .iter()
            .map(|append| encode_rows(append.batch))
            .collect::<Result<Vec<_>, _>>()?;
        let needed: u64 = encoded.iter().map(|rows| rows.len() as u64 + 256).sum();
        if self.offset > HEADER.len() as u64 && self.offset + needed > SEGMENT_BYTES {
            self.roll()?;
        }
        let mut buffer = Vec::with_capacity(needed as usize);
        let mut lsns = Vec::with_capacity(batches.len());
        for (append, rows) in batches.iter().zip(encoded) {
            let number = match self.shape_number(append.table, append.shape) {
                Some(number) => number,
                None => {
                    let number = self.shapes.len() as u32;
                    let mut body = append.table.to_le_bytes().to_vec();
                    body.extend_from_slice(&number.to_le_bytes());
                    body.extend_from_slice(&schema_header(&append.shape.segment_schema())?);
                    frame(&mut buffer, self.next, SHAPE, &body);
                    self.next += 1;
                    self.shapes
                        .push((append.table, append.shape.clone(), number));
                    number
                }
            };
            let source = append.source.as_str().as_bytes();
            let mut body = Vec::with_capacity(rows.len() + 40 + source.len());
            body.extend_from_slice(&number.to_le_bytes());
            body.extend_from_slice(&append.ingest_ns.to_le_bytes());
            body.extend_from_slice(&append.seq.to_le_bytes());
            body.extend_from_slice(&append.epoch.to_le_bytes());
            body.push(append.change.code());
            body.extend_from_slice(&(source.len() as u16).to_le_bytes());
            body.extend_from_slice(source);
            body.extend_from_slice(&rows);
            frame(&mut buffer, self.next, BATCH, &body);
            lsns.push(self.next);
            self.next += 1;
        }
        let path = segment_path(&self.dir, self.segments[self.segments.len() - 1]);
        let written = self
            .file
            .write_all(&buffer)
            .and_then(|()| self.file.sync_data());
        if let Err(e) = written {
            let why = format!("{}: {e}", path.display());
            self.broken = Some(why.clone());
            return Err(Error::Io(why));
        }
        self.offset += buffer.len() as u64;
        Ok(lsns)
    }

    /// Forgets the segments whose every entry is at or below `applied` (the partition files hold
    /// them now), never the one taking entries; their files, for the caller to delete outside
    /// whatever lock it holds.
    pub(crate) fn trim(&mut self, applied: u64) -> Vec<PathBuf> {
        let mut gone = Vec::new();
        while self.segments.len() > 1 && self.segments[1] <= applied + 1 {
            gone.push(segment_path(&self.dir, self.segments.remove(0)));
        }
        gone
    }

    fn shape_number(&self, table: i64, shape: &Shape) -> Option<u32> {
        self.shapes
            .iter()
            .find(|(t, s, _)| *t == table && s == shape)
            .map(|(_, _, number)| *number)
    }

    fn roll(&mut self) -> Result<(), Error> {
        self.file = start_segment(&self.disk, &self.dir, self.next)?;
        self.offset = HEADER.len() as u64;
        self.segments.push(self.next);
        self.shapes.clear();
        Ok(())
    }
}

fn segment_path(dir: &Path, first: u64) -> PathBuf {
    dir.join(format!("{first:020}.log"))
}

/// A new segment for entries from `first` on: preallocated (so an fsync need not also commit
/// the file's growth), its header and directory entry durable.
fn start_segment(disk: &DiskRef, dir: &Path, first: u64) -> Result<Box<dyn DiskFile>, Error> {
    let path = segment_path(dir, first);
    let mut file = disk.create(&path, true).map_err(|e| Error::io(&path, e))?;
    file.allocate(SEGMENT_BYTES)
        .map_err(|e| Error::Io(format!("{}: cannot preallocate: {e}", path.display())))?;
    file.write_all(HEADER)
        .and_then(|()| file.sync_all())
        .map_err(|e| Error::io(&path, e))?;
    disk.sync_dir(dir).map_err(|e| Error::io(dir, e))?;
    Ok(file)
}

fn frame(buffer: &mut Vec<u8>, lsn: u64, kind: u8, body: &[u8]) {
    let mut entry = Vec::with_capacity(9 + body.len());
    entry.extend_from_slice(&lsn.to_le_bytes());
    entry.push(kind);
    entry.extend_from_slice(body);
    buffer.extend_from_slice(&(entry.len() as u32).to_le_bytes());
    buffer.extend_from_slice(&crc32fast::hash(&entry).to_le_bytes());
    buffer.extend_from_slice(&entry);
}

/// `batch` as an Arrow IPC record batch message, without its schema.
fn encode_rows(batch: &RecordBatch) -> Result<Vec<u8>, Error> {
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
    let mut rows = Vec::new();
    write_message(&mut rows, encoded, &options).map_err(arrow)?;
    Ok(rows)
}

/// Reads the segment at `path`, whose first entry is `first`, handing each batch after `after`
/// to `each`; the LSN after its last entry.
fn read_segment(
    disk: &DiskRef,
    path: &Path,
    first: u64,
    after: u64,
    each: &mut dyn FnMut(Logged) -> Result<(), Error>,
) -> Result<u64, Error> {
    let file = disk.open(path, false).map_err(|e| Error::io(path, e))?;
    let mut reader = BufReader::with_capacity(1 << 20, file);
    let mut header = Vec::with_capacity(HEADER.len());
    (&mut reader)
        .take(HEADER.len() as u64)
        .read_to_end(&mut header)
        .map_err(|e| Error::io(path, e))?;
    if header != HEADER {
        // A process that died creating it left its header unwritten, or part of it: it commits
        // nothing, since the header is fsynced before anything is appended. So anything past
        // it but the preallocation's zeros is corruption.
        if HEADER.starts_with(&header) || header.iter().all(|&b| b == 0) {
            let mut rest = Vec::new();
            reader
                .read_to_end(&mut rest)
                .map_err(|e| Error::io(path, e))?;
            if rest.iter().all(|&b| b == 0) {
                return Ok(first);
            }
        }
        return Err(Error::Corrupt(format!(
            "{}: not a log segment this version can read",
            path.display()
        )));
    }
    let corrupt = |what: &str| Error::Corrupt(format!("{}: {what}", path.display()));
    // Each shape's table, columns, and schema message, by number.
    let mut shapes: HashMap<u32, (i64, Shape, Vec<u8>)> = HashMap::new();
    let mut next = first;
    let mut frame = [0; FRAME];
    loop {
        if reader.read_exact(&mut frame).is_err() {
            return Ok(next);
        }
        let length = u32::from_le_bytes(frame[..4].try_into().expect("4 bytes")) as usize;
        let crc = u32::from_le_bytes(frame[4..].try_into().expect("4 bytes"));
        if length < 9 {
            return Ok(next);
        }
        let mut entry = vec![0; length];
        if reader.read_exact(&mut entry).is_err() || crc32fast::hash(&entry) != crc {
            return Ok(next);
        }
        let lsn = u64::from_le_bytes(entry[..8].try_into().expect("8 bytes"));
        if lsn != next {
            return Ok(next);
        }
        next += 1;
        let body = &entry[9..];
        match entry[8] {
            SHAPE => {
                if body.len() < 12 {
                    return Err(corrupt("a shape entry too short to read"));
                }
                let table = i64::from_le_bytes(body[..8].try_into().expect("8 bytes"));
                let number = u32::from_le_bytes(body[8..12].try_into().expect("4 bytes"));
                let schema_message = body[12..].to_vec();
                let schema = StreamReader::try_new(Cursor::new(&schema_message), None)
                    .map_err(|e| corrupt(&e.to_string()))?
                    .schema();
                shapes.insert(number, (table, Shape::of_segment(&schema), schema_message));
            }
            BATCH if lsn > after => {
                let logged = decode_batch(body, lsn, &shapes).map_err(|e| corrupt(&e))?;
                each(logged)?;
            }
            BATCH => {}
            kind => {
                return Err(corrupt(&format!(
                    "an entry of kind {kind}, which this version cannot read"
                )));
            }
        }
    }
}

fn decode_batch(
    body: &[u8],
    lsn: u64,
    shapes: &HashMap<u32, (i64, Shape, Vec<u8>)>,
) -> Result<Logged, String> {
    let short = || "a batch entry too short to read".to_owned();
    let number = u32::from_le_bytes(body.get(..4).ok_or_else(short)?.try_into().expect("4"));
    let word = |at: usize| -> Result<[u8; 8], String> {
        Ok(body
            .get(at..at + 8)
            .ok_or_else(short)?
            .try_into()
            .expect("8"))
    };
    let ingest_ns = i64::from_le_bytes(word(4)?);
    let seq = u64::from_le_bytes(word(12)?);
    let epoch = u64::from_le_bytes(word(20)?);
    let code = *body.get(28).ok_or_else(short)?;
    let change = Change::from_code(u32::from(code))
        .ok_or_else(|| format!("a change kind ({code}) this version cannot read"))?;
    let length =
        u16::from_le_bytes(body.get(29..31).ok_or_else(short)?.try_into().expect("2")) as usize;
    let source = std::str::from_utf8(body.get(31..31 + length).ok_or_else(short)?)
        .map_err(|_| "a source that is not text".to_owned())?;
    let source = SourceId::parse(source).map_err(|e| e.to_string())?;
    let (table, shape, schema_message) = shapes
        .get(&number)
        .ok_or_else(|| format!("a batch of shape {number}, which the segment does not have"))?;
    let mut decoder = StreamDecoder::new();
    decoder
        .decode(&mut Buffer::from(schema_message.clone()))
        .map_err(|e| e.to_string())?;
    let batch = decoder
        .decode(&mut Buffer::from(body[31 + length..].to_vec()))
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "a batch entry without its rows".to_owned())?;
    // Read with the segment's schema (column IDs as field metadata); delivered in the shape's.
    let batch = shape.convert(batch, shape).map_err(|e| e.to_string())?;
    Ok(Logged {
        lsn,
        table: *table,
        shape: shape.clone(),
        source,
        seq,
        epoch,
        change,
        ingest_ns,
        batch,
    })
}

/// The LSN the partition files hold everything up to: 0 for a store before the log.
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

/// Records that the partition files hold everything up to `lsn`, atomically and durably.
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
    use std::sync::Arc;

    use arrow::array::{Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::{Append, Wal, read_applied, write_applied};
    use crate::disk::{DiskRef, real};
    use crate::shape::{ColumnId, Shape};
    use crate::{Change, SourceId};

    fn dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("kermodes-wal-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn batch(n: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![n, n + 1]))]).unwrap()
    }

    fn read(disk: &DiskRef, dir: &std::path::Path, after: u64) -> Vec<(u64, i64, u64)> {
        let mut seen = Vec::new();
        Wal::open(disk, dir, dir, after, &mut |logged| {
            let n = logged
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            seen.push((logged.lsn, n.value(0), logged.seq));
            Ok(())
        })
        .unwrap();
        seen
    }

    /// What is appended reads back in order after reopening, with LSNs counting shape entries
    /// too; a torn tail ends the log there, and appending goes on in a new segment.
    #[test]
    fn appended_batches_read_back_and_a_torn_tail_ends_the_log() {
        let dir = dir("torn");
        let disk = real(&dir.join("fsync-failed"));
        let schema = batch(0).schema();
        let shape = Shape::new(schema, Arc::new([ColumnId(1)]));
        let source = SourceId::parse("feed").unwrap();
        let mut wal = Wal::open(&disk, &dir, &dir, 0, &mut |_| unreachable!()).unwrap();
        let rows: Vec<RecordBatch> = (0..3).map(|i| batch(i * 10)).collect();
        let appends: Vec<Append> = rows
            .iter()
            .enumerate()
            .map(|(i, rows)| Append {
                table: 7,
                shape: &shape,
                source: &source,
                seq: i as u64 + 1,
                epoch: 1,
                change: Change::Rows,
                ingest_ns: i as i64,
                batch: rows,
            })
            .collect();
        // The shape takes LSN 1; the batches 2, 3, 4.
        assert_eq!(wal.append(&appends).unwrap(), [2, 3, 4]);
        drop(wal);
        assert_eq!(read(&disk, &dir, 0), [(2, 0, 1), (3, 10, 2), (4, 20, 3)]);
        assert_eq!(read(&disk, &dir, 3), [(4, 20, 3)]);

        // Tear the last entry of the first segment: the log ends before it, and the segments
        // opened since (one per open) start where it ends.
        let first = dir.join(format!("{:020}.log", 1));
        let mut bytes = std::fs::read(&first).unwrap();
        let end = bytes.iter().rposition(|&b| b != 0).unwrap();
        bytes[end] ^= 0xff;
        std::fs::write(&first, &bytes).unwrap();
        for name in std::fs::read_dir(&dir).unwrap() {
            let path = name.unwrap().path();
            if path != first {
                std::fs::remove_file(path).unwrap();
            }
        }
        assert_eq!(read(&disk, &dir, 0), [(2, 0, 1), (3, 10, 2)]);
        let mut wal = Wal::open(&disk, &dir, &dir, 0, &mut |_| Ok(())).unwrap();
        assert_eq!(wal.next_lsn(), 4);
        let again = batch(30);
        let lsn = wal
            .append(&[Append {
                table: 7,
                shape: &shape,
                source: &source,
                seq: 3,
                epoch: 2,
                change: Change::Rows,
                ingest_ns: 9,
                batch: &again,
            }])
            .unwrap();
        // The new segment writes its shape again.
        assert_eq!(lsn, [5]);
        drop(wal);
        assert_eq!(read(&disk, &dir, 0), [(2, 0, 1), (3, 10, 2), (5, 30, 3)]);

        assert_eq!(read_applied(&disk, &dir).unwrap(), 0);
        write_applied(&disk, &dir, 3).unwrap();
        assert_eq!(read_applied(&disk, &dir).unwrap(), 3);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
