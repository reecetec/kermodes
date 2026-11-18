//! One internal partition's batches: `(table, date, source)`, written by one writer.
//!
//! A partition is a directory of segments. Each segment is an Arrow IPC stream (`seg-N.arrows`)
//! and an index (`seg-N.idx`): an 8-byte header, then one fixed-size record per batch.
//!
//! Partition files are derived from the shard log (`wal`, architecture.md §7.6), the only
//! durable copy of a write until they are flushed: batches are written here without an fsync,
//! the store flushes every partition in bulk, and only then records how far the files hold the
//! log. Recovery keeps the records whose CRC holds, in order, up to that point, and the log is
//! replayed from there; writing always starts a new segment, so what a crash left beyond that
//! point is never read. Index records written before the log (format 1, without an LSN) were
//! fsynced one by one and are always kept. Sequence numbers rise strictly; a batch at or below
//! the last one is a resend and is dropped.
//!
//! A stream's schema keeps each column's ID (§5.4), so a segment is read in the shape it was
//! written in and projected by ID onto whatever shape its reader holds.

use std::io::{Read, Seek, SeekFrom, Take, Write};
use std::path::{Path, PathBuf};

use arrow::buffer::Buffer;
use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::{StreamDecoder, StreamReader};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

use crate::disk::{DiskFile, DiskRef, create_dirs};
use crate::shape::Shape;
use crate::{Change, Error};

/// Index file header: magic and format version. A new version is a new header; readers refuse
/// versions they do not know. Format 1 is from before the log; format 2 adds each batch's LSN.
const HEADER_1: &[u8; 8] = b"KMWEIR\x00\x01";
const RECORD_1: usize = 48;
const HEADER: &[u8; 8] = b"KMWEIR\x00\x02";
const RECORD: usize = 56;
/// A segment stops taking batches once its stream reaches this size.
const SEGMENT_BYTES: u64 = 64 << 20;

/// One batch. Its index record (little-endian) is: seq u64, rows u64, ingest time ns i64, end
/// offset u64, epoch u64 (the lease its writer held, §7.4), change kind u32 (§7.1), then in
/// format 2 the LSN u64, then the CRC-32 of everything before it.
#[derive(Clone, Copy, Debug)]
struct Entry {
    seq: u64,
    rows: u64,
    ingest_ns: i64,
    /// The stream's length once this batch is in it.
    end: u64,
    change: Change,
    /// Its place in the shard log; 0 for a batch written before the log.
    lsn: u64,
}

/// What a batch was committed as: its change kind; when it arrived (ns since the epoch, UTC),
/// which dates it; and its place in the shard log, 0 for a batch written before the log.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Stamp {
    pub change: Change,
    pub ingest_ns: i64,
    pub lsn: u64,
}

impl Stamp {
    /// Its place in commit order: by LSN, and before the log by ingest time (which a table's
    /// clock made rise in commit order), so batches from before the log come first.
    pub fn order(&self) -> (u64, i64) {
        (self.lsn, self.ingest_ns)
    }
}

impl Entry {
    fn encode(&self, epoch: u64) -> [u8; RECORD] {
        let mut record = [0; RECORD];
        record[0..8].copy_from_slice(&self.seq.to_le_bytes());
        record[8..16].copy_from_slice(&self.rows.to_le_bytes());
        record[16..24].copy_from_slice(&self.ingest_ns.to_le_bytes());
        record[24..32].copy_from_slice(&self.end.to_le_bytes());
        record[32..40].copy_from_slice(&epoch.to_le_bytes());
        record[40..44].copy_from_slice(&u32::from(self.change.code()).to_le_bytes());
        record[44..52].copy_from_slice(&self.lsn.to_le_bytes());
        let crc = crc32fast::hash(&record[..52]);
        record[52..56].copy_from_slice(&crc.to_le_bytes());
        record
    }

    /// The record (of format 1 or 2, by its length), unless it is torn, zeroed, or of a change
    /// kind this version does not know (recovery then ends there, as an older version's would).
    fn decode(record: &[u8]) -> Option<Self> {
        let word = |at: usize| u64::from_le_bytes(record[at..at + 8].try_into().expect("8 bytes"));
        let checked = record.len() - 4;
        let crc = u32::from_le_bytes(record[checked..].try_into().expect("4 bytes"));
        if crc32fast::hash(&record[..checked]) != crc {
            return None;
        }
        let change = u32::from_le_bytes(record[40..44].try_into().expect("4 bytes"));
        Some(Self {
            seq: word(0),
            rows: word(8),
            ingest_ns: word(16) as i64,
            end: word(24),
            change: Change::from_code(change)?,
            lsn: if record.len() == RECORD { word(44) } else { 0 },
        })
    }

    fn stamp(&self) -> Stamp {
        Stamp {
            change: self.change,
            ingest_ns: self.ingest_ns,
            lsn: self.lsn,
        }
    }
}

struct Segment {
    number: u32,
    entries: Vec<Entry>,
    /// Its stream's schema exactly as written: with column IDs, or without, before tables had
    /// them. A read by position finds where the batches start by encoding it again.
    written: SchemaRef,
    /// The columns its stream was written with.
    shape: Shape,
}

impl Segment {
    fn new(number: u32, entries: Vec<Entry>, written: SchemaRef) -> Self {
        let shape = Shape::of_segment(&written);
        Self {
            number,
            entries,
            written,
            shape,
        }
    }
}

/// The segment taking batches.
struct Open {
    segment: Segment,
    stream: StreamWriter<Box<dyn DiskFile>>,
    index: Box<dyn DiskFile>,
    /// Written since the last flush.
    dirty: bool,
}

pub(crate) struct Partition {
    disk: DiskRef,
    dir: PathBuf,
    /// The columns new segments are written with.
    shape: Shape,
    /// Segments no longer taking batches, in order.
    closed: Vec<Segment>,
    open: Option<Open>,
    last_seq: Option<u64>,
    /// What the next flush must fsync besides the open segment: segments closed since the last
    /// one, and directories that gained entries.
    unsynced: Vec<ToSync>,
    /// The directories leading down to this one, until this process first writes here: then the
    /// next flush fsyncs them, so what it writes outlasts a power loss with its directory.
    parents: Vec<PathBuf>,
}

/// What a flush must fsync: a file, by a handle of its own, or a directory whose entries
/// changed.
pub(crate) enum ToSync {
    File(Box<dyn DiskFile>),
    Dir(PathBuf),
}

impl ToSync {
    pub(crate) fn sync(self, disk: &DiskRef) -> std::io::Result<()> {
        match self {
            ToSync::File(file) => file.sync_all(),
            ToSync::Dir(dir) => disk.sync_dir(&dir),
        }
    }
}

/// How a partition's batches came to be. Each the log's own entry (`FromLog`), covered by the
/// flush that records its position, so recovery trusts it up to that position. Or folded from
/// the log (`Folded`: a keyed table's checkpoints), which holds a position the flush covering it
/// may have recorded before it was written: a crash can leave its index without its stream, and
/// recovery drops what is missing, to be folded again.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Written {
    FromLog,
    Folded,
}

impl Partition {
    /// Opens (creating if needed, under the store's directory `root`) the partition in `dir`,
    /// recovering its segments as far as the files hold the log, `trusted` (the batches after
    /// it are the log's to replay); it writes `shape`'s columns. Writing starts a new segment:
    /// one an earlier process left open is not appended to.
    pub(crate) fn open(
        disk: &DiskRef,
        root: &Path,
        dir: &Path,
        shape: Shape,
        trusted: u64,
        written: Written,
    ) -> Result<Self, Error> {
        let parents = create_dirs(&**disk, root, dir).map_err(|e| Error::io(dir, e))?;
        let mut numbers: Vec<u32> = disk
            .list(dir)
            .map_err(|e| Error::io(dir, e))?
            .iter()
            .filter_map(|name| {
                name.strip_prefix("seg-")?
                    .strip_suffix(".idx")?
                    .parse()
                    .ok()
            })
            .collect();
        numbers.sort_unstable();
        let mut closed = Vec::new();
        for number in numbers {
            if let Some(segment) = recover(disk, dir, number, trusted, written)? {
                closed.push(segment);
            }
        }
        let last_seq = closed
            .last()
            .and_then(|s: &Segment| s.entries.last())
            .map(|e| e.seq);
        Ok(Self {
            disk: disk.clone(),
            dir: dir.to_owned(),
            shape,
            closed,
            open: None,
            last_seq,
            unsynced: Vec::new(),
            parents,
        })
    }

    pub(crate) fn last_seq(&self) -> Option<u64> {
        self.last_seq
    }

    /// From now on writes `shape`'s columns: the segment taking batches is closed, so the next
    /// batch starts one whose schema says what it holds (§5.4).
    pub(crate) fn reshape(&mut self, shape: Shape) -> Result<(), Error> {
        self.close()?;
        self.shape = shape;
        Ok(())
    }

    /// Appends `batch`, the log's entry `lsn`, as `seq`, a `change` written under lease `epoch`;
    /// `false` (and nothing written) when `seq` is not above the last sequence number. Not
    /// fsynced: the log holds it until the store flushes (`unsynced`).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn append(
        &mut self,
        batch: &RecordBatch,
        lsn: u64,
        seq: u64,
        ingest_ns: i64,
        epoch: u64,
        change: Change,
    ) -> Result<bool, Error> {
        if self.last_seq.is_some_and(|last| seq <= last) {
            return Ok(false);
        }
        if self.open.is_none() {
            self.open = Some(self.start()?);
        }
        let open = self.open.as_mut().expect("opened above");
        let data = data_path(&self.dir, open.segment.number);
        open.stream
            .write(batch)
            .and_then(|()| open.stream.flush())
            .map_err(|e| Error::Arrow(format!("{}: {e}", data.display())))?;
        let end = open
            .stream
            .get_mut()
            .stream_position()
            .map_err(|e| Error::io(&data, e))?;
        let entry = Entry {
            seq,
            rows: batch.num_rows() as u64,
            ingest_ns,
            end,
            change,
            lsn,
        };
        let index = index_path(&self.dir, open.segment.number);
        open.index
            .write_all(&entry.encode(epoch))
            .map_err(|e| Error::io(&index, e))?;
        open.dirty = true;
        open.segment.entries.push(entry);
        self.last_seq = Some(seq);
        if end >= SEGMENT_BYTES {
            self.close()?;
        }
        Ok(true)
    }

    /// What must be fsynced for this partition's files to hold every batch written so far, for
    /// the caller to sync outside its lock: the open segment's files if written since, segments
    /// closed since, and directories that gained entries. The next call returns only what is
    /// written after this one.
    pub(crate) fn unsynced(&mut self) -> Result<Vec<ToSync>, Error> {
        let mut unsynced = std::mem::take(&mut self.unsynced);
        if let Some(open) = self.open.as_mut().filter(|open| open.dirty) {
            open.dirty = false;
            unsynced.extend(handles(&self.dir, open)?);
        }
        Ok(unsynced)
    }

    /// Fsyncs every file of the partition and the directories from `root` down to it, now,
    /// whichever process wrote them: for what must be durable before something it was folded
    /// from goes.
    pub(crate) fn sync_now(&self, root: &Path) -> Result<(), Error> {
        for segment in self.segments() {
            for path in [
                data_path(&self.dir, segment.number),
                index_path(&self.dir, segment.number),
            ] {
                self.disk
                    .open(&path, false)
                    .and_then(|file| file.sync_all())
                    .map_err(|e| Error::io(&path, e))?;
            }
        }
        let parents =
            create_dirs(&*self.disk, root, &self.dir).map_err(|e| Error::io(&self.dir, e))?;
        for dir in [self.dir.clone()].into_iter().chain(parents) {
            self.disk.sync_dir(&dir).map_err(|e| Error::io(&dir, e))?;
        }
        Ok(())
    }

    /// Stops writing the open segment, keeping what the next flush must sync of it.
    fn close(&mut self) -> Result<(), Error> {
        if let Some(open) = self.open.take() {
            if open.dirty {
                self.unsynced.extend(handles(&self.dir, &open)?);
            }
            self.closed.push(open.segment);
        }
        Ok(())
    }

    /// How many batches are committed.
    pub(crate) fn committed(&self) -> usize {
        self.segments().map(|s| s.entries.len()).sum()
    }

    /// How many rows the committed batches hold.
    pub(crate) fn rows(&self) -> u64 {
        self.segments()
            .flat_map(|s| s.entries.iter())
            .map(|e| e.rows)
            .sum()
    }

    /// The committed batches from the `from`th on (counting from 0), to read without the
    /// partition: a committed batch's bytes never change, so the cursor reads them while writers
    /// go on appending.
    pub(crate) fn cursor(&self, from: usize) -> Cursor {
        // Batches to pass over before the first one to deliver.
        let mut before = from;
        let mut first_skip = 0;
        let mut segments = Vec::new();
        let mut bytes = 0;
        for segment in self.segments() {
            let count = segment.entries.len();
            if before >= count {
                before -= count;
                continue;
            }
            if segments.is_empty() {
                first_skip = before;
            }
            let last = segment.entries.last().expect("a segment with batches");
            // Stream bytes from the end of the last batch delivered to the end of the segment.
            let start = before.checked_sub(1).map_or(0, |i| segment.entries[i].end);
            bytes += last.end - start;
            before = 0;
            segments.push(Committed {
                path: data_path(&self.dir, segment.number),
                end: last.end,
                entries: segment.entries.clone(),
                shape: segment.shape.clone(),
            });
        }
        Cursor {
            disk: self.disk.clone(),
            segments,
            skip: first_skip,
            bytes,
        }
    }

    /// How many committed batches come at or before `order` in commit order (`Stamp::order`).
    /// They rise within a partition, so the batches after a point are a cursor from here.
    pub(crate) fn at_or_before(&self, order: (u64, i64)) -> usize {
        self.segments()
            .map(|s| s.entries.partition_point(|e| (e.lsn, e.ingest_ns) <= order))
            .sum()
    }

    /// The newest committed batch's place in commit order (`Stamp::order`).
    pub(crate) fn last_order(&self) -> Option<(u64, i64)> {
        self.segments()
            .last()
            .and_then(|segment| segment.entries.last())
            .map(|entry| (entry.lsn, entry.ingest_ns))
    }

    /// The smallest LSN of a batch written since the log; `None` if every batch is from before.
    pub(crate) fn first_lsn(&self) -> Option<u64> {
        self.segments()
            .flat_map(|segment| segment.entries.iter())
            .map(|entry| entry.lsn)
            .find(|&lsn| lsn > 0)
    }

    /// Each committed batch's rows and place in commit order, in order.
    pub(crate) fn batch_rows(&self) -> impl Iterator<Item = (u64, (u64, i64))> + '_ {
        self.segments().flat_map(|segment| {
            segment
                .entries
                .iter()
                .map(|e| (e.rows, (e.lsn, e.ingest_ns)))
        })
    }

    /// The committed batches `first..first + count` (counting from 0) as `to`'s columns, read
    /// directly: a batch's bytes run from the end of the one before it, or of its segment's
    /// schema, to its own end. A committed batch never changes, so this reads while writers go
    /// on appending.
    pub(crate) fn read_batches(
        &self,
        first: usize,
        count: usize,
        to: &Shape,
    ) -> Result<Vec<RecordBatch>, Error> {
        let mut batches = Vec::with_capacity(count);
        let mut skip = first;
        for segment in self.segments() {
            if batches.len() == count {
                break;
            }
            let held = segment.entries.len();
            if skip >= held {
                skip -= held;
                continue;
            }
            let header = schema_header(&segment.written)?;
            let take = (count - batches.len()).min(held - skip);
            let path = data_path(&self.dir, segment.number);
            let mut file = self
                .disk
                .open(&path, false)
                .map_err(|e| Error::io(&path, e))?;
            let begin = match skip {
                0 => header.len() as u64,
                _ => segment.entries[skip - 1].end,
            };
            let end = segment.entries[skip + take - 1].end;
            let mut bytes = vec![0; (end - begin) as usize];
            if skip == 0 {
                // The first batch starts where the schema the writer put first ends.
                let mut written = vec![0; header.len()];
                file.read_exact(&mut written)
                    .map_err(|e| Error::io(&path, e))?;
                if written != header {
                    return Err(Error::Corrupt(format!(
                        "{}: the stream does not start with its table's schema",
                        path.display()
                    )));
                }
            }
            file.seek(SeekFrom::Start(begin))
                .and_then(|_| file.read_exact(&mut bytes))
                .map_err(|e| Error::io(&path, e))?;
            let arrow =
                |e: arrow::error::ArrowError| Error::Arrow(format!("{}: {e}", path.display()));
            let mut decoder = StreamDecoder::new();
            decoder
                .decode(&mut Buffer::from(header.clone()))
                .map_err(arrow)?;
            let mut body = Buffer::from(bytes);
            for entry in &segment.entries[skip..skip + take] {
                let batch = decoder.decode(&mut body).map_err(arrow)?;
                match batch {
                    Some(batch) if batch.num_rows() as u64 == entry.rows => {
                        batches.push(segment.shape.convert(batch, to)?);
                    }
                    _ => {
                        return Err(Error::Corrupt(format!(
                            "{}: the stream does not hold the batches its index commits",
                            path.display()
                        )));
                    }
                }
            }
            skip = 0;
        }
        if batches.len() < count {
            return Err(Error::NotFound(format!(
                "batches {first}..{} of {}: {} are committed",
                first + count,
                self.dir.display(),
                self.committed()
            )));
        }
        Ok(batches)
    }

    fn segments(&self) -> impl Iterator<Item = &Segment> {
        self.closed
            .iter()
            .chain(self.open.as_ref().map(|open| &open.segment))
            .filter(|segment| !segment.entries.is_empty())
    }

    fn start(&mut self) -> Result<Open, Error> {
        let number = self.closed.last().map_or(1, |segment| segment.number + 1);
        let data = data_path(&self.dir, number);
        let index = index_path(&self.dir, number);
        // Over whatever is there: a number past the last segment recovered commits nothing,
        // and a process killed between creating a segment's stream and its index leaves the
        // stream with no index to find it by.
        let create = |path: &Path| {
            self.disk
                .create(path, false)
                .map_err(|e| Error::io(path, e))
        };
        let written = self.shape.segment_schema();
        let stream = StreamWriter::try_new(create(&data)?, &written)
            .map_err(|e| Error::Arrow(format!("{}: {e}", data.display())))?;
        let mut index_file = create(&index)?;
        index_file
            .write_all(HEADER)
            .map_err(|e| Error::io(&index, e))?;
        // The new files' directory entries must reach the disk with them, and on this process's
        // first segment here, those of the directories leading down to them.
        self.unsynced.push(ToSync::Dir(self.dir.clone()));
        self.unsynced
            .extend(self.parents.drain(..).map(ToSync::Dir));
        Ok(Open {
            segment: Segment::new(number, Vec::new(), written),
            stream,
            index: index_file,
            dirty: true,
        })
    }
}

/// A segment's committed batches: its stream up to the last committed byte, and each batch's
/// record, to check what the stream holds against what the index commits.
struct Committed {
    path: PathBuf,
    end: u64,
    entries: Vec<Entry>,
    shape: Shape,
}

/// Committed batches of a partition, read one at a time (`Partition::cursor`).
pub(crate) struct Cursor {
    disk: DiskRef,
    segments: Vec<Committed>,
    /// Batches of the first segment already delivered.
    skip: usize,
    /// Bytes of the stream the batches to deliver take on disk.
    bytes: u64,
}

impl Cursor {
    pub(crate) fn bytes(&self) -> u64 {
        self.bytes
    }

    /// The batches in order as `to`'s columns, decoded one at a time as they are taken.
    pub(crate) fn batches(self, to: Shape) -> Batches {
        Batches {
            disk: self.disk,
            segments: self.segments.into_iter(),
            skip: self.skip,
            current: None,
            to,
        }
    }
}

/// A segment's stream, read up to its last committed byte.
type SegmentReader = StreamReader<Take<Box<dyn DiskFile>>>;

/// A cursor's batches, pulled one at a time (`Cursor::batches`).
pub(crate) struct Batches {
    disk: DiskRef,
    segments: std::vec::IntoIter<Committed>,
    skip: usize,
    /// The segment being read: its stream and the records of its batches still to come.
    current: Option<(Committed, SegmentReader, usize)>,
    /// The columns delivered.
    to: Shape,
}

impl Iterator for Batches {
    type Item = Result<(RecordBatch, Stamp), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current.is_none() {
                let segment = self.segments.next()?;
                let file = match self.disk.open(&segment.path, false) {
                    Ok(file) => file,
                    Err(e) => return Some(Err(Error::io(&segment.path, e))),
                };
                let reader = match StreamReader::try_new(file.take(segment.end), None) {
                    Ok(reader) => reader,
                    Err(e) => {
                        return Some(Err(Error::Arrow(format!(
                            "{}: {e}",
                            segment.path.display()
                        ))));
                    }
                };
                self.current = Some((segment, reader, 0));
            }
            let (segment, reader, at) = self.current.as_mut().expect("opened above");
            let corrupt = |segment: &Committed| {
                Error::Corrupt(format!(
                    "{}: the stream does not hold the {} batches its index commits",
                    segment.path.display(),
                    segment.entries.len()
                ))
            };
            let Some(&entry) = segment.entries.get(*at) else {
                // Every committed batch read: the stream must end there.
                let trailing = reader.next().is_some();
                let segment = self.current.take().expect("reading one").0;
                if trailing {
                    return Some(Err(corrupt(&segment)));
                }
                self.skip = 0;
                continue;
            };
            *at += 1;
            let batch = match reader.next() {
                Some(Ok(batch)) => batch,
                Some(Err(e)) => {
                    return Some(Err(Error::Arrow(format!(
                        "{}: {e}",
                        segment.path.display()
                    ))));
                }
                None => return Some(Err(corrupt(segment))),
            };
            if batch.num_rows() as u64 != entry.rows {
                return Some(Err(corrupt(segment)));
            }
            if *at > self.skip {
                return Some(
                    segment
                        .shape
                        .convert(batch, &self.to)
                        .map(|b| (b, entry.stamp())),
                );
            }
        }
    }
}

/// Batches of several partitions in the order they were committed (`Stamp::order`). One batch
/// of each partition in memory at a time.
pub(crate) struct InOrder {
    /// Each partition's batches, and its next one once read.
    sources: Vec<(Batches, Option<(RecordBatch, Stamp)>)>,
    primed: bool,
}

/// `cursors`' batches in commit order, as `to`'s columns.
pub(crate) fn in_order(cursors: impl IntoIterator<Item = Cursor>, to: &Shape) -> InOrder {
    InOrder {
        sources: cursors
            .into_iter()
            .map(|c| (c.batches(to.clone()), None))
            .collect(),
        primed: false,
    }
}

impl Iterator for InOrder {
    type Item = Result<(RecordBatch, Stamp), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.primed {
            self.primed = true;
            for (batches, next) in &mut self.sources {
                match batches.next().transpose() {
                    Ok(read) => *next = read,
                    Err(e) => return Some(Err(e)),
                }
            }
        }
        let (_, earliest) = self
            .sources
            .iter()
            .enumerate()
            .filter_map(|(i, (_, next))| next.as_ref().map(|(_, stamp)| (stamp.order(), i)))
            .min()?;
        let (batches, next) = &mut self.sources[earliest];
        let taken = next.take();
        match batches.next().transpose() {
            Ok(read) => *next = read,
            Err(e) => return Some(Err(e)),
        }
        taken.map(Ok)
    }
}

fn data_path(dir: &Path, number: u32) -> PathBuf {
    dir.join(format!("seg-{number:06}.arrows"))
}

fn index_path(dir: &Path, number: u32) -> PathBuf {
    dir.join(format!("seg-{number:06}.idx"))
}

/// The schema message a segment's stream starts with, as its writer wrote it: the same schema
/// encodes to the same bytes (each field has one metadata key, so its order cannot vary).
pub(crate) fn schema_header(schema: &SchemaRef) -> Result<Vec<u8>, Error> {
    let writer = StreamWriter::try_new(Vec::new(), schema)
        .map_err(|e| Error::Arrow(format!("cannot encode a schema: {e}")))?;
    Ok(writer.get_ref().clone())
}

/// The schema the stream at `data` was written with: its schema message, read alone.
fn written_schema(disk: &DiskRef, data: &Path) -> Result<SchemaRef, Error> {
    let file = disk.open(data, false).map_err(|e| Error::io(data, e))?;
    let reader = StreamReader::try_new(file, None)
        .map_err(|e| Error::Arrow(format!("{}: {e}", data.display())))?;
    Ok(reader.schema())
}

/// The files an open segment writes, to fsync.
fn handles(dir: &Path, open: &Open) -> Result<[ToSync; 2], Error> {
    let clone = |file: &dyn DiskFile, path: PathBuf| {
        file.try_clone()
            .map(ToSync::File)
            .map_err(|e| Error::io(&path, e))
    };
    Ok([
        clone(
            &**open.stream.get_ref(),
            data_path(dir, open.segment.number),
        )?,
        clone(&*open.index, index_path(dir, open.segment.number))?,
    ])
}

/// A segment's committed batches: its index records up to the first that fails its check, and
/// of format 2 none after `trusted` (the log's to replay; the index is truncated there). A
/// segment left without one is deleted, so its number is free.
fn recover(
    disk: &DiskRef,
    dir: &Path,
    number: u32,
    trusted: u64,
    written: Written,
) -> Result<Option<Segment>, Error> {
    let index = index_path(dir, number);
    let data = data_path(dir, number);
    let bytes = disk.read(&index).map_err(|e| Error::io(&index, e))?;
    let record = match bytes.get(..HEADER.len()) {
        Some(header) if header == HEADER => RECORD,
        Some(header) if header == HEADER_1 => RECORD_1,
        // Created and never flushed: whatever a crash left of it is the log's to replay.
        None => 0,
        // Its header never reached the disk (a failed fsync dropped it, and records written
        // after it did): no flush covered the segment, so nothing in it can be trusted. A
        // trusted record behind it means the header was lost later, to corruption, unless the
        // records are folded (`Written::Folded`) and so folded again.
        Some(header) if header.iter().all(|&b| b == 0) => {
            let trusted_behind = bytes[HEADER.len()..]
                .as_chunks::<RECORD>()
                .0
                .iter()
                .filter_map(|record| Entry::decode(record))
                .any(|entry| entry.lsn <= trusted);
            if trusted_behind && written == Written::FromLog {
                return Err(Error::Corrupt(format!(
                    "{}: its header is gone, and the batches after it are trusted",
                    index.display()
                )));
            }
            0
        }
        Some(_) => {
            return Err(Error::Corrupt(format!(
                "{}: not a weir index this version can read",
                index.display()
            )));
        }
    };
    let mut entries: Vec<Entry> = Vec::new();
    if record > 0 {
        for chunk in bytes[HEADER.len()..].chunks_exact(record) {
            let Some(entry) = Entry::decode(chunk) else {
                break;
            };
            let follows = entries
                .last()
                .is_none_or(|last| entry.seq > last.seq && entry.end > last.end);
            if !follows || entry.lsn > trusted {
                break;
            }
            entries.push(entry);
        }
    }
    if written == Written::Folded {
        // A folded batch whose stream did not reach the disk is folded again, with those after.
        let length = match disk.len(&data) {
            Ok(length) => length,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
            Err(e) => return Err(Error::io(&data, e)),
        };
        entries.truncate(entries.partition_point(|entry| entry.end <= length));
    }
    let Some(last) = entries.last() else {
        for path in [&index, &data] {
            match disk.remove_file(path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(Error::io(path, e)),
            }
        }
        return Ok(None);
    };
    let kept = (HEADER.len() + entries.len() * record) as u64;
    if (bytes.len() as u64) > kept {
        // Records past the point the files hold the log: dropped, so the replay writes them.
        disk.open(&index, true)
            .and_then(|file| file.set_len(kept).and_then(|()| file.sync_all()))
            .map_err(|e| Error::io(&index, e))?;
    }
    let length = disk.len(&data).map_err(|e| Error::io(&data, e))?;
    if length < last.end {
        return Err(Error::Corrupt(format!(
            "{}: the index commits {} bytes, the stream has {length}",
            data.display(),
            last.end
        )));
    }
    Ok(Some(Segment::new(
        number,
        entries,
        written_schema(disk, &data)?,
    )))
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Seek;
    use std::sync::Arc;

    use arrow::array::{AsArray, Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Int64Type, Schema};
    use arrow::ipc::writer::StreamWriter;

    use super::{Change, Entry, HEADER, Partition, Written, data_path, index_path};
    use crate::disk::real;
    use crate::shape::{ColumnId, Shape};

    /// A cursor from any batch delivers exactly the batches from there on, across segments,
    /// and counts their bytes.
    #[test]
    fn a_cursor_reads_on_from_any_batch_across_segments() {
        let dir = std::env::temp_dir().join(format!("kermodes-cursor-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let shape = Shape::new(schema.clone(), Arc::new([ColumnId(1)]));
        let disk = real(&dir.join("fsync-failed"));
        let mut partition =
            Partition::open(&disk, &dir, &dir, shape.clone(), u64::MAX, Written::FromLog).unwrap();
        // About 24 MiB a batch: three to a 64 MiB segment, so ten batches span four segments of
        // three, three, three, and one.
        let rows = 3 << 20;
        for v in 0..10_i64 {
            let n = Int64Array::from(vec![v; rows]);
            let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(n)]).unwrap();
            assert!(
                partition
                    .append(&batch, v as u64 + 1, v as u64 + 1, 0, 1, Change::Rows)
                    .unwrap()
            );
        }
        assert!(
            partition.segments().count() >= 3,
            "the batches span segments"
        );
        assert_eq!(partition.committed(), 10);
        let mut last_bytes = u64::MAX;
        for from in 0..=10 {
            let cursor = partition.cursor(from);
            assert!(cursor.bytes() < last_bytes || from == 10, "from {from}");
            last_bytes = cursor.bytes();
            let read: Vec<i64> = cursor
                .batches(shape.clone())
                .map(|read| {
                    read.unwrap()
                        .0
                        .column(0)
                        .as_primitive::<Int64Type>()
                        .value(0)
                })
                .collect();
            assert_eq!(read, (from as i64..10).collect::<Vec<_>>(), "from {from}");
        }
        assert_eq!(partition.cursor(10).bytes(), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A segment written before column IDs (its schema has none) reads both ways: by cursor,
    /// and by position, which finds where its batches start from its schema message.
    #[test]
    fn a_segment_written_before_column_ids_reads_by_cursor_and_by_position() {
        let dir = std::env::temp_dir().join(format!("kermodes-old-seg-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));
        // As the version before column IDs wrote it: the plain schema, then the index.
        let mut stream =
            StreamWriter::try_new(File::create(data_path(&dir, 1)).unwrap(), &schema).unwrap();
        let mut index = HEADER.to_vec();
        for seq in 1..=3_u64 {
            let n = Int64Array::from(vec![seq as i64 * 10; 4]);
            stream
                .write(&RecordBatch::try_new(schema.clone(), vec![Arc::new(n)]).unwrap())
                .unwrap();
            stream.flush().unwrap();
            let end = stream.get_mut().stream_position().unwrap();
            let entry = Entry {
                seq,
                rows: 4,
                ingest_ns: seq as i64,
                end,
                change: Change::Rows,
                lsn: seq,
            };
            index.extend_from_slice(&entry.encode(1));
        }
        std::fs::write(index_path(&dir, 1), index).unwrap();

        let shape = Shape::new(schema.clone(), Arc::new([ColumnId(1)]));
        let disk = real(&dir.join("fsync-failed"));
        let partition =
            Partition::open(&disk, &dir, &dir, shape.clone(), u64::MAX, Written::FromLog).unwrap();
        let first = |batch: &RecordBatch| batch.column(0).as_primitive::<Int64Type>().value(0);
        let by_cursor: Vec<i64> = partition
            .cursor(0)
            .batches(shape.clone())
            .map(|read| first(&read.unwrap().0))
            .collect();
        assert_eq!(by_cursor, [10, 20, 30]);
        for (from, count) in [(0, 3), (1, 2), (2, 1)] {
            let by_position: Vec<i64> = partition
                .read_batches(from, count, &shape)
                .unwrap()
                .iter()
                .map(first)
                .collect();
            assert_eq!(by_position, [10, 20, 30][from..], "from {from}");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
