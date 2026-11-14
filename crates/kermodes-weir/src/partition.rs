//! One internal partition's log: `(table, date, source)`, written by one writer.
//!
//! A partition is a directory of segments. Each segment is an Arrow IPC stream (`seg-N.arrows`)
//! and an index (`seg-N.idx`): an 8-byte header, then one fixed-size record per batch. A batch is
//! committed when its index record is: the batch is written and fsynced to the stream first, then
//! the record is written and fsynced. Recovery keeps the records whose CRC holds, in order, and
//! reads each stream only up to the last of them; writing always starts a new segment, so what a
//! crash left beyond that point is never read. A crash loses at most the batch being written,
//! which was never acknowledged. Sequence numbers rise strictly; a batch at or below the last one
//! is a resend and is dropped.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, Take, Write};
use std::path::{Path, PathBuf};

use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

use crate::{Change, Error};

/// Index file header: magic and format version. A new version is a new header; readers refuse
/// versions they do not know.
const HEADER: &[u8; 8] = b"KMWEIR\x00\x01";
const RECORD: usize = 48;
/// A segment stops taking batches once its stream reaches this size.
const SEGMENT_BYTES: u64 = 64 << 20;

/// One committed batch. Its index record (little-endian) is: seq u64, rows u64, ingest time ns
/// i64, end offset u64, epoch u64 (the lease its writer held, §7.4), change kind u32 (§7.1),
/// CRC-32 of the preceding 44 bytes.
#[derive(Clone, Copy, Debug)]
struct Entry {
    seq: u64,
    rows: u64,
    ingest_ns: i64,
    /// The stream's length once this batch is in it.
    end: u64,
    change: Change,
}

/// What a batch was committed as: its change kind and when it arrived.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Stamp {
    pub change: Change,
    pub ingest_ns: i64,
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
        let crc = crc32fast::hash(&record[..44]);
        record[44..48].copy_from_slice(&crc.to_le_bytes());
        record
    }

    /// The record, unless it is torn, zeroed, or of a change kind this version does not know
    /// (recovery then ends there, as an older version's would).
    fn decode(record: &[u8; RECORD]) -> Option<Self> {
        let word = |at: usize| u64::from_le_bytes(record[at..at + 8].try_into().expect("8 bytes"));
        let crc = u32::from_le_bytes(record[44..48].try_into().expect("4 bytes"));
        if crc32fast::hash(&record[..44]) != crc {
            return None;
        }
        let change = u32::from_le_bytes(record[40..44].try_into().expect("4 bytes"));
        Some(Self {
            seq: word(0),
            rows: word(8),
            ingest_ns: word(16) as i64,
            end: word(24),
            change: Change::from_code(change)?,
        })
    }

    fn stamp(&self) -> Stamp {
        Stamp {
            change: self.change,
            ingest_ns: self.ingest_ns,
        }
    }
}

struct Segment {
    number: u32,
    entries: Vec<Entry>,
}

/// The segment taking batches.
struct Open {
    segment: Segment,
    stream: StreamWriter<File>,
    index: File,
}

pub(crate) struct Partition {
    dir: PathBuf,
    schema: SchemaRef,
    /// Segments no longer taking batches, in order.
    closed: Vec<Segment>,
    open: Option<Open>,
    last_seq: Option<u64>,
}

impl Partition {
    /// Opens (creating if needed) the partition in `dir`, recovering its segments. Writing starts
    /// a new segment: one an earlier process left open is not appended to.
    pub(crate) fn open(dir: &Path, schema: SchemaRef) -> Result<Self, Error> {
        fs::create_dir_all(dir).map_err(|e| Error::io(dir, e))?;
        let mut numbers = Vec::new();
        for entry in fs::read_dir(dir).map_err(|e| Error::io(dir, e))? {
            let name = entry.map_err(|e| Error::io(dir, e))?.file_name();
            let name = name.to_string_lossy();
            if let Some(number) = name
                .strip_prefix("seg-")
                .and_then(|n| n.strip_suffix(".idx"))
                .and_then(|n| n.parse::<u32>().ok())
            {
                numbers.push(number);
            }
        }
        numbers.sort_unstable();
        let mut closed = Vec::new();
        for number in numbers {
            if let Some(segment) = recover(dir, number)? {
                closed.push(segment);
            }
        }
        let last_seq = closed
            .last()
            .and_then(|s: &Segment| s.entries.last())
            .map(|e| e.seq);
        Ok(Self {
            dir: dir.to_owned(),
            schema,
            closed,
            open: None,
            last_seq,
        })
    }

    pub(crate) fn last_seq(&self) -> Option<u64> {
        self.last_seq
    }

    /// Appends `batch` as `seq`, a `change` written under lease `epoch`, durably, before
    /// returning; `false` (and nothing written) when `seq` is not above the last sequence number.
    pub(crate) fn append(
        &mut self,
        batch: &RecordBatch,
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
        let file = open.stream.get_mut();
        file.sync_data().map_err(|e| Error::io(&data, e))?;
        let end = file.stream_position().map_err(|e| Error::io(&data, e))?;
        let entry = Entry {
            seq,
            rows: batch.num_rows() as u64,
            ingest_ns,
            end,
            change,
        };
        let index = index_path(&self.dir, open.segment.number);
        open.index
            .write_all(&entry.encode(epoch))
            .and_then(|()| open.index.sync_data())
            .map_err(|e| Error::io(&index, e))?;
        open.segment.entries.push(entry);
        self.last_seq = Some(seq);
        if end >= SEGMENT_BYTES {
            let full = self.open.take().expect("open above");
            self.closed.push(full.segment);
        }
        Ok(true)
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
            });
        }
        Cursor {
            segments,
            skip: first_skip,
            bytes,
        }
    }

    /// How many committed batches arrived at or before `ingest_ns`. Ingest times rise within a
    /// partition, so the batches after a point in time are a cursor from here.
    pub(crate) fn at_or_before(&self, ingest_ns: i64) -> usize {
        self.segments()
            .map(|s| s.entries.partition_point(|e| e.ingest_ns <= ingest_ns))
            .sum()
    }

    /// When the newest committed batch arrived.
    pub(crate) fn last_ingest_ns(&self) -> Option<i64> {
        self.segments()
            .last()
            .and_then(|segment| segment.entries.last())
            .map(|entry| entry.ingest_ns)
    }

    fn segments(&self) -> impl Iterator<Item = &Segment> {
        self.closed
            .iter()
            .chain(self.open.as_ref().map(|open| &open.segment))
            .filter(|segment| !segment.entries.is_empty())
    }

    fn start(&self) -> Result<Open, Error> {
        let number = self.closed.last().map_or(1, |segment| segment.number + 1);
        let data = data_path(&self.dir, number);
        let index = index_path(&self.dir, number);
        let create = |path: &Path| {
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)
                .map_err(|e| Error::io(path, e))
        };
        let stream = StreamWriter::try_new(create(&data)?, &self.schema)
            .map_err(|e| Error::Arrow(format!("{}: {e}", data.display())))?;
        stream
            .get_ref()
            .sync_data()
            .map_err(|e| Error::io(&data, e))?;
        let mut index_file = create(&index)?;
        index_file
            .write_all(HEADER)
            .and_then(|()| index_file.sync_data())
            .map_err(|e| Error::io(&index, e))?;
        // The new files' directory entries must survive a crash too.
        File::open(&self.dir)
            .and_then(|dir| dir.sync_all())
            .map_err(|e| Error::io(&self.dir, e))?;
        Ok(Open {
            segment: Segment {
                number,
                entries: Vec::new(),
            },
            stream,
            index: index_file,
        })
    }
}

/// A segment's committed batches: its stream up to the last committed byte, and each batch's
/// record, to check what the stream holds against what the index commits.
struct Committed {
    path: PathBuf,
    end: u64,
    entries: Vec<Entry>,
}

/// Committed batches of a partition, read one at a time (`Partition::cursor`).
pub(crate) struct Cursor {
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

    /// The batches in order, decoded one at a time as they are taken.
    pub(crate) fn batches(self) -> Batches {
        Batches {
            segments: self.segments.into_iter(),
            skip: self.skip,
            current: None,
        }
    }
}

/// A cursor's batches, pulled one at a time (`Cursor::batches`).
pub(crate) struct Batches {
    segments: std::vec::IntoIter<Committed>,
    skip: usize,
    /// The segment being read: its stream and the records of its batches still to come.
    current: Option<(Committed, StreamReader<Take<File>>, usize)>,
}

impl Iterator for Batches {
    type Item = Result<(RecordBatch, Stamp), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current.is_none() {
                let segment = self.segments.next()?;
                let file = match File::open(&segment.path) {
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
                return Some(Ok((batch, entry.stamp())));
            }
        }
    }
}

/// Batches of several partitions in the order they were committed: by ingest time, which a
/// table's clock makes rise across all its partitions (ties, from before it did, go to the
/// earlier partition). One batch of each partition in memory at a time.
pub(crate) struct InOrder {
    /// Each partition's batches, and its next one once read.
    sources: Vec<(Batches, Option<(RecordBatch, Stamp)>)>,
    primed: bool,
}

pub(crate) fn in_order(cursors: impl IntoIterator<Item = Cursor>) -> InOrder {
    InOrder {
        sources: cursors.into_iter().map(|c| (c.batches(), None)).collect(),
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
            .filter_map(|(i, (_, next))| next.as_ref().map(|(_, stamp)| (stamp.ingest_ns, i)))
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

/// A segment's committed batches: its index records up to the first that fails its check. A
/// segment without one (it crashed before its first commit) is deleted, so its number is free.
fn recover(dir: &Path, number: u32) -> Result<Option<Segment>, Error> {
    let index = index_path(dir, number);
    let data = data_path(dir, number);
    let bytes = fs::read(&index).map_err(|e| Error::io(&index, e))?;
    if bytes.len() >= HEADER.len() && &bytes[..HEADER.len()] != HEADER {
        return Err(Error::Corrupt(format!(
            "{}: not a weir index this version can read",
            index.display()
        )));
    }
    let mut entries: Vec<Entry> = Vec::new();
    if bytes.len() > HEADER.len() {
        for record in bytes[HEADER.len()..].as_chunks::<RECORD>().0 {
            let Some(entry) = Entry::decode(record) else {
                break;
            };
            let follows = entries
                .last()
                .is_none_or(|last| entry.seq > last.seq && entry.end > last.end);
            if !follows {
                break;
            }
            entries.push(entry);
        }
    }
    let Some(last) = entries.last() else {
        for path in [&index, &data] {
            match fs::remove_file(path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(Error::io(path, e)),
            }
        }
        return Ok(None);
    };
    let length = fs::metadata(&data).map_err(|e| Error::io(&data, e))?.len();
    if length < last.end {
        return Err(Error::Corrupt(format!(
            "{}: the index commits {} bytes, the stream has {length}",
            data.display(),
            last.end
        )));
    }
    Ok(Some(Segment { number, entries }))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{AsArray, Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Int64Type, Schema};

    use super::{Change, Partition};

    /// A cursor from any batch delivers exactly the batches from there on, across segments,
    /// and counts their bytes.
    #[test]
    fn a_cursor_reads_on_from_any_batch_across_segments() {
        let dir = std::env::temp_dir().join(format!("kermodes-cursor-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let mut partition = Partition::open(&dir, schema.clone()).unwrap();
        // About 24 MiB a batch: three to a 64 MiB segment, so ten batches span four segments of
        // three, three, three, and one.
        let rows = 3 << 20;
        for v in 0..10_i64 {
            let n = Int64Array::from(vec![v; rows]);
            let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(n)]).unwrap();
            assert!(
                partition
                    .append(&batch, v as u64 + 1, 0, 1, Change::Rows)
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
                .batches()
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
}
