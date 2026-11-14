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
use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};

use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

use crate::Error;

/// Index file header: magic and format version. A new version is a new header; readers refuse
/// versions they do not know.
const HEADER: &[u8; 8] = b"KMWEIR\x00\x01";
const RECORD: usize = 48;
/// The change kind of a batch of plain rows. Keyed tables add upserts and deletes.
const ROWS: u32 = 0;
/// A segment stops taking batches once its stream reaches this size.
const SEGMENT_BYTES: u64 = 64 << 20;

/// One committed batch. Its index record (little-endian) is: seq u64, rows u64, ingest time ns
/// i64, end offset u64, epoch u64 (the lease its writer held, §7.4), change kind u32, CRC-32 of
/// the preceding 44 bytes.
#[derive(Clone, Copy, Debug)]
struct Entry {
    seq: u64,
    rows: u64,
    /// The stream's length once this batch is in it.
    end: u64,
}

impl Entry {
    fn encode(&self, ingest_ns: i64, epoch: u64) -> [u8; RECORD] {
        let mut record = [0; RECORD];
        record[0..8].copy_from_slice(&self.seq.to_le_bytes());
        record[8..16].copy_from_slice(&self.rows.to_le_bytes());
        record[16..24].copy_from_slice(&ingest_ns.to_le_bytes());
        record[24..32].copy_from_slice(&self.end.to_le_bytes());
        record[32..40].copy_from_slice(&epoch.to_le_bytes());
        record[40..44].copy_from_slice(&ROWS.to_le_bytes());
        let crc = crc32fast::hash(&record[..44]);
        record[44..48].copy_from_slice(&crc.to_le_bytes());
        record
    }

    /// The record, unless it is torn, zeroed, or of a change kind this version does not know.
    fn decode(record: &[u8; RECORD]) -> Option<Self> {
        let word = |at: usize| u64::from_le_bytes(record[at..at + 8].try_into().expect("8 bytes"));
        let crc = u32::from_le_bytes(record[44..48].try_into().expect("4 bytes"));
        let change = u32::from_le_bytes(record[40..44].try_into().expect("4 bytes"));
        if crc32fast::hash(&record[..44]) != crc || change != ROWS {
            return None;
        }
        Some(Self {
            seq: word(0),
            rows: word(8),
            end: word(24),
        })
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

    /// Appends `batch` as `seq`, written under lease `epoch`, durably, before returning; `false`
    /// (and nothing written) when `seq` is not above the last sequence number.
    pub(crate) fn append(
        &mut self,
        batch: &RecordBatch,
        seq: u64,
        ingest_ns: i64,
        epoch: u64,
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
            end,
        };
        let index = index_path(&self.dir, open.segment.number);
        open.index
            .write_all(&entry.encode(ingest_ns, epoch))
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
                rows: segment.entries.iter().map(|e| e.rows).collect(),
            });
        }
        Cursor {
            segments,
            skip: first_skip,
            bytes,
        }
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

/// A segment's committed batches: its stream up to the last committed byte, and each batch's row
/// count, to check what the stream holds against what the index commits.
struct Committed {
    path: PathBuf,
    end: u64,
    rows: Vec<u64>,
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
    /// Feeds `receive` each batch in order, decoding one at a time, until it returns `false`;
    /// whether every batch was taken.
    pub(crate) fn read(self, mut receive: impl FnMut(RecordBatch) -> bool) -> Result<bool, Error> {
        let mut skip = self.skip;
        for segment in self.segments {
            let path = &segment.path;
            let corrupt = || {
                Error::Corrupt(format!(
                    "{}: the stream does not hold the {} batches its index commits",
                    path.display(),
                    segment.rows.len()
                ))
            };
            let file = File::open(path).map_err(|e| Error::io(path, e))?;
            let mut batches = StreamReader::try_new(file.take(segment.end), None)
                .map_err(|e| Error::Arrow(format!("{}: {e}", path.display())))?;
            for (i, rows) in segment.rows.iter().enumerate() {
                let batch = batches
                    .next()
                    .ok_or_else(corrupt)?
                    .map_err(|e| Error::Arrow(format!("{}: {e}", path.display())))?;
                if batch.num_rows() as u64 != *rows {
                    return Err(corrupt());
                }
                if i >= skip && !receive(batch) {
                    return Ok(false);
                }
            }
            if batches.next().is_some() {
                return Err(corrupt());
            }
            skip = 0;
        }
        Ok(true)
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

    use super::Partition;

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
            assert!(partition.append(&batch, v as u64 + 1, 0, 1).unwrap());
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
            let mut read = Vec::new();
            assert!(
                cursor
                    .read(|batch| {
                        read.push(batch.column(0).as_primitive::<Int64Type>().value(0));
                        true
                    })
                    .unwrap()
            );
            assert_eq!(read, (from as i64..10).collect::<Vec<_>>(), "from {from}");
        }
        assert_eq!(partition.cursor(10).bytes(), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
