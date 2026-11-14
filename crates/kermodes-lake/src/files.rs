//! History files in object storage (architecture.md §8.1): Parquet with a `Date` column first,
//! whole dates per file, a row group ending at every date and at about 128 MiB, and a file ending
//! once it reaches the target size, so a big date lies in several. ZSTD, dictionaries, and
//! page-level statistics with the page index, so readers skip by date, key, and time.

use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{Array, ArrayRef, AsArray, Date32Array, RecordBatch};
use arrow::datatypes::{DataType, Date32Type, Field, Schema, SchemaRef};
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt, TryFutureExt};
use kermodes_weir::{Date, Error, HistoryDate, HistoryFile};
use object_store::aws::AmazonS3Builder;
use object_store::buffered::BufWriter;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::prefix::PrefixStore;
use object_store::{GetOptions, GetRange, ObjectStore, ObjectStoreExt};
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use parquet::arrow::async_reader::{
    AsyncFileReader, MetadataSuffixFetch, ParquetRecordBatchStream,
};
use parquet::arrow::async_writer::AsyncArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::errors::ParquetError;
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use tokio::runtime::Runtime;

/// The size a history file grows to (§8.1).
pub const TARGET_FILE_BYTES: u64 = 256 << 20;

/// Row groups end at every date, and once this much is buffered.
const ROW_GROUP_BYTES: usize = 128 << 20;

/// The column every history file starts with.
pub const DATE: &str = "Date";

/// Where a store's history files live: a local directory, or `s3://bucket[/prefix]` with
/// credentials and endpoint from the usual `AWS_*` variables (`AWS_ENDPOINT`, and
/// `AWS_ALLOW_HTTP=true` for rustfs inside the cluster).
pub struct Lake {
    store: Arc<dyn ObjectStore>,
    runtime: Runtime,
}

/// What a write produced, for the catalog commit.
#[derive(Debug, Default)]
pub struct Written {
    pub files: Vec<HistoryFile>,
    pub dates: Vec<HistoryDate>,
}

/// A file being written.
struct Open {
    writer: AsyncArrowWriter<BufWriter>,
    name: String,
    first: Date,
    last: Date,
    rows: u64,
    /// Rows of the current date in this file, and the bytes written when it began here.
    date_rows: u64,
    date_start: usize,
}

impl Lake {
    pub fn open(url: &str) -> Result<Self, Error> {
        let io = |e: object_store::Error| Error::Io(format!("history store {url}: {e}"));
        let store: Arc<dyn ObjectStore> = match url.strip_prefix("s3://") {
            Some(rest) => {
                let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
                let s3 = AmazonS3Builder::from_env()
                    .with_bucket_name(bucket)
                    .build()
                    .map_err(io)?;
                match prefix.trim_matches('/') {
                    "" => Arc::new(s3),
                    prefix => Arc::new(PrefixStore::new(s3, prefix)),
                }
            }
            None => {
                let dir = url.strip_prefix("file://").unwrap_or(url);
                std::fs::create_dir_all(dir)
                    .map_err(|e| Error::Io(format!("history store {dir}: {e}")))?;
                Arc::new(LocalFileSystem::new_with_prefix(dir).map_err(io)?)
            }
        };
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("kermodes-lake")
            .enable_all()
            .build()
            .map_err(|e| Error::Io(format!("cannot start the history store's runtime: {e}")))?;
        Ok(Self { store, runtime })
    }

    /// Writes `dates`, in order, as files under `location` (a table's), each date's rows from
    /// `rows`, which hands them to the function it is given. A date with no rows is in no file.
    /// One file holds the dates until it reaches `target`; it then ends with the row group, so a
    /// big date continues in the next.
    pub fn write(
        &self,
        location: &str,
        schema: &Schema,
        dates: &[Date],
        mut rows: impl FnMut(
            Date,
            &mut dyn FnMut(RecordBatch) -> Result<(), Error>,
        ) -> Result<(), Error>,
        target: u64,
    ) -> Result<Written, Error> {
        if schema.column_with_name(DATE).is_some() {
            return Err(Error::Schema(format!(
                "a table with history cannot have its own `{DATE}` column: history adds it"
            )));
        }
        let file_schema = with_date_column(schema);
        let mut written = Written::default();
        let mut open: Option<Open> = None;
        for &date in dates {
            rows(date, &mut |batch| {
                if batch.num_rows() == 0 {
                    return Ok(());
                }
                let batch = dated(&file_schema, date, &batch)?;
                let file = match &mut open {
                    Some(file) => file,
                    None => open.insert(self.create(location, &file_schema, date)?),
                };
                if file.last != date {
                    file.last = date;
                    file.date_rows = 0;
                    file.date_start = file.writer.bytes_written();
                }
                self.runtime
                    .block_on(file.writer.write(&batch))
                    .map_err(parquet_error)?;
                file.rows += batch.num_rows() as u64;
                file.date_rows += batch.num_rows() as u64;
                if file.writer.in_progress_size() >= ROW_GROUP_BYTES {
                    self.runtime
                        .block_on(file.writer.flush())
                        .map_err(parquet_error)?;
                }
                let size = file.writer.bytes_written() + file.writer.in_progress_size();
                if size as u64 >= target {
                    let full = open.take().expect("a file is open");
                    self.finish(full, &mut written)?;
                }
                Ok(())
            })?;
            // A row group never holds two dates.
            if let Some(file) = &mut open
                && file.last == date
                && file.date_rows > 0
            {
                self.runtime
                    .block_on(file.writer.flush())
                    .map_err(parquet_error)?;
                written.dates.push(HistoryDate {
                    date,
                    file: file.name.clone(),
                    rows: file.date_rows,
                    bytes: (file.writer.bytes_written() - file.date_start) as u64,
                });
                file.date_rows = 0;
                file.date_start = file.writer.bytes_written();
            }
        }
        if let Some(file) = open {
            self.finish(file, &mut written)?;
        }
        Ok(written)
    }

    /// The rows of `first` to `last` in a history file, reading only the row groups of those
    /// dates.
    pub fn read(
        &self,
        location: &str,
        file: &str,
        first: Date,
        last: Date,
    ) -> Result<Batches<'_>, Error> {
        let path = Path::from(format!("{location}/{file}"));
        let reader = StoreReader {
            store: self.store.clone(),
            path,
        };
        self.runtime.block_on(async {
            let builder = ParquetRecordBatchStreamBuilder::new(reader)
                .await
                .map_err(parquet_error)?;
            let dates =
                StatisticsConverter::try_new(DATE, builder.schema(), builder.parquet_schema())
                    .map_err(parquet_error)?;
            let groups = builder.metadata().row_groups();
            let mins = dates.row_group_mins(groups.iter()).map_err(parquet_error)?;
            let maxes = dates
                .row_group_maxes(groups.iter())
                .map_err(parquet_error)?;
            let (mins, maxes) = (as_dates(&mins)?, as_dates(&maxes)?);
            let wanted: Vec<usize> = (0..groups.len())
                .filter(|&g| {
                    mins.is_valid(g)
                        && maxes.is_valid(g)
                        && mins.value(g) <= last.days()
                        && maxes.value(g) >= first.days()
                })
                .collect();
            let row_groups = (wanted.len(), groups.len());
            let stream = builder
                .with_row_groups(wanted)
                .build()
                .map_err(parquet_error)?;
            Ok(Batches {
                runtime: &self.runtime,
                stream,
                row_groups,
            })
        })
    }

    /// Deletes a history file; one already gone is deleted.
    pub fn delete(&self, location: &str, file: &str) -> Result<(), Error> {
        let path = Path::from(format!("{location}/{file}"));
        match self.runtime.block_on(self.store.delete(&path)) {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(Error::Io(format!("cannot delete {path}: {e}"))),
        }
    }

    fn create(&self, location: &str, schema: &SchemaRef, first: Date) -> Result<Open, Error> {
        let name = file_name(first);
        let path = Path::from(format!("{location}/{name}"));
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .set_statistics_enabled(EnabledStatistics::Page)
            .build();
        let writer = AsyncArrowWriter::try_new(
            BufWriter::new(self.store.clone(), path),
            schema.clone(),
            Some(props),
        )
        .map_err(parquet_error)?;
        Ok(Open {
            writer,
            name,
            first,
            last: first,
            rows: 0,
            date_rows: 0,
            date_start: 0,
        })
    }

    fn finish(&self, mut file: Open, written: &mut Written) -> Result<(), Error> {
        self.runtime
            .block_on(file.writer.flush())
            .map_err(parquet_error)?;
        if file.date_rows > 0 {
            written.dates.push(HistoryDate {
                date: file.last,
                file: file.name.clone(),
                rows: file.date_rows,
                bytes: (file.writer.bytes_written() - file.date_start) as u64,
            });
        }
        // Writes the footer and completes the upload; the size is then the file's.
        let mut writer = file.writer;
        self.runtime
            .block_on(writer.finish())
            .map_err(parquet_error)?;
        let bytes = writer.bytes_written() as u64;
        written.files.push(HistoryFile {
            name: file.name,
            first: file.first,
            last: file.last,
            rows: file.rows,
            bytes,
        });
        Ok(())
    }
}

/// A history file's rows, a batch at a time, from the row groups of the dates asked for.
pub struct Batches<'a> {
    runtime: &'a Runtime,
    stream: ParquetRecordBatchStream<StoreReader>,
    /// Row groups read, and in the file.
    pub row_groups: (usize, usize),
}

impl Iterator for Batches<'_> {
    type Item = Result<RecordBatch, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.runtime
            .block_on(self.stream.next())
            .map(|batch| batch.map_err(parquet_error))
    }
}

/// A table's columns after `Date`, as every history file has them.
pub fn with_date_column(schema: &Schema) -> SchemaRef {
    let mut fields = vec![Arc::new(Field::new(DATE, DataType::Date32, false))];
    fields.extend(schema.fields().iter().cloned());
    Arc::new(Schema::new(fields))
}

fn dated(schema: &SchemaRef, date: Date, batch: &RecordBatch) -> Result<RecordBatch, Error> {
    let mut columns: Vec<ArrayRef> = vec![Arc::new(Date32Array::from_value(
        date.days(),
        batch.num_rows(),
    ))];
    columns.extend(batch.columns().iter().cloned());
    RecordBatch::try_new(schema.clone(), columns).map_err(|e| Error::Arrow(e.to_string()))
}

fn as_dates(array: &ArrayRef) -> Result<&Date32Array, Error> {
    array
        .as_primitive_opt::<Date32Type>()
        .ok_or_else(|| Error::Corrupt(format!("a history file's `{DATE}` is not a date")))
}

/// Unique across processes and time: a file is never written twice under one name.
fn file_name(first: Date) -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    format!("{first}-{nanos:x}-{:x}-{n:x}.parquet", std::process::id())
}

fn parquet_error(error: parquet::errors::ParquetError) -> Error {
    Error::Arrow(error.to_string())
}

/// A history file read through the object store, in ranges: the footer, then the row groups
/// wanted.
struct StoreReader {
    store: Arc<dyn ObjectStore>,
    path: Path,
}

impl AsyncFileReader for StoreReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        self.store
            .get_range(&self.path, range)
            .map_err(store_to_parquet)
            .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        async move {
            self.store
                .get_ranges(&self.path, &ranges)
                .await
                .map_err(store_to_parquet)
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        async move {
            let metadata = ParquetMetaDataReader::new()
                .with_arrow_reader_options(options)
                .load_via_suffix_and_finish(self)
                .await?;
            Ok(Arc::new(metadata))
        }
        .boxed()
    }
}

impl MetadataSuffixFetch for &mut StoreReader {
    fn fetch_suffix(&mut self, suffix: usize) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        let options = GetOptions {
            range: Some(GetRange::Suffix(suffix as u64)),
            ..Default::default()
        };
        async move {
            let got = self
                .store
                .get_opts(&self.path, options)
                .await
                .map_err(store_to_parquet)?;
            got.bytes().await.map_err(store_to_parquet)
        }
        .boxed()
    }
}

fn store_to_parquet(error: object_store::Error) -> ParquetError {
    ParquetError::External(Box::new(error))
}
