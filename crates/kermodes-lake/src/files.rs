//! History files in object storage (architecture.md §8.1): Parquet with a `Date` column first,
//! whole dates per file, a row group ending at every date and at about 128 MiB, and a file ending
//! once it reaches the target size, so a big date lies in several. ZSTD, dictionaries, and
//! page-level statistics with the page index, so readers skip by date, key, and time.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{
    Array, ArrayRef, AsArray, Date32Array, Float64Array, Int64Array, RecordBatch,
    RecordBatchOptions, Scalar, StringArray,
};
use arrow::compute::kernels::cmp::{gt_eq, lt_eq};
use arrow::compute::{SortColumn, concat_batches, lexsort_to_indices, take_record_batch};
use arrow::datatypes::{DataType, Date32Type, Field, Schema, SchemaRef};
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt, TryFutureExt};
use kermodes_weir::{Change, ColumnId, Date, Error, HistoryDate, HistoryFile, Shape, TableEntry};
use object_store::aws::AmazonS3Builder;
use object_store::buffered::BufWriter;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::prefix::PrefixStore;
use object_store::{GetOptions, GetRange, ObjectStore, ObjectStoreExt};
use parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use parquet::arrow::arrow_reader::{ArrowReaderOptions, RowSelection, RowSelector};
use parquet::arrow::async_reader::{
    AsyncFileReader, MetadataSuffixFetch, ParquetRecordBatchStream,
};
use parquet::arrow::async_writer::AsyncArrowWriter;
use parquet::arrow::{PARQUET_FIELD_ID_META_KEY, ParquetRecordBatchStreamBuilder};
use parquet::basic::{Compression, ZstdLevel};
use parquet::errors::ParquetError;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use tokio::runtime::Runtime;

use crate::bounds::{Bound, Literal};

/// The size a history file grows to (§8.1).
pub const TARGET_FILE_BYTES: u64 = 256 << 20;

/// Row groups end at every date, and once this much is buffered.
const ROW_GROUP_BYTES: usize = 128 << 20;

/// The column every history file starts with.
pub const DATE: &str = "Date";

/// The column a keyed table's history adds after `Date`: each row's change kind (`upsert`,
/// `delete`, or `checkpoint`), since a keyed table's history is its log of changes.
pub const OP: &str = "Op";

/// The columns history keeps of a table, after `Date`: a keyed table's with `Op` first.
pub fn history_columns(entry: &TableEntry) -> Result<Schema, Error> {
    if entry.key.is_empty() {
        return Ok(entry.schema.as_ref().clone());
    }
    if entry.schema.column_with_name(OP).is_some() {
        return Err(Error::Schema(format!(
            "a keyed table with history cannot have its own `{OP}` column: history adds it"
        )));
    }
    let mut fields = vec![Arc::new(Field::new(OP, DataType::Utf8, false))];
    fields.extend(entry.schema.fields().iter().cloned());
    Ok(Schema::new(fields))
}

/// `history_columns` as a file is written with them: each of the table's columns with its ID
/// as its Parquet `field_id` (§8.1), so the file reads under the table's later columns.
pub fn file_columns(entry: &TableEntry) -> Result<Schema, Error> {
    let history = history_columns(entry)?;
    let system = history.fields().len() - entry.ids.len();
    let fields: Vec<Field> = history
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| match i.checked_sub(system) {
            None => field.as_ref().clone(),
            Some(at) => field.as_ref().clone().with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_owned(),
                entry.ids[at].0.to_string(),
            )])),
        })
        .collect();
    Ok(Schema::new(fields))
}

/// A history file's batch as the table's history has it now (`Date`, then
/// `history_columns`): its columns matched by ID, so a file written under earlier columns reads
/// under the current ones (§5.4); one written before columns had IDs, by position.
pub fn as_current(entry: &TableEntry, batch: RecordBatch) -> Result<RecordBatch, Error> {
    let schema = with_date_column(&history_columns(entry)?);
    // `Date`, and a keyed table's `Op`, are history's own.
    let system = schema.fields().len() - entry.ids.len();
    let written = batch.schema();
    let data = Schema::new(written.fields()[system..].to_vec());
    let shape = Shape::with_ids(&data, |_, field| {
        field
            .metadata()
            .get(PARQUET_FIELD_ID_META_KEY)
            .and_then(|id| id.parse().ok())
            .map(ColumnId)
    });
    let mut columns = batch.columns()[..system].to_vec();
    let current = entry.shape();
    if shape.reads_as(&current) {
        columns.extend(batch.columns()[system..].iter().cloned());
    } else {
        let data = RecordBatch::try_new_with_options(
            shape.schema.clone(),
            batch.columns()[system..].to_vec(),
            &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
        )
        .map_err(arrow_error)?;
        columns.extend(shape.project(&data, &current)?.columns().iter().cloned());
    }
    RecordBatch::try_new_with_options(
        schema,
        columns,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
    .map_err(arrow_error)
}

/// A batch weir holds, as history keeps it (`history_columns`).
pub fn with_op(
    entry: &TableEntry,
    batch: RecordBatch,
    change: Change,
) -> Result<RecordBatch, Error> {
    if entry.key.is_empty() {
        return Ok(batch);
    }
    let op = match change {
        Change::Upsert => "upsert",
        Change::Delete => "delete",
        Change::Checkpoint => "checkpoint",
        Change::Rows => {
            return Err(Error::Corrupt(format!(
                "{} is keyed but holds an append table's rows",
                entry.name
            )));
        }
    };
    let schema = Arc::new(history_columns(entry)?);
    let mut columns: Vec<ArrayRef> = vec![Arc::new(StringArray::from_iter_values(
        std::iter::repeat_n(op, batch.num_rows()),
    ))];
    columns.extend(batch.columns().iter().cloned());
    RecordBatch::try_new(schema, columns).map_err(|e| Error::Arrow(e.to_string()))
}

/// Where a store's history files live: a local directory, or `s3://bucket[/prefix]` with
/// credentials and endpoint from the usual `AWS_*` variables (`AWS_ENDPOINT`, and
/// `AWS_ALLOW_HTTP=true` for a store served over plain HTTP inside the cluster).
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
    schema: SchemaRef,
    name: String,
    first: Date,
    last: Date,
    rows: u64,
    /// Rows of the current date in this file, and the bytes written when it began here.
    date_rows: u64,
    date_start: usize,
    /// A sorted table's rows held back until they make a row group, which is then sorted.
    held: Vec<RecordBatch>,
    held_bytes: usize,
}

impl Lake {
    pub fn open(url: &str) -> Result<Self, Error> {
        let io = |e: object_store::Error| Error::Io(format!("lake {url}: {e}"));
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
                std::fs::create_dir_all(dir).map_err(|e| Error::Io(format!("lake {dir}: {e}")))?;
                Arc::new(LocalFileSystem::new_with_prefix(dir).map_err(io)?)
            }
        };
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("kermodes-lake")
            .enable_all()
            .build()
            .map_err(|e| Error::Io(format!("cannot start the lake's runtime: {e}")))?;
        Ok(Self { store, runtime })
    }

    /// Whether the store answers: lists its first object, if it has one. A missing bucket, a
    /// refused credential, or an endpoint that does not answer is an error.
    pub fn probe(&self) -> Result<(), Error> {
        self.runtime.block_on(async {
            match self.store.list(None).next().await {
                Some(Err(e)) => Err(Error::Io(format!("lake: {e}"))),
                _ => Ok(()),
            }
        })
    }

    /// Writes `dates`, in order, as files under `location` (a table's), each date's rows from
    /// `rows`, which hands them to the function it is given. A date with no rows is in no file.
    /// One file holds the dates until it reaches `target`; it then ends with the row group, so a
    /// big date continues in the next. With `sort`, each row group (about 128 MiB) is sorted by
    /// those columns, so a read filtering on them skips most pages by the page index; sorting a
    /// row group rather than a whole date keeps a merge's memory bounded however big the date.
    pub fn write(
        &self,
        location: &str,
        schema: &Schema,
        sort: &[String],
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
        let order = sort
            .iter()
            .map(|column| {
                file_schema.index_of(column).map_err(|_| {
                    Error::Schema(format!("the sort column `{column}` is not a column"))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
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
                file.rows += batch.num_rows() as u64;
                file.date_rows += batch.num_rows() as u64;
                if order.is_empty() {
                    self.runtime
                        .block_on(file.writer.write(&batch))
                        .map_err(parquet_error)?;
                    if file.writer.in_progress_size() >= ROW_GROUP_BYTES {
                        self.runtime
                            .block_on(file.writer.flush())
                            .map_err(parquet_error)?;
                    }
                } else {
                    file.held_bytes += batch.get_array_memory_size();
                    file.held.push(batch);
                    if file.held_bytes >= ROW_GROUP_BYTES {
                        self.write_held(file, &order)?;
                    }
                }
                let size = file.writer.bytes_written() + file.writer.in_progress_size();
                if size as u64 >= target {
                    let full = open.take().expect("a file is open");
                    self.finish(full, &order, &mut written)?;
                }
                Ok(())
            })?;
            // A row group never holds two dates.
            if let Some(file) = &mut open
                && file.last == date
                && file.date_rows > 0
            {
                self.write_held(file, &order)?;
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
            self.finish(file, &order, &mut written)?;
        }
        Ok(written)
    }

    /// The rows of `first` to `last` in a history file, reading only the row groups of those
    /// dates; and of those, only the row groups and pages whose statistics allow `bounds`. The
    /// rows read may still include some outside them: the caller applies its predicate.
    pub fn read(
        &self,
        location: &str,
        file: &str,
        first: Date,
        last: Date,
        bounds: &[Bound],
    ) -> Result<Batches<'_>, Error> {
        let path = Path::from(format!("{location}/{file}"));
        let reader = StoreReader {
            store: self.store.clone(),
            path,
        };
        let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional);
        self.runtime.block_on(async {
            let builder = ParquetRecordBatchStreamBuilder::new_with_options(reader, options)
                .await
                .map_err(parquet_error)?;
            let metadata = builder.metadata().clone();
            let groups = metadata.row_groups();
            let statistics = |column: &str| {
                StatisticsConverter::try_new(column, builder.schema(), builder.parquet_schema())
                    .map_err(parquet_error)
            };
            let dates = statistics(DATE)?;
            let mins = dates.row_group_mins(groups.iter()).map_err(parquet_error)?;
            let maxes = dates
                .row_group_maxes(groups.iter())
                .map_err(parquet_error)?;
            let (mins, maxes) = (as_dates(&mins)?, as_dates(&maxes)?);
            let mut wanted: Vec<usize> = (0..groups.len())
                .filter(|&g| {
                    mins.is_valid(g)
                        && maxes.is_valid(g)
                        && mins.value(g) <= last.days()
                        && maxes.value(g) >= first.days()
                })
                .collect();
            // Bounds by column, for the columns the file has.
            let mut columns: Vec<(&str, Vec<&Bound>)> = Vec::new();
            for bound in bounds {
                let column = bound.column();
                if builder.schema().column_with_name(column).is_none() {
                    continue;
                }
                match columns.iter_mut().find(|(c, _)| *c == column) {
                    Some((_, of)) => of.push(bound),
                    None => columns.push((column, vec![bound])),
                }
            }
            for (column, bounds) in &columns {
                let converter = statistics(column)?;
                let mins = converter
                    .row_group_mins(groups.iter())
                    .map_err(parquet_error)?;
                let maxes = converter
                    .row_group_maxes(groups.iter())
                    .map_err(parquet_error)?;
                let keep = allowed(&mins, &maxes, bounds);
                wanted.retain(|&g| keep[g]);
            }
            let mut selection: Option<RowSelection> = None;
            if let Some(page_index) = metadata.page_index()
                && !wanted.is_empty()
            {
                for (column, bounds) in &columns {
                    let converter = statistics(column)?;
                    let pages = |maxes: bool| {
                        if maxes {
                            converter.data_page_maxes(page_index.as_ref(), wanted.iter())
                        } else {
                            converter.data_page_mins(page_index.as_ref(), wanted.iter())
                        }
                        .map_err(parquet_error)
                    };
                    let (mins, maxes) = (pages(false)?, pages(true)?);
                    let counts = converter
                        .data_page_row_counts(page_index.as_ref(), groups, wanted.iter())
                        .map_err(parquet_error)?;
                    let Some(counts) = counts.filter(|c| c.null_count() == 0) else {
                        continue;
                    };
                    let keep = allowed(&mins, &maxes, bounds);
                    let pages: Vec<RowSelector> = (0..counts.len())
                        .map(|page| match keep[page] {
                            true => RowSelector::select(counts.value(page) as usize),
                            false => RowSelector::skip(counts.value(page) as usize),
                        })
                        .collect();
                    let pages = RowSelection::from(pages);
                    selection = Some(match selection {
                        Some(selection) => selection.intersection(&pages),
                        None => pages,
                    });
                }
            }
            let row_groups = (wanted.len(), groups.len());
            let in_groups: u64 = wanted.iter().map(|&g| groups[g].num_rows() as u64).sum();
            let selected = selection
                .as_ref()
                .map_or(in_groups, |selection| selection.row_count() as u64);
            let mut builder = builder.with_row_groups(wanted);
            if let Some(selection) = selection {
                builder = builder.with_row_selection(selection);
            }
            let stream = builder.build().map_err(parquet_error)?;
            Ok(Batches {
                runtime: &self.runtime,
                stream,
                row_groups,
                rows: (selected, in_groups),
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
            schema: schema.clone(),
            name,
            first,
            last: first,
            rows: 0,
            date_rows: 0,
            date_start: 0,
            held: Vec::new(),
            held_bytes: 0,
        })
    }

    /// Writes the rows held back for sorting, sorted by the `order` columns, as a row group.
    fn write_held(&self, file: &mut Open, order: &[usize]) -> Result<(), Error> {
        if file.held.is_empty() {
            return Ok(());
        }
        let held = concat_batches(&file.schema, &file.held).map_err(arrow_error)?;
        file.held.clear();
        file.held_bytes = 0;
        let columns: Vec<SortColumn> = order
            .iter()
            .map(|&column| SortColumn {
                values: held.column(column).clone(),
                options: None,
            })
            .collect();
        let indices = lexsort_to_indices(&columns, None).map_err(arrow_error)?;
        let sorted = take_record_batch(&held, &indices).map_err(arrow_error)?;
        self.runtime
            .block_on(file.writer.write(&sorted))
            .map_err(parquet_error)?;
        self.runtime
            .block_on(file.writer.flush())
            .map_err(parquet_error)
    }

    fn finish(&self, mut file: Open, order: &[usize], written: &mut Written) -> Result<(), Error> {
        self.write_held(&mut file, order)?;
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
    /// Rows read (the pages bounds allow), and in the row groups read.
    pub rows: (u64, u64),
}

/// Which of a column's row groups or pages, given their minimums and maximums, may hold values
/// within every one of `bounds`. Unknown statistics, or a literal that is not of the column's
/// type, rule nothing out.
fn allowed(mins: &ArrayRef, maxes: &ArrayRef, bounds: &[&Bound]) -> Vec<bool> {
    let mut keep = vec![true; mins.len()];
    for bound in bounds {
        let (side, literal) = match bound {
            Bound::Low(_, literal) => (maxes, literal),
            Bound::High(_, literal) => (mins, literal),
        };
        let Some(value) = literal_as(literal, side.data_type()) else {
            continue;
        };
        let value = Scalar::new(value);
        let within = match bound {
            Bound::Low(..) => gt_eq(side, &value),
            Bound::High(..) => lt_eq(side, &value),
        };
        let Ok(within) = within else {
            continue;
        };
        for (unit, keep) in keep.iter_mut().enumerate() {
            // A null comparison (no statistics) keeps the unit.
            *keep &= !within.is_valid(unit) || within.value(unit);
        }
    }
    keep
}

/// `literal` as a one-value array of `data_type`, if it is one.
fn literal_as(literal: &Literal, data_type: &DataType) -> Option<ArrayRef> {
    let written: ArrayRef = match literal {
        Literal::Text(text) => Arc::new(StringArray::from(vec![text.as_str()])),
        Literal::Number(number) => match number.parse::<i64>() {
            Ok(integer) => Arc::new(Int64Array::from(vec![integer])),
            Err(_) => Arc::new(Float64Array::from(vec![number.parse::<f64>().ok()?])),
        },
    };
    let cast = arrow::compute::cast(&written, data_type).ok()?;
    cast.is_valid(0).then_some(cast)
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

pub fn dated(schema: &SchemaRef, date: Date, batch: &RecordBatch) -> Result<RecordBatch, Error> {
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

fn arrow_error(error: arrow::error::ArrowError) -> Error {
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
