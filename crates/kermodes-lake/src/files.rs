//! History files in object storage (architecture.md §8.1): Parquet with a `Date` column first,
//! whole dates per file, a row group ending at every date and at about 128 MiB, and a file ending
//! once it reaches the target size, so a big date lies in several. ZSTD, dictionaries, and
//! page-level statistics with the page index, so readers skip by date, key, and time.

use std::collections::HashMap;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::buffered::BufWriter;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::prefix::PrefixStore;
use object_store::signer::{Method, Signer};
use object_store::{GetOptions, GetRange, ObjectStore, ObjectStoreExt};
use parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, RowSelection, RowSelector,
};
use parquet::arrow::async_reader::{
    AsyncFileReader, MetadataSuffixFetch, ParquetRecordBatchStream,
};
use parquet::arrow::async_writer::AsyncArrowWriter;
use parquet::arrow::{PARQUET_FIELD_ID_META_KEY, ParquetRecordBatchStreamBuilder, ProjectionMask};
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

/// The table columns a read of history keeps, by ID (§5.4), after the columns history always
/// reads (`Date`, and a keyed table's `Op`): `system` of them.
pub struct Projection<'a> {
    pub system: usize,
    pub ids: &'a [ColumnId],
}

/// The table's current columns, all of them or those with the `wanted` IDs, in its order.
pub fn projected(entry: &TableEntry, wanted: Option<&[ColumnId]>) -> Shape {
    let Some(wanted) = wanted else {
        return entry.shape();
    };
    let keep: Vec<usize> = (0..entry.ids.len())
        .filter(|&i| wanted.contains(&entry.ids[i]))
        .collect();
    Shape::new(
        Arc::new(
            entry
                .schema
                .project(&keep)
                .expect("positions of the table's own columns"),
        ),
        keep.iter().map(|&i| entry.ids[i]).collect(),
    )
}

/// A history file's batch as the table's history has it now (`Date`, then
/// `history_columns`, all of them or those with the `wanted` IDs): its columns matched by ID,
/// so a file written under earlier columns reads under the current ones (§5.4); one written
/// before columns had IDs, by position (such a file is read whole: `Lake::read`).
pub fn as_current(
    entry: &TableEntry,
    batch: RecordBatch,
    wanted: Option<&[ColumnId]>,
) -> Result<RecordBatch, Error> {
    let full = with_date_column(&history_columns(entry)?);
    // `Date`, and a keyed table's `Op`, are history's own.
    let system = full.fields().len() - entry.ids.len();
    let current = projected(entry, wanted);
    let mut fields = full.fields()[..system].to_vec();
    fields.extend(current.schema.fields().iter().cloned());
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
        Arc::new(Schema::new(fields)),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
    .map_err(arrow_error)
}

/// The columns of a file to read for `projection`: those history always reads, and each table
/// column the file holds under a wanted ID. `None` for a file written before columns had IDs,
/// whose columns are known only by position among all of them: it is read whole.
fn roots(file: &Schema, projection: &Projection) -> Option<Vec<usize>> {
    let mut roots: Vec<usize> = (0..projection.system.min(file.fields().len())).collect();
    for (i, field) in file.fields().iter().enumerate().skip(projection.system) {
        let id = field
            .metadata()
            .get(PARQUET_FIELD_ID_META_KEY)
            .and_then(|id| id.parse().ok())
            .map(ColumnId)?;
        if projection.ids.contains(&id) {
            roots.push(i);
        }
    }
    Some(roots)
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
/// `AWS_ALLOW_HTTP=true` for a store served over plain HTTP inside the cluster). Opened with
/// its keys (`open`) by weir and merge Jobs only; every other process reads history through a
/// `reader`, from where weir says each file is (architecture.md §8.4).
pub struct Lake {
    keys: Option<Keys>,
    runtime: OwnRuntime,
    /// Reads files weir vends at an `http(s)://` URL.
    http: reqwest::Client,
}

/// The lake's own tokio runtime, shut down without waiting when the lake goes: a lake may go
/// inside another runtime (a Flight server's, holding the function that vends its files), where
/// waiting for a runtime's threads is not allowed.
struct OwnRuntime(Option<Runtime>);

impl std::ops::Deref for OwnRuntime {
    type Target = Runtime;

    fn deref(&self) -> &Runtime {
        self.0.as_ref().expect("held until the lake goes")
    }
}

impl Drop for OwnRuntime {
    fn drop(&mut self) {
        if let Some(runtime) = self.0.take() {
            runtime.shutdown_background();
        }
    }
}

/// A lake opened with its keys: its store, and how it says where a reader without them reads
/// a file.
struct Keys {
    store: Arc<dyn ObjectStore>,
    vending: Vending,
}

enum Vending {
    /// The lake's directory, absolute: a reader on the same machine reads the file itself.
    Local(PathBuf),
    /// The bucket's client, which signs, and the lake's prefix in the bucket.
    S3 { s3: AmazonS3, prefix: String },
}

/// How long a vended URL is valid. Every ranged GET of a read is checked against it, not only
/// the first, so it must outlast the slowest read of one file.
const VENDED_FOR: Duration = Duration::from_secs(60 * 60);

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
        let (store, vending): (Arc<dyn ObjectStore>, Vending) = match url.strip_prefix("s3://") {
            Some(rest) => {
                let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
                let s3 = AmazonS3Builder::from_env()
                    .with_bucket_name(bucket)
                    .build()
                    .map_err(io)?;
                let prefix = prefix.trim_matches('/').to_owned();
                let store: Arc<dyn ObjectStore> = match prefix.as_str() {
                    "" => Arc::new(s3.clone()),
                    prefix => Arc::new(PrefixStore::new(s3.clone(), prefix)),
                };
                (store, Vending::S3 { s3, prefix })
            }
            None => {
                let dir = url.strip_prefix("file://").unwrap_or(url);
                let missing = |e: std::io::Error| Error::Io(format!("lake {dir}: {e}"));
                std::fs::create_dir_all(dir).map_err(missing)?;
                let dir = std::fs::canonicalize(dir).map_err(missing)?;
                let store = Arc::new(LocalFileSystem::new_with_prefix(&dir).map_err(io)?);
                (store, Vending::Local(dir))
            }
        };
        Self::with(Some(Keys { store, vending }))
    }

    /// A lake held without its keys: it reads only files weir says where to read (their `at`),
    /// and writes nothing.
    pub fn reader() -> Result<Self, Error> {
        Self::with(None)
    }

    fn with(keys: Option<Keys>) -> Result<Self, Error> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("kermodes-lake")
            .enable_all()
            .build()
            .map_err(|e| Error::Io(format!("cannot start the lake's runtime: {e}")))?;
        let http = reqwest::Client::builder()
            .build()
            .map_err(|e| Error::Io(format!("cannot start the lake's HTTP client: {e}")))?;
        Ok(Self {
            keys,
            runtime: OwnRuntime(Some(runtime)),
            http,
        })
    }

    /// The lake's own store, which only a lake opened with its keys has.
    fn store(&self) -> Result<&Arc<dyn ObjectStore>, Error> {
        self.keys.as_ref().map(|keys| &keys.store).ok_or_else(|| {
            Error::NotFound(
                "writing the lake needs its keys: this process holds none (set KERMODES_LAKE)"
                    .into(),
            )
        })
    }

    /// Where a reader without the lake's keys reads `file` of the table at `location`: a
    /// `file://` path for a local lake, a presigned GET URL valid for an hour for S3.
    pub fn vend(&self, location: &str, file: &str) -> Result<String, Error> {
        match self.keys.as_ref().map(|keys| &keys.vending) {
            None => Err(Error::NotFound(format!(
                "cannot say where {location}/{file} is: this process holds no lake keys"
            ))),
            Some(Vending::Local(dir)) => Ok(format!("file://{}/{location}/{file}", dir.display())),
            Some(Vending::S3 { s3, prefix }) => {
                let path = match prefix.as_str() {
                    "" => Path::from(format!("{location}/{file}")),
                    prefix => Path::from(format!("{prefix}/{location}/{file}")),
                };
                let url = self
                    .runtime
                    .block_on(s3.signed_url(Method::GET, &path, VENDED_FOR))
                    .map_err(|e| Error::Io(format!("cannot sign {path}: {e}")))?;
                Ok(url.to_string())
            }
        }
    }

    /// Whether the store answers: lists its first object, if it has one. A missing bucket, a
    /// refused credential, or an endpoint that does not answer is an error.
    pub fn probe(&self) -> Result<(), Error> {
        self.runtime.block_on(async {
            match self.store()?.list(None).next().await {
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
    /// dates; and of those, only the row groups and pages whose statistics allow `bounds`, and
    /// only the columns `projection` keeps (all if `None`). The rows read may still include some
    /// outside them: the caller applies its predicate. The file is read `at` where weir vended it
    /// (a `file://` path or an `http(s)://` URL), else through the lake's own store.
    #[allow(clippy::too_many_arguments)]
    pub fn read(
        &self,
        location: &str,
        file: &str,
        at: Option<&str>,
        first: Date,
        last: Date,
        bounds: &[Bound],
        projection: Option<&Projection>,
    ) -> Result<Batches<'_>, Error> {
        let file = self.plan_file(location, file, at, first, last, bounds, projection)?;
        let whole = FilePart {
            groups: file.groups.clone(),
            selection: file.selection.clone(),
            rows: file.rows.0,
        };
        self.read_part(&file, whole)
    }

    /// A part of a file `plan_file` read, a batch at a time.
    pub(crate) fn read_part(&self, file: &FileRead, part: FilePart) -> Result<Batches<'_>, Error> {
        Ok(Batches {
            runtime: &self.runtime,
            stream: file.stream(part)?,
            row_groups: file.row_groups,
            rows: file.rows,
            columns: file.columns,
        })
    }

    /// What `read` reads of a file, from its footer and page index, read once here: the parts
    /// of it are then read from that, each by its own stream.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn plan_file(
        &self,
        location: &str,
        file: &str,
        at: Option<&str>,
        first: Date,
        last: Date,
        bounds: &[Bound],
        projection: Option<&Projection>,
    ) -> Result<FileRead, Error> {
        let source = match at {
            Some(at) => match at.strip_prefix("file://") {
                Some(local) => FileSource::Store(
                    Arc::new(LocalFileSystem::new()),
                    Path::from_filesystem_path(local)
                        .map_err(|e| Error::Io(format!("history file {at}: {e}")))?,
                ),
                None if at.starts_with("https://") || at.starts_with("http://") => {
                    FileSource::Http(self.http.clone(), at.to_owned())
                }
                None => {
                    return Err(Error::Corrupt(format!(
                        "weir vended {location}/{file} at {at}, which is neither file:// nor \
                         http(s)://"
                    )));
                }
            },
            None => FileSource::Store(
                self.keys
                    .as_ref()
                    .ok_or_else(|| {
                        Error::NotFound(format!(
                            "history file {location}/{file}: this process holds no lake keys, \
                             and weir did not say where the file is"
                        ))
                    })?
                    .store
                    .clone(),
                Path::from(format!("{location}/{file}")),
            ),
        };
        let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional);
        let mut footer = source.reader();
        let reader = ArrowReaderMetadata::load_async(&mut footer, options);
        let reader = self.runtime.block_on(reader).map_err(parquet_error)?;
        let metadata = reader.metadata().clone();
        let groups = metadata.row_groups();
        let statistics = |column: &str| {
            StatisticsConverter::try_new(column, reader.schema(), reader.parquet_schema())
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
            if reader.schema().column_with_name(column).is_none() {
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
        let in_file = reader.schema().fields().len();
        let mut columns = (in_file, in_file);
        let mut mask = None;
        if let Some(roots) = projection.and_then(|p| roots(reader.schema(), p)) {
            columns.0 = roots.len();
            mask = Some(ProjectionMask::roots(reader.parquet_schema(), roots));
        }
        Ok(FileRead {
            source,
            reader,
            groups: wanted,
            selection,
            mask,
            row_groups,
            rows: (selected, in_groups),
            columns,
        })
    }

    /// Deletes a history file; one already gone is deleted.
    pub fn delete(&self, location: &str, file: &str) -> Result<(), Error> {
        let path = Path::from(format!("{location}/{file}"));
        match self.runtime.block_on(self.store()?.delete(&path)) {
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
            BufWriter::new(self.store()?.clone(), path),
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
            at: None,
        });
        Ok(())
    }
}

/// A history file's rows, a batch at a time, from the row groups of the dates asked for.
pub struct Batches<'a> {
    runtime: &'a Runtime,
    stream: ParquetRecordBatchStream<Box<dyn AsyncFileReader>>,
    /// Row groups read, and in the file.
    pub row_groups: (usize, usize),
    /// Rows read (the pages bounds allow), and in the row groups read.
    pub rows: (u64, u64),
    /// Columns read, and in the file.
    pub columns: (usize, usize),
}

/// Where a history file is read from: each part of a read opens its own reader on it.
#[derive(Clone)]
enum FileSource {
    Store(Arc<dyn ObjectStore>, Path),
    /// A URL weir vended.
    Http(reqwest::Client, String),
}

impl FileSource {
    fn reader(&self) -> Box<dyn AsyncFileReader> {
        match self {
            Self::Store(store, path) => Box::new(StoreReader {
                store: store.clone(),
                path: path.clone(),
            }),
            Self::Http(client, url) => Box::new(HttpReader {
                client: client.clone(),
                url: url.clone(),
            }),
        }
    }
}

/// A history file's footer, read once, and what of it a read wants: its row groups, pages and
/// columns.
pub(crate) struct FileRead {
    source: FileSource,
    reader: ArrowReaderMetadata,
    groups: Vec<usize>,
    /// The pages wanted, over the rows of `groups` in order.
    selection: Option<RowSelection>,
    mask: Option<ProjectionMask>,
    /// Row groups read, and in the file.
    pub row_groups: (usize, usize),
    /// Rows read (the pages bounds allow), and in the row groups read.
    pub rows: (u64, u64),
    /// Columns read, and in the file.
    pub columns: (usize, usize),
}

/// Some of a file's wanted row groups, read on their own.
pub(crate) struct FilePart {
    groups: Vec<usize>,
    selection: Option<RowSelection>,
    /// Rows it reads.
    pub rows: u64,
}

impl FileRead {
    /// The wanted row groups in order, as parts of at least `rows` rows each (the last may have
    /// fewer): row groups are not split, so a big one is a part alone. Parts that read no rows
    /// (their pages all skipped) are left out.
    pub(crate) fn parts(&self, rows: u64) -> Vec<FilePart> {
        let all = self.reader.metadata().row_groups();
        let mut selection = self.selection.clone();
        let mut parts = Vec::new();
        let mut groups = Vec::new();
        let mut in_groups = 0;
        for (i, &group) in self.groups.iter().enumerate() {
            groups.push(group);
            in_groups += all[group].num_rows() as u64;
            if in_groups >= rows || i + 1 == self.groups.len() {
                let selection = selection
                    .as_mut()
                    .map(|selection| selection.split_off(in_groups as usize));
                let rows = selection
                    .as_ref()
                    .map_or(in_groups, |selection| selection.row_count() as u64);
                if rows > 0 {
                    parts.push(FilePart {
                        groups: std::mem::take(&mut groups),
                        selection,
                        rows,
                    });
                }
                groups.clear();
                in_groups = 0;
            }
        }
        parts
    }

    fn stream(
        &self,
        part: FilePart,
    ) -> Result<ParquetRecordBatchStream<Box<dyn AsyncFileReader>>, Error> {
        let mut builder = ParquetRecordBatchStreamBuilder::new_with_metadata(
            self.source.reader(),
            self.reader.clone(),
        )
        .with_row_groups(part.groups);
        if let Some(selection) = part.selection {
            builder = builder.with_row_selection(selection);
        }
        if let Some(mask) = &self.mask {
            builder = builder.with_projection(mask.clone());
        }
        builder.build().map_err(parquet_error)
    }
}

/// A part of a history file's rows, a batch at a time, decoded on the thread that takes them;
/// it holds the lake, whose runtime does the reads, so it can go to any thread.
pub(crate) struct PartBatches {
    lake: Arc<Lake>,
    stream: ParquetRecordBatchStream<Box<dyn AsyncFileReader>>,
}

impl PartBatches {
    pub(crate) fn new(lake: Arc<Lake>, file: &FileRead, part: FilePart) -> Result<Self, Error> {
        Ok(Self {
            stream: file.stream(part)?,
            lake,
        })
    }
}

impl Iterator for PartBatches {
    type Item = Result<RecordBatch, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.lake
            .runtime
            .block_on(self.stream.next())
            .map(|batch| batch.map_err(parquet_error))
    }
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

/// A history file weir vended at an `http(s)://` URL (a presigned one), read in ranged GETs as
/// `StoreReader` reads the store: the footer, then the row groups wanted.
struct HttpReader {
    client: reqwest::Client,
    url: String,
}

impl HttpReader {
    /// The bytes `range` names (an HTTP `Range` value): only a partial answer is them, since a
    /// server ignoring the range answers the whole file.
    async fn get(&self, range: String) -> parquet::errors::Result<Bytes> {
        let response = self
            .client
            .get(&self.url)
            .header(reqwest::header::RANGE, &range)
            .send()
            .await
            .map_err(http_to_parquet)?;
        let status = response.status();
        if status != reqwest::StatusCode::PARTIAL_CONTENT {
            // The URL's query is its signature: kept out of the error.
            let url = self.url.split('?').next().unwrap_or_default();
            return Err(ParquetError::General(format!(
                "reading {range} of {url} answered {status}"
            )));
        }
        response.bytes().await.map_err(http_to_parquet)
    }
}

impl AsyncFileReader for HttpReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        self.get(format!("bytes={}-{}", range.start, range.end - 1))
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

impl MetadataSuffixFetch for &mut HttpReader {
    fn fetch_suffix(&mut self, suffix: usize) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        self.get(format!("bytes=-{suffix}")).boxed()
    }
}

fn http_to_parquet(error: reqwest::Error) -> ParquetError {
    // Without the URL, whose query is its signature.
    ParquetError::External(Box::new(error.without_url()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An S3 lake vends a URL signed for its bucket and the file's whole path, prefix included,
    /// valid for an hour; signing is local, so no endpoint need answer.
    #[test]
    fn an_s3_lake_vends_a_presigned_url_for_the_files_path() {
        let s3 = AmazonS3Builder::new()
            .with_bucket_name("history")
            .with_region("us-east-1")
            .with_endpoint("http://lake.invalid:9000")
            .with_allow_http(true)
            .with_access_key_id("key")
            .with_secret_access_key("secret")
            .build()
            .unwrap();
        let lake = Lake::with(Some(Keys {
            store: Arc::new(s3.clone()),
            vending: Vending::S3 {
                s3,
                prefix: "prod".into(),
            },
        }))
        .unwrap();
        let url = lake.vend("main/md/ticks.1", "f.parquet").unwrap();
        let (path, query) = url.split_once('?').unwrap();
        assert_eq!(
            path,
            "http://lake.invalid:9000/history/prod/main/md/ticks.1/f.parquet"
        );
        assert!(query.contains("X-Amz-Expires=3600"), "{query}");
        assert!(query.contains("X-Amz-Signature="), "{query}");
        assert!(query.contains("X-Amz-Credential=key%2F"), "{query}");
    }

    #[test]
    fn a_reader_lake_writes_nothing_and_vends_nothing() {
        let lake = Lake::reader().unwrap();
        assert!(matches!(lake.delete("t", "f"), Err(Error::NotFound(_))));
        assert!(matches!(lake.vend("t", "f"), Err(Error::NotFound(_))));
    }
}
