//! Reading a table's history (architecture.md §8.4): from the files the catalog says hold the
//! dates asked for, only their row groups, and of those the row groups and pages a predicate's
//! bounds allow; and the past dates weir holds that are not merged yet, so history has no gap.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, RecordBatchOptions, UInt64Array};
use arrow::compute::{concat_batches, take};
use arrow::datatypes::{Field, SchemaRef};
use kermodes_weir::{ColumnId, Date, Error, TableEntry, TableName};

use crate::bounds::Bound;
use crate::files::{
    AsCurrent, FilePart, FileRead, Lake, PartBatches, Projection, dated, history_columns,
    projected, with_date_column, with_op, within,
};
use crate::weir::Weir;

/// A weir date's batches are its writes as written: an ingester's are ~14 rows each, and Arrow
/// costs about 1 KB a batch, more than such rows. Those under `SMALL_ROWS` are combined into
/// chunks of `CHUNK_ROWS` as they are read, as a table's storage combines them.
const SMALL_ROWS: usize = 4_096;
const CHUNK_ROWS: usize = 1 << 16;

/// Dates of a table's history, read.
pub struct History {
    /// The table's columns after `Date`.
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
    pub explain: Explain,
}

/// What a read of history touched: what `EXPLAIN` shows.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Explain {
    /// Files read, and in the table's history.
    pub files: (usize, usize),
    /// Row groups read, and in the files read.
    pub row_groups: (usize, usize),
    /// Rows read from files (the pages bounds allow), and in the row groups read.
    pub rows: (u64, u64),
    /// Columns read, and in the table's history (with `Date`).
    pub columns: (usize, usize),
    /// Bytes of the dates read, and of the table's whole history.
    pub bytes: (u64, u64),
    /// Past dates read from weir, not merged yet.
    pub weir_dates: usize,
}

impl fmt::Display for Explain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} of {} files, {} of {} row groups, {} of {} rows, {} of {} columns, {} of {}",
            self.files.0,
            self.files.1,
            self.row_groups.0,
            self.row_groups.1,
            self.rows.0,
            self.rows.1,
            self.columns.0,
            self.columns.1,
            size(self.bytes.0),
            size(self.bytes.1)
        )?;
        if self.weir_dates > 0 {
            let s = if self.weir_dates == 1 { "" } else { "s" };
            write!(f, "; {} date{s} from weir, not merged yet", self.weir_dates)?;
        }
        Ok(())
    }
}

/// What of a table's history to read: the dates `first` to `last` (all where `None`), from
/// files only the row groups and pages `bounds` allow, only the table's `columns` (all where
/// `None`; `Date`, and a keyed table's `Op`, always), and only its first `limit` rows in date
/// order (all where `None`). Rows outside the bounds may still be read: the caller applies its
/// predicate. A column name the table lacks reads nothing.
#[derive(Clone, Copy, Debug, Default)]
pub struct Reading<'a> {
    pub first: Option<Date>,
    pub last: Option<Date>,
    pub bounds: &'a [Bound],
    pub columns: Option<&'a [String]>,
    pub limit: Option<usize>,
}

/// `name`'s history, as `reading` says: at least `limit` rows where there are that many, each
/// file and row group read only while they are still needed. A file is read where weir says it
/// is (its `at`), else through `lake`'s own store, which a reader holding no lake keys lacks.
pub fn read_history(
    weir: &dyn Weir,
    lake: &Lake,
    name: &TableName,
    reading: &Reading<'_>,
) -> Result<History, Error> {
    let entry = weir.entry(name)?;
    history(weir, lake, &entry, reading, true)
}

/// What `read_history` would touch without a limit, from the catalog and the files' footers
/// and page indexes alone.
pub fn explain_history(
    weir: &dyn Weir,
    lake: &Lake,
    name: &TableName,
    reading: &Reading<'_>,
) -> Result<Explain, Error> {
    let reading = Reading {
        limit: None,
        ..*reading
    };
    let entry = weir.entry(name)?;
    history(weir, lake, &entry, &reading, false).map(|history| history.explain)
}

pub type HistoryBatches = Box<dyn Iterator<Item = Result<RecordBatch, Error>> + Send>;

/// `entry`'s history as `read_history` reads it, planned once, as parts to read independently
/// and in parallel (`HistoryParts`). A part is a run of row groups of one file (each file's
/// footer read here, once) or one past date weir holds (read here: `weir` is only borrowed).
/// With a `limit`, only the leading parts that hold at least that many rows. The caller's
/// `entry`, so the rows it reads beside history (today's) are of the same columns.
pub fn history_parts(
    weir: &dyn Weir,
    lake: &Arc<Lake>,
    entry: &TableEntry,
    reading: &Reading<'_>,
) -> Result<HistoryParts, Error> {
    let planned = plan(weir, lake, entry, reading, true)?;
    Ok(HistoryParts {
        schema: planned.schema,
        lake: lake.clone(),
        entry: entry.clone(),
        ids: planned.ids,
        pieces: planned.pieces,
    })
}

/// What a part's statistics say of its rows (`HistoryParts::stats`).
#[derive(Debug, Clone)]
pub struct PartStats {
    /// Its rows: exactly when `whole`, else at most.
    pub rows: u64,
    /// Whether the statistics are of exactly the rows the part reads: none of its row groups'
    /// pages skipped. Otherwise they only bound them.
    pub whole: bool,
    /// Per column asked, its extremes and NULLs; `None` where not known.
    pub columns: Vec<Option<ColumnStats>>,
}

/// A column's least and greatest non-NULL values, as one-row arrays of its type (NULL where it
/// holds only NULLs), and its NULLs.
#[derive(Debug, Clone)]
pub struct ColumnStats {
    pub min: ArrayRef,
    pub max: ArrayRef,
    pub nulls: u64,
}

/// A table's history as parts, `read_history`'s rows in its order: all of part i's rows come
/// before part i+1's. A part reads the same rows in the same order whichever of its columns
/// are read, so a reader can choose rows by some columns, then read the others for the rows it
/// keeps alone (engine.md §4.1).
pub struct HistoryParts {
    /// The columns read: `Date`, a keyed table's `Op`, then the reading's columns of the table.
    pub schema: SchemaRef,
    lake: Arc<Lake>,
    entry: TableEntry,
    /// The IDs of `schema`'s columns of the table, after history's own.
    ids: Arc<[ColumnId]>,
    pieces: Vec<Piece>,
}

impl HistoryParts {
    pub fn len(&self) -> usize {
        self.pieces.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pieces.is_empty()
    }

    /// What part `part`'s Parquet statistics say of `schema`'s columns at `columns`, without
    /// reading its rows (`FileRead::stats`); `None` for a past date weir holds, which has none.
    pub fn stats(&self, part: usize, columns: &[usize]) -> Result<Option<PartStats>, Error> {
        let piece = self
            .pieces
            .get(part)
            .ok_or_else(|| Error::NotFound(format!("part {part} of {}", self.pieces.len())))?;
        let Piece::File(file, whole) = piece else {
            return Ok(None);
        };
        let system = self.schema.fields().len() - self.ids.len();
        let wanted: Vec<Option<(ColumnId, &Field)>> = columns
            .iter()
            .map(|&at| {
                let id = self.ids.get(at.checked_sub(system)?)?;
                Some((*id, self.schema.field(at)))
            })
            .collect();
        Ok(Some(file.stats(whole, &wanted)))
    }

    /// Part `part`'s rows, a batch at a time, decoded on whichever thread takes them: only
    /// `schema`'s columns at `columns` (ascending), and only the rows at `rows` (positions among
    /// the part's rows, ascending; all where `None`). Of a file, only the columns, row groups,
    /// and (by the page index) pages these need are read.
    pub fn read(
        &self,
        part: usize,
        columns: &[usize],
        rows: Option<&[u64]>,
    ) -> Result<HistoryBatches, Error> {
        let piece = self
            .pieces
            .get(part)
            .ok_or_else(|| Error::NotFound(format!("part {part} of {}", self.pieces.len())))?;
        let schema = Arc::new(self.schema.project(columns).map_err(arrow_error)?);
        let options = |rows: usize| RecordBatchOptions::new().with_row_count(Some(rows));
        match piece {
            Piece::File(file, whole) => {
                let system = self.schema.fields().len() - self.ids.len();
                let ids: Vec<ColumnId> = columns
                    .iter()
                    .filter_map(|&at| at.checked_sub(system).map(|at| self.ids[at]))
                    .collect();
                // Where each column is in what a file reads as now: history's own columns, then
                // the table's columns wanted, in order.
                let mut next = system;
                let read_at: Vec<usize> = columns
                    .iter()
                    .map(|&at| {
                        if at < system {
                            return at;
                        }
                        next += 1;
                        next - 1
                    })
                    .collect();
                let mask = file.mask(&Projection { system, ids: &ids });
                let part = match rows {
                    Some(rows) => file.chosen(whole, rows)?,
                    None => whole.clone(),
                };
                let batches = PartBatches::new(self.lake.clone(), file, part, mask)?;
                let mut current = AsCurrent::new(&self.entry, Some(&ids))?;
                Ok(Box::new(batches.map(move |batch| {
                    let batch = current.batch(batch?)?;
                    let columns = read_at.iter().map(|&at| batch.column(at).clone());
                    RecordBatch::try_new_with_options(
                        schema.clone(),
                        columns.collect(),
                        &options(batch.num_rows()),
                    )
                    .map_err(arrow_error)
                })))
            }
            Piece::Weir(batches) => {
                let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
                if let Some(&last) = rows.and_then(<[u64]>::last)
                    && last >= total as u64
                {
                    return Err(Error::NotFound(format!(
                        "row {last} of a part of {total} rows"
                    )));
                }
                let mut read = Vec::with_capacity(batches.len());
                let mut start = 0;
                for batch in batches {
                    let end = start + batch.num_rows() as u64;
                    let kept = columns.iter().map(|&at| batch.column(at).clone());
                    let (kept, count) = match rows {
                        None => (kept.collect(), batch.num_rows()),
                        Some(rows) => {
                            let from = rows.partition_point(|&row| row < start);
                            let to = rows.partition_point(|&row| row < end);
                            let indices = UInt64Array::from_iter_values(
                                rows[from..to].iter().map(|row| row - start),
                            );
                            let kept = kept
                                .map(|column| take(&column, &indices, None))
                                .collect::<Result<_, _>>()
                                .map_err(arrow_error)?;
                            (kept, to - from)
                        }
                    };
                    start = end;
                    if count > 0 {
                        let batch = RecordBatch::try_new_with_options(
                            schema.clone(),
                            kept,
                            &options(count),
                        )
                        .map_err(arrow_error)?;
                        read.push(Ok(batch));
                    }
                }
                Ok(Box::new(read.into_iter()))
            }
        }
    }
}

/// Of the rows a part of a file gathers row groups up to: enough to be worth a thread, few
/// enough that a limit reads little past itself. A bigger row group is a part alone.
const PART_ROWS: u64 = 1 << 17;

/// Where a part of history is: a file of the lake, or a past date weir holds.
enum Source<'a> {
    File(&'a kermodes_weir::HistoryFile),
    Weir(Date),
}

/// A part of history to read: some row groups of a file, or a past date weir held, read.
enum Piece {
    File(Arc<FileRead>, FilePart),
    Weir(Vec<RecordBatch>),
}

/// What a read of history reads, part by part, in date order.
struct Planned {
    schema: SchemaRef,
    explain: Explain,
    /// Files' batches as the table's columns are now.
    current: AsCurrent,
    /// The IDs of `schema`'s columns of the table.
    ids: Arc<[ColumnId]>,
    pieces: Vec<Piece>,
}

/// `read_history`, one part after another.
fn history(
    weir: &dyn Weir,
    lake: &Lake,
    entry: &TableEntry,
    reading: &Reading<'_>,
    rows: bool,
) -> Result<History, Error> {
    let mut planned = plan(weir, lake, entry, reading, rows)?;
    let covered = |taken| reading.limit.is_some_and(|limit| taken >= limit);
    let mut batches = Vec::new();
    let mut taken = 0;
    for piece in planned.pieces {
        if covered(taken) {
            break;
        }
        match piece {
            Piece::File(file, part) => {
                for batch in lake.read_part(&file, part)? {
                    let batch = planned.current.batch(batch?)?;
                    taken += batch.num_rows();
                    batches.push(batch);
                    if covered(taken) {
                        break;
                    }
                }
            }
            Piece::Weir(read) => {
                taken += read.iter().map(RecordBatch::num_rows).sum::<usize>();
                batches.extend(read);
            }
        }
    }
    Ok(History {
        schema: planned.schema,
        batches,
        explain: planned.explain,
    })
}

/// The parts `reading` reads of `entry`'s history, from the catalog and the files' footers, and
/// with `rows` weir's past dates read. Files are opened in date order only until the parts so
/// far hold `limit` rows.
fn plan(
    weir: &dyn Weir,
    lake: &Lake,
    entry: &TableEntry,
    reading: &Reading<'_>,
    rows: bool,
) -> Result<Planned, Error> {
    let Reading {
        first,
        last,
        bounds,
        columns,
        limit,
    } = *reading;
    let name = &entry.name;
    let full = with_date_column(&history_columns(entry)?);
    let system = full.fields().len() - entry.ids.len();
    let wanted: Option<Vec<_>> = columns.map(|names| {
        entry
            .schema
            .fields()
            .iter()
            .zip(entry.ids.iter())
            .filter(|(field, _)| names.iter().any(|n| n == field.name()))
            .map(|(_, id)| *id)
            .collect()
    });
    let kept = projected(entry, wanted.as_deref());
    // Where the kept columns are among all of them, for weir's dates, read whole.
    let positions: Vec<usize> = (0..system)
        .chain(
            kept.schema
                .fields()
                .iter()
                .filter_map(|f| full.index_of(f.name()).ok()),
        )
        .collect();
    let schema = Arc::new(full.project(&positions).map_err(arrow_error)?);
    let projection = wanted.as_deref().map(|ids| Projection { system, ids });
    let first = first.unwrap_or(Date::MIN);
    let last = last.unwrap_or(Date::MAX);
    let held = weir.history(name)?;
    let in_range = |date: Date| first <= date && date <= last;
    let mut explain = Explain {
        columns: (schema.fields().len(), full.fields().len()),
        bytes: (
            held.iter()
                .filter(|h| in_range(h.date))
                .map(|h| h.bytes)
                .sum(),
            held.iter().map(|h| h.bytes).sum(),
        ),
        ..Explain::default()
    };
    explain.files.1 = held.iter().map(|h| &h.file).collect::<BTreeSet<_>>().len();
    let mut pieces = Vec::new();
    if first <= last {
        let files = weir.history_files(name, first, last)?;
        let merged: BTreeSet<Date> = held.iter().map(|h| h.date).collect();
        // Each part, by the date it starts at: read in date order, so a limit keeps the first.
        let mut parts: Vec<(Date, Source<'_>)> = files
            .iter()
            .map(|file| (file.first.max(first), Source::File(file)))
            .collect();
        for date in weir.past_dates(name)? {
            // A date merged and not yet dropped is history's to give.
            if in_range(date) && !merged.contains(&date) {
                parts.push((date, Source::Weir(date)));
            }
        }
        parts.sort_by_key(|(date, _)| *date);
        let mut covered = 0;
        let full_enough = |covered: u64| limit.is_some_and(|limit| covered >= limit as u64);
        for (_, source) in parts {
            if full_enough(covered) {
                break;
            }
            match source {
                Source::File(file) => {
                    let read = lake.plan_file(
                        &entry.location,
                        &file.name,
                        file.at.as_deref(),
                        first,
                        last,
                        bounds,
                        projection.as_ref(),
                    )?;
                    explain.files.0 += 1;
                    explain.row_groups.0 += read.row_groups.0;
                    explain.row_groups.1 += read.row_groups.1;
                    explain.rows.0 += read.rows.0;
                    explain.rows.1 += read.rows.1;
                    let read = Arc::new(read);
                    for part in read.parts(PART_ROWS) {
                        if full_enough(covered) {
                            break;
                        }
                        covered += part.rows;
                        pieces.push(Piece::File(read.clone(), part));
                    }
                }
                Source::Weir(date) => {
                    let mut batches = Vec::new();
                    if rows {
                        let (mut small, mut gathered) = (Vec::new(), 0);
                        weir.read_date(name, date, &mut |batch, stamp| {
                            let whole = dated(&full, date, &with_op(entry, batch, stamp.change)?)?;
                            let batch = whole.project(&positions).map_err(arrow_error)?;
                            // No statistics to prune by: the bounds choose rows as they come.
                            let Some(batch) = within(batch, bounds)? else {
                                return Ok(());
                            };
                            covered += batch.num_rows() as u64;
                            if batch.num_rows() >= SMALL_ROWS {
                                gather(&mut batches, &mut small)?;
                                gathered = 0;
                                batches.push(batch);
                            } else {
                                gathered += batch.num_rows();
                                small.push(batch);
                                if gathered >= CHUNK_ROWS {
                                    gather(&mut batches, &mut small)?;
                                    gathered = 0;
                                }
                            }
                            Ok(())
                        })?;
                        gather(&mut batches, &mut small)?;
                    }
                    pieces.push(Piece::Weir(batches));
                    explain.weir_dates += 1;
                }
            }
        }
    }
    Ok(Planned {
        schema,
        explain,
        current: AsCurrent::new(entry, wanted.as_deref())?,
        ids: kept.ids,
        pieces,
    })
}

/// `small`, if any, as one batch after `batches`.
fn gather(batches: &mut Vec<RecordBatch>, small: &mut Vec<RecordBatch>) -> Result<(), Error> {
    if let Some(first) = small.first() {
        batches.push(concat_batches(&first.schema(), small.iter()).map_err(arrow_error)?);
        small.clear();
    }
    Ok(())
}

fn arrow_error(e: arrow::error::ArrowError) -> Error {
    Error::Arrow(e.to_string())
}

fn size(bytes: u64) -> String {
    match bytes {
        b if b >= 1 << 30 => format!("{:.1} GiB", b as f64 / (1u64 << 30) as f64),
        b if b >= 1 << 20 => format!("{:.1} MiB", b as f64 / (1u64 << 20) as f64),
        b if b >= 1 << 10 => format!("{:.1} KiB", b as f64 / 1024.0),
        b => format!("{b} B"),
    }
}
