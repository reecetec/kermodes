//! Reading a table's history (architecture.md §8.4): from the files the catalog says hold the
//! dates asked for, only their row groups, and of those the row groups and pages a predicate's
//! bounds allow; and the past dates weir holds that are not merged yet, so history has no gap.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use kermodes_weir::{Date, Error, TableEntry, TableName};

use crate::bounds::Bound;
use crate::files::{
    AsCurrent, FilePart, FileRead, Lake, PartBatches, Projection, dated, history_columns,
    projected, with_date_column, with_op,
};
use crate::weir::Weir;

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

/// One part of a table's history, read on whichever thread runs it: its batches in order, as
/// `read_history` gives them.
pub type HistoryPart = Box<dyn FnOnce() -> Result<HistoryBatches, Error> + Send>;
pub type HistoryBatches = Box<dyn Iterator<Item = Result<RecordBatch, Error>> + Send>;

/// `entry`'s history as `read_history` reads it, as parts to read independently and in
/// parallel: all of part i's rows come before part i+1's. A part is a run of row groups of one
/// file (each file's footer read here, once) or one past date weir holds (read here: `weir` is
/// only borrowed). With a `limit`, only the leading parts that hold at least that many rows. The
/// caller's `entry`, so the rows it reads beside history (today's) are of the same columns.
pub fn history_parts(
    weir: &dyn Weir,
    lake: &Arc<Lake>,
    entry: &TableEntry,
    reading: &Reading<'_>,
) -> Result<(SchemaRef, Vec<HistoryPart>), Error> {
    let planned = plan(weir, lake, entry, reading, true)?;
    let parts = planned
        .pieces
        .into_iter()
        .map(|piece| -> HistoryPart {
            match piece {
                Piece::File(file, part) => {
                    let (lake, mut current) = (lake.clone(), planned.current.clone());
                    Box::new(move || {
                        let batches = PartBatches::new(lake, &file, part)?;
                        Ok(Box::new(batches.map(move |batch| current.batch(batch?))))
                    })
                }
                Piece::Weir(batches) => Box::new(move || Ok(Box::new(batches.into_iter().map(Ok)))),
            }
        })
        .collect();
    Ok((planned.schema, parts))
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
                        weir.read_date(name, date, &mut |batch, stamp| {
                            let whole = dated(&full, date, &with_op(entry, batch, stamp.change)?)?;
                            let batch = whole.project(&positions).map_err(arrow_error)?;
                            covered += batch.num_rows() as u64;
                            batches.push(batch);
                            Ok(())
                        })?;
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
        pieces,
    })
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
