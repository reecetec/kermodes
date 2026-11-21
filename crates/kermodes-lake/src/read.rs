//! Reading a table's history (architecture.md §8.4): from the files the catalog says hold the
//! dates asked for, only their row groups, and of those the row groups and pages a predicate's
//! bounds allow; and the past dates weir holds that are not merged yet, so history has no gap.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use kermodes_weir::{Date, Error, TableName};

use crate::bounds::Bound;
use crate::files::{
    Lake, Projection, as_current, dated, history_columns, projected, with_date_column, with_op,
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

/// `name`'s history from `first` to `last` (all of it where `None`), reading from files only
/// the row groups and pages `bounds` allow, and only the table's `columns` (all where `None`;
/// `Date`, and a keyed table's `Op`, always). Rows outside the bounds may still be read: the
/// caller applies its predicate. A name the table lacks reads nothing.
pub fn read_history(
    weir: &dyn Weir,
    lake: &Lake,
    name: &TableName,
    first: Option<Date>,
    last: Option<Date>,
    bounds: &[Bound],
    columns: Option<&[String]>,
) -> Result<History, Error> {
    history(weir, lake, name, first, last, bounds, columns, true)
}

/// What `read_history` would touch, from the catalog and the files' footers and page indexes
/// alone.
pub fn explain_history(
    weir: &dyn Weir,
    lake: &Lake,
    name: &TableName,
    first: Option<Date>,
    last: Option<Date>,
    bounds: &[Bound],
    columns: Option<&[String]>,
) -> Result<Explain, Error> {
    history(weir, lake, name, first, last, bounds, columns, false).map(|history| history.explain)
}

#[allow(clippy::too_many_arguments)]
fn history(
    weir: &dyn Weir,
    lake: &Lake,
    name: &TableName,
    first: Option<Date>,
    last: Option<Date>,
    bounds: &[Bound],
    columns: Option<&[String]>,
    rows: bool,
) -> Result<History, Error> {
    let entry = weir.entry(name)?;
    let full = with_date_column(&history_columns(&entry)?);
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
    let kept = projected(&entry, wanted.as_deref());
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
    // Each part of the result, by the date it starts at.
    let mut parts: Vec<(Date, Vec<RecordBatch>)> = Vec::new();
    if first <= last {
        for file in weir.history_files(name, first, last)? {
            // A file's footer is read here; its row groups only as the batches are taken.
            let mut batches = lake.read(
                &entry.location,
                &file.name,
                first,
                last,
                bounds,
                projection.as_ref(),
            )?;
            let read = if rows {
                batches
                    .by_ref()
                    .map(|batch| {
                        batch.and_then(|batch| as_current(&entry, batch, wanted.as_deref()))
                    })
                    .collect::<Result<Vec<_>, _>>()?
            } else {
                Vec::new()
            };
            explain.files.0 += 1;
            explain.row_groups.0 += batches.row_groups.0;
            explain.row_groups.1 += batches.row_groups.1;
            explain.rows.0 += batches.rows.0;
            explain.rows.1 += batches.rows.1;
            parts.push((file.first.max(first), read));
        }
        let merged: BTreeSet<Date> = held.iter().map(|h| h.date).collect();
        for date in weir.past_dates(name)? {
            // A date merged and not yet dropped is history's to give.
            if !in_range(date) || merged.contains(&date) {
                continue;
            }
            let mut read = Vec::new();
            if rows {
                weir.read_date(name, date, &mut |batch, stamp| {
                    let whole = dated(&full, date, &with_op(&entry, batch, stamp.change)?)?;
                    read.push(whole.project(&positions).map_err(arrow_error)?);
                    Ok(())
                })?;
            }
            explain.weir_dates += 1;
            parts.push((date, read));
        }
    }
    parts.sort_by_key(|(date, _)| *date);
    Ok(History {
        schema,
        batches: parts.into_iter().flat_map(|(_, batches)| batches).collect(),
        explain,
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
