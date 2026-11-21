//! Writing whole dates into history (architecture.md §8.3): a past date merged from weir, or
//! written directly by a batch job. Either way a date is planned, written, its rows checked,
//! and committed in place of what history held of it; a merge then lets weir drop the date.
//! Safe to run again at any point: a date weir no longer holds is merged already, and one
//! history holds whole is only dropped.

use std::time::Duration;

use arrow::array::RecordBatch;
use kermodes_weir::{Date, Error, HistoryCommit, HistoryDate, TableEntry, TableName};

use crate::files::{AsCurrent, Lake, TARGET_FILE_BYTES, Written, file_columns, with_op};
use crate::plan::{Held, Incoming, plan};
use crate::weir::Weir;

/// Replaced files are deleted this long after their commit, when no read can still be using
/// them.
pub const GARBAGE_GRACE: Duration = Duration::from_secs(600);

/// How often a write plans again when other commits keep changing the history under it.
const ATTEMPTS: usize = 5;

/// Takes a date's rows, a batch at a time, into the file being written.
type Push<'a> = dyn FnMut(RecordBatch) -> Result<(), Error> + 'a;

/// What a merge or a direct write did.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Merged {
    /// Rows of the date moved into history.
    pub rows: u64,
    /// Files written, and files they replaced.
    pub files: usize,
    pub replaced: usize,
}

/// Moves a past `date` of `name` from weir into history.
pub fn merge(weir: &dyn Weir, lake: &Lake, name: &TableName, date: Date) -> Result<Merged, Error> {
    let entry = weir.entry(name)?;
    let (rows, bytes) = weir.date_rows(name, date)?;
    if rows == 0 {
        return Ok(Merged::default());
    }
    let mut refused = None;
    for _ in 0..ATTEMPTS {
        let held = weir.history(name)?;
        if rows_on(&held, date) == rows {
            weir.drop_date(name, date)?;
            return Ok(Merged::default());
        }
        let mut from_weir = |push: &mut Push| {
            weir.read_date(name, date, &mut |batch, stamp| {
                push(with_op(&entry, batch, stamp.change)?)
            })
        };
        match replace_date(
            weir,
            lake,
            &entry,
            date,
            (rows, bytes),
            &held,
            &mut from_weir,
        ) {
            Ok(merged) => {
                weir.drop_date(name, date)?;
                collect_garbage(weir, lake, GARBAGE_GRACE)?;
                return Ok(merged);
            }
            Err(e @ Error::Busy(_)) => refused = Some(e),
            Err(e) => return Err(e),
        }
    }
    Err(refused.expect("every attempt refused"))
}

/// Writes `batches`, every row the append table `name` has on the past `date`, into history in
/// place of what it held of the date: what a batch job (an end-of-day file, a vendor's data, a
/// backfill, a correction) writes, never through weir. No rows leave the date empty.
///
/// The date must be past and one weir holds nothing of, so each date stays served from exactly
/// one place (§8): weir says both (`Weir::date_rows`, by its own clock, however the write
/// reaches it), and a date weir holds is merged, not written over. A keyed table is refused:
/// its history is its log of changes in commit order, which only its writers make.
pub fn write_history(
    weir: &dyn Weir,
    lake: &Lake,
    name: &TableName,
    date: Date,
    batches: &[RecordBatch],
) -> Result<Merged, Error> {
    let entry = weir.entry(name)?;
    if !entry.key.is_empty() {
        return Err(Error::Schema(format!(
            "{name} is keyed: its history is its log of changes, which only its writers make; \
             write_history writes an append table's rows"
        )));
    }
    let batches = batches
        .iter()
        .map(|batch| as_columns(&entry, batch))
        .collect::<Result<Vec<_>, _>>()?;
    let (in_weir, _) = weir.date_rows(name, date)?;
    if in_weir > 0 {
        return Err(Error::Busy(format!(
            "weir holds {in_weir} rows of {name} on {date}: merge the date rather than write it"
        )));
    }
    let rows = batches.iter().map(|b| b.num_rows() as u64).sum();
    let bytes = batches
        .iter()
        .map(|b| b.get_array_memory_size() as u64)
        .sum();
    let mut refused = None;
    for _ in 0..ATTEMPTS {
        let held = weir.history(name)?;
        let mut given = |push: &mut Push| batches.iter().try_for_each(|batch| push(batch.clone()));
        match replace_date(weir, lake, &entry, date, (rows, bytes), &held, &mut given) {
            Ok(merged) => {
                collect_garbage(weir, lake, GARBAGE_GRACE)?;
                return Ok(merged);
            }
            Err(e @ Error::Busy(_)) => refused = Some(e),
            Err(e) => return Err(e),
        }
    }
    // Refused each time, the history changing under it or the date not past: the last says why.
    Err(refused.expect("every attempt refused"))
}

/// Deletes history files replaced at least `grace` ago.
pub fn collect_garbage(weir: &dyn Weir, lake: &Lake, grace: Duration) -> Result<(), Error> {
    for garbage in weir.history_garbage(grace)? {
        lake.delete(&garbage.location, &garbage.file)?;
        weir.forget_garbage(&garbage)?;
    }
    Ok(())
}

/// One attempt at putting `date`'s `rows` (and their size in `bytes`) into history in place of
/// what `held` says it holds of it: planned with its neighbours, written, the rows checked, and
/// committed. `read` pushes the date's rows as history keeps them. Files written but not
/// committed are deleted: no one else can know of them. `Error::Busy` if another commit changed
/// the history since `held` was read.
fn replace_date(
    weir: &dyn Weir,
    lake: &Lake,
    entry: &TableEntry,
    date: Date,
    (rows, bytes): (u64, u64),
    held: &[HistoryDate],
    read: &mut dyn FnMut(&mut Push) -> Result<(), Error>,
) -> Result<Merged, Error> {
    let name = &entry.name;
    let columns = file_columns(entry)?;
    let planned = plan(
        &held
            .iter()
            .map(|h| Held {
                date: h.date,
                file: h.file.clone(),
                bytes: h.bytes,
            })
            .collect::<Vec<_>>(),
        &[Incoming { date, bytes }],
        TARGET_FILE_BYTES,
    );
    let mut written = Written::default();
    let wrote = planned.files.iter().try_for_each(|dates| {
        let part = lake.write(
            &entry.location,
            &columns,
            &entry.sort,
            dates,
            |day, push| {
                if day == date {
                    return read(push);
                }
                // A neighbour rewritten into the new file, from the file it was in.
                for from in held.iter().filter(|h| h.date == day) {
                    let read = lake.read(&entry.location, &from.file, None, day, day, &[], None)?;
                    let mut current = AsCurrent::new(entry, None)?;
                    for batch in read {
                        push(without_date(current.batch(batch?)?)?)?;
                    }
                }
                Ok(())
            },
            TARGET_FILE_BYTES,
        );
        part.map(|part| {
            written.files.extend(part.files);
            written.dates.extend(part.dates);
        })
    });
    let checked = wrote.and_then(|()| {
        let put = rows_on(&written.dates, date);
        if put != rows {
            return Err(Error::Corrupt(format!(
                "writing {name} {date} into history: {rows} rows given, {put} were written"
            )));
        }
        Ok(())
    });
    let commit = HistoryCommit {
        written: vec![date],
        replaced: planned.replaced.clone(),
        files: written.files.clone(),
        dates: written.dates.clone(),
    };
    match checked.and_then(|()| weir.commit_history(name, &commit)) {
        Ok(()) => Ok(Merged {
            rows,
            files: commit.files.len(),
            replaced: commit.replaced.len(),
        }),
        Err(e) => {
            for file in &written.files {
                lake.delete(&entry.location, &file.name)?;
            }
            Err(e)
        }
    }
}

/// A batch given to `write_history` as the table's columns, in its order: every column by
/// name, of the table's type, and no other.
fn as_columns(entry: &TableEntry, batch: &RecordBatch) -> Result<RecordBatch, Error> {
    let name = &entry.name;
    let given = batch.schema();
    let mut columns = Vec::with_capacity(entry.schema.fields().len());
    for field in entry.schema.fields() {
        let (at, found) = given.column_with_name(field.name()).ok_or_else(|| {
            Error::Schema(format!(
                "{name} has a column `{}` the rows lack",
                field.name()
            ))
        })?;
        if found.data_type() != field.data_type() {
            return Err(Error::Schema(format!(
                "{name}'s column `{}` is {}, but the rows' is {}",
                field.name(),
                field.data_type(),
                found.data_type()
            )));
        }
        columns.push(batch.column(at).clone());
    }
    if let Some(extra) = given
        .fields()
        .iter()
        .find(|f| entry.schema.column_with_name(f.name()).is_none())
    {
        return Err(Error::Schema(format!(
            "the rows have a column `{}` {name} lacks",
            extra.name()
        )));
    }
    RecordBatch::try_new(entry.schema.clone(), columns)
        .map_err(|e| Error::Schema(format!("rows for {name}: {e}")))
}

fn rows_on(held: &[HistoryDate], date: Date) -> u64 {
    held.iter().filter(|h| h.date == date).map(|h| h.rows).sum()
}

/// A history file's batch as the table's columns: its `Date` is the writer's to add.
fn without_date(batch: RecordBatch) -> Result<RecordBatch, Error> {
    let columns: Vec<usize> = (1..batch.num_columns()).collect();
    batch
        .project(&columns)
        .map_err(|e| Error::Arrow(e.to_string()))
}
