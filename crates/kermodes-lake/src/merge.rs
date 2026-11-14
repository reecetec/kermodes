//! Merging a past date from weir into history (architecture.md §8.3): plan, write, check the
//! rows, commit, and only then let weir drop the date. Safe to run again at any point: a date
//! weir no longer holds is merged already, and one history holds whole is only dropped.

use std::time::Duration;

use arrow::array::RecordBatch;
use kermodes_weir::{Date, Error, HistoryCommit, HistoryDate, Store, TableName};

use crate::files::{Lake, TARGET_FILE_BYTES, Written};
use crate::plan::{Held, Incoming, plan};

/// Replaced files are deleted this long after their commit, when no read can still be using
/// them.
pub const GARBAGE_GRACE: Duration = Duration::from_secs(600);

/// How often a merge plans again when other commits keep changing the history under it.
const ATTEMPTS: usize = 5;

/// What a merge did.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Merged {
    /// Rows of the date moved into history.
    pub rows: u64,
    /// Files written, and files they replaced.
    pub files: usize,
    pub replaced: usize,
}

/// Moves a past `date` of `name` from weir into history.
pub fn merge(store: &Store, lake: &Lake, name: &TableName, date: Date) -> Result<Merged, Error> {
    let entry = store.table(name)?;
    let (rows, bytes) = store.date_rows(name, date)?;
    if rows == 0 {
        return Ok(Merged::default());
    }
    for _ in 0..ATTEMPTS {
        let held = store.history(name)?;
        if rows_on(&held, date) == rows {
            store.drop_date(name, date)?;
            return Ok(Merged::default());
        }
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
                &entry.schema,
                dates,
                |day, push| {
                    if day == date {
                        return store.read_date(name, day, push);
                    }
                    // A neighbour rewritten into the new file, from the file it was in.
                    for from in held.iter().filter(|h| h.date == day) {
                        for batch in lake.read(&entry.location, &from.file, day, day)? {
                            push(without_date(batch?)?)?;
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
            let merged = rows_on(&written.dates, date);
            if merged != rows {
                return Err(Error::Corrupt(format!(
                    "merging {name} {date}: weir holds {rows} rows, {merged} were written"
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
        match checked.and_then(|()| store.commit_history(name, &commit)) {
            Ok(()) => {
                store.drop_date(name, date)?;
                collect_garbage(store, lake, GARBAGE_GRACE)?;
                return Ok(Merged {
                    rows,
                    files: commit.files.len(),
                    replaced: commit.replaced.len(),
                });
            }
            Err(e) => {
                // Written but never committed: no one else can know of these files.
                for file in &written.files {
                    lake.delete(&entry.location, &file.name)?;
                }
                if !matches!(e, Error::Busy(_)) {
                    return Err(e);
                }
            }
        }
    }
    Err(Error::Busy(format!(
        "merging {name} {date}: its history kept changing; try again"
    )))
}

/// Deletes history files replaced at least `grace` ago.
pub fn collect_garbage(store: &Store, lake: &Lake, grace: Duration) -> Result<(), Error> {
    for garbage in store.history_garbage(grace)? {
        lake.delete(&garbage.location, &garbage.file)?;
        store.forget_garbage(&garbage)?;
    }
    Ok(())
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
