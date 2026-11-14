//! What a write of whole dates turns into (architecture.md §8.1): the files to write, each a run
//! of whole dates, and the files they replace. A pure decision; the writer carries it out.
//!
//! - A file holding a date being written is rewritten, with that date replaced.
//! - A small date outside every file joins the file just before it, or else the one just after,
//!   if the result stays within the target size; otherwise it starts a file of its own.
//! - The dates rewritten are packed in date order up to the target size, never across a file kept
//!   as it is; a date of the target size or more is a file of its own.
//!
//! So a write rewrites at most the files it touches, and small dates end up together.

use std::collections::{BTreeMap, BTreeSet};
use std::mem;

use kermodes_weir::Date;

/// A date the lake holds: the file it is in and the bytes it takes there.
#[derive(Clone, Debug)]
pub struct Held<F> {
    pub date: Date,
    pub file: F,
    pub bytes: u64,
}

/// A date being written, replacing what the lake holds for it, and the bytes it will take; `0`
/// (no rows) removes the date.
#[derive(Clone, Copy, Debug)]
pub struct Incoming {
    pub date: Date,
    pub bytes: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Plan<F> {
    /// The files to write, each the whole dates it holds, in date order. A date past the target
    /// is written as several files, each about the target size.
    pub files: Vec<Vec<Date>>,
    /// The files they replace, which the commit removes.
    pub replaced: Vec<F>,
}

/// `held` is every date the table's history holds; `target` is the size a file grows to.
pub fn plan<F: Clone + Ord>(held: &[Held<F>], incoming: &[Incoming], target: u64) -> Plan<F> {
    // Each file's first date, last date, and bytes.
    let mut files: BTreeMap<F, (Date, Date, u64)> = BTreeMap::new();
    for h in held {
        files
            .entry(h.file.clone())
            .and_modify(|(first, last, bytes)| {
                *first = (*first).min(h.date);
                *last = (*last).max(h.date);
                *bytes += h.bytes;
            })
            .or_insert((h.date, h.date, h.bytes));
    }
    let incoming: BTreeMap<Date, u64> = incoming.iter().map(|i| (i.date, i.bytes)).collect();
    // The files of a date split over several: none takes on another date.
    let mut holders: BTreeMap<Date, BTreeSet<&F>> = BTreeMap::new();
    for h in held {
        holders.entry(h.date).or_default().insert(&h.file);
    }
    let split: BTreeSet<&F> = holders
        .into_values()
        .filter(|files| files.len() > 1)
        .flatten()
        .collect();

    let mut replaced = BTreeSet::new();
    // The bytes of small dates each neighbouring file has taken on so far.
    let mut taken: BTreeMap<F, u64> = BTreeMap::new();
    for (&date, &bytes) in &incoming {
        // A big date may lie in several files: all of them are rewritten.
        let mut covered = false;
        for (file, (first, last, _)) in &files {
            if *first <= date && date <= *last {
                replaced.insert(file.clone());
                covered = true;
            }
        }
        if covered {
            continue;
        }
        if bytes == 0 || bytes >= target {
            continue;
        }
        let before = files
            .iter()
            .filter(|(_, (_, last, _))| *last < date)
            .max_by_key(|(_, (_, last, _))| *last);
        let after = files
            .iter()
            .filter(|(_, (first, _, _))| *first > date)
            .min_by_key(|(_, (first, _, _))| *first);
        for (file, (_, _, size)) in before.into_iter().chain(after) {
            if split.contains(file) {
                continue;
            }
            let so_far = taken.get(file).copied().unwrap_or(0);
            if size + so_far + bytes <= target {
                taken.insert(file.clone(), so_far + bytes);
                replaced.insert(file.clone());
                break;
            }
        }
    }

    // Every date the new files hold, with its size (a big date's, over all its files).
    let mut dates: BTreeMap<Date, u64> = BTreeMap::new();
    for h in held.iter().filter(|h| replaced.contains(&h.file)) {
        *dates.entry(h.date).or_default() += h.bytes;
    }
    for (&date, &bytes) in &incoming {
        if bytes == 0 {
            dates.remove(&date);
        } else {
            dates.insert(date, bytes);
        }
    }

    // A kept file's dates lie between rewritten ones only at its start: no new file may span it.
    let kept_starts: Vec<Date> = files
        .iter()
        .filter(|(file, _)| !replaced.contains(*file))
        .map(|(_, (first, _, _))| *first)
        .collect();
    let mut out = Vec::new();
    let mut current = Vec::new();
    let mut size = 0;
    let mut previous: Option<Date> = None;
    for (&date, &bytes) in &dates {
        let across_kept =
            previous.is_some_and(|p| kept_starts.iter().any(|&start| p < start && start < date));
        if across_kept || bytes >= target || size + bytes > target {
            if !current.is_empty() {
                out.push(mem::take(&mut current));
            }
            size = 0;
        }
        current.push(date);
        size += bytes;
        if bytes >= target {
            out.push(mem::take(&mut current));
            size = 0;
        }
        previous = Some(date);
    }
    if !current.is_empty() {
        out.push(current);
    }
    Plan {
        files: out,
        replaced: replaced.into_iter().collect(),
    }
}

#[cfg(test)]
mod tests {
    use kermodes_weir::Date;

    use super::{Held, Incoming, Plan, plan};

    const TARGET: u64 = 100;

    fn d(day: u32) -> Date {
        Date::parse(&format!("2026-11-{day:02}")).unwrap()
    }

    /// `(day, file, bytes)` held.
    fn held(dates: &[(u32, &'static str, u64)]) -> Vec<Held<&'static str>> {
        dates
            .iter()
            .map(|&(day, file, bytes)| Held {
                date: d(day),
                file,
                bytes,
            })
            .collect()
    }

    fn incoming(dates: &[(u32, u64)]) -> Vec<Incoming> {
        dates
            .iter()
            .map(|&(day, bytes)| Incoming {
                date: d(day),
                bytes,
            })
            .collect()
    }

    fn expect(files: &[&[u32]], replaced: &[&'static str]) -> Plan<&'static str> {
        Plan {
            files: files
                .iter()
                .map(|days| days.iter().map(|&day| d(day)).collect())
                .collect(),
            replaced: replaced.to_vec(),
        }
    }

    /// What a case says, what the lake holds `(day, file, bytes)`, what is written
    /// `(day, bytes)`, and the plan: its files' days and the files they replace.
    type Case = (
        &'static str,
        &'static [(u32, &'static str, u64)],
        &'static [(u32, u64)],
        &'static [&'static [u32]],
        &'static [&'static str],
    );

    #[test]
    fn writes_turn_into_files_of_whole_dates() {
        #[rustfmt::skip]
        let cases: &[Case] = &[
            ("a first small date is a file of its own",
             &[], &[(1, 1)], &[&[1]], &[]),
            ("a small date joins the pack before it",
             &[(1, "a", 1)], &[(2, 1)], &[&[1, 2]], &["a"]),
            ("1 row, 1 row, then 10M rows: the big day stands alone",
             &[(1, "a", 1), (2, "a", 1)], &[(3, 500)], &[&[3]], &[]),
            ("a pack without room is left alone",
             &[(1, "a", 60)], &[(2, 50)], &[&[2]], &[]),
            ("a backfill going backwards joins the pack after it",
             &[(5, "a", 10)], &[(4, 10)], &[&[4, 5]], &["a"]),
            ("before is preferred, after is the fallback",
             &[(1, "a", 95), (5, "b", 10)], &[(3, 10)], &[&[3, 5]], &["b"]),
            ("replacing a packed date rewrites its pack",
             &[(1, "a", 10), (2, "a", 10), (3, "a", 10)], &[(2, 20)], &[&[1, 2, 3]], &["a"]),
            ("a packed date grown past the target splits the pack",
             &[(1, "a", 10), (2, "a", 10), (3, "a", 10)], &[(2, 150)], &[&[1], &[2], &[3]], &["a"]),
            ("a date inside a pack's range but not in it goes into the pack",
             &[(1, "a", 10), (5, "a", 10)], &[(3, 10)], &[&[1, 3, 5]], &["a"]),
            ("removing a packed date rewrites the pack without it",
             &[(1, "a", 10), (2, "a", 10)], &[(2, 0)], &[&[1]], &["a"]),
            ("removing a file's only date leaves no file",
             &[(1, "a", 10)], &[(1, 0)], &[], &["a"]),
            ("removing a date the lake never held changes nothing",
             &[(1, "a", 10)], &[(2, 0)], &[], &[]),
            ("a big date replacing a big date is one file",
             &[(1, "a", 400)], &[(1, 300)], &[&[1]], &["a"]),
            ("new files never span a file kept as it is",
             &[(1, "a", 10), (3, "b", 200), (5, "c", 10)], &[(1, 10), (5, 10)], &[&[1], &[5]], &["a", "c"]),
            ("small packs a write touches are packed together",
             &[(1, "a", 30), (2, "b", 30)], &[(1, 30), (2, 30)], &[&[1, 2]], &["a", "b"]),
            ("several small dates fill one neighbour up to the target",
             &[(1, "a", 70)], &[(2, 20), (3, 20)], &[&[1, 2], &[3]], &["a"]),
            ("a big date in several files is rewritten from all of them",
             &[(1, "a", 100), (1, "b", 100)], &[(1, 300)], &[&[1]], &["a", "b"]),
            ("the last piece of a split date takes on no small date",
             &[(1, "a", 100), (1, "b", 20)], &[(2, 10)], &[&[2]], &[]),
            ("a small date skips a split date for the pack after it",
             &[(1, "a", 100), (1, "b", 20), (5, "c", 10)], &[(3, 10)], &[&[3, 5]], &["c"]),
        ];
        for &(case, held_dates, incoming_dates, files, replaced) in cases {
            assert_eq!(
                plan(&held(held_dates), &incoming(incoming_dates), TARGET),
                expect(files, replaced),
                "{case}"
            );
        }
    }
}
