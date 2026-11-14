//! History files in a real local directory: written whole by date, read back by date.

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::array::{ArrayRef, AsArray, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Date32Type, Field, Int64Type, Schema};
use kermodes_lake::{Lake, Written};
use kermodes_weir::{Date, Error};

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "kermodes-lake-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&dir);
        Self(dir)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("n", DataType::Int64, true),
        Field::new("sym", DataType::Utf8, true),
    ])
}

fn batch(values: std::ops::Range<i64>) -> RecordBatch {
    let syms: Vec<String> = values.clone().map(|v| format!("sym-{v:08}")).collect();
    RecordBatch::try_from_iter([
        (
            "n",
            Arc::new(Int64Array::from_iter_values(values)) as ArrayRef,
        ),
        ("sym", Arc::new(StringArray::from(syms))),
    ])
    .unwrap()
}

fn d(day: u32) -> Date {
    Date::parse(&format!("2026-11-{day:02}")).unwrap()
}

/// Each row read: its `Date` and its `n`.
fn read(lake: &Lake, file: &str, first: Date, last: Date) -> (Vec<(i32, i64)>, (usize, usize)) {
    let mut batches = lake.read("md/ticks.1", file, first, last, &[]).unwrap();
    let mut rows = Vec::new();
    for batch in batches.by_ref() {
        let batch = batch.unwrap();
        let dates = batch.column(0).as_primitive::<Date32Type>();
        let n = batch.column(1).as_primitive::<Int64Type>();
        rows.extend(
            dates
                .values()
                .iter()
                .copied()
                .zip(n.values().iter().copied()),
        );
    }
    (rows, batches.row_groups)
}

/// A pack of small dates is one file; a date with no rows is in none; reading a date fetches
/// only its row group.
#[test]
fn dates_are_written_whole_and_read_back_by_date() {
    let dir = TempDir::new();
    let lake = Lake::open(dir.0.to_str().unwrap()).unwrap();
    let rows = |date: Date| match date {
        date if date == d(1) => vec![batch(0..2), batch(2..3)],
        date if date == d(3) => vec![batch(10..12)],
        _ => vec![],
    };
    let Written { files, dates } = lake
        .write(
            "md/ticks.1",
            &schema(),
            &[],
            &[d(1), d(2), d(3)],
            |date, push| rows(date).into_iter().try_for_each(push),
            1 << 30,
        )
        .unwrap();

    assert_eq!(files.len(), 1);
    let file = &files[0];
    assert_eq!((file.first, file.last, file.rows), (d(1), d(3), 5));
    let held: Vec<(Date, u64)> = dates.iter().map(|h| (h.date, h.rows)).collect();
    assert_eq!(held, [(d(1), 3), (d(3), 2)]);
    assert!(dates.iter().all(|h| h.file == file.name && h.bytes > 0));
    let on_disk = fs::metadata(dir.0.join("md/ticks.1").join(&file.name)).unwrap();
    assert_eq!(on_disk.len(), file.bytes);

    let (third, groups) = read(&lake, &file.name, d(3), d(3));
    assert_eq!(third, [(d(3).days(), 10), (d(3).days(), 11)]);
    assert_eq!(groups, (1, 2), "only the date's own row group is read");
    let (all, groups) = read(&lake, &file.name, d(1), d(30));
    let n: Vec<i64> = all.iter().map(|&(_, n)| n).collect();
    assert_eq!(n, [0, 1, 2, 10, 11]);
    assert_eq!(groups, (2, 2));
    assert!(read(&lake, &file.name, d(4), d(30)).0.is_empty());
}

/// A date past the target size lies in several files, each about the target, all of it there in
/// order.
#[test]
fn a_big_date_is_split_over_files_of_about_the_target() {
    let dir = TempDir::new();
    let lake = Lake::open(dir.0.to_str().unwrap()).unwrap();
    let target = 256 << 10;
    let Written { files, dates } = lake
        .write(
            "md/ticks.1",
            &schema(),
            &[],
            &[d(1)],
            |_, push| (0..40).try_for_each(|i| push(batch(i * 5_000..(i + 1) * 5_000))),
            target,
        )
        .unwrap();

    assert!(files.len() > 2, "{} files", files.len());
    assert!(files.iter().all(|f| f.first == d(1) && f.last == d(1)));
    // A file ends once it reaches the target, with the batch that took it there.
    assert!(files.iter().all(|f| f.bytes < 2 * target), "{files:?}");
    let per_file: Vec<(String, u64)> = files.iter().map(|f| (f.name.clone(), f.rows)).collect();
    let per_date: Vec<(String, u64)> = dates.iter().map(|h| (h.file.clone(), h.rows)).collect();
    assert_eq!(per_date, per_file);

    let mut n = Vec::new();
    for file in &files {
        n.extend(
            read(&lake, &file.name, d(1), d(1))
                .0
                .into_iter()
                .map(|(_, n)| n),
        );
    }
    assert_eq!(n, (0..200_000).collect::<Vec<_>>());
}

#[test]
fn a_table_with_its_own_date_column_is_refused() {
    let dir = TempDir::new();
    let lake = Lake::open(dir.0.to_str().unwrap()).unwrap();
    let schema = Schema::new(vec![Field::new("Date", DataType::Utf8, true)]);
    let refused = lake.write("md/x.1", &schema, &[], &[d(1)], |_, _| Ok(()), 1 << 30);
    assert!(matches!(refused, Err(Error::Schema(_))), "{refused:?}");
}

/// A sorted table's dates are written sorted by its sort columns, every row kept; a sort column
/// that is not a column is refused.
#[test]
fn a_sorted_tables_dates_are_written_in_sort_order() {
    let dir = TempDir::new();
    let lake = Lake::open(dir.0.to_str().unwrap()).unwrap();
    // Each date arrives as several batches, its syms in no order.
    let shuffled = |date: u32, part: i64| {
        let n: Vec<i64> = (0..50)
            .map(|i| (i * 37 + part * 11 + i64::from(date)) % 50)
            .collect();
        let syms: Vec<String> = n
            .iter()
            .map(|v| format!("sym-{:08}", (v * 7919) % 50))
            .collect();
        RecordBatch::try_from_iter([
            ("n", Arc::new(Int64Array::from(n)) as ArrayRef),
            ("sym", Arc::new(StringArray::from(syms))),
        ])
        .unwrap()
    };
    let written = lake
        .write(
            "md/ticks.1",
            &schema(),
            &["sym".to_string()],
            &[d(1), d(2), d(3)],
            |date, push| {
                for part in 0..4 {
                    push(shuffled(date.days() as u32, part))?;
                }
                Ok(())
            },
            1 << 30,
        )
        .unwrap();
    let file = &written.files[0].name;
    for day in 1..=3 {
        let mut syms = Vec::new();
        for batch in lake.read("md/ticks.1", file, d(day), d(day), &[]).unwrap() {
            let batch = batch.unwrap();
            let column = batch.column(2).as_string::<i32>();
            syms.extend((0..batch.num_rows()).map(|i| column.value(i).to_string()));
        }
        assert_eq!(syms.len(), 200, "every row of 2026-11-{day:02}");
        assert!(syms.is_sorted(), "2026-11-{day:02} is in sym order");
    }
    let refused = lake.write(
        "md/x.1",
        &schema(),
        &["px".to_string()],
        &[d(1)],
        |_, _| Ok(()),
        1 << 30,
    );
    assert!(matches!(refused, Err(Error::Schema(_))), "{refused:?}");
}

/// A read bounded on a sorted column reads only the pages that can hold it, and still every row
/// that matches.
#[test]
fn a_bounded_read_of_a_sorted_date_skips_the_pages_it_cannot_need() {
    let dir = TempDir::new();
    let lake = Lake::open(dir.0.to_str().unwrap()).unwrap();
    const ROWS: i64 = 400_000;
    let day = |_: Date, push: &mut dyn FnMut(RecordBatch) -> Result<(), Error>| {
        let n: Vec<i64> = (0..ROWS).collect();
        let syms: Vec<String> = n
            .iter()
            .map(|v| format!("sym-{:08}", (v * 7919) % 200))
            .collect();
        push(
            RecordBatch::try_from_iter([
                ("n", Arc::new(Int64Array::from(n)) as ArrayRef),
                ("sym", Arc::new(StringArray::from(syms))),
            ])
            .unwrap(),
        )
    };
    let written = lake
        .write(
            "md/ticks.1",
            &schema(),
            &["sym".to_string()],
            &[d(1)],
            day,
            1 << 30,
        )
        .unwrap();
    let file = &written.files[0].name;
    let bounds = kermodes_lake::column_bounds("sym = 'sym-00000042'");
    let mut batches = lake.read("md/ticks.1", file, d(1), d(1), &bounds).unwrap();
    let (mut matching, mut returned) = (0, 0);
    for batch in batches.by_ref() {
        let batch = batch.unwrap();
        returned += batch.num_rows() as u64;
        let syms = batch.column(2).as_string::<i32>();
        matching += (0..batch.num_rows())
            .filter(|&i| syms.value(i) == "sym-00000042")
            .count();
    }
    assert_eq!(
        matching,
        (ROWS / 200) as usize,
        "every matching row is read"
    );
    let (read, held) = batches.rows;
    assert_eq!(
        (read, held),
        (returned, ROWS as u64),
        "the rows it says it reads"
    );
    assert!(read < held / 10, "{read} of {held} rows read");
}
