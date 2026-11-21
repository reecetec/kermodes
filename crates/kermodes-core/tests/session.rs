use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use kermodes_core::{Error, Session, Table};

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("sym", DataType::Utf8, true),
        Field::new("size", DataType::Int64, true),
    ])
}

fn rows(syms: &[&str], sizes: &[i64]) -> RecordBatch {
    let columns: Vec<(&str, ArrayRef)> = vec![
        ("sym", Arc::new(StringArray::from(syms.to_vec()))),
        ("size", Arc::new(Int64Array::from(sizes.to_vec()))),
    ];
    RecordBatch::try_from_iter(columns).unwrap()
}

fn sizes(table: &Table) -> Vec<i64> {
    let batches = table.snapshot().unwrap();
    let all = concat_batches(table.schema(), &batches).unwrap();
    let column = all.column_by_name("size").unwrap();
    let column = column.as_any().downcast_ref::<Int64Array>().unwrap();
    column.values().to_vec()
}

#[test]
fn derived_tables_update_only_when_a_cycle_runs() {
    let session = Session::new();
    let input = session.input_table(&schema()).unwrap();
    let big = input.filter("size > 5").unwrap();

    input.append(&[rows(&["a", "b"], &[3, 8])]).unwrap();
    assert_eq!(big.num_rows().unwrap(), 0);

    session.step();
    assert_eq!(sizes(&big), vec![8]);

    input.append(&[rows(&["c"], &[13])]).unwrap();
    session.step();
    assert_eq!(sizes(&big), vec![8, 13]);
}

#[test]
fn tables_record_the_cycle_that_last_changed_them() {
    let session = Session::new();
    let input = session.input_table(&schema()).unwrap();
    let big = input.filter("size > 5").unwrap();
    assert_eq!((input.changed(), big.changed()), (0, 0));

    input.append(&[rows(&["a"], &[8])]).unwrap();
    let first = session.step();
    assert_eq!((input.changed(), big.changed()), (first, first));

    // A row the filter drops changes the input only; an idle cycle changes nothing.
    input.append(&[rows(&["b"], &[3])]).unwrap();
    let second = session.step();
    session.step();
    assert_eq!((input.changed(), big.changed()), (second, first));
}

#[test]
fn tables_built_later_start_from_current_rows() {
    let session = Session::new();
    let input = session.input_table(&schema()).unwrap();
    input.append(&[rows(&["a", "b"], &[3, 8])]).unwrap();
    session.step();

    let doubled = input.update(&["size = size * 2"]).unwrap();
    assert_eq!(sizes(&doubled), vec![6, 16]);
}

#[test]
fn tiny_appends_are_held_in_large_batches() {
    let session = Session::new();
    let input = session.input_table(&schema()).unwrap();
    let descending = input.sort(&["size desc"]).unwrap();
    let numbered = |from: i64, n: i64| {
        let sizes: Vec<i64> = (from..from + n).collect();
        rows(&vec!["s"; n as usize], &sizes)
    };

    // 200,000 rows in small batches, one a cycle, as an ingester flushing often gives them: held
    // as a few large batches plus the latest small ones.
    for from in (0..200_000).step_by(100) {
        input.append(&[numbered(from, 100)]).unwrap();
        session.step();
    }
    assert!(input.snapshot().unwrap().len() < 200);

    // A large batch arriving while small ones wait leaves no run of them behind.
    input.append(&[numbered(200_000, 70_000)]).unwrap();
    session.step();
    assert!(input.snapshot().unwrap().len() < 10);

    let all: Vec<i64> = (0..270_000).collect();
    assert_eq!(sizes(&input), all);
    assert_eq!(
        sizes(&descending),
        all.iter().rev().copied().collect::<Vec<_>>()
    );
    let later = input.update(&["size = size * 2"]).unwrap();
    assert_eq!(
        sizes(&later),
        all.iter().map(|size| size * 2).collect::<Vec<_>>()
    );
}

#[test]
fn static_tables_compute_immediately_and_never_tick() {
    let session = Session::new();
    let data = rows(&["a", "b", "c"], &[1, 2, 3]);
    let fixed = session.static_table(data.schema(), vec![data]).unwrap();
    let odd = fixed.filter("size % 2 = 1").unwrap();
    assert!(!odd.is_refreshing());
    assert_eq!(sizes(&odd), vec![1, 3]);
}

#[test]
fn update_sees_earlier_assignments_and_replaces_columns() {
    let session = Session::new();
    let input = session.input_table(&schema()).unwrap();
    let out = input
        .update(&[
            "notional = size * 1.5",
            "size = size + 1",
            "check = notional / size",
        ])
        .unwrap();
    input.append(&[rows(&["a"], &[4])]).unwrap();
    session.step();

    let names: Vec<_> = out
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert_eq!(names, ["sym", "size", "notional", "check"]);
    let batch = &out.snapshot().unwrap()[0];
    let check = batch.column_by_name("check").unwrap();
    assert_eq!(
        check.as_ref(),
        &Float64Array::from(vec![1.2]) as &dyn arrow::array::Array
    );
}

#[test]
fn a_failed_table_fails_its_dependents_but_not_the_session() {
    let session = Session::new();
    let input = session.input_table(&schema()).unwrap();
    let risky = input.update(&["big = size * 4611686018427387904"]).unwrap();
    let downstream = risky.filter("big > 0").unwrap();
    let sibling = input.filter("size > 0").unwrap();

    input.append(&[rows(&["a"], &[1])]).unwrap();
    session.step();
    assert_eq!(downstream.num_rows().unwrap(), 1);

    input.append(&[rows(&["b"], &[4])]).unwrap();
    session.step();
    input.append(&[rows(&["c"], &[2])]).unwrap();
    session.step();

    assert!(matches!(risky.snapshot(), Err(Error::Compute { .. })));
    assert!(matches!(downstream.snapshot(), Err(Error::Upstream { .. })));
    assert_eq!(sizes(&sibling), vec![1, 4, 2]);
    assert!(matches!(
        risky.filter("big > 0"),
        Err(Error::Upstream { .. })
    ));
}

#[test]
fn a_fed_table_that_loses_its_source_fails_with_its_dependents() {
    let session = Session::new();
    let (fed, feed) = session.fed_table(&schema(), "live md.ticks").unwrap();
    feed.ready();
    let downstream = fed.filter("size > 0").unwrap();
    feed.append(&[rows(&["a"], &[1])]).unwrap();
    session.step();
    assert_eq!(downstream.num_rows().unwrap(), 1);

    assert!(feed.fail("the store closed the stream".into()));
    session.step();
    match fed.snapshot() {
        Err(Error::Source { table, message }) => {
            assert_eq!(
                (table.as_str(), message.as_str()),
                ("live md.ticks", "the store closed the stream")
            );
        }
        other => panic!("expected a lost source, got {other:?}"),
    }
    assert!(matches!(downstream.snapshot(), Err(Error::Upstream { .. })));
    // Rows after the failure change nothing.
    feed.append(&[rows(&["b"], &[2])]).unwrap();
    session.step();
    assert!(matches!(fed.snapshot(), Err(Error::Source { .. })));
}

#[test]
fn appends_must_match_the_table() {
    let session = Session::new();
    let input = session.input_table(&schema()).unwrap();

    let wrong_type: Vec<(&str, ArrayRef)> = vec![
        ("sym", Arc::new(StringArray::from(vec!["a"]))),
        ("size", Arc::new(Float64Array::from(vec![1.0]))),
    ];
    let error = input
        .append(&[RecordBatch::try_from_iter(wrong_type).unwrap()])
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "column `size` is float64, but the table expects int64"
    );

    let extra: Vec<(&str, ArrayRef)> = vec![
        ("size", Arc::new(Int64Array::from(vec![1]))),
        ("sym", Arc::new(StringArray::from(vec!["a"]))),
        ("venue", Arc::new(StringArray::from(vec!["x"]))),
    ];
    let error = input
        .append(&[RecordBatch::try_from_iter(extra).unwrap()])
        .unwrap_err();
    assert_eq!(error.to_string(), "unexpected columns: venue");

    let derived = input.filter("size > 0").unwrap();
    assert!(matches!(
        derived.append(&[rows(&["a"], &[1])]),
        Err(Error::Unsupported(_))
    ));
}

#[test]
fn columns_are_matched_by_name_not_position() {
    let session = Session::new();
    let input = session.input_table(&schema()).unwrap();
    let reordered: Vec<(&str, ArrayRef)> = vec![
        ("size", Arc::new(Int64Array::from(vec![7]))),
        ("sym", Arc::new(StringArray::from(vec!["a"]))),
    ];
    input
        .append(&[RecordBatch::try_from_iter(reordered).unwrap()])
        .unwrap();
    session.step();
    assert_eq!(sizes(&input), vec![7]);
}

/// Re-running code that builds a table (a notebook cell, say) replaces the old table rather
/// than adding to the session: dropping the last handle releases it.
#[test]
fn rebuilding_a_table_replaces_the_old_one() {
    let session = Session::new();
    let input = session.input_table(&schema()).unwrap();
    let mut latest = input.last_by(&["sym"]).unwrap();
    for size in 0..5 {
        input
            .append(&[rows(&["a", "b"], &[size, size + 1])])
            .unwrap();
        session.step();
        latest = input.last_by(&["sym"]).unwrap();
        assert_eq!(session.table_count(), 2, "the input and one last_by");
    }
    assert_eq!(sizes(&latest), vec![4, 5]);
}

/// A table lives while it has a handle or a live table is computed from it; dropping the end of
/// a chain releases the whole unused chain, and the rest keeps ticking.
#[test]
fn tables_live_while_held_or_needed() {
    let session = Session::new();
    let input = session.input_table(&schema()).unwrap();
    let chained = input
        .filter("size > 1")
        .unwrap()
        .update(&["size = size * 10"])
        .unwrap();
    assert_eq!(
        session.table_count(),
        3,
        "the unnamed filter stays for the update"
    );

    let other = session.input_table(&schema()).unwrap();
    let both = Table::merge(&[&input.filter("size > 2").unwrap(), &other]).unwrap();
    drop(other);
    assert_eq!(session.table_count(), 6, "a merge keeps its inputs");

    drop(chained);
    assert_eq!(
        session.table_count(),
        4,
        "the update and its filter went together"
    );

    input.append(&[rows(&["a", "b", "c"], &[1, 2, 3])]).unwrap();
    session.step();
    assert_eq!(sizes(&both), vec![3]);
    drop(both);
    assert_eq!(session.table_count(), 1);
}

/// A table built on a static table has its own rows, so the static table can go.
#[test]
fn static_results_outlive_their_inputs() {
    let session = Session::new();
    let fixed = session
        .static_table(Arc::new(schema()), vec![rows(&["a", "b", "a"], &[1, 2, 3])])
        .unwrap();
    let total = fixed
        .agg_by(&["sym"], &[("size", "sum(size)")])
        .unwrap()
        .sort(&["sym"])
        .unwrap();
    drop(fixed);
    assert_eq!(session.table_count(), 1);
    assert_eq!(sizes(&total), vec![4, 2]);
}

/// Rows by position (what the table viewer fetches) match the snapshot, for each way a table
/// keeps its rows: chunks, slots, a merge's inputs, and a join computed on read.
#[test]
fn rows_at_positions_match_the_snapshot() {
    let session = Session::new();
    let input = session.input_table(&schema()).unwrap();
    let keyed = session.keyed_input_table(&schema(), &["sym"]).unwrap();
    let names = ["a", "b", "c", "d", "e", "f", "g", "h"];
    for round in 0..4 {
        let sizes: Vec<i64> = (0..8).map(|i| round * 10 + i).collect();
        input.append(&[rows(&names, &sizes)]).unwrap();
        keyed
            .upsert(&[rows(&names[round as usize..], &sizes[round as usize..])])
            .unwrap();
        session.step();
    }
    let merged = Table::merge(&[&input, &input.filter("size > 20").unwrap()]).unwrap();
    let joined = input
        .natural_join(&keyed, &["sym"], Some(&["latest=size"]))
        .unwrap();
    let latest = input.last_by(&["sym"]).unwrap();
    for table in [&input, &keyed, &merged, &joined, &latest] {
        let all = concat_batches(table.schema(), &table.snapshot().unwrap()).unwrap();
        let len = all.num_rows();
        for (start, count) in [
            (0, 3),
            (2, 5),
            (len.saturating_sub(2), 10),
            (len + 5, 3),
            (0, len),
        ] {
            let window = table.rows_at(start, count).unwrap();
            let start = start.min(len);
            let want = all.slice(start, count.min(len - start));
            assert_eq!(window.total, len, "{}", table.description());
            assert_eq!(
                window.rows,
                want,
                "{} at {start}+{count}",
                table.description()
            );
            let every = table.rows_at(0, len).unwrap().keys;
            assert_eq!(window.keys, every[start..start + want.num_rows()]);
            let distinct: std::collections::HashSet<u64> = every.iter().copied().collect();
            assert_eq!(distinct.len(), len, "{}", table.description());
        }
    }
}

/// A table that keeps a merge's row keys (a filter over a merge of aggregations, which tick and
/// so are followed by key) can be merged again, ticking and static.
#[test]
fn a_merge_takes_a_table_keyed_by_a_merge() {
    let session = Session::new();
    let input = session.input_table(&schema()).unwrap();
    let fixed = session
        .static_table(Arc::new(schema()), vec![rows(&["a", "b", "a"], &[1, 2, 3])])
        .unwrap();
    let merged_twice = |table: &Table| {
        let totals = table.agg_by(&["sym"], &[("size", "sum(size)")]).unwrap();
        let both = Table::merge(&[&totals, &totals]).unwrap();
        let big = both.filter("size > 2").unwrap();
        Table::merge(&[&big, &both, &big]).unwrap()
    };
    let (ticking, static_) = (merged_twice(&input), merged_twice(&fixed));
    assert_eq!(sizes(&static_), [4, 4, 4, 2, 4, 2, 4, 4]);

    input.append(&[rows(&["a", "b", "a"], &[1, 2, 3])]).unwrap();
    session.step();
    assert_eq!(sizes(&ticking), [4, 4, 4, 2, 4, 2, 4, 4]);
    input.append(&[rows(&["b"], &[5])]).unwrap();
    session.step();
    assert_eq!(sizes(&ticking), [4, 7, 4, 7, 4, 7, 4, 7, 4, 7, 4, 7]);
}

/// A table built on a fed table keeps it fed after the code that made the fed table let it go
/// (`live_table(...).where(...)`); once nothing uses it, the feed says it is gone.
#[test]
fn a_fed_table_is_fed_while_a_table_is_built_on_it() {
    let session = Session::new();
    let (fed, feed) = session.fed_table(&schema(), "live md.ticks").unwrap();
    feed.ready();
    let downstream = fed.filter("size > 0").unwrap();
    drop(fed);
    assert!(feed.append(&[rows(&["a"], &[1])]).unwrap());
    session.step();
    assert!(feed.append(&[rows(&["b"], &[2])]).unwrap());
    session.step();
    assert_eq!(sizes(&downstream), [1, 2]);

    drop(downstream);
    assert!(
        !feed.append(&[rows(&["c"], &[3])]).unwrap(),
        "nothing uses it"
    );
}

/// A fed table loads whole: what its store held when it subscribed shows in one cycle, once the
/// feed says it is all there, not batch by batch as it arrives. A lost source fails it at once.
#[test]
fn a_fed_table_loads_whole_once_ready() {
    let session = Session::new();
    let (fed, feed) = session.fed_table(&schema(), "live md.ticks").unwrap();
    let downstream = fed.filter("size > 0").unwrap();
    assert!(fed.is_loading());
    assert!(
        downstream.is_loading(),
        "a table built on it is loading too"
    );
    feed.append(&[rows(&["a"], &[1])]).unwrap();
    session.step();
    feed.append(&[rows(&["b"], &[2])]).unwrap();
    session.step();
    assert!(
        sizes(&downstream).is_empty(),
        "nothing shows while it loads"
    );

    feed.ready();
    assert!(
        fed.is_loading() && downstream.is_loading() && sizes(&downstream).is_empty(),
        "ready, but its rows wait for the next cycle: never current and empty meanwhile"
    );
    let cycle = session.step();
    assert!(!fed.is_loading() && !downstream.is_loading());
    assert_eq!(sizes(&downstream), [1, 2]);
    assert_eq!(fed.changed(), cycle, "all of it in one cycle");
    feed.append(&[rows(&["c"], &[3])]).unwrap();
    session.step();
    assert_eq!(sizes(&downstream), [1, 2, 3]);

    let (empty, feed) = session.fed_table(&schema(), "live md.trades").unwrap();
    feed.ready();
    assert!(
        !empty.is_loading(),
        "a store that held nothing: current at once"
    );

    let (lost, feed) = session.fed_table(&schema(), "live md.quotes").unwrap();
    feed.fail("the store closed the stream".into());
    session.step();
    assert!(matches!(lost.snapshot(), Err(Error::Source { .. })));
}
