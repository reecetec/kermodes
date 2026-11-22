//! `Plan::pushed_down`: what a plan's scans are asked to read. `Plan::rewritten`: which sums of
//! a column plus a constant it takes from the column's sum and count, giving the same rows.

use std::sync::Arc;

use arrow::array::{Float64Array, Int16Array, Int64Array, RecordBatch, StringArray};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema};
use kermodes_core::plan::{Plan, Scan};
use kermodes_core::{Session, SortKey, Table, Window};

fn scan() -> Plan {
    Plan::Scan(Scan {
        name: "md.ticks".into(),
        predicate: None,
        columns: None,
        limit: None,
        live: false,
    })
}

fn on(input: Plan, op: impl FnOnce(Box<Plan>) -> Plan) -> Plan {
    op(Box::new(input))
}

fn strings(names: &[&str]) -> Vec<String> {
    names.iter().map(|n| n.to_string()).collect()
}

/// The pushed-down plan's one scan.
fn pushed(plan: Plan) -> Scan {
    let plan = plan.pushed_down();
    let scans = plan.scans();
    let [scan] = scans.as_slice() else {
        panic!("one scan");
    };
    (*scan).clone()
}

#[test]
fn filters_over_a_scan_become_its_predicate() {
    let plan = on(
        on(scan(), |input| Plan::Filter {
            input,
            predicate: "sym = 'AAPL'".into(),
        }),
        |input| Plan::Filter {
            input,
            predicate: "px > 1".into(),
        },
    );
    let scan = pushed(plan);
    assert_eq!(
        scan.predicate.as_deref(),
        Some("(sym = 'AAPL') and (px > 1)")
    );
    // A scan with a predicate reads every column, the predicate's among them.
    assert_eq!(scan.columns, None);
}

/// Decision table: which heads and slices limit the scan they read.
#[test]
fn a_head_limits_a_scan_through_operators_that_keep_rows_in_place() {
    let select = |input| Plan::Select {
        input,
        columns: strings(&["ts", "px"]),
    };
    let cases: [(Plan, Option<usize>); 5] = [
        (on(scan(), |input| Plan::Head { input, rows: 10 }), Some(10)),
        (
            on(on(scan(), select), |input| Plan::Head { input, rows: 10 }),
            Some(10),
        ),
        (
            on(scan(), |input| Plan::Slice {
                input,
                start: 5,
                stop: Some(15),
            }),
            Some(15),
        ),
        // A filter's rows are not the scan's first rows.
        (
            on(
                on(scan(), |input| Plan::Filter {
                    input,
                    predicate: "px > 1".into(),
                }),
                |input| Plan::Head { input, rows: 10 },
            ),
            None,
        ),
        // Nor a sort's.
        (
            on(
                on(scan(), |input| Plan::Sort {
                    input,
                    by: vec![SortKey::parse("px")],
                }),
                |input| Plan::Head { input, rows: 10 },
            ),
            None,
        ),
    ];
    for (plan, limit) in cases {
        let described = format!("{plan:?}");
        assert_eq!(pushed(plan).limit, limit, "{described}");
    }
}

/// Decision table: the columns a scan reads, given the operators over it.
#[test]
fn a_scan_reads_only_the_columns_the_plan_uses() {
    let cases: [(Plan, Option<&[&str]>); 5] = [
        (scan(), None),
        // An assignment's inputs are read; the column it makes is not.
        (
            on(
                on(scan(), |input| Plan::Update {
                    input,
                    assignments: strings(&["notional = px * size"]),
                }),
                |input| Plan::Select {
                    input,
                    columns: strings(&["sym", "notional"]),
                },
            ),
            Some(&["px", "size", "sym"]),
        ),
        (
            on(scan(), |input| Plan::Agg {
                input,
                by: strings(&["sym"]),
                aggregations: vec![
                    ("n".into(), "count(*)".into()),
                    ("avg_px".into(), "mean(px)".into()),
                ],
                aliases: Vec::new(),
            }),
            Some(&["px", "sym"]),
        ),
        (
            on(
                on(scan(), |input| Plan::Rolling {
                    input,
                    by: strings(&["sym"]),
                    window: Window::Time {
                        at: "ts".into(),
                        span: 5,
                    },
                    aggregations: vec![("v".into(), "sum(size)".into())],
                }),
                |input| Plan::Select {
                    input,
                    columns: strings(&["v"]),
                },
            ),
            Some(&["size", "sym", "ts"]),
        ),
        // A predicate's columns are read with the scan's.
        (
            on(
                on(scan(), |input| Plan::Filter {
                    input,
                    predicate: "venue = 'X'".into(),
                }),
                |input| Plan::Select {
                    input,
                    columns: strings(&["px"]),
                },
            ),
            Some(&["px", "venue"]),
        ),
    ];
    for (plan, columns) in cases {
        let described = format!("{plan:?}");
        assert_eq!(pushed(plan).columns, columns.map(strings), "{described}");
    }
}

/// Groups `a` and `b` with values and NULLs in `x`, and `c` with only NULLs.
fn sums_table(session: &Session) -> Table {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Utf8, true),
        Field::new("x", DataType::Int64, true),
        Field::new("y", DataType::Int16, true),
        Field::new("f", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a", "a", "b", "b", "c"])),
            Arc::new(Int64Array::from(vec![
                Some(1),
                None,
                Some(5),
                Some(-7),
                None,
            ])),
            Arc::new(Int16Array::from(vec![2, 3, 4, 5, 6])),
            Arc::new(Float64Array::from(vec![0.1, 0.2, 0.3, 0.4, 0.5])),
        ],
    )
    .unwrap();
    session.static_table(schema, vec![batch]).unwrap()
}

fn results(plan: &Plan, table: &Table) -> RecordBatch {
    let built = plan.build(&mut |_| Ok(table.clone())).unwrap();
    concat_batches(built.schema(), &built.snapshot().unwrap()).unwrap()
}

fn rewritten(plan: &Plan, table: &Table) -> Plan {
    plan.clone().rewritten(&mut |_| Ok(table.clone())).unwrap()
}

fn aggregated(assignments: &[&str], by: &[&str], aggregations: &[(&str, &str)]) -> Plan {
    let updated = on(scan(), |input| Plan::Update {
        input,
        assignments: strings(assignments),
    });
    on(updated, |input| Plan::Agg {
        input,
        by: strings(by),
        aggregations: aggregations
            .iter()
            .map(|(name, spec)| (name.to_string(), spec.to_string()))
            .collect(),
        aliases: Vec::new(),
    })
}

#[test]
fn sums_of_a_column_plus_a_constant_come_from_its_sum_and_count() {
    let session = Session::new();
    let table = sums_table(&session);
    let assignments = [
        "p = x + 3",
        "q = 3 + x",
        "m = x - 3",
        "n = 3 - x",
        "g = x + -2",
        "h = (y + 1)",
    ];
    let aggregations = [
        ("s", "sum(x)"),
        ("sp", "sum(p)"),
        ("sq", "sum(q)"),
        ("sm", "sum(m)"),
        ("sn", "sum(n)"),
        ("sg", "sum(g)"),
        ("sh", "sum(h)"),
        ("c", "count(*)"),
        ("mx", "mean(x)"),
    ];
    for by in [vec!["k"], Vec::new()] {
        let plan = aggregated(&assignments, &by, &aggregations);
        let folded = rewritten(&plan, &table);
        assert_ne!(folded, plan, "by {by:?}: rewritten");
        assert_eq!(
            results(&folded, &table),
            results(&plan, &table),
            "by {by:?}"
        );
    }
    // As SQL writes it: one column per distinct expression, then the same rewrite.
    let sql = "SELECT k, SUM(x), SUM(x + 1), SUM(x + 2), SUM(5 - x) FROM md.ticks GROUP BY k";
    let plan = kermodes_core::sql::parse(sql).unwrap().plan;
    let folded = rewritten(&plan, &table);
    assert_ne!(folded, plan, "SQL rewritten");
    assert_eq!(results(&folded, &table), results(&plan, &table), "SQL");
}

#[test]
fn sums_stay_as_written_where_the_rewrite_could_change_them() {
    let session = Session::new();
    let table = sums_table(&session);
    let cases = [
        // A float column: x + 1 rounds per row.
        aggregated(&["e = f + 1"], &["k"], &[("s", "sum(e)")]),
        // The column is read by more than sums: by another aggregation, a key, an assignment.
        aggregated(&["e = x + 1"], &["k"], &[("s", "sum(e)"), ("m", "mean(e)")]),
        aggregated(&["e = x + 1"], &["k", "e"], &[("s", "sum(e)")]),
        aggregated(
            &["e = x + 1", "d = e * 2"],
            &["k"],
            &[("s", "sum(e)"), ("t", "sum(d)")],
        ),
        // The column is not the update's input's: the update replaces it first.
        aggregated(&["x = x * 2", "e = x + 1"], &["k"], &[("s", "sum(e)")]),
        // Not a column plus or minus a constant.
        aggregated(&["e = x + y"], &["k"], &[("s", "sum(e)")]),
        aggregated(&["e = x * 2"], &["k"], &[("s", "sum(e)")]),
    ];
    for plan in cases {
        assert_eq!(rewritten(&plan, &table), plan);
    }
}
