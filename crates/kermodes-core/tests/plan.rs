//! `Plan::pushed_down`: what a plan's scans are asked to read.

use kermodes_core::plan::{Plan, Scan};
use kermodes_core::{SortKey, Window};

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
