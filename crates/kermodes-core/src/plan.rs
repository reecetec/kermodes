//! A query as the engine's operators (architecture.md §5.3), whichever API wrote it: SQL binds
//! to a `Plan` (`sql::parse`), and so do the Python API's operators on stored tables. Nothing is
//! read until `Plan::build` makes it tables, asking its caller for each table it names (`Scan`);
//! `Plan::explain` prints it.

use std::collections::BTreeSet;
use std::fmt::Write as _;

use crate::session::Table;
use crate::{Error, SortKey, Window, agg, expr};

fn names(names: &[String]) -> Vec<&str> {
    names.iter().map(String::as_str).collect()
}

fn pairs(pairs: &[(String, String)]) -> Vec<(&str, &str)> {
    pairs
        .iter()
        .map(|(a, b)| (a.as_str(), b.as_str()))
        .collect()
}

/// `name=spec, ...`, as `explain` shows aggregations.
fn assigned(pairs: &[(String, String)]) -> String {
    let pairs: Vec<String> = pairs.iter().map(|(a, b)| format!("{a}={b}")).collect();
    pairs.join(", ")
}

/// A table a plan reads, by the name it is written with (`ns.table` or a name the caller
/// supplies), the conditions on it alone, the columns the plan may read of it (all where
/// `None`), how many of its first rows, in the table's order, the plan needs (all where `None`;
/// only without a predicate), and whether it is read ticking (all its rows, then every change)
/// or as it is now: a store reads only the files, row groups, and columns they allow. A
/// caller's scan gives the rows that satisfy them, with at least those columns and at least
/// those first rows; a name among the columns the table lacks is no column of it.
#[derive(Debug, Clone, PartialEq)]
pub struct Scan {
    pub name: String,
    pub predicate: Option<String>,
    pub columns: Option<Vec<String>>,
    pub limit: Option<usize>,
    pub live: bool,
}

/// A query as the engine's operators, each input built before the operator over it.
#[derive(Debug, Clone, PartialEq)]
pub enum Plan {
    Scan(Scan),
    Filter {
        input: Box<Plan>,
        predicate: String,
    },
    Update {
        input: Box<Plan>,
        assignments: Vec<String>,
    },
    Select {
        input: Box<Plan>,
        columns: Vec<String>,
    },
    /// `agg_by`. `aliases`: names `GROUP BY` took as the aliases of other expressions, which
    /// must not be columns of the input: SQL groups by the column of that name instead.
    Agg {
        input: Box<Plan>,
        by: Vec<String>,
        aggregations: Vec<(String, String)>,
        aliases: Vec<String>,
    },
    Sort {
        input: Box<Plan>,
        by: Vec<SortKey>,
    },
    Head {
        input: Box<Plan>,
        rows: usize,
    },
    Tail {
        input: Box<Plan>,
        rows: usize,
    },
    LastBy {
        input: Box<Plan>,
        by: Vec<String>,
    },
    Rolling {
        input: Box<Plan>,
        by: Vec<String>,
        window: Window,
        aggregations: Vec<(String, String)>,
    },
    /// The rows at positions `start` up to `stop` (to the end if `None`), in the input's order.
    Slice {
        input: Box<Plan>,
        start: usize,
        stop: Option<usize>,
    },
    /// Every column but these: those `ORDER BY` read and the result does not show.
    Drop {
        input: Box<Plan>,
        columns: Vec<String>,
    },
    /// A `LEFT JOIN` (`at` empty, `natural_join`) or `ASOF JOIN` (`as_of_join`). It adds the
    /// right table's `columns` (`r.c=c`: its `c` as a column `r.c`, all of them if `None`), and
    /// the `bare` names the left table lacks and the right has.
    Join {
        left: Box<Plan>,
        right: Box<Plan>,
        on: Vec<String>,
        at: Option<String>,
        columns: Option<Vec<String>>,
        bare: Vec<String>,
    },
    Merge(Vec<Plan>),
}

impl Plan {
    /// The plan's table, each scan's rows from `scan`.
    pub fn build(
        &self,
        scan: &mut dyn FnMut(&Scan) -> Result<Table, Error>,
    ) -> Result<Table, Error> {
        match self {
            Plan::Scan(s) => scan(s),
            Plan::Filter { input, predicate } => input.build(scan)?.filter(predicate),
            Plan::Update { input, assignments } => input.build(scan)?.update(&names(assignments)),
            Plan::Select { input, columns } => input.build(scan)?.select(&names(columns)),
            Plan::Agg {
                input,
                by,
                aggregations,
                aliases,
            } => {
                let input = input.build(scan)?;
                if let Some(alias) = aliases
                    .iter()
                    .find(|alias| input.schema().field_with_name(alias).is_ok())
                {
                    return Err(Error::Unsupported(format!(
                        "SQL: GROUP BY {alias}: {alias} is both a column of the table and an \
                         alias in SELECT, and SQL groups by the column; name the alias apart"
                    )));
                }
                input.agg_by(&names(by), &pairs(aggregations))
            }
            Plan::Sort { input, by } => input.build(scan)?.sort_by(by),
            Plan::Head { input, rows } => input.build(scan)?.head(*rows),
            Plan::Tail { input, rows } => input.build(scan)?.tail(*rows),
            Plan::LastBy { input, by } => input.build(scan)?.last_by(&names(by)),
            Plan::Rolling {
                input,
                by,
                window,
                aggregations,
            } => input
                .build(scan)?
                .rolling(&names(by), window, &pairs(aggregations)),
            Plan::Slice { input, start, stop } => {
                input.build(scan)?.slice(*start, stop.unwrap_or(usize::MAX))
            }
            Plan::Drop { input, columns } => {
                let input = input.build(scan)?;
                let kept: Vec<String> = input
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .filter(|name| !columns.contains(name))
                    .collect();
                input.select(&names(&kept))
            }
            Plan::Join {
                left,
                right,
                on,
                at,
                columns,
                bare,
            } => {
                let (left, right) = (left.build(scan)?, right.build(scan)?);
                // Which unqualified names are the right table's needs both schemas.
                let joins = columns.as_ref().map(|columns| {
                    let mut joins = columns.clone();
                    joins.extend(
                        bare.iter()
                            .filter(|name| {
                                right.schema().field_with_name(name).is_ok()
                                    && left.schema().field_with_name(name).is_err()
                            })
                            .cloned(),
                    );
                    joins
                });
                let joins = joins.as_deref().map(names);
                match at {
                    None => left.natural_join(&right, &names(on), joins.as_deref()),
                    Some(at) => left.as_of_join(&right, &names(on), at, joins.as_deref()),
                }
            }
            Plan::Merge(parts) => {
                let tables = parts
                    .iter()
                    .map(|part| part.build(scan))
                    .collect::<Result<Vec<_>, _>>()?;
                Table::merge(&tables.iter().collect::<Vec<_>>())
            }
        }
    }

    /// The plan as a tree of operators, each marked static or ticking, its scans described by
    /// `scan` (what a store would read, and whether it ticks), without building anything.
    pub fn explain(&self, scan: &mut Describe<'_>) -> Result<String, Error> {
        let mut lines = Vec::new();
        self.explain_into(scan, 0, &mut lines)?;
        Ok(lines.join("\n"))
    }

    /// Writes this node and its inputs at `depth`; whether it ticks.
    fn explain_into(
        &self,
        scan: &mut Describe<'_>,
        depth: usize,
        lines: &mut Vec<String>,
    ) -> Result<bool, Error> {
        let mut below = Vec::new();
        let mut ticks = false;
        for input in self.inputs() {
            ticks |= input.explain_into(scan, depth + 1, &mut below)?;
        }
        let what = match self {
            Plan::Scan(s) => {
                let (described, scan_ticks) = scan(s)?;
                ticks = scan_ticks;
                let mut what = format!("scan {}", s.name);
                if let Some(predicate) = &s.predicate {
                    write!(what, " where {predicate}").ok();
                }
                if !described.is_empty() {
                    write!(what, ": {described}").ok();
                }
                what
            }
            other => other.operator(),
        };
        let path = if ticks { "ticking" } else { "static " };
        lines.push(format!("{path}  {}{what}", "  ".repeat(depth)));
        lines.extend(below);
        Ok(ticks)
    }

    /// The plan's scans, left to right.
    pub fn scans(&self) -> Vec<&Scan> {
        match self {
            Plan::Scan(scan) => vec![scan],
            other => other.inputs().into_iter().flat_map(Plan::scans).collect(),
        }
    }

    /// Replaces each scan for which `with` gives a plan by that plan.
    pub fn replace_scans(&mut self, with: &mut dyn FnMut(&Scan) -> Option<Plan>) {
        if let Plan::Scan(scan) = self {
            if let Some(plan) = with(scan) {
                *self = plan;
            }
            return;
        }
        for input in self.inputs_mut() {
            input.replace_scans(with);
        }
    }

    fn inputs(&self) -> Vec<&Plan> {
        match self {
            Plan::Scan(_) => Vec::new(),
            Plan::Filter { input, .. }
            | Plan::Update { input, .. }
            | Plan::Select { input, .. }
            | Plan::Agg { input, .. }
            | Plan::Sort { input, .. }
            | Plan::Head { input, .. }
            | Plan::Tail { input, .. }
            | Plan::LastBy { input, .. }
            | Plan::Rolling { input, .. }
            | Plan::Slice { input, .. }
            | Plan::Drop { input, .. } => vec![input],
            Plan::Join { left, right, .. } => vec![left, right],
            Plan::Merge(parts) => parts.iter().collect(),
        }
    }

    fn inputs_mut(&mut self) -> Vec<&mut Plan> {
        match self {
            Plan::Scan(_) => Vec::new(),
            Plan::Filter { input, .. }
            | Plan::Update { input, .. }
            | Plan::Select { input, .. }
            | Plan::Agg { input, .. }
            | Plan::Sort { input, .. }
            | Plan::Head { input, .. }
            | Plan::Tail { input, .. }
            | Plan::LastBy { input, .. }
            | Plan::Rolling { input, .. }
            | Plan::Slice { input, .. }
            | Plan::Drop { input, .. } => vec![input],
            Plan::Join { left, right, .. } => vec![left, right],
            Plan::Merge(parts) => parts.iter_mut().collect(),
        }
    }

    /// The plan with what its scans can do themselves moved into them, so a store reads less:
    /// filters right over a scan become its predicate; the first rows a head or slice needs of
    /// a scan without one, through operators that keep rows in place, its limit; and the columns
    /// the plan reads of each scan its columns, where it does not say already.
    pub fn pushed_down(mut self) -> Plan {
        self.push_filters();
        self.push_limits();
        self.require(None);
        self
    }

    fn push_filters(&mut self) {
        for input in self.inputs_mut() {
            input.push_filters();
        }
        let Plan::Filter { input, .. } = self else {
            return;
        };
        if !matches!(input.as_ref(), Plan::Scan(scan) if scan.limit.is_none()) {
            return;
        }
        // A stand-in while the filter is taken apart; replaced below.
        let Plan::Filter { input, predicate } = std::mem::replace(self, Plan::Merge(Vec::new()))
        else {
            unreachable!("matched above")
        };
        let Plan::Scan(mut scan) = *input else {
            unreachable!("matched above")
        };
        scan.predicate = Some(match scan.predicate.take() {
            Some(first) => format!("({first}) and ({predicate})"),
            None => predicate,
        });
        *self = Plan::Scan(scan);
    }

    fn push_limits(&mut self) {
        match self {
            Plan::Head { input, rows } => input.limit_to(*rows),
            Plan::Slice {
                input,
                stop: Some(stop),
                ..
            } => input.limit_to(*stop),
            _ => {}
        }
        for input in self.inputs_mut() {
            input.push_limits();
        }
    }

    /// Only the first `rows` of this plan's rows are needed.
    fn limit_to(&mut self, rows: usize) {
        match self {
            Plan::Scan(scan) if scan.predicate.is_none() => {
                scan.limit = Some(scan.limit.map_or(rows, |limit| limit.min(rows)));
            }
            Plan::Select { input, .. } | Plan::Update { input, .. } | Plan::Drop { input, .. } => {
                input.limit_to(rows)
            }
            _ => {}
        }
    }

    /// Says which columns each scan below reads, given those of this node's rows that are
    /// `needed` (all where `None`). A name may be no column of the scan's table; every column
    /// read is named.
    fn require(&mut self, needed: Option<BTreeSet<String>>) {
        let and = |needed: &Option<BTreeSet<String>>,
                   more: Option<Vec<String>>|
         -> Option<BTreeSet<String>> {
            match (needed, more) {
                (Some(needed), Some(more)) => Some(needed.iter().cloned().chain(more).collect()),
                _ => None,
            }
        };
        let specs = |aggregations: &[(String, String)]| -> Vec<String> {
            aggregations
                .iter()
                .filter_map(|(_, spec)| agg::reads(spec).map(str::to_owned))
                .collect()
        };
        match self {
            Plan::Scan(scan) => {
                if scan.columns.is_none() {
                    let reads = scan
                        .predicate
                        .as_deref()
                        .map_or(Some(Vec::new()), expr::reads);
                    scan.columns = and(&needed, reads).map(|columns| columns.into_iter().collect());
                }
            }
            Plan::Filter { input, predicate } => {
                input.require(and(&needed, expr::reads(predicate)))
            }
            Plan::Update { input, assignments } => {
                let mut needed = needed;
                for assignment in assignments.iter().rev() {
                    needed = match (needed, expr::assignment_reads(assignment)) {
                        (Some(mut needed), Some((name, reads))) => {
                            needed.remove(&name);
                            needed.extend(reads);
                            Some(needed)
                        }
                        _ => None,
                    };
                }
                input.require(needed);
            }
            Plan::Select { input, columns } => {
                let mut needed = BTreeSet::new();
                for column in columns.iter() {
                    // A column's name, or `name = expression`: the engine tries the name first.
                    needed.insert(column.trim().to_owned());
                    if let Some((_, reads)) = expr::assignment_reads(column) {
                        needed.extend(reads);
                    }
                }
                input.require(Some(needed));
            }
            Plan::Agg {
                input,
                by,
                aggregations,
                ..
            } => {
                let needed = by.iter().cloned().chain(specs(aggregations)).collect();
                input.require(Some(needed));
            }
            Plan::Sort { input, by } => {
                let keys = by.iter().map(|key| key.column.clone()).collect();
                input.require(and(&needed, Some(keys)));
            }
            Plan::LastBy { input, by } => input.require(and(&needed, Some(by.clone()))),
            Plan::Rolling {
                input,
                by,
                window,
                aggregations,
            } => {
                let mut reads = by.clone();
                reads.extend(specs(aggregations));
                if let Window::Time { at, .. } = window {
                    reads.push(at.clone());
                }
                // The columns rolling adds are its own, not its input's.
                let needed = needed.map(|mut needed| {
                    for (name, _) in aggregations.iter() {
                        needed.remove(name);
                    }
                    needed
                });
                input.require(and(&needed, Some(reads)));
            }
            Plan::Head { input, .. }
            | Plan::Tail { input, .. }
            | Plan::Slice { input, .. }
            | Plan::Drop { input, .. } => input.require(needed),
            Plan::Join {
                left,
                right,
                on,
                at,
                columns,
                bare,
            } => {
                // `name`, or `left=right` (`new=old` for the columns a join adds).
                let side = |entry: &String, right: bool| {
                    let (l, r) = entry.split_once('=').unwrap_or((entry, entry));
                    (if right { r } else { l }).trim().to_owned()
                };
                let keys = |right: bool| -> Vec<String> {
                    on.iter().chain(at.iter()).map(|e| side(e, right)).collect()
                };
                // The columns the join adds are the right table's, not the left's.
                let left_needed = needed.map(|mut needed| {
                    for added in columns.iter().flatten() {
                        needed.remove(&side(added, false));
                    }
                    needed
                });
                left.require(and(&left_needed, Some(keys(false))));
                let added = columns.as_ref().map(|columns| {
                    let mut added: Vec<String> = columns.iter().map(|e| side(e, true)).collect();
                    added.extend(bare.iter().cloned());
                    added.extend(keys(true));
                    added.into_iter().collect()
                });
                right.require(added);
            }
            Plan::Merge(parts) => {
                for part in parts {
                    part.require(needed.clone());
                }
            }
        }
    }

    /// This node's operator, as the Python API writes it.
    fn operator(&self) -> String {
        match self {
            Plan::Scan(s) => format!("scan {}", s.name),
            Plan::Filter { predicate, .. } => format!("where {predicate}"),
            Plan::Update { assignments, .. } => format!("update {}", assignments.join(", ")),
            Plan::Select { columns, .. } => format!("select {}", columns.join(", ")),
            Plan::Agg {
                by, aggregations, ..
            } => {
                format!("agg_by [{}]: {}", by.join(", "), assigned(aggregations))
            }
            Plan::Sort { by, .. } => {
                let by: Vec<String> = by.iter().map(SortKey::to_string).collect();
                format!("sort {}", by.join(", "))
            }
            Plan::Head { rows, .. } => format!("head {rows}"),
            Plan::Tail { rows, .. } => format!("tail {rows}"),
            Plan::LastBy { by, .. } => format!("last_by {}", by.join(", ")),
            Plan::Rolling {
                by,
                window,
                aggregations,
                ..
            } => {
                let window = match window {
                    Window::Rows(n) => format!("rows={n}"),
                    Window::Time { at, span } => format!("time={span} at {at}"),
                };
                format!(
                    "rolling [{}] {window}: {}",
                    by.join(", "),
                    assigned(aggregations)
                )
            }
            Plan::Slice { start, stop, .. } => match stop {
                Some(stop) => format!("slice {start}..{stop}"),
                None => format!("slice {start}.."),
            },
            Plan::Drop { columns, .. } => format!("drop {}", columns.join(", ")),
            Plan::Join {
                on, at, columns, ..
            } => {
                let mut what = match at {
                    None => format!("natural_join on {}", on.join(", ")),
                    Some(at) => format!("as_of_join on {} at {at}", on.join(", ")),
                };
                if let Some(columns) = columns.as_ref().filter(|c| !c.is_empty()) {
                    write!(what, " adding {}", columns.join(", ")).ok();
                }
                what
            }
            Plan::Merge(_) => "merge".to_owned(),
        }
    }
}

/// What `Plan::explain` asks of each scan: what a store would read for it, and whether it ticks.
pub type Describe<'a> = dyn FnMut(&Scan) -> Result<(String, bool), Error> + 'a;
