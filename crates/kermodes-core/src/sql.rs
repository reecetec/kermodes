//! SQL (architecture.md §9.8): a frontend onto the engine's operators, as Deephaven's SQL compiles
//! to its table operations. A query ticks when a table it reads ticks, and is static otherwise.
//!
//! `parse` binds a query to a `Plan`, a tree of the operators the Python API has, without
//! reading anything; `Plan::build` makes it tables, asking its caller for each table the query
//! names (`Scan`), and `Plan::explain` prints it. The conditions of `WHERE` on the first table
//! read alone go with its scan (`Scan::predicate`), so a store reads only the dates, files, and
//! row groups they allow; the rest filter the joined rows.
//!
//! Supported: `SELECT` (columns, `*`, expressions named with `AS`), `WHERE`, `GROUP BY` with
//! aggregates (`count`, `sum`, `avg`, `min`, `max`, `first`, `last`, `stddev`) and expressions
//! over them, `HAVING`, `ORDER BY` columns of the result, `LIMIT`, `LEFT JOIN … ON` keys (one
//! right row per key), `ASOF JOIN … MATCH_CONDITION (left >= right) ON` keys, `UNION ALL`, `WITH`,
//! and subqueries in `FROM`. Anything else is refused, naming the nearest supported form.

use std::collections::{BTreeSet, HashMap};
use std::fmt::Write as _;
use std::ops::ControlFlow;

use sqlparser::ast::{
    BinaryOperator, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr,
    Ident, JoinConstraint, JoinOperator, LimitClause, ObjectNamePart, OrderByKind, OrderBySort,
    Query, Select, SelectItem, SetExpr, SetOperator, SetQuantifier, Statement, TableFactor, Value,
    visit_expressions, visit_expressions_mut,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::Error;
use crate::session::Table;

/// A table a query reads, by the name it is written with (`ns.table`, `live.ns.table`,
/// `hist.ns.table`, or a name the caller supplies), and the conditions on it alone: a store reads
/// only the dates and files they allow. A caller's scan gives the rows that satisfy them.
#[derive(Debug, Clone, PartialEq)]
pub struct Scan {
    pub name: String,
    pub predicate: Option<String>,
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
    Agg {
        input: Box<Plan>,
        by: Vec<String>,
        aggregations: Vec<(String, String)>,
    },
    Sort {
        input: Box<Plan>,
        by: Vec<String>,
    },
    Head {
        input: Box<Plan>,
        rows: usize,
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

/// A parsed statement: its plan, and whether it was `EXPLAIN`ed.
#[derive(Debug, Clone, PartialEq)]
pub struct Parsed {
    pub plan: Plan,
    pub explain: bool,
}

/// Binds `sql`, one query or `EXPLAIN` of one, to a plan.
pub fn parse(sql: &str) -> Result<Parsed, Error> {
    let statements = Parser::parse_sql(&GenericDialect {}, sql).map_err(|e| Error::Expression {
        text: sql.trim().to_owned(),
        column: None,
        message: e.to_string(),
    })?;
    let [statement] = statements.as_slice() else {
        return Err(unsupported("one statement at a time"));
    };
    let (query, explain) = match statement {
        Statement::Query(query) => (query, false),
        Statement::Explain { statement, .. } => match statement.as_ref() {
            Statement::Query(query) => (query, true),
            _ => return Err(unsupported("EXPLAIN of a query: EXPLAIN SELECT ...")),
        },
        _ => return Err(unsupported("queries only: SELECT ...")),
    };
    let plan = Binder::default().query(query)?;
    Ok(Parsed { plan, explain })
}

fn unsupported(what: &str) -> Error {
    Error::Unsupported(format!("SQL: {what}"))
}

fn names(names: &[String]) -> Vec<&str> {
    names.iter().map(String::as_str).collect()
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
            } => {
                let aggregations: Vec<(&str, &str)> = aggregations
                    .iter()
                    .map(|(name, spec)| (name.as_str(), spec.as_str()))
                    .collect();
                input.build(scan)?.agg_by(&names(by), &aggregations)
            }
            Plan::Sort { input, by } => input.build(scan)?.sort(&names(by)),
            Plan::Head { input, rows } => input.build(scan)?.head(*rows),
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

    fn inputs(&self) -> Vec<&Plan> {
        match self {
            Plan::Scan(_) => Vec::new(),
            Plan::Filter { input, .. }
            | Plan::Update { input, .. }
            | Plan::Select { input, .. }
            | Plan::Agg { input, .. }
            | Plan::Sort { input, .. }
            | Plan::Head { input, .. } => vec![input],
            Plan::Join { left, right, .. } => vec![left, right],
            Plan::Merge(parts) => parts.iter().collect(),
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
                let aggregations: Vec<String> = aggregations
                    .iter()
                    .map(|(name, spec)| format!("{name}={spec}"))
                    .collect();
                format!("agg_by [{}]: {}", by.join(", "), aggregations.join(", "))
            }
            Plan::Sort { by, .. } => format!("sort {}", by.join(", ")),
            Plan::Head { rows, .. } => format!("head {rows}"),
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

/// Binds queries, the `WITH` names in scope.
#[derive(Default, Clone)]
struct Binder {
    ctes: HashMap<String, Plan>,
}

/// The names an aggregate goes by in SQL, and in `agg_by`.
fn aggregate(name: &str) -> Option<&'static str> {
    Some(match name.to_ascii_lowercase().as_str() {
        "count" => "count",
        "sum" => "sum",
        "avg" | "mean" => "mean",
        "min" => "min",
        "max" => "max",
        "first" => "first",
        "last" => "last",
        "stddev" | "std" => "std",
        _ => return None,
    })
}

fn function_name(function: &Function) -> String {
    function.name.to_string()
}

/// A column read through a join's right table, `r.c`, is the column `"r.c"` the join adds; any
/// other qualifier is dropped (`o.price` is `price`). `rights`: the right tables' names.
fn qualified(parts: &[Ident], rights: &[String]) -> Option<String> {
    let [.., table, column] = parts else {
        return None;
    };
    Some(if rights.contains(&table.value) {
        format!("{}.{}", table.value, column.value)
    } else {
        column.value.clone()
    })
}

/// `expr` as text, its columns named as the joined table has them (`qualified`).
fn text(expr: &Expr, rights: &[String]) -> String {
    let mut expr = expr.clone();
    let _ = visit_expressions_mut(&mut expr, |e| {
        if let Expr::CompoundIdentifier(parts) = e
            && let Some(name) = qualified(parts, rights)
        {
            *e = Expr::Identifier(if name.contains('.') {
                Ident::with_quote('"', name)
            } else {
                Ident::new(name)
            });
        }
        ControlFlow::<()>::Continue(())
    });
    expr.to_string()
}

/// The column `expr` reads, if it is one, as the joined table names it (`qualified`).
fn name(expr: &Expr, rights: &[String]) -> Option<String> {
    match expr {
        Expr::Identifier(ident) => Some(ident.value.clone()),
        Expr::CompoundIdentifier(parts) => qualified(parts, rights),
        Expr::Nested(inner) => name(inner, rights),
        _ => None,
    }
}

/// The column `expr` names, if it is one, as written without its qualifier: the name a
/// result column takes, and what join keys and `ORDER BY` name.
fn column(expr: &Expr) -> Option<&Ident> {
    match expr {
        Expr::Identifier(ident) => Some(ident),
        Expr::CompoundIdentifier(parts) => parts.last(),
        Expr::Nested(inner) => column(inner),
        _ => None,
    }
}

/// The qualifiers of the columns `expr` reads, and whether it reads any unqualified.
fn qualifiers(expr: &Expr) -> (Vec<String>, bool) {
    let (mut found, mut bare) = (Vec::new(), false);
    let _ = visit_expressions(expr, |e| {
        match e {
            Expr::Identifier(_) => bare = true,
            Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
                found.push(parts[parts.len() - 2].value.clone());
            }
            _ => {}
        }
        ControlFlow::<()>::Continue(())
    });
    (found, bare)
}

/// `expr`'s conditions joined by `AND`, each alone.
fn conjuncts(expr: &Expr) -> Vec<&Expr> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            let mut all = conjuncts(left);
            all.extend(conjuncts(right));
            all
        }
        Expr::Nested(inner)
            if matches!(
                inner.as_ref(),
                Expr::BinaryOp {
                    op: BinaryOperator::And,
                    ..
                }
            ) =>
        {
            conjuncts(inner)
        }
        other => vec![other],
    }
}

fn and(conditions: &[String]) -> Option<String> {
    match conditions {
        [] => None,
        [one] => Some(one.clone()),
        many => Some(
            many.iter()
                .map(|c| format!("({c})"))
                .collect::<Vec<_>>()
                .join(" AND "),
        ),
    }
}

impl Binder {
    fn query(&self, query: &Query) -> Result<Plan, Error> {
        let mut scope = self.clone();
        if let Some(with) = &query.with {
            if with.recursive {
                return Err(unsupported("WITH RECURSIVE"));
            }
            for cte in &with.cte_tables {
                let plan = scope.query(&cte.query)?;
                scope.ctes.insert(cte.alias.name.value.clone(), plan);
            }
        }
        if query.fetch.is_some()
            || !query.locks.is_empty()
            || query.for_clause.is_some()
            || query.settings.is_some()
            || query.format_clause.is_some()
            || !query.pipe_operators.is_empty()
        {
            return Err(unsupported(
                "this clause; a query is SELECT ... [ORDER BY ...] [LIMIT n]",
            ));
        }
        let mut plan = scope.set(&query.body)?;
        if let Some(order) = &query.order_by {
            let OrderByKind::Expressions(items) = &order.kind else {
                return Err(unsupported("ORDER BY ALL: name the columns"));
            };
            let by = items
                .iter()
                .map(|item| {
                    let Some(name) = column(&item.expr) else {
                        return Err(unsupported(&format!(
                            "ORDER BY {}: order by a column of the result, by name",
                            item.expr
                        )));
                    };
                    if item.options.nulls_first.is_some() || item.with_fill.is_some() {
                        return Err(unsupported("NULLS FIRST/LAST and WITH FILL in ORDER BY"));
                    }
                    Ok(match item.options.sort {
                        Some(OrderBySort::Desc) => format!("{} desc", name.value),
                        Some(OrderBySort::Asc) | None => name.value.clone(),
                        Some(OrderBySort::Using(_)) => {
                            return Err(unsupported("ORDER BY ... USING"));
                        }
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            plan = Plan::Sort {
                input: Box::new(plan),
                by,
            };
        }
        if let Some(limit) = &query.limit_clause {
            let LimitClause::LimitOffset {
                limit,
                offset,
                limit_by,
            } = limit
            else {
                return Err(unsupported("LIMIT offset, n: write LIMIT n"));
            };
            if offset.is_some() || !limit_by.is_empty() {
                return Err(unsupported("OFFSET and LIMIT BY"));
            }
            if let Some(limit) = limit {
                plan = Plan::Head {
                    input: Box::new(plan),
                    rows: rows(limit)?,
                };
            }
        }
        Ok(plan)
    }

    fn set(&self, body: &SetExpr) -> Result<Plan, Error> {
        match body {
            SetExpr::Select(select) => self.select(select),
            SetExpr::Query(query) => self.query(query),
            SetExpr::SetOperation {
                op: SetOperator::Union,
                set_quantifier: SetQuantifier::All,
                left,
                right,
            } => {
                let mut parts = Vec::new();
                for side in [left, right] {
                    match self.set(side)? {
                        Plan::Merge(more) => parts.extend(more),
                        one => parts.push(one),
                    }
                }
                Ok(Plan::Merge(parts))
            }
            SetExpr::SetOperation {
                op: SetOperator::Union,
                ..
            } => Err(unsupported("UNION: write UNION ALL, which keeps every row")),
            _ => Err(unsupported("this set operation: UNION ALL")),
        }
    }

    /// A table in `FROM` and the name it is known by in the query.
    fn factor(&self, factor: &TableFactor) -> Result<(Plan, String), Error> {
        match factor {
            TableFactor::Table {
                name,
                alias,
                args: None,
                version: None,
                ..
            } => {
                let parts: Vec<&str> = name
                    .0
                    .iter()
                    .map(|part| match part {
                        ObjectNamePart::Identifier(ident) => Ok(ident.value.as_str()),
                        ObjectNamePart::Function(_) => Err(unsupported("a function as a name")),
                    })
                    .collect::<Result<_, _>>()?;
                let known = alias.as_ref().map_or_else(
                    || parts[parts.len() - 1].to_owned(),
                    |a| a.name.value.clone(),
                );
                let plan = match parts.as_slice() {
                    [one] if self.ctes.contains_key(*one) => self.ctes[*one].clone(),
                    [_] | [_, _] | ["live" | "hist", _, _] => Plan::Scan(Scan {
                        name: parts.join("."),
                        predicate: None,
                    }),
                    _ => {
                        return Err(unsupported(&format!(
                            "the table name {name}: ns.table, live.ns.table, or hist.ns.table"
                        )));
                    }
                };
                Ok((plan, known))
            }
            TableFactor::Table {
                version: Some(_), ..
            } => Err(unsupported(
                "FOR SYSTEM_TIME AS OF: catalog snapshots are not readable yet",
            )),
            TableFactor::Derived {
                lateral: false,
                subquery,
                alias,
                ..
            } => {
                let known = alias
                    .as_ref()
                    .map_or_else(String::new, |a| a.name.value.clone());
                Ok((self.query(subquery)?, known))
            }
            _ => Err(unsupported(
                "this FROM: a table name, or a subquery in parentheses",
            )),
        }
    }

    fn select(&self, select: &Select) -> Result<Plan, Error> {
        if select.distinct.is_some()
            || select.top.is_some()
            || select.into.is_some()
            || !select.lateral_views.is_empty()
            || select.prewhere.is_some()
            || !select.connect_by.is_empty()
            || !select.cluster_by.is_empty()
            || !select.distribute_by.is_empty()
            || !select.sort_by.is_empty()
            || select.qualify.is_some()
            || !select.named_window.is_empty()
            || select.exclude.is_some()
            || select.value_table_mode.is_some()
        {
            return Err(unsupported(
                "this SELECT clause (DISTINCT, TOP, QUALIFY, windows...): SELECT columns FROM \
                 ... WHERE ... GROUP BY ... HAVING ...",
            ));
        }
        let from = match select.from.as_slice() {
            [one] => one,
            [] => return Err(unsupported("SELECT without FROM")),
            _ => return Err(unsupported("FROM a, b: write JOIN ... ON")),
        };
        let (mut plan, first) = self.factor(&from.relation)?;
        let mut joins = Vec::new();
        for join in &from.joins {
            let (right, name) = self.factor(&join.relation)?;
            joins.push((join, right, name));
        }
        // `*` keeps every column of every table, under its own name; otherwise a join adds only
        // the right table's columns the query reads.
        let every = select
            .projection
            .iter()
            .any(|item| matches!(item, SelectItem::Wildcard(_)));
        let rights: Vec<String> = if every {
            Vec::new()
        } else {
            joins.iter().map(|(_, _, name)| name.clone()).collect()
        };
        let mut read: HashMap<&str, BTreeSet<String>> = HashMap::new();
        let mut bare = BTreeSet::new();
        for expr in expressions(select) {
            let _ = visit_expressions(expr, |e| {
                match e {
                    Expr::Identifier(ident) => {
                        bare.insert(ident.value.clone());
                    }
                    Expr::CompoundIdentifier(parts) => {
                        if let [.., table, column] = parts.as_slice()
                            && let Some(right) = rights.iter().find(|r| **r == table.value)
                        {
                            read.entry(right).or_default().insert(column.value.clone());
                        }
                    }
                    _ => {}
                }
                ControlFlow::<()>::Continue(())
            });
        }
        let conditions: Vec<&Expr> = select.selection.as_ref().map_or_else(Vec::new, conjuncts);
        // Conditions on the first table alone go with its scan (a join keeps all its rows, so
        // only the first table's can); with no join, every condition is on it.
        let (pushed, kept): (Vec<&Expr>, Vec<&Expr>) =
            conditions.into_iter().partition(|condition| {
                let (qualifiers, bare) = qualifiers(condition);
                matches!(plan, Plan::Scan(_))
                    && (joins.is_empty() || (!bare && qualifiers.iter().all(|q| *q == first)))
            });
        if let Plan::Scan(scan) = &mut plan {
            // A `WITH` table's scan keeps its own conditions
            let mut all: Vec<String> = scan.predicate.take().into_iter().collect();
            all.extend(pushed.iter().map(|c| text(c, &rights)));
            scan.predicate = and(&all);
        }
        for (join, right, name) in joins {
            let (on, at) = match &join.join_operator {
                JoinOperator::Left(constraint) | JoinOperator::LeftOuter(constraint) => {
                    (keys(constraint, &name, true)?, None)
                }
                JoinOperator::AsOf {
                    match_condition,
                    constraint,
                } => (
                    keys(constraint, &name, false)?,
                    Some(as_of(match_condition, &name)?),
                ),
                JoinOperator::Join(_) | JoinOperator::Inner(_) => {
                    return Err(unsupported(
                        "INNER JOIN, which matches many rows to many: LEFT JOIN when each row \
                         has at most one match on its keys, or ASOF JOIN",
                    ));
                }
                _ => return Err(unsupported("this join: LEFT JOIN or ASOF JOIN")),
            };
            let columns = (!every).then(|| {
                read.get(name.as_str())
                    .into_iter()
                    .flatten()
                    .map(|c| format!("{name}.{c}={c}"))
                    .collect()
            });
            plan = Plan::Join {
                left: Box::new(plan),
                right: Box::new(right),
                on,
                at,
                columns,
                bare: if every {
                    Vec::new()
                } else {
                    bare.iter().cloned().collect()
                },
            };
        }
        if let Some(predicate) = and(&kept.iter().map(|c| text(c, &rights)).collect::<Vec<_>>()) {
            plan = Plan::Filter {
                input: Box::new(plan),
                predicate,
            };
        }
        let by: Vec<String> = match &select.group_by {
            GroupByExpr::Expressions(exprs, modifiers) if modifiers.is_empty() => exprs
                .iter()
                .map(|e| {
                    name(e, &rights).ok_or_else(|| {
                        unsupported(&format!("GROUP BY {e}: group by columns, by name"))
                    })
                })
                .collect::<Result<_, _>>()?,
            _ => return Err(unsupported("GROUP BY ALL, ROLLUP, and CUBE")),
        };
        let aggregating = !by.is_empty()
            || select.having.is_some()
            || select.projection.iter().any(|item| match item {
                SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                    has_aggregate(e)
                }
                _ => false,
            });
        if aggregating {
            aggregated(plan, select, by, &rights)
        } else {
            projected(plan, select, &rights)
        }
    }
}

/// The expressions of a `SELECT` that read columns of its `FROM`.
fn expressions(select: &Select) -> Vec<&Expr> {
    let mut all: Vec<&Expr> = select
        .projection
        .iter()
        .filter_map(|item| match item {
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => Some(e),
            _ => None,
        })
        .collect();
    all.extend(&select.selection);
    if let GroupByExpr::Expressions(exprs, _) = &select.group_by {
        all.extend(exprs);
    }
    all.extend(&select.having);
    all
}

fn rows(limit: &Expr) -> Result<usize, Error> {
    match limit {
        Expr::Value(v) => match &v.value {
            Value::Number(n, _) => n
                .parse()
                .map_err(|_| unsupported(&format!("LIMIT {n}: a whole number of rows"))),
            other => Err(unsupported(&format!("LIMIT {other}: a number of rows"))),
        },
        other => Err(unsupported(&format!("LIMIT {other}: a number of rows"))),
    }
}

/// A join's key columns as `natural_join` and `as_of_join` take them: `name`, or
/// `left=right` where the names differ. `required`: a LEFT JOIN needs keys.
fn keys(constraint: &JoinConstraint, right: &str, required: bool) -> Result<Vec<String>, Error> {
    match constraint {
        JoinConstraint::On(expr) => conjuncts(expr)
            .into_iter()
            .map(|condition| match condition {
                Expr::BinaryOp {
                    left,
                    op: BinaryOperator::Eq,
                    right: other,
                } => {
                    let (Some(a), Some(b)) = (column(left), column(other)) else {
                        return Err(unsupported(&format!(
                            "ON {condition}: a join matches equal columns, a.k = b.k"
                        )));
                    };
                    // Which side is the right table's: the one qualified by its name.
                    let right_first = matches!(left.as_ref(), Expr::CompoundIdentifier(parts)
                        if parts.len() >= 2 && parts[parts.len() - 2].value == right);
                    let (l, r) = if right_first { (b, a) } else { (a, b) };
                    Ok(if l.value == r.value {
                        l.value.clone()
                    } else {
                        format!("{}={}", l.value, r.value)
                    })
                }
                _ => Err(unsupported(&format!(
                    "ON {condition}: a join matches equal columns, a.k = b.k"
                ))),
            })
            .collect(),
        JoinConstraint::Using(names) => Ok(names.iter().map(|n| n.to_string()).collect()),
        JoinConstraint::None if !required => Ok(Vec::new()),
        _ => Err(unsupported("a LEFT JOIN joins on keys: ON a.k = b.k")),
    }
}

/// An ASOF JOIN's time columns, as `as_of_join` takes them: the right row is the latest at or
/// before the left row's time (`left.ts >= right.ts`).
fn as_of(condition: &Expr, right: &str) -> Result<String, Error> {
    let refused = || {
        unsupported(&format!(
            "MATCH_CONDITION ({condition}): the latest right row at or before the left row's \
             time, left.ts >= right.ts"
        ))
    };
    let Expr::BinaryOp {
        left,
        op,
        right: other,
    } = condition
    else {
        return Err(refused());
    };
    let (Some(a), Some(b)) = (column(left), column(other)) else {
        return Err(refused());
    };
    let a_right = matches!(left.as_ref(), Expr::CompoundIdentifier(parts)
        if parts.len() >= 2 && parts[parts.len() - 2].value == right);
    // `left >= right`, or the same written the other way round
    let (l, r) = match (op, a_right) {
        (BinaryOperator::GtEq, false) => (a, b),
        (BinaryOperator::LtEq, true) => (b, a),
        _ => return Err(refused()),
    };
    Ok(if l.value == r.value {
        l.value.clone()
    } else {
        format!("{}={}", l.value, r.value)
    })
}

fn has_aggregate(expr: &Expr) -> bool {
    visit_expressions(expr, |e| match e {
        Expr::Function(f) if aggregate(&function_name(f)).is_some() => ControlFlow::Break(()),
        _ => ControlFlow::Continue(()),
    })
    .is_break()
}

/// The aggregates of a grouped query, computed under names of their own (`__a0`...).
struct Aggregates<'a> {
    rights: &'a [String],
    /// Each aggregate's column and `agg_by` spec.
    aggregations: Vec<(String, String)>,
    /// Columns computed before grouping, for aggregates over expressions.
    before: Vec<String>,
}

impl Aggregates<'_> {
    /// `expr` with each aggregate replaced by the column holding it.
    fn rewrite(&mut self, expr: &Expr) -> Result<Expr, Error> {
        let mut expr = expr.clone();
        let mut failed = None;
        let _ = visit_expressions_mut(&mut expr, |e| {
            if let Expr::Function(function) = e
                && let Some(kind) = aggregate(&function_name(function))
            {
                match self.column(function, kind) {
                    Ok(name) => *e = Expr::Identifier(Ident::new(name)),
                    Err(error) => {
                        failed = Some(error);
                        return ControlFlow::Break(());
                    }
                }
            }
            ControlFlow::Continue(())
        });
        match failed {
            Some(error) => Err(error),
            None => Ok(expr),
        }
    }

    /// The column holding `function`, an aggregate of `kind`, added if new.
    fn column(&mut self, function: &Function, kind: &str) -> Result<String, Error> {
        if function.over.is_some() || function.filter.is_some() || !function.within_group.is_empty()
        {
            return Err(unsupported(&format!(
                "{function}: window functions and FILTER are not supported yet"
            )));
        }
        let FunctionArguments::List(list) = &function.args else {
            return Err(unsupported(&format!(
                "{function}: an aggregate of one column or *"
            )));
        };
        if list.duplicate_treatment.is_some() || !list.clauses.is_empty() {
            return Err(unsupported(&format!(
                "{function}: DISTINCT inside an aggregate"
            )));
        }
        let argument = match list.args.as_slice() {
            [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)] if kind == "count" => "*".to_owned(),
            [FunctionArg::Unnamed(FunctionArgExpr::Expr(arg))] => match name(arg, self.rights) {
                Some(column) => column,
                None => {
                    let column = format!("__e{}", self.before.len());
                    self.before
                        .push(format!("{column} = {}", text(arg, self.rights)));
                    column
                }
            },
            _ => {
                return Err(unsupported(&format!(
                    "{function}: an aggregate of one column or *"
                )));
            }
        };
        let spec = format!("{kind}({argument})");
        if let Some((column, _)) = self.aggregations.iter().find(|(_, s)| *s == spec) {
            return Ok(column.clone());
        }
        let column = format!("__a{}", self.aggregations.len());
        self.aggregations.push((column.clone(), spec));
        Ok(column)
    }
}

/// A grouped query over `plan`: grouped by `by`, then each output computed from the groups'
/// columns and aggregates, then `HAVING`, then only the outputs kept.
fn aggregated(
    mut plan: Plan,
    select: &Select,
    by: Vec<String>,
    rights: &[String],
) -> Result<Plan, Error> {
    let mut aggregates = Aggregates {
        rights,
        aggregations: Vec::new(),
        before: Vec::new(),
    };
    let (mut names, mut assignments) = (Vec::<String>::new(), Vec::<String>::new());
    for item in &select.projection {
        let (expr, alias) = match item {
            SelectItem::UnnamedExpr(e) => (e, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias)),
            _ => {
                return Err(unsupported(
                    "SELECT * with GROUP BY: name the grouped columns and aggregates",
                ));
            }
        };
        let rewritten = aggregates.rewrite(expr)?;
        // Only grouped columns and aggregates remain once grouped.
        let mut stray = None;
        let _ = visit_expressions(&rewritten, |e| {
            if matches!(e, Expr::Identifier(_) | Expr::CompoundIdentifier(_))
                && let Some(column) = name(e, rights)
                && !column.starts_with("__a")
                && !by.contains(&column)
            {
                stray = Some(column);
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        });
        if let Some(stray) = stray {
            return Err(unsupported(&format!(
                "{stray} is neither grouped by nor inside an aggregate"
            )));
        }
        let output = match (alias, column(expr), expr) {
            (Some(alias), _, _) => alias.value.clone(),
            (None, Some(c), _) => c.value.clone(),
            (None, None, Expr::Function(f)) if aggregate(&function_name(f)).is_some() => {
                function_name(f).to_ascii_lowercase()
            }
            (None, None, _) => {
                return Err(unsupported(&format!("name the expression: {expr} AS name")));
            }
        };
        if names.contains(&output) {
            return Err(unsupported(&format!(
                "two columns named {output}: name them apart with AS"
            )));
        }
        if name(&rewritten, rights).as_ref() != Some(&output) {
            let computed = text(&rewritten, rights);
            assignments.push(format!("{} = {computed}", Ident::new(&output)));
        }
        names.push(output);
    }
    let having = select
        .having
        .as_ref()
        .map(|h| aggregates.rewrite(h).map(|e| text(&e, rights)))
        .transpose()?;
    if !aggregates.before.is_empty() {
        plan = Plan::Update {
            input: Box::new(plan),
            assignments: aggregates.before,
        };
    }
    plan = Plan::Agg {
        input: Box::new(plan),
        by,
        aggregations: aggregates.aggregations,
    };
    if !assignments.is_empty() {
        plan = Plan::Update {
            input: Box::new(plan),
            assignments,
        };
    }
    if let Some(predicate) = having {
        plan = Plan::Filter {
            input: Box::new(plan),
            predicate,
        };
    }
    Ok(Plan::Select {
        input: Box::new(plan),
        columns: names,
    })
}

/// An ungrouped query over `plan`: its expressions computed, then its columns kept (all of
/// them with `*`).
fn projected(mut plan: Plan, select: &Select, rights: &[String]) -> Result<Plan, Error> {
    let (mut names, mut assignments, mut every) = (Vec::new(), Vec::new(), false);
    for item in &select.projection {
        let (expr, output) = match item {
            SelectItem::Wildcard(options) => {
                if options.opt_exclude.is_some()
                    || options.opt_except.is_some()
                    || options.opt_replace.is_some()
                    || options.opt_rename.is_some()
                    || options.opt_ilike.is_some()
                {
                    return Err(unsupported("SELECT * with EXCLUDE, REPLACE, or RENAME"));
                }
                every = true;
                continue;
            }
            SelectItem::UnnamedExpr(expr) => match column(expr) {
                Some(c) => (expr, c.clone()),
                None => {
                    return Err(unsupported(&format!("name the expression: {expr} AS name")));
                }
            },
            SelectItem::ExprWithAlias { expr, alias } => (expr, alias.clone()),
            _ => return Err(unsupported("t.*: write * or name the columns")),
        };
        if name(expr, rights).as_ref() != Some(&output.value) {
            assignments.push(format!("{output} = {}", text(expr, rights)));
        }
        names.push(output.value);
    }
    if !assignments.is_empty() {
        plan = Plan::Update {
            input: Box::new(plan),
            assignments,
        };
    }
    if !every {
        plan = Plan::Select {
            input: Box::new(plan),
            columns: names,
        };
    }
    Ok(plan)
}
