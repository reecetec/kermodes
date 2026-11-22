//! SQL (architecture.md §9.8): a frontend onto the engine's operators, as Deephaven's SQL compiles
//! to its table operations. A query ticks when a table it reads ticks, and is static otherwise.
//!
//! `parse` binds a query to a `Plan`, a tree of the operators the Python API has, without
//! reading anything; `Plan::build` makes it tables, asking its caller for each table the query
//! names (`Scan`), and `Plan::explain` prints it. The conditions of `WHERE` on the first table
//! read alone go with its scan (`Scan::predicate`), so a store reads only the dates, files, and
//! row groups they allow; the rest filter the joined rows.
//!
//! Supported, with DuckDB's meaning: `SELECT` (columns, `*`, expressions; one not named with
//! `AS` takes the name DuckDB gives it, as `count_star()` or `sum((a + 1))`), `WHERE`, `GROUP
//! BY` columns, expressions, aliases, and positions, with aggregates (`count`, `count(DISTINCT
//! …)`, `sum`, `avg`, `min`, `max`, `first`, `last`, `stddev`) and expressions over them,
//! `HAVING`, `ORDER BY` result columns (by name, position, or expression) and other expressions,
//! `LIMIT` and `OFFSET`, `LEFT JOIN … ON` keys (one right row per key), `ASOF JOIN …
//! MATCH_CONDITION (left >= right) ON` keys, `UNION ALL`, `WITH`, and subqueries in `FROM`.
//! Anything else is refused, naming the nearest supported form.

use std::collections::{BTreeSet, HashMap};
use std::fmt::Write as _;
use std::ops::ControlFlow;

use sqlparser::ast::{
    BinaryOperator, DuplicateTreatment, Expr, Function, FunctionArg, FunctionArgExpr,
    FunctionArguments, GroupByExpr, Ident, InTarget, JoinConstraint, JoinOperator, LimitClause,
    ObjectNamePart, OrderByExpr, OrderByKind, OrderBySort, Query, Select, SelectItem, SetExpr,
    SetOperator, SetQuantifier, Statement, TableFactor, UnaryOperator, Value, VisitMut, VisitorMut,
    visit_expressions, visit_expressions_mut,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::plan::{Plan, Scan};
use crate::{Error, SortKey};

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
    let (plan, _) = Binder::default().query(query)?;
    Ok(Parsed { plan, explain })
}

fn unsupported(what: &str) -> Error {
    Error::Unsupported(format!("SQL: {what}"))
}

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

/// The name a result column takes when `SELECT` gives it none: its expression as DuckDB prints
/// it (function names lower case, each operation in parentheses, `count(*)` as `count_star()`,
/// `<>` as `!=`, `LIKE` as `~~`), so results name their columns as DuckDB's do.
fn duck_name(expr: &Expr) -> String {
    printed(expr, &[])
}

/// `expr` as `duck_name` prints it, its columns named as the joined table has them
/// (`qualified`, with `rights`): two expressions print alike exactly when SQL takes them for the
/// same, which is how `GROUP BY` and `ORDER BY` find the result columns they name.
fn printed(expr: &Expr, rights: &[String]) -> String {
    let all = |exprs: &[Expr]| {
        exprs
            .iter()
            .map(|e| printed(e, rights))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let not = |negated: bool| if negated { "NOT " } else { "" };
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => name(expr, rights).unwrap_or_default(),
        Expr::Nested(inner) => printed(inner, rights),
        Expr::Value(value) => match &value.value {
            Value::SingleQuotedString(s) => format!("'{}'", s.replace('\'', "''")),
            Value::Boolean(b) => b.to_string(),
            Value::Null => "NULL".to_owned(),
            other => other.to_string(),
        },
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => match expr.as_ref() {
            Expr::Value(v) if matches!(v.value, Value::Number(..)) => format!("-{}", v.value),
            other => format!("(-({}))", printed(other, rights)),
        },
        Expr::UnaryOp { op, expr } => format!("({op} {})", printed(expr, rights)),
        Expr::BinaryOp { left, op, right } => {
            let op = match op {
                BinaryOperator::NotEq => "!=".to_owned(),
                other => other.to_string(),
            };
            format!(
                "({} {op} {})",
                printed(left, rights),
                printed(right, rights)
            )
        }
        Expr::Like {
            negated,
            expr,
            pattern,
            ..
        } => {
            let op = if *negated { "!~~" } else { "~~" };
            format!(
                "({} {op} {})",
                printed(expr, rights),
                printed(pattern, rights)
            )
        }
        Expr::ILike {
            negated,
            expr,
            pattern,
            ..
        } => {
            let op = if *negated { "!~~*" } else { "~~*" };
            format!(
                "({} {op} {})",
                printed(expr, rights),
                printed(pattern, rights)
            )
        }
        Expr::IsNull(e) => format!("({} IS NULL)", printed(e, rights)),
        Expr::IsNotNull(e) => format!("({} IS NOT NULL)", printed(e, rights)),
        Expr::In {
            expr,
            target: InTarget::List(list),
            negated,
            ..
        } => format!(
            "({} {}IN ({}))",
            printed(expr, rights),
            not(*negated),
            all(list)
        ),
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => format!(
            "({} {}BETWEEN {} AND {})",
            printed(expr, rights),
            not(*negated),
            printed(low, rights),
            printed(high, rights)
        ),
        Expr::Extract { field, expr, .. } => format!(
            "date_part('{}', {})",
            field.to_string().to_ascii_uppercase(),
            printed(expr, rights)
        ),
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            // DuckDB prints a simple CASE as the searched one it means.
            let mut out = "CASE ".to_owned();
            for when in conditions {
                let condition = match operand {
                    Some(operand) => format!(
                        "({} = {})",
                        printed(operand, rights),
                        printed(&when.condition, rights)
                    ),
                    None => printed(&when.condition, rights),
                };
                let result = printed(&when.result, rights);
                write!(out, " WHEN ({condition}) THEN ({result})").ok();
            }
            let otherwise = else_result
                .as_deref()
                .map_or_else(|| "NULL".to_owned(), |e| printed(e, rights));
            write!(out, " ELSE {otherwise} END").ok();
            out
        }
        Expr::Function(function) => {
            let name = function_name(function).to_ascii_lowercase();
            let FunctionArguments::List(list) = &function.args else {
                return format!("{name}()");
            };
            let args: Vec<String> = list
                .args
                .iter()
                .map(|arg| match arg {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => printed(e, rights),
                    other => other.to_string(),
                })
                .collect();
            if name == "count"
                && matches!(
                    list.args.as_slice(),
                    [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)]
                )
            {
                return "count_star()".to_owned();
            }
            let distinct = if matches!(list.duplicate_treatment, Some(DuplicateTreatment::Distinct))
            {
                "DISTINCT "
            } else {
                ""
            };
            // Keywords in DuckDB's grammar, so it quotes them.
            let shown = if matches!(name.as_str(), "first" | "last") {
                format!("\"{name}\"")
            } else {
                name
            };
            format!("{shown}({distinct}{})", args.join(", "))
        }
        other => other.to_string(),
    }
}

/// `name` as an identifier in an engine expression: quoted unless it is a plain word.
fn ident(name: &str) -> Ident {
    let plain = name
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    if plain {
        Ident::new(name)
    } else {
        Ident::with_quote('"', name)
    }
}

/// Replaces each part of an expression that prints as a group key (`printed`) by the key's
/// column, outermost first: a key inside a larger key is the larger one.
struct Keys<'a> {
    keys: &'a [(String, String)],
    rights: &'a [String],
}

impl VisitorMut for Keys<'_> {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
        let shown = printed(expr, self.rights);
        if let Some((_, column)) = self.keys.iter().find(|(key, _)| *key == shown) {
            *expr = Expr::Identifier(ident(column));
        }
        ControlFlow::Continue(())
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
    /// The query's operators, and its result's columns in order (`None` after `SELECT *`).
    fn query(&self, query: &Query) -> Result<(Plan, Option<Vec<String>>), Error> {
        let mut scope = self.clone();
        if let Some(with) = &query.with {
            if with.recursive {
                return Err(unsupported("WITH RECURSIVE"));
            }
            for cte in &with.cte_tables {
                let (plan, _) = scope.query(&cte.query)?;
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
                "this clause; a query is SELECT ... [ORDER BY ...] [LIMIT n [OFFSET m]]",
            ));
        }
        let order: &[OrderByExpr] = match &query.order_by {
            None => &[],
            Some(order) => match &order.kind {
                OrderByKind::Expressions(items) => items,
                OrderByKind::All(_) => return Err(unsupported("ORDER BY ALL: name the columns")),
            },
        };
        for item in order {
            if item.options.nulls_first.is_some() || item.with_fill.is_some() {
                return Err(unsupported("NULLS FIRST/LAST and WITH FILL in ORDER BY"));
            }
            if matches!(item.options.sort, Some(OrderBySort::Using(_))) {
                return Err(unsupported("ORDER BY ... USING"));
            }
        }
        let Selected {
            mut plan,
            outputs,
            sort,
            hidden,
        } = match query.body.as_ref() {
            SetExpr::Select(select) => scope.select(select, order)?,
            body => {
                let (plan, outputs) = scope.set(body)?;
                let sort = order
                    .iter()
                    .map(|item| match output_named(&item.expr, outputs.as_deref())? {
                        Some(column) => Ok(sort_key(column, item)),
                        None => Err(unsupported(&format!(
                            "ORDER BY {}: order a UNION by its columns, by name or position",
                            item.expr
                        ))),
                    })
                    .collect::<Result<_, _>>()?;
                Selected {
                    plan,
                    outputs,
                    sort,
                    hidden: Vec::new(),
                }
            }
        };
        let (offset, limit) = window(query.limit_clause.as_ref())?;
        let sorted = !sort.is_empty();
        if sorted {
            plan = Plan::Sort {
                input: Box::new(plan),
                by: sort,
            };
        }
        // Rows by position follow a sort's order, which a ticking sort keeps only in how its
        // rows are read: a slice reads them so, where `head` would take the first by key.
        if offset > 0 || (sorted && (limit.is_some() || !hidden.is_empty())) {
            plan = Plan::Slice {
                input: Box::new(plan),
                start: offset,
                stop: limit.map(|rows| offset + rows),
            };
        } else if let Some(rows) = limit {
            plan = Plan::Head {
                input: Box::new(plan),
                rows,
            };
        }
        if !hidden.is_empty() {
            plan = Plan::Drop {
                input: Box::new(plan),
                columns: hidden,
            };
        }
        Ok((plan, outputs))
    }

    fn set(&self, body: &SetExpr) -> Result<(Plan, Option<Vec<String>>), Error> {
        match body {
            SetExpr::Select(select) => {
                let selected = self.select(select, &[])?;
                Ok((selected.plan, selected.outputs))
            }
            SetExpr::Query(query) => self.query(query),
            SetExpr::SetOperation {
                op: SetOperator::Union,
                set_quantifier: SetQuantifier::All,
                left,
                right,
            } => {
                let (mut parts, mut columns) = (Vec::new(), None);
                for side in [left, right] {
                    let (plan, outputs) = self.set(side)?;
                    // A union's columns are named by its first part, as in SQL.
                    columns = columns.or(Some(outputs));
                    match plan {
                        Plan::Merge(more) => parts.extend(more),
                        one => parts.push(one),
                    }
                }
                Ok((Plan::Merge(parts), columns.flatten()))
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
                    [_] | [_, _] => Plan::Scan(Scan {
                        name: parts.join("."),
                        predicate: None,
                        columns: None,
                        limit: None,
                        live: false,
                    }),
                    _ => {
                        return Err(unsupported(&format!(
                            "the table name {name}: ns.table, or a table passed by name"
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
                Ok((self.query(subquery)?.0, known))
            }
            _ => Err(unsupported(
                "this FROM: a table name, or a subquery in parentheses",
            )),
        }
    }

    /// `select` bound for `order`, its query's `ORDER BY`.
    fn select(&self, select: &Select, order: &[OrderByExpr]) -> Result<Selected, Error> {
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
        for expr in expressions(select, order) {
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
        let group: &[Expr] = match &select.group_by {
            GroupByExpr::Expressions(exprs, modifiers) if modifiers.is_empty() => exprs,
            _ => return Err(unsupported("GROUP BY ALL, ROLLUP, and CUBE")),
        };
        let aggregating = !group.is_empty()
            || select.having.is_some()
            || select.projection.iter().any(|item| match item {
                SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                    has_aggregate(e)
                }
                _ => false,
            })
            || order.iter().any(|item| has_aggregate(&item.expr));
        if aggregating {
            aggregated(plan, select, group, &rights, order)
        } else {
            projected(plan, select, &rights, order)
        }
    }
}

/// A `SELECT` bound for its query's `ORDER BY`: its operators up to its result's columns and
/// those `ORDER BY` reads that the result does not show (`hidden`), the result's columns
/// (`None` after `SELECT *`), and the sort.
struct Selected {
    plan: Plan,
    outputs: Option<Vec<String>>,
    sort: Vec<SortKey>,
    hidden: Vec<String>,
}

fn sort_key(column: String, item: &OrderByExpr) -> SortKey {
    SortKey {
        column,
        descending: matches!(item.options.sort, Some(OrderBySort::Desc)),
    }
}

/// The result column `expr` in `ORDER BY` names, if it names one: by its position (from 1), or
/// by its name (a name the result has, whatever the tables have, as in SQL). `outputs`: the
/// result's columns, `None` after `SELECT *`, where any name may be one.
fn output_named(expr: &Expr, outputs: Option<&[String]>) -> Result<Option<String>, Error> {
    if let Expr::Value(value) = expr
        && let Value::Number(n, _) = &value.value
    {
        let Some(outputs) = outputs else {
            return Err(unsupported(&format!(
                "ORDER BY {n} after SELECT *: order by a column's name"
            )));
        };
        return match n.parse::<usize>() {
            Ok(at) if (1..=outputs.len()).contains(&at) => Ok(Some(outputs[at - 1].clone())),
            _ => Err(unsupported(&format!(
                "ORDER BY {n}: the result has columns 1 to {}",
                outputs.len()
            ))),
        };
    }
    Ok(match (expr, outputs) {
        (Expr::Identifier(ident), None) => Some(ident.value.clone()),
        (Expr::Identifier(ident), Some(outputs)) => outputs
            .iter()
            .find(|output| **output == ident.value)
            .cloned(),
        _ => None,
    })
}

/// `LIMIT` and `OFFSET`: the rows skipped, and how many after them are kept (all if `None`).
fn window(clause: Option<&LimitClause>) -> Result<(usize, Option<usize>), Error> {
    let Some(clause) = clause else {
        return Ok((0, None));
    };
    let LimitClause::LimitOffset {
        limit,
        offset,
        limit_by,
    } = clause
    else {
        return Err(unsupported("LIMIT offset, n: write LIMIT n OFFSET offset"));
    };
    if !limit_by.is_empty() {
        return Err(unsupported("LIMIT BY"));
    }
    let offset = offset.as_ref().map(|o| rows(&o.value)).transpose()?;
    Ok((offset.unwrap_or(0), limit.as_ref().map(rows).transpose()?))
}

/// Each of a `SELECT`'s result columns: its expression and its name, given or DuckDB's
/// (`duck_name`); `None` for `*`, which keeps every column.
fn items(select: &Select) -> Result<Vec<Option<(&Expr, String)>>, Error> {
    select
        .projection
        .iter()
        .map(|item| match item {
            SelectItem::UnnamedExpr(expr) => Ok(Some((expr, duck_name(expr)))),
            SelectItem::ExprWithAlias { expr, alias } => Ok(Some((expr, alias.value.clone()))),
            SelectItem::Wildcard(options) => {
                if options.opt_exclude.is_some()
                    || options.opt_except.is_some()
                    || options.opt_replace.is_some()
                    || options.opt_rename.is_some()
                    || options.opt_ilike.is_some()
                {
                    return Err(unsupported("SELECT * with EXCLUDE, REPLACE, or RENAME"));
                }
                Ok(None)
            }
            _ => Err(unsupported("t.*: write * or name the columns")),
        })
        .collect()
}

/// Whether `name` is already among the result's columns: SQL allows two of one name, a table
/// does not.
fn named_once(names: &[String], name: &str) -> Result<(), Error> {
    if names.iter().any(|n| n == name) {
        return Err(unsupported(&format!(
            "two columns named {name}: name them apart with AS"
        )));
    }
    Ok(())
}

/// The expressions of a `SELECT`, and of its query's `ORDER BY`, that read columns of its `FROM`.
fn expressions<'a>(select: &'a Select, order: &'a [OrderByExpr]) -> Vec<&'a Expr> {
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
    all.extend(order.iter().map(|item| &item.expr));
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
        let distinct = matches!(list.duplicate_treatment, Some(DuplicateTreatment::Distinct));
        if (distinct && kind != "count") || !list.clauses.is_empty() {
            return Err(unsupported(&format!(
                "{function}: DISTINCT inside an aggregate other than count"
            )));
        }
        let argument = match list.args.as_slice() {
            [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)] if kind == "count" && !distinct => {
                "*".to_owned()
            }
            [FunctionArg::Unnamed(FunctionArgExpr::Expr(arg))] => match name(arg, self.rights) {
                Some(column) => column,
                // An expression computed once however many aggregates read it.
                None => {
                    let computed = text(arg, self.rights);
                    let already = self.before.iter().find_map(|assignment| {
                        let (column, expression) = assignment.split_once(" = ")?;
                        (expression == computed).then(|| column.to_owned())
                    });
                    already.unwrap_or_else(|| {
                        let column = format!("__e{}", self.before.len());
                        self.before.push(format!("{column} = {computed}"));
                        column
                    })
                }
            },
            _ => {
                return Err(unsupported(&format!(
                    "{function}: an aggregate of one column or *"
                )));
            }
        };
        let kind = if distinct { "count_distinct" } else { kind };
        let spec = format!("{kind}({argument})");
        if let Some((column, _)) = self.aggregations.iter().find(|(_, s)| *s == spec) {
            return Ok(column.clone());
        }
        let column = format!("__a{}", self.aggregations.len());
        self.aggregations.push((column.clone(), spec));
        Ok(column)
    }
}

/// A grouped query over `plan`, as SQL groups (DuckDB the oracle). Each `GROUP BY` item is a
/// result column's position, the alias of a result expression, or an expression; a key that is
/// not a column is computed before grouping. Each result column, `HAVING`, and each `ORDER BY`
/// item that names no result column is then computed from the keys and aggregates: a part that
/// is a key reads the key's column.
fn aggregated(
    mut plan: Plan,
    select: &Select,
    group: &[Expr],
    rights: &[String],
    order: &[OrderByExpr],
) -> Result<Selected, Error> {
    let items: Vec<(&Expr, String)> = items(select)?
        .into_iter()
        .map(|item| {
            item.ok_or_else(|| {
                unsupported("SELECT * with GROUP BY: name the grouped columns and aggregates")
            })
        })
        .collect::<Result<_, _>>()?;
    // Each key as it prints, and the column holding it.
    let mut keys: Vec<(String, String)> = Vec::new();
    let (mut before, mut aliases) = (Vec::new(), Vec::new());
    for (i, expr) in group.iter().enumerate() {
        let expr = if let Expr::Value(value) = expr
            && let Value::Number(n, _) = &value.value
        {
            match n.parse::<usize>() {
                Ok(at) if (1..=items.len()).contains(&at) => items[at - 1].0,
                _ => {
                    return Err(unsupported(&format!(
                        "GROUP BY {n}: the result has columns 1 to {}",
                        items.len()
                    )));
                }
            }
        } else if let Expr::Identifier(id) = expr
            && let Some((aliased, _)) = items.iter().find(|(e, alias)| {
                *alias == id.value && column(e).is_none_or(|c| c.value != id.value)
            })
        {
            aliases.push(id.value.clone());
            aliased
        } else {
            expr
        };
        if has_aggregate(expr) {
            return Err(unsupported(&format!(
                "GROUP BY {expr}: an aggregate is computed from the groups, not grouped by"
            )));
        }
        let shown = printed(expr, rights);
        if keys.iter().any(|(key, _)| *key == shown) {
            continue;
        }
        let column = match name(expr, rights) {
            Some(column) => column,
            None => {
                let column = format!("__g{i}");
                before.push(format!("{column} = {}", text(expr, rights)));
                column
            }
        };
        keys.push((shown, column));
    }
    let mut aggregates = Aggregates {
        rights,
        aggregations: Vec::new(),
        before: Vec::new(),
    };
    let mut names = Vec::new();
    let mut results = Vec::new();
    for (expr, output) in &items {
        named_once(&names, output)?;
        results.push((output, grouped(expr, &mut aggregates, &keys, rights)?));
        names.push(output.clone());
    }
    let having = select
        .having
        .as_ref()
        .map(|h| grouped(h, &mut aggregates, &keys, rights).map(|e| text(&e, rights)))
        .transpose()?;
    let (mut assignments, mut hidden, mut sort) = (Vec::new(), Vec::new(), Vec::new());
    for (i, item) in order.iter().enumerate() {
        let column = match ordered(&item.expr, &items, &names, rights)? {
            Some(column) => column,
            None => {
                let computed = grouped(&item.expr, &mut aggregates, &keys, rights)?;
                let column = format!("__o{i}");
                assignments.push(format!("{column} = {}", text(&computed, rights)));
                hidden.push(column.clone());
                column
            }
        };
        sort.push(sort_key(column, item));
    }
    // After the hidden columns, which may read a key a result column of its name replaces.
    for (output, computed) in results {
        if name(&computed, rights).as_ref() != Some(output) {
            assignments.push(format!("{} = {}", ident(output), text(&computed, rights)));
        }
    }
    before.extend(aggregates.before);
    if !before.is_empty() {
        plan = Plan::Update {
            input: Box::new(plan),
            assignments: before,
        };
    }
    plan = Plan::Agg {
        input: Box::new(plan),
        by: keys.into_iter().map(|(_, column)| column).collect(),
        aggregations: aggregates.aggregations,
        aliases,
    };
    // Before the result's columns, which may replace a key `HAVING` reads.
    if let Some(predicate) = having {
        plan = Plan::Filter {
            input: Box::new(plan),
            predicate,
        };
    }
    if !assignments.is_empty() {
        plan = Plan::Update {
            input: Box::new(plan),
            assignments,
        };
    }
    let mut columns = names.clone();
    columns.extend(hidden.iter().cloned());
    Ok(Selected {
        plan: Plan::Select {
            input: Box::new(plan),
            columns,
        },
        outputs: Some(names),
        sort,
        hidden,
    })
}

/// `expr` as computed from the groups: its aggregates in their columns (`Aggregates`), then
/// each part that is a group key in the key's column. Any other column is refused: it is
/// neither grouped by nor inside an aggregate.
fn grouped(
    expr: &Expr,
    aggregates: &mut Aggregates,
    keys: &[(String, String)],
    rights: &[String],
) -> Result<Expr, Error> {
    let mut rewritten = aggregates.rewrite(expr)?;
    let _ = rewritten.visit(&mut Keys { keys, rights });
    let mut stray = None;
    let _ = visit_expressions(&rewritten, |e| {
        if matches!(e, Expr::Identifier(_) | Expr::CompoundIdentifier(_))
            && let Some(column) = name(e, rights)
            && !column.starts_with("__a")
            && !keys.iter().any(|(_, key)| *key == column)
        {
            stray = Some(column);
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    });
    match stray {
        Some(stray) => Err(unsupported(&format!(
            "{stray} is neither grouped by nor inside an aggregate"
        ))),
        None => Ok(rewritten),
    }
}

/// The result column an `ORDER BY` item names: by position or name (`output_named`), or by
/// being the same expression as a result column's.
fn ordered(
    expr: &Expr,
    items: &[(&Expr, String)],
    names: &[String],
    rights: &[String],
) -> Result<Option<String>, Error> {
    if let Some(column) = output_named(expr, Some(names))? {
        return Ok(Some(column));
    }
    let shown = printed(expr, rights);
    Ok(items
        .iter()
        .find(|(e, _)| printed(e, rights) == shown)
        .map(|(_, name)| name.clone()))
}

/// An ungrouped query over `plan`: its expressions computed, then its columns kept (all of
/// them with `*`), with those `ORDER BY` reads that the result does not show.
fn projected(
    mut plan: Plan,
    select: &Select,
    rights: &[String],
    order: &[OrderByExpr],
) -> Result<Selected, Error> {
    let items = items(select)?;
    let every = items.iter().any(Option::is_none);
    let items: Vec<(&Expr, String)> = items.into_iter().flatten().collect();
    let mut names = Vec::new();
    for (_, output) in &items {
        named_once(&names, output)?;
        names.push(output.clone());
    }
    let (mut assignments, mut hidden, mut sort) = (Vec::new(), Vec::new(), Vec::new());
    for (i, item) in order.iter().enumerate() {
        let named = match output_named(&item.expr, (!every).then_some(names.as_slice()))? {
            Some(column) => Some(column),
            None => ordered(&item.expr, &items, &names, rights)?,
        };
        let column = match (named, name(&item.expr, rights)) {
            (Some(column), _) => column,
            // A column of the table the result does not show: kept until sorted.
            (None, Some(column)) => {
                if !every && !hidden.contains(&column) {
                    hidden.push(column.clone());
                }
                column
            }
            (None, None) => {
                let column = format!("__o{i}");
                assignments.push(format!("{column} = {}", text(&item.expr, rights)));
                hidden.push(column.clone());
                column
            }
        };
        sort.push(sort_key(column, item));
    }
    // After the hidden columns, which may read a column a result column of its name replaces.
    for (expr, output) in &items {
        if name(expr, rights).as_ref() != Some(output) {
            assignments.push(format!("{} = {}", ident(output), text(expr, rights)));
        }
    }
    if !assignments.is_empty() {
        plan = Plan::Update {
            input: Box::new(plan),
            assignments,
        };
    }
    if !every {
        let mut columns = names.clone();
        columns.extend(hidden.iter().cloned());
        plan = Plan::Select {
            input: Box::new(plan),
            columns,
        };
    }
    Ok(Selected {
        plan,
        outputs: (!every).then_some(names),
        sort,
        hidden,
    })
}
