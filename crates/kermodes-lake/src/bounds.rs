//! The dates a `where` predicate can be true on (architecture.md §8.4), from its top-level `and`
//! terms comparing `Date` with a date literal. Reading only those dates is safe: the engine still
//! applies the whole predicate, so a bound missed only costs reading more.

use kermodes_weir::Date;
use sqlparser::ast::{BinaryOperator, Expr, Value};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::files::DATE;

/// The first and last date `predicate` can be true on; `None` where it does not say.
pub fn date_bounds(predicate: &str) -> (Option<Date>, Option<Date>) {
    let parsed = Parser::new(&GenericDialect {})
        .try_with_sql(predicate)
        .and_then(|mut parser| parser.parse_expr());
    let Ok(ast) = parsed else {
        // The engine reports what is wrong with it.
        return (None, None);
    };
    let (mut first, mut last) = (None::<Date>, None::<Date>);
    for term in terms(&ast) {
        let Expr::BinaryOp { left, op, right } = term else {
            continue;
        };
        let (op, date) = match (is_date_column(left), date_literal(right)) {
            (true, Some(date)) => (op.clone(), date),
            _ => match (date_literal(left), is_date_column(right)) {
                (Some(date), true) => (flipped(op), date),
                _ => continue,
            },
        };
        let after = |date: Date| Date::from_days(date.days() + 1);
        let before = |date: Date| Date::from_days(date.days() - 1);
        let (from, to) = match op {
            BinaryOperator::Eq => (Some(date), Some(date)),
            BinaryOperator::Gt => (Some(after(date)), None),
            BinaryOperator::GtEq => (Some(date), None),
            BinaryOperator::Lt => (None, Some(before(date))),
            BinaryOperator::LtEq => (None, Some(date)),
            _ => continue,
        };
        first = first.max(from);
        last = match (last, to) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
    }
    (first, last)
}

/// The terms of a predicate's top-level `and`s.
fn terms(ast: &Expr) -> Vec<&Expr> {
    match ast {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            let mut terms = self::terms(left);
            terms.extend(self::terms(right));
            terms
        }
        Expr::Nested(inner) => terms(inner),
        term => vec![term],
    }
}

fn is_date_column(ast: &Expr) -> bool {
    matches!(ast, Expr::Identifier(ident) if ident.value == DATE)
}

fn date_literal(ast: &Expr) -> Option<Date> {
    match ast {
        Expr::Value(value) => match &value.value {
            Value::SingleQuotedString(text) => Date::parse(text).ok(),
            _ => None,
        },
        _ => None,
    }
}

/// `literal op Date` as `Date op' literal`.
fn flipped(op: &BinaryOperator) -> BinaryOperator {
    match op {
        BinaryOperator::Gt => BinaryOperator::Lt,
        BinaryOperator::GtEq => BinaryOperator::LtEq,
        BinaryOperator::Lt => BinaryOperator::Gt,
        BinaryOperator::LtEq => BinaryOperator::GtEq,
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use kermodes_weir::Date;

    use super::date_bounds;

    fn d(day: u32) -> Option<Date> {
        Some(Date::parse(&format!("2026-11-{day:02}")).unwrap())
    }

    #[test]
    fn predicates_bound_the_dates_they_can_be_true_on() {
        #[rustfmt::skip]
        let cases = [
            ("Date = '2026-11-12'", (d(12), d(12))),
            ("Date >= '2026-11-01' and Date < '2026-11-05'", (d(1), d(4))),
            ("Date > '2026-11-01' and Date <= '2026-11-05'", (d(2), d(5))),
            ("sym = 'AAPL' and Date = '2026-11-12'", (d(12), d(12))),
            ("'2026-11-12' <= Date", (d(12), None)),
            ("'2026-11-12' > Date", (None, d(11))),
            ("(Date > '2026-11-01') and (n > 3 and Date < '2026-11-09')", (d(2), d(8))),
            // Contradictory: no date at all.
            ("Date = '2026-11-12' and Date = '2026-11-13'", (d(13), d(12))),
            // Anything not a top-level `and` of date comparisons says nothing.
            ("Date = '2026-11-12' or sym = 'x'", (None, None)),
            ("not Date = '2026-11-12'", (None, None)),
            ("Date <> '2026-11-12'", (None, None)),
            ("date = '2026-11-12'", (None, None)),
            ("Date = 'soon'", (None, None)),
            ("Date = ", (None, None)),
            ("n > 3", (None, None)),
        ];
        for (predicate, bounds) in cases {
            assert_eq!(date_bounds(predicate), bounds, "{predicate}");
        }
    }
}
