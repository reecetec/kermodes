//! Scalar expressions in SQL syntax, bound to a schema and evaluated with Arrow kernels.

use std::sync::{Arc, LazyLock};

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Datum, Float64Array, Int16Array, Int32Array,
    Int64Array, NullArray, Scalar, StringArray, UInt32Array, new_null_array,
};
use arrow::compute::kernels::comparison::{ilike, like, nilike, nlike};
use arrow::compute::kernels::zip::zip;
use arrow::compute::kernels::{cmp, numeric};
use arrow::compute::{and_kleene, cast, is_not_null, is_null, not, nullif, or_kleene, take};
use arrow::datatypes::{DataType, Schema};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use sqlparser::ast::{
    BinaryOperator, CaseWhen, Expr as Ast, InTarget, Spanned, UnaryOperator, Value as Literal,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::{Span, Token};

use crate::{Error, type_name};

mod functions;

static DIALECT: GenericDialect = GenericDialect {};
static NO_COLUMNS: LazyLock<RecordBatch> =
    LazyLock::new(|| RecordBatch::new_empty(Arc::new(Schema::empty())));

/// A bound, type-checked expression over one input schema.
#[derive(Debug, Clone)]
pub struct Expression {
    text: String,
    node: Node,
    data_type: DataType,
}

impl Expression {
    /// Parses a boolean expression, as used by `where`.
    pub fn predicate(text: &str, schema: &Schema) -> Result<Self, Error> {
        let mut parser = parser(text)?;
        let ast = parser.parse_expr().map_err(|e| parse_error(text, e))?;
        expect_end(text, &parser)?;
        let binder = Binder { text, schema };
        let (node, data_type) = binder.bind(&ast)?;
        let node = binder.boolean(&ast, node, data_type)?;
        Ok(Self {
            text: text.to_owned(),
            node,
            data_type: DataType::Boolean,
        })
    }

    /// Parses `name = expression`, as used by `update`.
    pub fn assignment(text: &str, schema: &Schema) -> Result<(String, Self), Error> {
        let mut parser = parser(text)?;
        let name = parser
            .parse_identifier()
            .map_err(|e| parse_error(text, e))?;
        parser
            .expect_token(&Token::Eq)
            .map_err(|_| Error::Expression {
                text: text.to_owned(),
                column: None,
                message: "expected `name = expression`".into(),
            })?;
        let ast = parser.parse_expr().map_err(|e| parse_error(text, e))?;
        expect_end(text, &parser)?;
        let binder = Binder { text, schema };
        let (node, data_type) = binder.bind(&ast)?;
        if data_type == DataType::Null {
            return Err(binder.error(
                ast.span(),
                format!("cannot infer a column type for `{}`", name.value),
            ));
        }
        let expression = Self {
            text: text.to_owned(),
            node,
            data_type,
        };
        Ok((name.value, expression))
    }

    pub fn text(&self) -> &str {
        &self.text
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Evaluates the expression for every row of `batch`.
    pub fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef, ArrowError> {
        self.node.eval(batch)?.into_array(batch.num_rows())
    }
}

fn parser(text: &str) -> Result<Parser<'static>, Error> {
    Parser::new(&DIALECT)
        .try_with_sql(text)
        .map_err(|e| parse_error(text, e))
}

fn parse_error(text: &str, error: ParserError) -> Error {
    Error::Expression {
        text: text.to_owned(),
        column: None,
        message: error.to_string(),
    }
}

fn expect_end(text: &str, parser: &Parser<'_>) -> Result<(), Error> {
    let next = parser.peek_token();
    if next.token == Token::EOF {
        return Ok(());
    }
    Err(Error::Expression {
        text: text.to_owned(),
        column: column_of(next.span),
        message: format!("unexpected `{}` after the expression", next.token),
    })
}

fn column_of(span: Span) -> Option<u64> {
    (span.start.line != 0).then_some(span.start.column)
}

struct Binder<'a> {
    text: &'a str,
    schema: &'a Schema,
}

impl Binder<'_> {
    fn error(&self, span: Span, message: String) -> Error {
        Error::Expression {
            text: self.text.to_owned(),
            column: column_of(span),
            message,
        }
    }

    fn bind(&self, ast: &Ast) -> Result<(Node, DataType), Error> {
        match ast {
            Ast::Identifier(ident) => self.column(ast, &ident.value),
            Ast::Nested(inner) => self.bind(inner),
            Ast::Value(value) => self.literal(ast, &value.value),
            Ast::IsNull(inner) | Ast::IsNotNull(inner) => {
                let (node, _) = self.bind(inner)?;
                let node = Node::IsNull(Box::new(node), matches!(ast, Ast::IsNotNull(_)));
                Ok((self.fold(ast, node)?, DataType::Boolean))
            }
            Ast::UnaryOp { op, expr } => {
                let (node, data_type) = self.bind(expr)?;
                match op {
                    UnaryOperator::Not => {
                        let node = Node::Not(Box::new(self.boolean(expr, node, data_type)?));
                        Ok((self.fold(ast, node)?, DataType::Boolean))
                    }
                    UnaryOperator::Minus if is_numeric(&data_type) => {
                        Ok((self.fold(ast, Node::Negate(Box::new(node)))?, data_type))
                    }
                    UnaryOperator::Plus if is_numeric(&data_type) => Ok((node, data_type)),
                    UnaryOperator::Minus | UnaryOperator::Plus => Err(self.error(
                        ast.span(),
                        format!(
                            "`{op}` needs a number, but `{expr}` is {}",
                            type_name(&data_type)
                        ),
                    )),
                    other => Err(self.error(ast.span(), format!("`{other}` is not supported"))),
                }
            }
            Ast::BinaryOp { left, op, right } => self.binary(ast, left, op, right),
            Ast::Like {
                negated,
                any,
                expr,
                pattern,
                escape_char,
            }
            | Ast::ILike {
                negated,
                any,
                expr,
                pattern,
                escape_char,
            } => {
                if *any || escape_char.is_some() {
                    return Err(self.error(
                        ast.span(),
                        "`LIKE ANY` and `ESCAPE` are not supported".into(),
                    ));
                }
                let pattern_match = PatternMatch {
                    case_insensitive: matches!(ast, Ast::ILike { .. }),
                    negated: *negated,
                };
                let text = self.string(expr, "LIKE")?;
                let pattern = self.string(pattern, "LIKE")?;
                let node = Node::Like(pattern_match, Box::new(text), Box::new(pattern));
                Ok((self.fold(ast, node)?, DataType::Boolean))
            }
            Ast::Function(function) => self.function(ast, function),
            Ast::Extract { field, expr, .. } => self.extract(ast, field, expr),
            Ast::Case {
                operand,
                conditions,
                else_result,
                ..
            } => self.case(ast, operand.as_deref(), conditions, else_result.as_deref()),
            Ast::In {
                expr,
                target: InTarget::List(list),
                negated,
                global: false,
            } => self.in_list(ast, expr, list, *negated),
            other => Err(self.error(
                ast.span(),
                format!("`{other}` is not supported in expressions yet"),
            )),
        }
    }

    fn column(&self, ast: &Ast, name: &str) -> Result<(Node, DataType), Error> {
        if let Ok(index) = self.schema.index_of(name) {
            let data_type = self.schema.field(index).data_type().clone();
            return Ok((Node::Column(index), data_type));
        }
        let names: Vec<&str> = self
            .schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        let close: Vec<&str> = names
            .iter()
            .copied()
            .filter(|n| n.eq_ignore_ascii_case(name) || edit_distance(n, name) <= 2)
            .take(3)
            .collect();
        let hint = if close.is_empty() {
            format!("columns are: {}", names.join(", "))
        } else {
            format!("did you mean {}?", quoted(&close))
        };
        Err(self.error(ast.span(), format!("unknown column `{name}`; {hint}")))
    }

    fn literal(&self, ast: &Ast, value: &Literal) -> Result<(Node, DataType), Error> {
        let array: ArrayRef = match value {
            Literal::Number(digits, _) if digits.contains(['.', 'e', 'E']) => {
                let parsed = digits.parse::<f64>().map_err(|_| {
                    self.error(ast.span(), format!("`{digits}` is not a valid number"))
                })?;
                Arc::new(Float64Array::from(vec![parsed]))
            }
            Literal::Number(digits, _) => {
                let parsed = digits.parse::<i64>().map_err(|_| {
                    self.error(
                        ast.span(),
                        format!("integer `{digits}` does not fit in int64"),
                    )
                })?;
                Arc::new(Int64Array::from(vec![parsed]))
            }
            Literal::SingleQuotedString(text) => Arc::new(StringArray::from(vec![text.as_str()])),
            Literal::Boolean(flag) => Arc::new(BooleanArray::from(vec![*flag])),
            Literal::Null => Arc::new(NullArray::new(1)),
            other => {
                return Err(self.error(ast.span(), format!("literal `{other}` is not supported")));
            }
        };
        let data_type = array.data_type().clone();
        Ok((Node::Literal(array), data_type))
    }

    fn binary(
        &self,
        ast: &Ast,
        left: &Ast,
        op: &BinaryOperator,
        right: &Ast,
    ) -> Result<(Node, DataType), Error> {
        let (l, lt) = self.bind(left)?;
        let (r, rt) = self.bind(right)?;
        let (node, data_type) = match op {
            BinaryOperator::And | BinaryOperator::Or => {
                let l = Box::new(self.boolean(left, l, lt)?);
                let r = Box::new(self.boolean(right, r, rt)?);
                let node = match op {
                    BinaryOperator::And => Node::And(l, r),
                    _ => Node::Or(l, r),
                };
                (node, DataType::Boolean)
            }
            BinaryOperator::Plus
            | BinaryOperator::Minus
            | BinaryOperator::Multiply
            | BinaryOperator::Divide
            | BinaryOperator::Modulo => {
                let (l, r, data_type) = self.unify(ast, op, (l, lt), (r, rt))?;
                if !is_numeric(&data_type) {
                    return Err(self.error(
                        ast.span(),
                        format!("`{op}` needs numbers, but got {}", type_name(&data_type)),
                    ));
                }
                let arithmetic = match op {
                    BinaryOperator::Plus => Arithmetic::Add,
                    BinaryOperator::Minus => Arithmetic::Sub,
                    BinaryOperator::Multiply => Arithmetic::Mul,
                    BinaryOperator::Divide => Arithmetic::Div,
                    _ => Arithmetic::Rem,
                };
                let node = Node::Arithmetic(arithmetic, Box::new(l), Box::new(r));
                (node, data_type)
            }
            BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq => {
                let (l, r, _) = self.unify(ast, op, (l, lt), (r, rt))?;
                let comparison = match op {
                    BinaryOperator::Eq => Comparison::Eq,
                    BinaryOperator::NotEq => Comparison::NotEq,
                    BinaryOperator::Lt => Comparison::Lt,
                    BinaryOperator::LtEq => Comparison::LtEq,
                    BinaryOperator::Gt => Comparison::Gt,
                    _ => Comparison::GtEq,
                };
                let node = Node::Compare(comparison, Box::new(l), Box::new(r));
                (node, DataType::Boolean)
            }
            other => return Err(self.error(ast.span(), format!("`{other}` is not supported"))),
        };
        Ok((self.fold(ast, node)?, data_type))
    }

    /// Brings both operands to one type, as `common_type` chooses it.
    fn unify(
        &self,
        ast: &Ast,
        op: &BinaryOperator,
        (l, lt): (Node, DataType),
        (r, rt): (Node, DataType),
    ) -> Result<(Node, Node, DataType), Error> {
        let literals = (matches!(l, Node::Literal(_)), matches!(r, Node::Literal(_)));
        let Some(to) = common_type((&lt, literals.0), (&rt, literals.1)) else {
            return Err(self.error(
                ast.span(),
                format!(
                    "cannot apply `{op}` to {} and {}",
                    type_name(&lt),
                    type_name(&rt)
                ),
            ));
        };
        let l = self.coerce(ast, l, &lt, &to)?;
        let r = self.coerce(ast, r, &rt, &to)?;
        Ok((l, r, to))
    }

    /// Binds operands that must share a type (the values `IN` compares, the results of a
    /// `CASE`) and brings them to it, as `common_type` chooses it pairwise. A string literal
    /// reads as a date or timestamp only while every operand before it was a literal too.
    fn unify_all<'a>(
        &self,
        what: &str,
        operands: impl IntoIterator<Item = &'a Ast>,
    ) -> Result<(Vec<Node>, DataType), Error> {
        let mut bound = Vec::new();
        let mut to = DataType::Null;
        let mut all_literals = true;
        for ast in operands {
            let (node, data_type) = self.bind(ast)?;
            let literal = matches!(node, Node::Literal(_));
            to = common_type((&to, all_literals), (&data_type, literal)).ok_or_else(|| {
                self.error(
                    ast.span(),
                    format!(
                        "{what} cannot mix {} with `{ast}`, which is {}",
                        type_name(&to),
                        type_name(&data_type)
                    ),
                )
            })?;
            all_literals &= literal;
            bound.push((ast, node, data_type));
        }
        let nodes = bound
            .into_iter()
            .map(|(ast, node, data_type)| self.coerce(ast, node, &data_type, &to))
            .collect::<Result<_, _>>()?;
        Ok((nodes, to))
    }

    /// `CASE [operand] WHEN ... THEN ... [ELSE ...] END`. As in DuckDB, `CASE x WHEN v` tests
    /// `x = v`, so a NULL `x` matches no branch, and a branch whose condition is NULL is not taken.
    fn case(
        &self,
        ast: &Ast,
        operand: Option<&Ast>,
        conditions: &[CaseWhen],
        otherwise: Option<&Ast>,
    ) -> Result<(Node, DataType), Error> {
        let (operand, whens) = match operand {
            Some(operand) => {
                let values =
                    std::iter::once(operand).chain(conditions.iter().map(|c| &c.condition));
                let (nodes, _) = self.unify_all("`CASE` and its `WHEN` values", values)?;
                let mut nodes = nodes.into_iter();
                (nodes.next().map(Box::new), nodes.collect())
            }
            None => {
                let whens = conditions
                    .iter()
                    .map(|c| {
                        let (node, data_type) = self.bind(&c.condition)?;
                        self.boolean(&c.condition, node, data_type)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                (None, whens)
            }
        };
        let results = conditions.iter().map(|c| &c.result).chain(otherwise);
        let (mut thens, data_type) = self.unify_all("`CASE` results", results)?;
        if data_type == DataType::Null {
            return Ok((Node::Literal(Arc::new(NullArray::new(1))), DataType::Null));
        }
        let otherwise = if otherwise.is_some() {
            thens.pop()
        } else {
            None
        };
        let otherwise = otherwise.unwrap_or_else(|| Node::Literal(new_null_array(&data_type, 1)));
        let node = Node::Case {
            operand,
            branches: whens.into_iter().zip(thens).collect(),
            otherwise: Box::new(otherwise),
        };
        Ok((self.fold(ast, node)?, data_type))
    }

    /// `x [NOT] IN (v1, v2, ...)`, which is `x = v1 OR x = v2 ...` in three-valued logic: NULL when
    /// nothing matches and `x` or a value is NULL.
    fn in_list(
        &self,
        ast: &Ast,
        expr: &Ast,
        list: &[Ast],
        negated: bool,
    ) -> Result<(Node, DataType), Error> {
        let (nodes, _) = self.unify_all("`IN`", std::iter::once(expr).chain(list))?;
        let mut nodes = nodes.into_iter();
        let Some(value) = nodes.next() else {
            unreachable!("`IN` binds its left operand first");
        };
        let node = Node::In(Box::new(value), nodes.collect(), negated);
        Ok((self.fold(ast, node)?, DataType::Boolean))
    }

    fn cast(&self, ast: &Ast, node: Node, to: &DataType) -> Result<Node, Error> {
        if matches!(&node, Node::Literal(a) if a.data_type() == to) {
            return Ok(node);
        }
        self.fold(ast, Node::Cast(Box::new(node), to.clone()))
    }

    fn coerce(&self, ast: &Ast, node: Node, from: &DataType, to: &DataType) -> Result<Node, Error> {
        if from == to {
            return Ok(node);
        }
        self.cast(ast, node, to)
    }

    /// A string operand of `what`, or NULL read as one.
    fn string(&self, ast: &Ast, what: &str) -> Result<Node, Error> {
        let (node, data_type) = self.bind(ast)?;
        match data_type {
            DataType::Utf8 => Ok(node),
            DataType::Null => self.cast(ast, node, &DataType::Utf8),
            other => Err(self.error(
                ast.span(),
                format!(
                    "`{what}` needs strings, but `{ast}` is {}",
                    type_name(&other)
                ),
            )),
        }
    }

    fn boolean(&self, ast: &Ast, node: Node, data_type: DataType) -> Result<Node, Error> {
        match data_type {
            DataType::Boolean => Ok(node),
            DataType::Null => self.cast(ast, node, &DataType::Boolean),
            other => Err(self.error(
                ast.span(),
                format!("`{ast}` must be boolean, but is {}", type_name(&other)),
            )),
        }
    }

    /// Replaces a node whose operands are all literals by its value.
    fn fold(&self, ast: &Ast, node: Node) -> Result<Node, Error> {
        let constant = !matches!(node, Node::Column(_))
            && node
                .children()
                .iter()
                .all(|c| matches!(c, Node::Literal(_)));
        if !constant {
            return Ok(node);
        }
        match node.eval(&NO_COLUMNS) {
            Ok(Value::Scalar(array)) => Ok(Node::Literal(array)),
            Ok(Value::Array(_)) => Ok(node),
            Err(e) => Err(self.error(ast.span(), e.to_string())),
        }
    }
}

#[derive(Debug, Clone)]
enum Node {
    Column(usize),
    /// A one-row array.
    Literal(ArrayRef),
    Cast(Box<Node>, DataType),
    Negate(Box<Node>),
    Not(Box<Node>),
    /// `true` means `IS NOT NULL`.
    IsNull(Box<Node>, bool),
    And(Box<Node>, Box<Node>),
    Or(Box<Node>, Box<Node>),
    Compare(Comparison, Box<Node>, Box<Node>),
    Arithmetic(Arithmetic, Box<Node>, Box<Node>),
    /// Text, then pattern (`%` any run, `_` one character).
    Like(PatternMatch, Box<Node>, Box<Node>),
    /// `length` or `strlen` of a string.
    Length(Box<Node>, functions::Count),
    /// `regexp_replace` of a string, its pattern compiled when binding.
    RegexpReplace(Box<Node>, Box<functions::Replace>),
    /// `date_part` or `extract`.
    DatePart(Box<Node>, functions::Field),
    DateTrunc(Box<Node>, functions::Unit),
    /// The operand a simple `CASE` compares each `WHEN` value with, the (`WHEN`, `THEN`) pairs
    /// in order, and the `ELSE` result (NULL when the query has none).
    Case {
        operand: Option<Box<Node>>,
        branches: Vec<(Node, Node)>,
        otherwise: Box<Node>,
    },
    /// The value, the list it is looked up in, and `true` for `NOT IN`.
    In(Box<Node>, Vec<Node>, bool),
}

#[derive(Debug, Clone, Copy)]
struct PatternMatch {
    case_insensitive: bool,
    negated: bool,
}

#[derive(Debug, Clone, Copy)]
enum Comparison {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

#[derive(Debug, Clone, Copy)]
enum Arithmetic {
    Add,
    Sub,
    Mul,
    Div,
    Rem,
}

impl Node {
    fn children(&self) -> Vec<&Node> {
        match self {
            Node::Column(_) | Node::Literal(_) => vec![],
            Node::Cast(n, _) | Node::Negate(n) | Node::Not(n) | Node::IsNull(n, _) => vec![n],
            Node::And(l, r)
            | Node::Or(l, r)
            | Node::Compare(_, l, r)
            | Node::Arithmetic(_, l, r)
            | Node::Like(_, l, r) => vec![l, r],
            Node::Length(n, _)
            | Node::RegexpReplace(n, _)
            | Node::DatePart(n, _)
            | Node::DateTrunc(n, _) => vec![n],
            Node::Case {
                operand,
                branches,
                otherwise,
            } => operand
                .as_deref()
                .into_iter()
                .chain(branches.iter().flat_map(|(when, then)| [when, then]))
                .chain([&**otherwise])
                .collect(),
            Node::In(value, list, _) => std::iter::once(&**value).chain(list).collect(),
        }
    }

    fn eval(&self, batch: &RecordBatch) -> Result<Value, ArrowError> {
        match self {
            Node::Column(index) => Ok(Value::Array(batch.column(*index).clone())),
            Node::Literal(array) => Ok(Value::Scalar(array.clone())),
            Node::Cast(node, to) => node.eval(batch)?.map(|a| cast(a, to)),
            Node::Negate(node) => node.eval(batch)?.map(numeric::neg),
            Node::Not(node) => node
                .eval(batch)?
                .map(|a| Ok(Arc::new(not(a.as_boolean())?) as ArrayRef)),
            Node::IsNull(node, negated) => node.eval(batch)?.map(|a| {
                let mask = if *negated {
                    is_not_null(a)?
                } else {
                    is_null(a)?
                };
                Ok(Arc::new(mask) as ArrayRef)
            }),
            Node::And(l, r) => boolean(l.eval(batch)?, r.eval(batch)?, and_kleene),
            Node::Or(l, r) => boolean(l.eval(batch)?, r.eval(batch)?, or_kleene),
            Node::Compare(op, l, r) => binary(l.eval(batch)?, r.eval(batch)?, |a, b| {
                let mask = match op {
                    Comparison::Eq => cmp::eq(a, b),
                    Comparison::NotEq => cmp::neq(a, b),
                    Comparison::Lt => cmp::lt(a, b),
                    Comparison::LtEq => cmp::lt_eq(a, b),
                    Comparison::Gt => cmp::gt(a, b),
                    Comparison::GtEq => cmp::gt_eq(a, b),
                }?;
                Ok(Arc::new(mask) as ArrayRef)
            }),
            Node::Like(how, l, r) => binary(l.eval(batch)?, r.eval(batch)?, |a, b| {
                let mask = match (how.case_insensitive, how.negated) {
                    (false, false) => like(a, b),
                    (false, true) => nlike(a, b),
                    (true, false) => ilike(a, b),
                    (true, true) => nilike(a, b),
                }?;
                Ok(Arc::new(mask) as ArrayRef)
            }),
            Node::Arithmetic(op, l, r) => {
                let l = l.eval(batch)?;
                let mut r = r.eval(batch)?;
                if matches!(op, Arithmetic::Div | Arithmetic::Rem)
                    && matches!(
                        r.data_type(),
                        DataType::Int16 | DataType::Int32 | DataType::Int64
                    )
                {
                    r = r.map(null_integer_zeros)?;
                }
                binary(l, r, |a, b| match op {
                    Arithmetic::Add => numeric::add(a, b),
                    Arithmetic::Sub => numeric::sub(a, b),
                    Arithmetic::Mul => numeric::mul(a, b),
                    Arithmetic::Div => numeric::div(a, b),
                    Arithmetic::Rem => numeric::rem(a, b),
                })
            }
            Node::Length(node, count) => node.eval(batch)?.map(|a| functions::length(a, *count)),
            Node::RegexpReplace(node, replace) => node.eval(batch)?.map(|a| replace.apply(a)),
            Node::DatePart(node, field) => node.eval(batch)?.map(|a| functions::extract(a, *field)),
            Node::DateTrunc(node, unit) => node.eval(batch)?.map(|a| functions::truncate(a, *unit)),
            Node::Case {
                operand,
                branches,
                otherwise,
            } => {
                let operand = operand.as_ref().map(|o| o.eval(batch)).transpose()?;
                let mut result = otherwise.eval(batch)?;
                // Later branches first, each then overwritten where an earlier one matches: the
                // first branch whose condition holds wins. A NULL condition picks the later side.
                for (when, then) in branches.iter().rev() {
                    let mut mask = when.eval(batch)?;
                    if let Some(operand) = &operand {
                        mask = equal(operand.clone(), mask)?;
                    }
                    result = match mask {
                        Value::Scalar(mask) => {
                            let mask = mask.as_boolean();
                            if mask.is_valid(0) && mask.value(0) {
                                then.eval(batch)?
                            } else {
                                result
                            }
                        }
                        Value::Array(mask) => {
                            let then = then.eval(batch)?;
                            let chosen = zip(
                                mask.as_boolean(),
                                then.datum().as_ref(),
                                result.datum().as_ref(),
                            )?;
                            Value::Array(chosen)
                        }
                    };
                }
                Ok(result)
            }
            Node::In(value, list, negated) => {
                let value = value.eval(batch)?;
                let mut found: Option<Value> = None;
                for item in list {
                    let matched = equal(value.clone(), item.eval(batch)?)?;
                    found = Some(match found {
                        Some(found) => boolean(found, matched, or_kleene)?,
                        None => matched,
                    });
                }
                let found = found
                    .unwrap_or_else(|| Value::Scalar(Arc::new(BooleanArray::from(vec![false]))));
                if *negated {
                    found.map(|a| Ok(Arc::new(not(a.as_boolean())?) as ArrayRef))
                } else {
                    Ok(found)
                }
            }
        }
    }
}

/// A column of values, or one value standing for every row.
#[derive(Clone)]
enum Value {
    Array(ArrayRef),
    /// A one-row array.
    Scalar(ArrayRef),
}

impl Value {
    fn is_scalar(&self) -> bool {
        matches!(self, Value::Scalar(_))
    }

    fn data_type(&self) -> &DataType {
        match self {
            Value::Array(array) | Value::Scalar(array) => array.data_type(),
        }
    }

    fn datum(&self) -> Box<dyn Datum> {
        match self {
            Value::Array(array) => Box::new(array.clone()),
            Value::Scalar(array) => Box::new(Scalar::new(array.clone())),
        }
    }

    fn map(
        self,
        f: impl FnOnce(&dyn Array) -> Result<ArrayRef, ArrowError>,
    ) -> Result<Value, ArrowError> {
        Ok(match self {
            Value::Array(array) => Value::Array(f(array.as_ref())?),
            Value::Scalar(array) => Value::Scalar(f(array.as_ref())?),
        })
    }

    fn into_array(self, len: usize) -> Result<ArrayRef, ArrowError> {
        match self {
            Value::Array(array) => Ok(array),
            Value::Scalar(array) => take(&array, &UInt32Array::from(vec![0; len]), None),
        }
    }
}

fn binary(
    l: Value,
    r: Value,
    f: impl FnOnce(&dyn Datum, &dyn Datum) -> Result<ArrayRef, ArrowError>,
) -> Result<Value, ArrowError> {
    let both_scalar = l.is_scalar() && r.is_scalar();
    let out = f(l.datum().as_ref(), r.datum().as_ref())?;
    Ok(if both_scalar {
        Value::Scalar(out)
    } else {
        Value::Array(out)
    })
}

fn equal(l: Value, r: Value) -> Result<Value, ArrowError> {
    binary(l, r, |a, b| Ok(Arc::new(cmp::eq(a, b)?) as ArrayRef))
}

/// The type two operands are brought to: numbers widen, NULL takes the other side's type, and a
/// string literal meeting a date or timestamp is read as one. Each side says whether it is a
/// literal.
fn common_type(
    (a, a_literal): (&DataType, bool),
    (b, b_literal): (&DataType, bool),
) -> Option<DataType> {
    if a == b || *b == DataType::Null {
        return Some(a.clone());
    }
    if *a == DataType::Null {
        return Some(b.clone());
    }
    if is_numeric(a) && is_numeric(b) {
        let wide = if numeric_rank(a) >= numeric_rank(b) {
            a
        } else {
            b
        };
        return Some(wide.clone());
    }
    if *a == DataType::Utf8 && a_literal && is_temporal(b) {
        return Some(b.clone());
    }
    if *b == DataType::Utf8 && b_literal && is_temporal(a) {
        return Some(a.clone());
    }
    None
}

fn boolean(
    l: Value,
    r: Value,
    f: fn(&BooleanArray, &BooleanArray) -> Result<BooleanArray, ArrowError>,
) -> Result<Value, ArrowError> {
    let len = match (&l, &r) {
        (Value::Array(a), _) | (_, Value::Array(a)) => a.len(),
        _ => 1,
    };
    let both_scalar = l.is_scalar() && r.is_scalar();
    let (l, r) = (l.into_array(len)?, r.into_array(len)?);
    let out = Arc::new(f(l.as_boolean(), r.as_boolean())?) as ArrayRef;
    Ok(if both_scalar {
        Value::Scalar(out)
    } else {
        Value::Array(out)
    })
}

/// Integer division or remainder by zero yields NULL instead of failing the table.
fn null_integer_zeros(divisor: &dyn Array) -> Result<ArrayRef, ArrowError> {
    let zero: ArrayRef = match divisor.data_type() {
        DataType::Int16 => Arc::new(Int16Array::from(vec![0])),
        DataType::Int32 => Arc::new(Int32Array::from(vec![0])),
        _ => Arc::new(Int64Array::from(vec![0])),
    };
    let is_zero = cmp::eq(&divisor, &Scalar::new(zero))?;
    nullif(divisor, &is_zero)
}

fn is_numeric(data_type: &DataType) -> bool {
    numeric_rank(data_type) > 0
}

fn numeric_rank(data_type: &DataType) -> u8 {
    match data_type {
        DataType::Int16 => 1,
        DataType::Int32 => 2,
        DataType::Int64 => 3,
        DataType::Float64 => 4,
        _ => 0,
    }
}

fn is_temporal(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Date32 | DataType::Timestamp(_, _))
}

fn quoted(names: &[&str]) -> String {
    names
        .iter()
        .map(|n| format!("`{n}`"))
        .collect::<Vec<_>>()
        .join(" or ")
}

fn edit_distance(a: &str, b: &str) -> usize {
    let b: Vec<char> = b.chars().collect();
    let mut previous: Vec<usize> = (0..=b.len()).collect();
    for (i, ca) in a.chars().enumerate() {
        let mut current = vec![i + 1];
        for (j, cb) in b.iter().enumerate() {
            let substitution = previous[j] + usize::from(ca != *cb);
            current.push(substitution.min(previous[j + 1] + 1).min(current[j] + 1));
        }
        previous = current;
    }
    previous[b.len()]
}
