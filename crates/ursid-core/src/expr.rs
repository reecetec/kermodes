//! Scalar expressions in SQL syntax, bound to a schema and evaluated with Arrow kernels.

use std::sync::{Arc, LazyLock};

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Datum, Float64Array, Int32Array, Int64Array, NullArray,
    Scalar, StringArray, UInt32Array,
};
use arrow::compute::kernels::{cmp, numeric};
use arrow::compute::{and_kleene, cast, is_not_null, is_null, not, nullif, or_kleene, take};
use arrow::datatypes::{DataType, Schema};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use sqlparser::ast::{BinaryOperator, Expr as Ast, Spanned, UnaryOperator, Value as Literal};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::{Span, Token};

use crate::{Error, type_name};

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

    /// Brings both operands to one type: numbers widen, NULL takes the other side's type, and
    /// a string literal compared with a date or timestamp is read as one.
    fn unify(
        &self,
        ast: &Ast,
        op: &BinaryOperator,
        (l, lt): (Node, DataType),
        (r, rt): (Node, DataType),
    ) -> Result<(Node, Node, DataType), Error> {
        if lt == rt {
            return Ok((l, r, lt));
        }
        if lt == DataType::Null {
            return Ok((self.cast(ast, l, &rt)?, r, rt));
        }
        if rt == DataType::Null {
            return Ok((l, self.cast(ast, r, &lt)?, lt));
        }
        if is_numeric(&lt) && is_numeric(&rt) {
            let wide = if numeric_rank(&lt) >= numeric_rank(&rt) {
                lt
            } else {
                rt
            };
            return Ok((self.cast(ast, l, &wide)?, self.cast(ast, r, &wide)?, wide));
        }
        if lt == DataType::Utf8 && is_temporal(&rt) && matches!(l, Node::Literal(_)) {
            return Ok((self.cast(ast, l, &rt)?, r, rt));
        }
        if rt == DataType::Utf8 && is_temporal(&lt) && matches!(r, Node::Literal(_)) {
            return Ok((l, self.cast(ast, r, &lt)?, lt));
        }
        Err(self.error(
            ast.span(),
            format!(
                "cannot apply `{op}` to {} and {}",
                type_name(&lt),
                type_name(&rt)
            ),
        ))
    }

    fn cast(&self, ast: &Ast, node: Node, to: &DataType) -> Result<Node, Error> {
        if matches!(&node, Node::Literal(a) if a.data_type() == to) {
            return Ok(node);
        }
        self.fold(ast, Node::Cast(Box::new(node), to.clone()))
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
            | Node::Arithmetic(_, l, r) => vec![l, r],
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
            Node::Arithmetic(op, l, r) => {
                let l = l.eval(batch)?;
                let mut r = r.eval(batch)?;
                if matches!(op, Arithmetic::Div | Arithmetic::Rem)
                    && matches!(r.data_type(), DataType::Int32 | DataType::Int64)
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
        }
    }
}

/// A column of values, or one value standing for every row.
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
        DataType::Int32 => 1,
        DataType::Int64 => 2,
        DataType::Float64 => 3,
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
