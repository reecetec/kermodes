use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float64Array, Int32Array, Int64Array, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use ursid_core::{Error, Expression};

fn batch() -> RecordBatch {
    let columns: Vec<(&str, ArrayRef)> = vec![
        (
            "sym",
            Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])),
        ),
        (
            "price",
            Arc::new(Float64Array::from(vec![Some(1.5), Some(-2.0), None])),
        ),
        (
            "size",
            Arc::new(Int64Array::from(vec![Some(10), Some(0), None])),
        ),
        (
            "small",
            Arc::new(Int32Array::from(vec![Some(3), Some(-4), Some(5)])),
        ),
        (
            "flag",
            Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
        ),
        (
            "day",
            Arc::new(Date32Array::from(vec![Some(20_402), Some(20_403), None])),
        ),
        (
            "big",
            Arc::new(Int64Array::from(vec![Some(i64::MAX), Some(1), Some(2)])),
        ),
    ];
    RecordBatch::try_from_iter(columns).unwrap()
}

fn eval(text: &str) -> Result<ArrayRef, String> {
    let batch = batch();
    let (_, expression) = Expression::assignment(&format!("x = {text}"), &batch.schema())
        .map_err(|e| e.to_string())?;
    expression.evaluate(&batch).map_err(|e| e.to_string())
}

fn ints(values: &[Option<i64>]) -> ArrayRef {
    Arc::new(Int64Array::from(values.to_vec()))
}

fn floats(values: &[Option<f64>]) -> ArrayRef {
    Arc::new(Float64Array::from(values.to_vec()))
}

fn bools(values: &[Option<bool>]) -> ArrayRef {
    Arc::new(BooleanArray::from(values.to_vec()))
}

#[test]
fn values_follow_sql_semantics() {
    let cases: Vec<(&str, ArrayRef)> = vec![
        ("price * size", floats(&[Some(15.0), Some(-0.0), None])),
        ("size / 4", ints(&[Some(2), Some(0), None])),
        ("size / size", ints(&[Some(1), None, None])),
        ("size % 3", ints(&[Some(1), Some(0), None])),
        ("small + 1", ints(&[Some(4), Some(-3), Some(6)])),
        ("-small", Arc::new(Int32Array::from(vec![-3, 4, -5]))),
        ("1 + 2 * 3", ints(&[Some(7), Some(7), Some(7)])),
        ("price > 1", bools(&[Some(true), Some(false), None])),
        ("sym = 'a'", bools(&[Some(true), Some(false), None])),
        (
            "sym <> 'a' OR sym IS NULL",
            bools(&[Some(false), Some(true), Some(true)]),
        ),
        ("flag AND NULL", bools(&[None, Some(false), None])),
        ("flag OR NULL", bools(&[Some(true), None, None])),
        ("NOT flag", bools(&[Some(false), Some(true), None])),
        (
            "size IS NULL",
            bools(&[Some(false), Some(false), Some(true)]),
        ),
        (
            "price IS NOT NULL",
            bools(&[Some(true), Some(true), Some(false)]),
        ),
        (
            "day = '2025-11-10'",
            bools(&[Some(true), Some(false), None]),
        ),
        ("(price + 1) * 2", floats(&[Some(5.0), Some(-2.0), None])),
    ];
    for (text, expected) in cases {
        let got = eval(text).unwrap_or_else(|e| panic!("{text}: {e}"));
        assert_eq!(&got, &expected, "{text}");
    }
}

#[test]
fn integer_overflow_fails_evaluation() {
    let error = eval("big * 2").unwrap_err();
    assert!(error.contains("Overflow"), "{error}");
}

#[test]
fn constant_overflow_is_reported_when_binding() {
    let error = eval("9223372036854775807 + 1").unwrap_err();
    assert!(error.contains("Overflow"), "{error}");
}

#[test]
fn binding_errors_name_the_problem_and_its_position() {
    let schema = batch().schema();
    let cases = [
        ("prise > 1", Some(1), "did you mean `price`?"),
        ("price > 1 AND nope", Some(15), "columns are:"),
        ("sym + 1", Some(1), "cannot apply `+` to utf8 and int64"),
        ("size", Some(1), "must be boolean, but is int64"),
        (
            "price > 1 price",
            Some(11),
            "unexpected `price` after the expression",
        ),
        (
            "price IN (1, 2)",
            Some(1),
            "not supported in expressions yet",
        ),
    ];
    for (text, column, message) in cases {
        match Expression::predicate(text, &schema) {
            Err(Error::Expression {
                column: got_column,
                message: got,
                ..
            }) => {
                assert!(got.contains(message), "{text}: {got}");
                assert_eq!(got_column, column, "{text}: {got}");
            }
            other => panic!("{text}: expected an expression error, got {other:?}"),
        }
    }
}

#[test]
fn assignments_need_a_name_and_a_typed_value() {
    let schema = Schema::new(vec![Field::new("a", DataType::Int64, true)]);
    let missing_name = Expression::assignment("a + 1", &schema).unwrap_err();
    assert!(
        missing_name
            .to_string()
            .contains("expected `name = expression`")
    );
    let untyped = Expression::assignment("b = NULL", &schema).unwrap_err();
    assert!(
        untyped
            .to_string()
            .contains("cannot infer a column type for `b`")
    );
}

#[test]
fn constant_expressions_fill_every_row() {
    let got = eval("2 > 1").unwrap();
    assert_eq!(got.len(), 3);
    assert_eq!(&got, &bools(&[Some(true), Some(true), Some(true)]));
}
