use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    StringArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use kermodes_core::{Error, Expression};

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
            "tiny",
            Arc::new(Int16Array::from(vec![Some(7), Some(0), Some(i16::MAX)])),
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
    eval_on(&batch(), text)
}

fn eval_on(batch: &RecordBatch, text: &str) -> Result<ArrayRef, String> {
    let (_, expression) = Expression::assignment(&format!("x = {text}"), &batch.schema())
        .map_err(|e| e.to_string())?;
    expression.evaluate(batch).map_err(|e| e.to_string())
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

fn strings(values: &[Option<&str>]) -> ArrayRef {
    Arc::new(StringArray::from(values.to_vec()))
}

fn dates(values: &[Option<i32>]) -> ArrayRef {
    Arc::new(Date32Array::from(values.to_vec()))
}

fn stamps(values: &[Option<i64>]) -> ArrayRef {
    Arc::new(TimestampNanosecondArray::from(values.to_vec()).with_timezone("UTC"))
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
        ("-tiny", Arc::new(Int16Array::from(vec![-7, 0, -i16::MAX]))),
        (
            "tiny / tiny",
            Arc::new(Int16Array::from(vec![Some(1), None, Some(1)])),
        ),
        ("tiny % 0", ints(&[None, None, None])),
        (
            "tiny + small",
            Arc::new(Int32Array::from(vec![10, -4, 32_772])),
        ),
        ("tiny + 1", ints(&[Some(8), Some(1), Some(32_768)])),
        (
            "tiny * 0.5",
            floats(&[Some(3.5), Some(0.0), Some(16_383.5)]),
        ),
        ("tiny > 5", bools(&[Some(true), Some(false), Some(true)])),
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
        ("sym LIKE 'a%'", bools(&[Some(true), Some(false), None])),
        ("sym LIKE 'A'", bools(&[Some(false), Some(false), None])),
        ("sym ILIKE 'A'", bools(&[Some(true), Some(false), None])),
        (
            "sym NOT ILIKE '_'",
            bools(&[Some(false), Some(false), None]),
        ),
        ("sym NOT LIKE 'b'", bools(&[Some(true), Some(false), None])),
        ("sym LIKE NULL", bools(&[None, None, None])),
    ];
    for (text, expected) in cases {
        let got = eval(text).unwrap_or_else(|e| panic!("{text}: {e}"));
        assert_eq!(&got, &expected, "{text}");
    }
}

#[test]
fn integer_overflow_fails_evaluation() {
    for text in ["big * 2", "tiny * tiny"] {
        let error = eval(text).unwrap_err();
        assert!(error.contains("Overflow"), "{text}: {error}");
    }
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
            "EXISTS (SELECT 1)",
            Some(9),
            "not supported in expressions yet",
        ),
        (
            "size LIKE '1%'",
            Some(1),
            "`LIKE` needs strings, but `size` is int64",
        ),
        ("lenght(sym) > 1", Some(1), "did you mean `length`?"),
        (
            "length(size) > 1",
            Some(8),
            "`length` needs strings, but `size` is int64",
        ),
        (
            "length(sym, sym) > 1",
            Some(1),
            "`length` is called as `length(string)`, but got 2 arguments",
        ),
        (
            "regexp_replace(sym, sym, 'x') = 'a'",
            Some(21),
            "the pattern of `regexp_replace` must be a string literal",
        ),
        (
            "regexp_replace(sym, '(', 'x') = 'a'",
            Some(21),
            "invalid pattern `(` for `regexp_replace`",
        ),
        (
            "regexp_replace(sym, 'a', 'b', 'i') = 'a'",
            Some(31),
            "option `i` is not supported",
        ),
        (
            "regexp_replace(sym, 'a', '\\2') = 'a'",
            Some(26),
            "refers to group 2, but the pattern has 0 groups",
        ),
        (
            "regexp_replace(sym, 'a', '\\q') = 'a'",
            Some(26),
            "`\\q` in the replacement is neither a group",
        ),
        (
            "date_trunc('fortnight', day) IS NULL",
            Some(12),
            "unknown unit `fortnight` for `date_trunc`",
        ),
        (
            "date_trunc(sym, day) IS NULL",
            Some(12),
            "the unit of `date_trunc` must be a string literal",
        ),
        (
            "date_part('hour', day) IS NULL",
            Some(11),
            "a date32 has no `hour`",
        ),
        (
            "extract(hour FROM day) IS NULL",
            Some(19),
            "a date32 has no `hour`",
        ),
        (
            "date_part('year', size) IS NULL",
            Some(19),
            "`date_part` needs a date32 or timestamp, but `size` is int64",
        ),
        (
            "CASE WHEN flag THEN 1 ELSE 'x' END = 1",
            Some(28),
            "`CASE` results cannot mix int64 with `'x'`, which is utf8",
        ),
        (
            "CASE WHEN size THEN 1 END = 1",
            Some(11),
            "`size` must be boolean, but is int64",
        ),
        (
            "sym IN (1, 2)",
            Some(9),
            "`IN` cannot mix utf8 with `1`, which is int64",
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

fn check(batch: &RecordBatch, cases: Vec<(&str, ArrayRef)>) {
    for (text, expected) in cases {
        let got = eval_on(batch, text).unwrap_or_else(|e| panic!("{text}: {e}"));
        assert_eq!(&got, &expected, "{text}");
    }
}

#[test]
fn case_takes_the_first_branch_whose_condition_is_true() {
    check(
        &batch(),
        vec![
            (
                "CASE WHEN price > 0 THEN 'up' WHEN price < 0 THEN 'down' END",
                strings(&[Some("up"), Some("down"), None]),
            ),
            (
                "CASE WHEN small > 0 THEN 'pos' WHEN small > 2 THEN 'big' END",
                strings(&[Some("pos"), None, Some("pos")]),
            ),
            // A NULL condition is not true: the row falls through to ELSE.
            (
                "CASE WHEN flag THEN size ELSE small END",
                ints(&[Some(10), Some(-4), Some(5)]),
            ),
            (
                "CASE WHEN flag THEN tiny ELSE 0.5 END",
                floats(&[Some(7.0), Some(0.5), Some(0.5)]),
            ),
            (
                "CASE WHEN true THEN tiny END",
                Arc::new(Int16Array::from(vec![7, 0, i16::MAX])),
            ),
            ("case when flag then 1 end", ints(&[Some(1), None, None])),
            (
                "CASE WHEN NULL THEN 1 ELSE 2 END",
                ints(&[Some(2), Some(2), Some(2)]),
            ),
            (
                "CASE WHEN size > 5 THEN NULL ELSE size END",
                ints(&[None, Some(0), None]),
            ),
            (
                "CASE WHEN flag THEN day ELSE '2020-01-01' END",
                dates(&[Some(20_402), Some(18_262), Some(18_262)]),
            ),
            (
                "CASE sym WHEN 'a' THEN 1 WHEN 'b' THEN 2 ELSE 0 END",
                ints(&[Some(1), Some(2), Some(0)]),
            ),
            // `CASE x WHEN v` is `x = v`: NULL matches nothing, not even NULL.
            (
                "CASE sym WHEN NULL THEN 1 ELSE 0 END",
                ints(&[Some(0), Some(0), Some(0)]),
            ),
            (
                "CASE small WHEN 3 THEN 'three' WHEN 5.0 THEN 'five' END",
                strings(&[Some("three"), None, Some("five")]),
            ),
        ],
    );
}

#[test]
fn in_follows_three_valued_logic() {
    check(
        &batch(),
        vec![
            ("size IN (10, 20)", bools(&[Some(true), Some(false), None])),
            (
                "size NOT IN (10, 20)",
                bools(&[Some(false), Some(true), None]),
            ),
            ("size IN (0, NULL)", bools(&[None, Some(true), None])),
            ("size NOT IN (0, NULL)", bools(&[None, Some(false), None])),
            (
                "small IN (3, 5.0)",
                bools(&[Some(true), Some(false), Some(true)]),
            ),
            ("sym IN ('a', 'c')", bools(&[Some(true), Some(false), None])),
            (
                "day IN ('2025-11-10', '2000-01-01')",
                bools(&[Some(true), Some(false), None]),
            ),
            (
                "tiny IN (small, 0)",
                bools(&[Some(false), Some(true), Some(false)]),
            ),
            ("NULL IN (1, 2)", bools(&[None, None, None])),
            (
                "1 IN (1, NULL)",
                bools(&[Some(true), Some(true), Some(true)]),
            ),
        ],
    );
}

fn text_batch() -> RecordBatch {
    let url = strings(&[
        Some("https://www.example.com/a/b"),
        Some("héllo wörld"),
        Some(""),
        None,
    ]);
    RecordBatch::try_from_iter([("url", url)]).unwrap()
}

#[test]
fn string_lengths_count_characters_or_bytes() {
    check(
        &text_batch(),
        vec![
            ("length(url)", ints(&[Some(27), Some(11), Some(0), None])),
            ("LENGTH(url)", ints(&[Some(27), Some(11), Some(0), None])),
            ("strlen(url)", ints(&[Some(27), Some(13), Some(0), None])),
            ("length(NULL)", ints(&[None, None, None, None])),
            ("length('ß')", ints(&[Some(1), Some(1), Some(1), Some(1)])),
            ("strlen('ß')", ints(&[Some(2), Some(2), Some(2), Some(2)])),
        ],
    );
}

#[test]
fn regexp_replace_follows_duckdb() {
    let each = |value: &str| strings(&[Some(value); 4]);
    check(
        &text_batch(),
        vec![
            (
                r"regexp_replace(url, '^https?://(?:www\.)?([^/]+)/.*$', '\1')",
                strings(&[Some("example.com"), Some("héllo wörld"), Some(""), None]),
            ),
            // The first match only, unless 'g'.
            (
                "regexp_replace(url, 'l', 'L')",
                strings(&[
                    Some("https://www.exampLe.com/a/b"),
                    Some("héLlo wörld"),
                    Some(""),
                    None,
                ]),
            ),
            (
                "Regexp_Replace(url, 'l', 'L', 'g')",
                strings(&[
                    Some("https://www.exampLe.com/a/b"),
                    Some("héLLo wörLd"),
                    Some(""),
                    None,
                ]),
            ),
            (
                r"regexp_replace(url, '(\w+)\.(\w+)', '\2.\1')",
                strings(&[
                    Some("https://example.www.com/a/b"),
                    Some("héllo wörld"),
                    Some(""),
                    None,
                ]),
            ),
            // RE2's global replace skips an empty match where the previous match ended.
            ("regexp_replace('abc', 'x*', '-', 'g')", each("-a-b-c-")),
            ("regexp_replace('aaa', 'a*', 'X', 'g')", each("X")),
            ("regexp_replace('abc', 'a*', 'X', 'g')", each("XbXcX")),
            ("regexp_replace('héé', '', '-', 'g')", each("-h-é-é-")),
            // `\\` is a backslash; `$` has no meaning in a replacement.
            (r"regexp_replace('a.b', '\.', '\\')", each(r"a\b")),
            ("regexp_replace('ab', 'a', '$1')", each("$1b")),
            // `.` does not match a newline, as in DuckDB (RE2's default).
            ("regexp_replace('a\nb', 'a.b', 'X')", each("a\nb")),
            ("regexp_replace('a\nb', 'a', 'X')", each("X\nb")),
            ("regexp_replace(NULL, 'a', 'b')", strings(&[None; 4])),
        ],
    );
}

const SECOND: i64 = 1_000_000_000;
const DAY: i64 = 86_400 * SECOND;

/// 2024-02-29 13:45:30.25 (a Thursday), 1969-12-31 23:59:59.5 (a Wednesday), 2025-01-05 (a
/// Sunday), and NULL; `day` holds the same dates.
fn temporal_batch() -> RecordBatch {
    let ts = stamps(&[
        Some(19_782 * DAY + 49_530 * SECOND + 250_000_000),
        Some(-SECOND / 2),
        Some(20_093 * DAY),
        None,
    ]);
    let day = dates(&[Some(19_782), Some(-1), Some(20_093), None]);
    RecordBatch::try_from_iter([("ts", ts), ("day", day)]).unwrap()
}

#[test]
fn date_parts_follow_duckdb() {
    check(
        &temporal_batch(),
        vec![
            (
                "extract(year FROM ts)",
                ints(&[Some(2024), Some(1969), Some(2025), None]),
            ),
            (
                "date_part('month', ts)",
                ints(&[Some(2), Some(12), Some(1), None]),
            ),
            (
                "EXTRACT(DAY FROM ts)",
                ints(&[Some(29), Some(31), Some(5), None]),
            ),
            (
                "date_part('hour', ts)",
                ints(&[Some(13), Some(23), Some(0), None]),
            ),
            (
                "extract(minute FROM ts)",
                ints(&[Some(45), Some(59), Some(0), None]),
            ),
            (
                "date_part('Second', ts)",
                ints(&[Some(30), Some(59), Some(0), None]),
            ),
            (
                "extract(dow FROM ts)",
                ints(&[Some(4), Some(3), Some(0), None]),
            ),
            (
                "Date_Part('doy', ts)",
                ints(&[Some(60), Some(365), Some(5), None]),
            ),
            (
                "extract(epoch FROM ts)",
                floats(&[
                    Some(1_709_214_330.25),
                    Some(-0.5),
                    Some(1_736_035_200.0),
                    None,
                ]),
            ),
            (
                "date_part('years', day)",
                ints(&[Some(2024), Some(1969), Some(2025), None]),
            ),
            (
                "extract(month FROM day)",
                ints(&[Some(2), Some(12), Some(1), None]),
            ),
            (
                "date_part('d', day)",
                ints(&[Some(29), Some(31), Some(5), None]),
            ),
            (
                "date_part('dayofweek', day)",
                ints(&[Some(4), Some(3), Some(0), None]),
            ),
            (
                "extract(doy FROM day)",
                ints(&[Some(60), Some(365), Some(5), None]),
            ),
            (
                "date_part('epoch', day)",
                floats(&[
                    Some(1_709_164_800.0),
                    Some(-86_400.0),
                    Some(1_736_035_200.0),
                    None,
                ]),
            ),
            ("date_part('year', NULL)", ints(&[None; 4])),
        ],
    );
}

#[test]
fn date_trunc_rounds_down_and_keeps_the_type() {
    let feb29 = 19_782 * DAY;
    let jan5 = Some(20_093 * DAY);
    check(
        &temporal_batch(),
        vec![
            (
                "date_trunc('second', ts)",
                stamps(&[Some(feb29 + 49_530 * SECOND), Some(-SECOND), jan5, None]),
            ),
            (
                "date_trunc('minute', ts)",
                stamps(&[
                    Some(feb29 + 49_500 * SECOND),
                    Some(-60 * SECOND),
                    jan5,
                    None,
                ]),
            ),
            (
                "DATE_TRUNC('HOUR', ts)",
                stamps(&[
                    Some(feb29 + 46_800 * SECOND),
                    Some(-3_600 * SECOND),
                    jan5,
                    None,
                ]),
            ),
            (
                "date_trunc('day', ts)",
                stamps(&[Some(feb29), Some(-DAY), jan5, None]),
            ),
            // Weeks start on Monday: 2024-02-26, 1969-12-29, 2024-12-30.
            (
                "date_trunc('week', ts)",
                stamps(&[Some(19_779 * DAY), Some(-3 * DAY), Some(20_087 * DAY), None]),
            ),
            (
                "date_trunc('months', ts)",
                stamps(&[
                    Some(19_754 * DAY),
                    Some(-31 * DAY),
                    Some(20_089 * DAY),
                    None,
                ]),
            ),
            (
                "date_trunc('quarter', ts)",
                stamps(&[
                    Some(19_723 * DAY),
                    Some(-92 * DAY),
                    Some(20_089 * DAY),
                    None,
                ]),
            ),
            (
                "date_trunc('year', ts)",
                stamps(&[
                    Some(19_723 * DAY),
                    Some(-365 * DAY),
                    Some(20_089 * DAY),
                    None,
                ]),
            ),
            (
                "date_trunc('hour', day)",
                dates(&[Some(19_782), Some(-1), Some(20_093), None]),
            ),
            (
                "date_trunc('day', day)",
                dates(&[Some(19_782), Some(-1), Some(20_093), None]),
            ),
            (
                "date_trunc('week', day)",
                dates(&[Some(19_779), Some(-3), Some(20_087), None]),
            ),
            (
                "date_trunc('month', day)",
                dates(&[Some(19_754), Some(-31), Some(20_089), None]),
            ),
            (
                "date_trunc('quarter', day)",
                dates(&[Some(19_723), Some(-92), Some(20_089), None]),
            ),
            (
                "date_trunc('y', day)",
                dates(&[Some(19_723), Some(-365), Some(20_089), None]),
            ),
            ("date_trunc('day', NULL)", stamps(&[None; 4])),
        ],
    );
}

#[test]
fn date_trunc_out_of_range_fails_evaluation() {
    let batch = RecordBatch::try_from_iter([("ts", stamps(&[Some(i64::MIN)]))]).unwrap();
    let error = eval_on(&batch, "date_trunc('year', ts)").unwrap_err();
    assert!(error.contains("outside the range of timestamp"), "{error}");
}
