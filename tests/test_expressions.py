"""Expression functions, CASE, and IN against pyarrow.compute and plain Python.

pyarrow's regex kernels run RE2, as DuckDB's do, and its temporal kernels read UTC timestamps the
way DuckDB does, so both stand in for DuckDB here. Each expression runs on a static table and on
an input table that ticks, which must agree.
"""

import kermodes as km
import pyarrow as pa
import pyarrow.compute as pc

COLUMNS = {
    "url": km.string(),
    "ts": km.timestamp(),
    "day": km.date32(),
    "n": km.int64(),
}
HOST = r"^https?://(?:www\.)?([^/]+)/.*$"


def data() -> pa.Table:
    return pa.table(
        {
            "url": pa.array(
                [
                    "https://www.example.com/a/b",
                    "http://test.org/",
                    "héllo wörld",
                    "",
                    None,
                    "abbc",
                ],
                pa.string(),
            ),
            "ts": pa.array(
                [
                    1_709_214_330_250_000_000,  # 2024-02-29 13:45:30.25, a Thursday
                    -500_000_000,  # 1969-12-31 23:59:59.5
                    1_736_035_200_000_000_000,  # 2025-01-05, a Sunday
                    None,
                    0,
                    1_700_000_000_123_456_789,
                ],
                pa.timestamp("ns", tz="UTC"),
            ),
            "day": pa.array([19_782, -1, 20_093, None, 0, 19_675], pa.date32()),
            "n": pa.array([1, None, 3, 4, 5, 2], pa.int64()),
        }
    )


EXPRESSIONS = {
    "chars": "length(url)",
    "bytes": "STRLEN(url)",
    "host": f"regexp_replace(url, '{HOST}', '\\1')",
    "first_o": "regexp_replace(url, 'o', '0')",
    "every_l": "regexp_replace(url, 'l', 'L', 'g')",
    "stars": "regexp_replace(url, 'b*', '*', 'g')",
    "ts_year": "extract(year FROM ts)",
    "ts_month": "date_part('month', ts)",
    "ts_day": "extract(day FROM ts)",
    "ts_hour": "date_part('hour', ts)",
    "ts_minute": "extract(minute FROM ts)",
    "ts_second": "date_part('second', ts)",
    "ts_dow": "extract(dow FROM ts)",
    "ts_doy": "date_part('doy', ts)",
    "ts_epoch": "extract(epoch FROM ts)",
    "day_year": "date_part('year', day)",
    "day_month": "extract(month FROM day)",
    "day_day": "date_part('day', day)",
    "day_dow": "date_part('dow', day)",
    "day_doy": "extract(doy FROM day)",
    "day_epoch": "date_part('epoch', day)",
    **{f"ts_{u}s": f"date_trunc('{u}', ts)" for u in ("second", "minute", "hour")},
    **{
        f"{c}_{u}s": f"date_trunc('{u}', {c})"
        for c in ("ts", "day")
        for u in ("day", "week", "month", "quarter", "year")
    },
    "day_hours": "date_trunc('hour', day)",
    "size": "CASE WHEN n > 3 THEN 'big' WHEN n > 1 THEN 'mid' ELSE 'small' END",
    "pick": "CASE n WHEN 1 THEN 'one' WHEN 3 THEN 'three' END",
    "member": "n IN (1, 3)",
    "outside": "n NOT IN (1, NULL)",
}


def expected(t: pa.Table) -> dict[str, pa.Array]:
    url, ts, day = (t[c].combine_chunks() for c in ("url", "ts", "day"))
    n = t["n"].to_pylist()
    int64 = pa.int64()

    def epoch(values: list[int | None], per_second: int, scale: int) -> pa.Array:
        # DuckDB divides microseconds as a double.
        return pa.array(
            [None if v is None else (v * scale // per_second) / 1e6 for v in values],
            pa.float64(),
        )

    # Compute functions pyarrow generates at run time, which its type stubs do not name.
    def compute(name: str, values: pa.Array, options=None) -> pa.Array:
        return pc.call_function(name, [values], options)

    def part(name: str, values: pa.Array) -> pa.Array:
        return compute(name, values).cast(int64)

    def sunday0(values: pa.Array) -> pa.Array:
        options = pc.DayOfWeekOptions(count_from_zero=True, week_start=7)
        return compute("day_of_week", values, options).cast(int64)

    def size(v: int | None) -> str:
        # A NULL `n` makes both conditions NULL, which is not true: ELSE.
        if v is not None and v > 3:
            return "big"
        if v is not None and v > 1:
            return "mid"
        return "small"

    def regex(pattern: str, replacement: str, first_only: bool = False) -> pa.Array:
        options = pc.ReplaceSubstringOptions(
            pattern, replacement, max_replacements=1 if first_only else None
        )
        return compute("replace_substring_regex", url, options)

    out = {
        "chars": part("utf8_length", url),
        "bytes": part("binary_length", url),
        "host": regex(HOST, r"\1", first_only=True),
        "first_o": regex("o", "0", first_only=True),
        "every_l": regex("l", "L"),
        "stars": regex("b*", "*"),
        "ts_year": part("year", ts),
        "ts_month": part("month", ts),
        "ts_day": part("day", ts),
        "ts_hour": part("hour", ts),
        "ts_minute": part("minute", ts),
        "ts_second": part("second", ts),
        "ts_dow": sunday0(ts),
        "ts_doy": part("day_of_year", ts),
        "ts_epoch": epoch(ts.cast(int64).to_pylist(), 1_000, 1),
        "day_year": part("year", day),
        "day_month": part("month", day),
        "day_day": part("day", day),
        "day_dow": sunday0(day),
        "day_doy": part("day_of_year", day),
        "day_epoch": epoch(day.cast(pa.int32()).to_pylist(), 1, 86_400 * 1_000_000),
        "day_hours": day,
        "size": pa.array([size(v) for v in n], pa.string()),
        "pick": pa.array([{1: "one", 3: "three"}.get(v) for v in n], pa.string()),
        "member": pa.array([None if v is None else v in (1, 3) for v in n], pa.bool_()),
        # No match and a NULL in the list: unknown.
        "outside": pa.array([False if v == 1 else None for v in n], pa.bool_()),
    }
    for u in ("second", "minute", "hour", "day", "week", "month", "quarter", "year"):
        floor = pc.RoundTemporalOptions(unit=u, week_starts_monday=True)
        out[f"ts_{u}s"] = compute("floor_temporal", ts, floor)
    for u in ("day", "week", "month", "quarter", "year"):
        floor = pc.RoundTemporalOptions(unit=u, week_starts_monday=True)
        out[f"day_{u}s"] = compute("floor_temporal", day, floor)
    return out


def check(got: pa.Table, t: pa.Table) -> None:
    for name, want in expected(t).items():
        column = got[name]
        assert column.type == want.type, name
        assert column.to_pylist() == want.to_pylist(), name


def test_functions_case_and_in_match_pyarrow_on_static_and_ticking_tables():
    s = km.connect(":memory:", manual=True)
    t = data()
    exprs = [f"{name} = {text}" for name, text in EXPRESSIONS.items()]

    check(pa.table(s.static_table(t).update(*exprs)), t)

    src = s.input_table(COLUMNS)
    live = src.update(*exprs)
    src.append(t.slice(0, 3))
    s.step()
    src.append(t.slice(3))
    s.step()
    check(pa.table(live), t)
