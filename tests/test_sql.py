"""SQL over stored and passed tables, each result checked against pyarrow over the same rows."""

from datetime import UTC, date, datetime

import kermodes as km
import pyarrow as pa
import pyarrow.compute as pc
import pytest
from test_history import DAYS, store_with_past_days

TODAY = datetime.now(UTC).date()


def every_row(today: dict) -> pa.Table:
    """The oracle's input: every past day's rows and today's, as `(Date, sym, px)`."""
    days = {**{date.fromisoformat(d): c for d, c in DAYS.items()}, TODAY: today}
    rows = [
        (day, sym, px) for day, c in days.items() for sym, px in zip(c["sym"], c["px"])
    ]
    return pa.table(
        {
            "Date": [r[0] for r in rows],
            "sym": [r[1] for r in rows],
            "px": [r[2] for r in rows],
        }
    )


def rows(table: km.Table | pa.Table, columns: list[str]) -> list[tuple]:
    t = pa.table(table)
    return sorted(zip(*(t[c].to_pylist() for c in columns)))


def test_a_query_reads_history_once_and_ticks_today(tmp_path):
    s = store_with_past_days(tmp_path)
    s.merge_history("md.ticks", "2026-11-01")
    w = s.writer("md.ticks", source="feed")
    today = {"sym": ["a", "d"], "px": [7.0, 0.1]}
    w.write(pa.table(today))
    board = s.sql(
        "SELECT sym, count(*) AS n, sum(px) AS total, max(px) - min(px) AS spread "
        "FROM md.ticks WHERE Date >= '2026-11-01' AND px > 0.5 "
        "GROUP BY sym HAVING count(*) >= 2"
    )
    assert board.is_refreshing
    for more in ({}, {"sym": ["b", "d"], "px": [9.0, 2.0]}):
        if more:
            w.write(pa.table(more))
            today = {c: today[c] + more[c] for c in today}
        s.step()
        kept = every_row(today).filter(pc.field("px") > 0.5)
        want = (
            kept.group_by("sym")
            .aggregate([("px", "count"), ("px", "sum"), ("px", "max"), ("px", "min")])
            .filter(pc.field("px_count") >= 2)
        )
        assert 0 < want.num_rows < len(set(kept["sym"].to_pylist()))
        high, low = want["px_max"].to_pylist(), want["px_min"].to_pylist()
        assert rows(board, ["sym", "n", "total", "spread"]) == sorted(
            zip(
                want["sym"].to_pylist(),
                want["px_count"].to_pylist(),
                want["px_sum"].to_pylist(),
                [h - lo for h, lo in zip(high, low)],
            )
        )


def test_dates_before_today_are_static_and_read_only_their_files(tmp_path):
    s = store_with_past_days(tmp_path)
    s.merge_history("md.ticks", "2026-11-01")
    s.merge_history("md.ticks", "2026-11-02")
    query = "SELECT sym, px * 2 AS double FROM hist.md.ticks WHERE Date = '2026-11-02'"
    result = s.sql(query)
    assert not result.is_refreshing
    day = every_row({"sym": [], "px": []}).filter(
        pc.field("Date") == pa.scalar(date(2026, 11, 2))
    )
    assert rows(result, ["sym", "double"]) == sorted(
        zip(day["sym"].to_pylist(), [px * 2 for px in day["px"].to_pylist()])
    )
    plan = s.explain(query)
    assert "static" in plan and "ticking" not in plan
    # The date went with the scan: one of history's files, and of it one day's row groups.
    assert "scan hist.md.ticks where Date = '2026-11-02'" in plan
    assert "1 of 1 files, 1 of 2 row groups" in plan


def test_joins_read_passed_tables_and_today_ticking(tmp_path):
    s = km.connect(tmp_path, manual=True)
    s.create_table("md.ticks", {"sym": km.string(), "px": km.float64()})
    w = s.writer("md.ticks", source="feed")
    w.write(pa.table({"sym": ["a", "b", "z"], "px": [1.0, 2.0, 3.0]}))
    ref = pa.table({"sym": ["a", "b"], "px": [10.0, 20.0], "tier": [1, 2]})
    query = (
        "SELECT t.sym, t.px, r.px AS ref_px, tier FROM live.md.ticks t "
        "LEFT JOIN refs r ON t.sym = r.sym WHERE t.px > 1.5"
    )
    joined = s.sql(query, refs=s.static_table(ref))
    s.step()
    w.write(pa.table({"sym": ["a"], "px": [4.0]}))
    s.step()
    ticks = pa.table({"sym": ["a", "b", "z", "a"], "px": [1.0, 2.0, 3.0, 4.0]})
    want = ticks.filter(pc.field("px") > 1.5).join(
        ref.rename_columns(["sym", "ref_px", "tier"]), "sym", join_type="left outer"
    )
    columns = ["sym", "px", "ref_px", "tier"]
    assert rows(joined, columns) == rows(want, columns)
    plan = s.explain(query, refs=s.static_table(ref))
    assert "ticking" in plan and "scan refs: passed in" in plan


def test_what_a_query_cannot_say_is_refused_by_name(tmp_path):
    s = store_with_past_days(tmp_path)
    with pytest.raises(ValueError, match="say which dates"):
        s.sql("SELECT sym FROM md.ticks WHERE px > 1")
    with pytest.raises(ValueError, match="no table board"):
        s.sql("SELECT * FROM board")
    with pytest.raises(km.KermodesError, match="INNER JOIN"):
        s.sql("SELECT * FROM a INNER JOIN b ON a.k = b.k")
    with pytest.raises(ValueError, match="db.explain"):
        s.sql("EXPLAIN SELECT sym FROM live.md.ticks")
