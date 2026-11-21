"""SQL over stored and passed tables, each result checked against pyarrow over the same rows."""

import kermodes as km
import pyarrow as pa
import pyarrow.compute as pc
import pytest
from test_history import DAYS, store_with_past_days


def every_row(today: dict) -> pa.Table:
    """The oracle's input: every past day's rows and today's, as `(sym, px)`."""
    days = [*DAYS.values(), today]
    return pa.table(
        {
            "sym": [sym for c in days for sym in c["sym"]],
            "px": pa.array([px for c in days for px in c["px"]], pa.float64()),
        }
    )


def rows(table: km.Table | pa.Table, columns: list[str]) -> list[tuple]:
    t = pa.table(table)
    return sorted(zip(*(t[c].to_pylist() for c in columns)))


BOARD = (
    "SELECT sym, count(*) AS n, sum(px) AS total, max(px) - min(px) AS spread "
    "FROM md.ticks WHERE px > 0.5 GROUP BY sym HAVING count(*) >= 2"
)


def test_a_query_reads_history_and_today_static_or_ticking(tmp_path):
    s = store_with_past_days(tmp_path)
    s.merge_history("md.ticks", "2026-11-01")
    w = s.writer("md.ticks", source="feed")
    today = {"sym": ["a", "d"], "px": [7.0, 0.1]}
    w.write(pa.table(today))
    board = s.live_sql(BOARD)
    assert board.is_refreshing
    # A live table starts when first used: history's groups at once, today's rows at a manual
    # session's next step.
    assert rows(board, ["sym", "n"]) == [("a", 2), ("b", 2)]
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
        columns = ["sym", "n", "total", "spread"]
        assert rows(board, columns) == sorted(
            zip(
                want["sym"].to_pylist(),
                want["px_count"].to_pylist(),
                want["px_sum"].to_pylist(),
                [h - lo for h, lo in zip(high, low)],
            )
        )
        # The same query, static, reads the same rows as they are now.
        static = s.sql(BOARD)
        assert not static.is_refreshing
        assert rows(static, columns) == rows(board, columns)


def test_a_query_reads_only_what_it_needs_of_the_files(tmp_path):
    s = store_with_past_days(tmp_path)
    s.merge_history("md.ticks", "2026-11-01")
    s.merge_history("md.ticks", "2026-11-02")
    result = s.sql("SELECT sym, px * 2 AS double FROM md.ticks WHERE px > 3.5")
    assert not result.is_refreshing
    kept = every_row({"sym": [], "px": []}).filter(pc.field("px") > 3.5)
    assert rows(result, ["sym", "double"]) == sorted(
        zip(kept["sym"].to_pylist(), [px * 2 for px in kept["px"].to_pylist()])
    )
    plan = result.explain()
    assert "static" in plan and "ticking" not in plan
    # The condition went with the scan: one file, and of it the row group its prices allow.
    assert "scan md.ticks where px > 3.5" in plan
    assert "1 of 1 files, 1 of 2 row groups" in plan


def test_joins_read_passed_tables_and_today_ticking(tmp_path):
    s = km.connect(tmp_path, manual=True)
    s.create_table("md.ticks", {"sym": km.string(), "px": km.float64()})
    w = s.writer("md.ticks", source="feed")
    w.write(pa.table({"sym": ["a", "b", "z"], "px": [1.0, 2.0, 3.0]}))
    ref = pa.table({"sym": ["a", "b"], "px": [10.0, 20.0], "tier": [1, 2]})
    query = (
        "SELECT t.sym, t.px, r.px AS ref_px, tier FROM md.ticks t "
        "LEFT JOIN refs r ON t.sym = r.sym WHERE t.px > 1.5"
    )
    joined = s.live_sql(query, refs=s.static_table(ref))
    assert len(joined) == 0
    s.step()
    w.write(pa.table({"sym": ["a"], "px": [4.0]}))
    s.step()
    ticks = pa.table({"sym": ["a", "b", "z", "a"], "px": [1.0, 2.0, 3.0, 4.0]})
    want = ticks.filter(pc.field("px") > 1.5).join(
        ref.rename_columns(["sym", "ref_px", "tier"]), "sym", join_type="left outer"
    )
    columns = ["sym", "px", "ref_px", "tier"]
    assert rows(joined, columns) == rows(want, columns)
    plan = joined.explain()
    assert "ticking" in plan and ": in memory" in plan


def test_a_passed_plan_is_part_of_the_query(tmp_path):
    s = store_with_past_days(tmp_path)
    s.merge_history("md.ticks", "2026-11-01")
    high = s.table("md.ticks").where("px > 3.5")
    result = s.sql("SELECT sym, count(*) AS n FROM high GROUP BY sym", high=high)
    kept = every_row({"sym": [], "px": []}).filter(pc.field("px") > 3.5)
    want = kept.group_by("sym").aggregate([("px", "count")])
    assert rows(result, ["sym", "n"]) == rows(
        want.rename_columns(["sym", "n"]), ["sym", "n"]
    )
    # Its filter still reaches the files.
    assert "1 of 1 files, 0 of 1 row groups" in result.explain()


def test_what_a_query_cannot_say_is_refused_by_name(tmp_path):
    s = store_with_past_days(tmp_path)
    with pytest.raises(ValueError, match="no table board"):
        s.sql("SELECT * FROM board")
    with pytest.raises(km.KermodesError, match="INNER JOIN"):
        s.sql("SELECT * FROM a INNER JOIN b ON a.k = b.k")
    with pytest.raises(km.KermodesError, match="the table name live.md.ticks"):
        s.sql("SELECT sym FROM live.md.ticks")
    with pytest.raises(ValueError, match=r"explain\(\)"):
        s.sql("EXPLAIN SELECT sym FROM md.ticks")
    # Columns are checked when the query is given, before anything is read.
    with pytest.raises(km.KermodesError, match="nope"):
        s.sql("SELECT nope FROM md.ticks")
