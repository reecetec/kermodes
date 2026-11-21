import gc
from datetime import UTC, date, datetime

import kermodes as km
import pyarrow as pa
import pytest

COLUMNS = {"sym": km.string(), "px": km.float64()}

DAYS = {
    "2026-11-01": {"sym": ["a", "b", "a"], "px": [1.0, 2.0, 3.0]},
    "2026-11-02": {"sym": ["b", "a"], "px": [4.0, 0.5]},
    "2026-11-03": {"sym": ["c"], "px": [6.0]},
}


def store_with_past_days(path):
    """A store holding `DAYS` the way weir meets them after a restart: written today, then each
    day's partition renamed to the date it stands for."""
    today = datetime.now(UTC).date().isoformat()
    for day, columns in DAYS.items():
        s = km.connect(path, manual=True)
        s.create_table("md.ticks", COLUMNS)
        s.writer("md.ticks", source="feed").write(pa.table(columns))
        del s
        gc.collect()
        partition = next((path / "weir").glob(f"**/{today}"))
        partition.rename(partition.with_name(day))
    return km.connect(path, manual=True)


def rows(table) -> list[tuple]:
    t = pa.table(table)
    return sorted(zip(t["sym"].to_pylist(), t["px"].to_pylist(), strict=True))


def expected(keep) -> list[tuple]:
    """The oracle: every day's rows as `(sym, px)`, those `keep(day, sym, px)` keeps."""
    return sorted(
        (sym, px)
        for day, c in DAYS.items()
        for sym, px in zip(c["sym"], c["px"], strict=True)
        if keep(date.fromisoformat(day), sym, px)
    )


def test_past_days_merge_into_history_and_read_back_like_pyarrow(tmp_path):
    s = store_with_past_days(tmp_path)
    assert s.merge_history("md.ticks", "2026-11-01") == {
        "rows": 3,
        "files": 1,
        "replaced": 0,
    }
    # A small day joins the first in one file.
    assert s.merge_history("md.ticks", "2026-11-02")["replaced"] == 1
    # Again: nothing left in weir to move.
    assert s.merge_history("md.ticks", "2026-11-01")["rows"] == 0
    ticks = s.table("md.ticks")

    # One table: the merged file, and 2026-11-03 from weir, so history has no gap.
    assert pa.table(ticks).column_names == ["sym", "px"]
    assert rows(ticks) == expected(lambda *_: True)
    assert rows(ticks.where("sym = 'a'")) == expected(lambda day, sym, px: sym == "a")

    # A filter reaches the files: the 11-01 row group's prices rule it out.
    high = ticks.where("px > 3.5")
    assert rows(high) == expected(lambda day, sym, px: px > 3.5)
    plan = high.explain()
    assert "scan md.ticks where px > 3.5" in plan
    assert "1 of 1 files, 1 of 2 row groups" in plan and "1 date from weir" in plan


def test_a_client_without_lake_keys_reads_the_history_weir_vends(tmp_path, monkeypatch):
    monkeypatch.delenv("KERMODES_LAKE", raising=False)
    s = store_with_past_days(tmp_path)
    s.merge_history("md.ticks", "2026-11-01")
    s.merge_history("md.ticks", "2026-11-02")
    server = s.serve_store()
    # Connected over Flight, the client is told where each history file is.
    con = km.connect(server.url, manual=True)
    assert rows(con.table("md.ticks")) == expected(lambda *_: True)


def test_today_is_not_history_yet(tmp_path):
    s = km.connect(tmp_path, manual=True)
    s.create_table("md.ticks", COLUMNS)
    today = datetime.now(UTC).date().isoformat()
    with pytest.raises(km.KermodesError, match="still being written"):
        s.merge_history("md.ticks", today)
    with pytest.raises(km.KermodesError, match="no table"):
        s.table("md.nope")


def test_a_batch_job_writes_past_days_straight_into_history(tmp_path):
    s = km.connect(tmp_path, manual=True)
    s.create_table("md.ticks", COLUMNS)
    for day, columns in DAYS.items():
        # Columns by name, in any order.
        given = pa.table({"px": columns["px"], "sym": columns["sym"]})
        assert s.write_history("md.ticks", day, given)["rows"] == len(columns["px"])
    ticks = s.table("md.ticks")
    assert rows(ticks) == expected(lambda *_: True)
    assert "not merged yet" not in ticks.explain()
    # Written again, a day holds the new rows in place of the old; a static table reads the
    # data as it is when used.
    s.write_history("md.ticks", "2026-11-02", pa.table({"sym": ["z"], "px": [9.0]}))
    assert rows(ticks) == sorted(
        expected(lambda day, *_: day != date(2026, 11, 2)) + [("z", 9.0)]
    )
    by_sym = s.sql("SELECT sym, count(*) AS n FROM md.ticks GROUP BY sym ORDER BY sym")
    assert not by_sym.is_refreshing
    assert pa.table(by_sym).to_pylist() == [
        {"sym": "a", "n": 2},
        {"sym": "b", "n": 1},
        {"sym": "c", "n": 1},
        {"sym": "z", "n": 1},
    ]
    today = datetime.now(UTC).date().isoformat()
    with pytest.raises(km.KermodesError, match="still being written"):
        s.write_history("md.ticks", today, pa.table({"sym": ["a"], "px": [1.0]}))
    with pytest.raises(km.KermodesError, match="column `px` is"):
        s.write_history("md.ticks", "2026-11-04", pa.table({"sym": ["a"], "px": [1]}))


def test_a_stored_table_is_its_history_and_today_static_or_ticking(tmp_path):
    s = store_with_past_days(tmp_path)
    s.merge_history("md.ticks", "2026-11-01")  # 11-02 and 11-03 stay in weir, unmerged
    w = s.writer("md.ticks", source="feed")
    w.write(pa.table({"sym": ["d", "e"], "px": [7.0, 0.1]}))
    want = expected(lambda day, sym, px: px > 1)

    rich = s.live_table("md.ticks").where("px > 1")
    # A live table starts when first used: history at once, today's rows at a manual
    # session's next step.
    assert rows(rich) == want
    s.step()
    assert rows(rich) == sorted(want + [("d", 7.0)])
    w.write(pa.table({"sym": ["f"], "px": [8.0]}))
    s.step()
    assert rows(rich) == sorted(want + [("d", 7.0), ("f", 8.0)])
    plan = rich.explain()
    assert "ticking  scan md.ticks where px > 1" in plan and "then ticking" in plan

    # Static: the same rows as they are when used, no step needed.
    static = s.table("md.ticks").where("px > 1")
    assert not static.is_refreshing
    assert rows(static) == sorted(want + [("d", 7.0), ("f", 8.0)])
    w.write(pa.table({"sym": ["g"], "px": [9.0]}))
    assert rows(static) == sorted(want + [("d", 7.0), ("f", 8.0), ("g", 9.0)])
