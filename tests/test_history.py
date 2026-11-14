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
        s = km.local(path, manual=True)
        s.create_table("md.ticks", COLUMNS)
        s.writer("md.ticks", source="feed").write(pa.table(columns))
        del s
        gc.collect()
        partition = next((path / "weir").glob(f"**/{today}"))
        partition.rename(partition.with_name(day))
    return km.local(path, manual=True)


def rows(table: pa.Table) -> list[tuple]:
    return sorted(zip(*(table[c].to_pylist() for c in ["Date", "sym", "px"])))


def expected(keep) -> list[tuple]:
    """The oracle: every day's rows as `(Date, sym, px)`, those `keep` keeps."""
    every = [
        (date.fromisoformat(day), sym, px)
        for day, c in DAYS.items()
        for sym, px in zip(c["sym"], c["px"])
    ]
    return sorted(row for row in every if keep(*row))


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
    history = s.historical_table("md.ticks")

    second = history.where("Date = '2026-11-02' and px > 1")
    assert rows(pa.table(second)) == expected(
        lambda day, sym, px: day == date(2026, 11, 2) and px > 1
    )
    # 2026-11-03 is not merged yet: it comes from weir, so history has no gap.
    assert rows(pa.table(history.table())) == expected(lambda day, sym, px: px > 0)
    assert rows(pa.table(history.where("sym = 'a'"))) == expected(
        lambda day, sym, px: sym == "a"
    )

    # Read only from the catalog and the files' footers.
    assert history.explain("Date = '2026-11-02'").startswith(
        "md.ticks 2026-11-02 to 2026-11-02: 1 of 1 files, 1 of 2 row groups, "
    )
    whole = history.explain()
    assert "2 of 2 row groups" in whole and "1 date from weir" in whole


def test_today_is_not_history_yet(tmp_path):
    s = km.local(tmp_path, manual=True)
    s.create_table("md.ticks", COLUMNS)
    today = datetime.now(UTC).date().isoformat()
    with pytest.raises(km.KermodesError, match="still being written"):
        s.merge_history("md.ticks", today)
    with pytest.raises(km.KermodesError, match="no table"):
        s.historical_table("md.nope")
