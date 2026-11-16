"""A stored table's columns changing (architecture.md §5.4): `create_table` with the new columns
renames and adds, and what was written under the old ones reads under the new."""

from datetime import UTC, date, datetime

import kermodes as km
import pyarrow as pa
import pytest
from test_history import DAYS, store_with_past_days


def rows(table, columns) -> list[tuple]:
    t = pa.table(table)
    return sorted(zip(*(t[c].to_pylist() for c in columns)))


def test_history_weir_and_today_read_under_the_new_columns(tmp_path):
    s = store_with_past_days(tmp_path)
    s.merge_history("md.ticks", "2026-11-01")  # a history file under the old columns
    before = s.live_table("md.ticks")
    # 2026-11-02 and 11-03 stay in weir, in segments under the old columns.
    new = {
        "symbol": km.string(was="sym"),
        "px": km.float64(),
        "size": km.int64(),
    }
    s.create_table("md.ticks", new)
    s.create_table("md.ticks", new)  # declared again: nothing changes
    s.writer("md.ticks", source="feed").write(
        pa.table({"symbol": ["d"], "px": [7.0], "size": [3]})
    )
    # Merging after the change rewrites the 11-01 file beside 11-02, both by column ID.
    assert s.merge_history("md.ticks", "2026-11-02")["replaced"] == 1
    ticks = s.table("md.ticks").where("Date >= '2026-11-01'")
    s.step()
    today = datetime.now(UTC).date()
    past = [
        (date.fromisoformat(day), sym, px, None)
        for day, c in DAYS.items()
        for sym, px in zip(c["sym"], c["px"])
    ]
    assert rows(ticks, ["Date", "symbol", "px", "size"]) == sorted(
        past + [(today, "d", 7.0, 3)]
    )
    # A table opened before the change keeps its columns; the rename follows the column.
    assert pa.table(before).column_names == ["sym", "px"]
    assert rows(before, ["sym", "px"]) == [("d", 7.0)]


def test_changes_that_are_not_compatible_are_refused_by_name(tmp_path):
    s = km.local(tmp_path, manual=True)
    s.create_table("md.ticks", {"sym": km.string(), "q": km.int32()})
    with pytest.raises(
        km.KermodesError, match="column `q` the columns given leave out"
    ):
        s.create_table("md.ticks", {"sym": km.string()})
    with pytest.raises(km.KermodesError, match="making it utf8 is not a compatible"):
        s.create_table("md.ticks", {"sym": km.string(), "q": km.string()})
    # Lossless widening is compatible.
    s.create_table("md.ticks", {"sym": km.string(), "q": km.int64()})
    assert km.float64(was=["a", "b"]) == "float64 was a, b"
