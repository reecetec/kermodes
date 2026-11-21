"""A stored table's columns changing (architecture.md §5.4): `create_table` with the new columns
renames and adds, and what was written under the old ones reads under the new."""

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
    # A live table starts when first used: this one reads under the old columns, history at
    # once (2026-11-02 and 11-03 from weir), today at the next step.
    assert len(before) == sum(len(c["px"]) for c in DAYS.values())
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
    ticks = s.table("md.ticks")
    s.step()
    past = [
        (sym, px, None)
        for c in DAYS.values()
        for sym, px in zip(c["sym"], c["px"], strict=True)
    ]
    assert rows(ticks, ["symbol", "px", "size"]) == sorted(past + [("d", 7.0, 3)])
    # A table opened before the change keeps its columns; the rename follows the column.
    assert pa.table(before).column_names == ["sym", "px"]
    assert rows(before, ["sym", "px"]) == sorted(
        [(sym, px) for sym, px, _ in past] + [("d", 7.0)]
    )


def test_changes_that_are_not_compatible_are_refused_by_name(tmp_path):
    s = km.connect(tmp_path, manual=True)
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


def test_int16_columns_store_as_int16_and_widen(tmp_path):
    s = km.connect(tmp_path, manual=True)
    s.create_table("md.lots", {"n": km.int64(), "lot": km.int16()})
    s.writer("md.lots", source="feed").write(
        pa.table(
            {
                "n": pa.array([1, 2], pa.int64()),
                "lot": pa.array([-32768, 7], pa.int16()),
            }
        )
    )
    lots = s.table("md.lots")
    assert pa.table(lots).schema.field("lot").type == pa.int16()
    assert rows(lots, ["n", "lot"]) == [(1, -32768), (2, 7)]
    # int16 widens to int32; older segments read in the wider type.
    s.create_table("md.lots", {"n": km.int64(), "lot": km.int32()})
    widened = s.table("md.lots")
    assert pa.table(widened).schema.field("lot").type == pa.int32()
    assert rows(widened, ["n", "lot"]) == [(1, -32768), (2, 7)]
    with pytest.raises(km.KermodesError, match="making it int16 is not a compatible"):
        s.create_table("md.lots", {"n": km.int64(), "lot": km.int16()})
