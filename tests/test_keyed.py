import random
import time

import kermodes as km
import pyarrow as pa
import pytest

QUOTES = {"sym": km.string(), "px": km.float64()}


def quotes(rows: list[tuple[str, float]]) -> pa.Table:
    return pa.table(
        {
            "sym": [s for s, _ in rows],
            "px": pa.array([p for _, p in rows], pa.float64()),
        }
    )


def as_dict(table) -> dict:
    t = pa.table(table)
    return dict(zip(t["sym"].to_pylist(), t["px"].to_pylist(), strict=True))


def test_keyed_tables_hold_each_keys_latest_row(tmp_path):
    s = km.connect(tmp_path, manual=True)
    s.create_table("md.quotes", QUOTES, key="sym")
    w = s.writer("md.quotes", source="feed")
    assert w.upsert(quotes([("a", 1.0), ("b", 2.0)]))
    live = s.live_table("md.quotes")
    w.upsert(quotes([("a", 10.0), ("c", 3.0)]))
    # A delete needs only the key.
    w.delete(pa.table({"sym": ["b"]}))
    s.step()
    assert as_dict(live) == {"a": 10.0, "c": 3.0}
    with pytest.raises(km.KermodesError, match="keyed table"):
        w.write(quotes([("d", 4.0)]))

    del s, w, live
    s = km.connect(tmp_path, manual=True)
    reopened = s.live_table("md.quotes")
    s.step()
    assert as_dict(reopened) == {"a": 10.0, "c": 3.0}


def until_equal(s, reader, table) -> None:
    """Steps until the published table reads as `table`: publishing writes from its own thread."""
    deadline = time.monotonic() + 10
    while True:
        s.step()
        if as_dict(reader) == as_dict(table):
            return
        assert time.monotonic() < deadline, (as_dict(reader), as_dict(table))
        time.sleep(0.005)


def test_a_published_table_follows_its_table_and_replaces_on_restart(tmp_path):
    s = km.connect(tmp_path, manual=True)
    s.create_table("md.ticks", {"sym": km.string(), "px": km.float64()})
    w = s.writer("md.ticks", source="feed")
    ticks = s.live_table("md.ticks")
    # Rows come, change, and go as each symbol's last price crosses 20.
    high = ticks.last_by("sym").where("px > 20")
    publication = s.publish("md.high", high, key="sym")
    reader = s.live_table("md.high")
    rng = random.Random(7)
    for _ in range(40):
        w.write(
            quotes(
                [(f"s{rng.randrange(8)}", float(rng.randrange(40))) for _ in range(3)]
            )
        )
        until_equal(s, reader, high)
    assert publication.error is None
    publication.stop()

    # A new publication replaces the stored table: keys only the old one held are gone.
    s.create_table("md.other", QUOTES)
    other = s.live_table("md.other")
    s.writer("md.other").write(quotes([("zz", 1.0)]))
    s.step()
    replacing = s.publish("md.high", other, key="sym")
    until_equal(s, reader, other)
    assert as_dict(reader) == {"zz": 1.0}
    del replacing


def test_changes_are_a_keyed_tables_log(tmp_path):
    s = km.connect(tmp_path, manual=True)
    s.create_table("md.quotes", QUOTES, key="sym")
    w = s.writer("md.quotes")
    changes = s.changes("md.quotes")
    w.upsert(quotes([("a", 1.0), ("b", 2.0)]))
    w.delete(pa.table({"sym": ["a"]}))
    w.upsert(quotes([("b", 3.0)]))
    s.step()
    log = pa.table(changes)
    assert log.column_names == ["Op", "sym", "px"]
    assert list(zip(*(log[c].to_pylist() for c in log.column_names), strict=True)) == [
        ("upsert", "a", 1.0),
        ("upsert", "b", 2.0),
        ("delete", "a", None),
        ("upsert", "b", 3.0),
    ]
    s.create_table("md.ticks", QUOTES)
    with pytest.raises(ValueError, match="append table"):
        s.changes("md.ticks")


def test_a_keyed_stored_table_is_its_latest_state(tmp_path):
    s = km.connect(tmp_path, manual=True)
    s.create_table("md.quotes", QUOTES, key="sym")
    w = s.writer("md.quotes")
    w.upsert(quotes([("a", 1.0), ("b", 20.0), ("c", 30.0)]))
    # No dates to say: a keyed table is its latest state.
    rich = s.table("md.quotes").where("px > 10")
    s.step()
    assert as_dict(rich) == {"b": 20.0, "c": 30.0}
    w.upsert(quotes([("a", 40.0), ("b", 2.0)]))
    w.delete(pa.table({"sym": ["c"]}))
    s.step()
    assert as_dict(rich) == {"a": 40.0}
