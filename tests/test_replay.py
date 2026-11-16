"""A store's past dates replayed through a graph, against a plain recomputation of the rows
each cycle has fed: one date merged into history, one still in weir."""

import gc
from datetime import UTC, datetime

import kermodes as km
import pyarrow as pa
import pytest

COLUMNS = {"ts": km.timestamp(), "sym": km.string(), "px": km.float64()}


def at(day: str, second: float) -> datetime:
    return datetime.fromisoformat(f"{day}T09:30:00+00:00").replace(
        second=int(second), microsecond=int(second % 1 * 1e6)
    )


# Written out of event-time order, with ties, as feeds deliver them
DAYS = {
    "2026-11-01": [
        (at("2026-11-01", 3.0), "a", 1.0),
        (at("2026-11-01", 1.0), "b", 2.0),
        (at("2026-11-01", 1.0), "a", 3.0),
        (at("2026-11-01", 2.25), "b", 4.0),
    ],
    "2026-11-02": [
        (at("2026-11-02", 0.5), "a", 5.0),
        (at("2026-11-02", 0.5), "c", 6.0),
        (at("2026-11-02", 7.0), "b", 7.0),
    ],
}


def store_with_past_days(path):
    """`DAYS` the way weir holds them after a restart: written today, then each day's partition
    renamed to the date it stands for. The first is merged into history."""
    today = datetime.now(UTC).date().isoformat()
    for day, rows in DAYS.items():
        s = km.local(path, manual=True)
        s.create_table("md.ticks", COLUMNS)
        ts, sym, px = zip(*rows)
        s.writer("md.ticks", source="feed").write(
            pa.table(
                {"ts": pa.array(ts, pa.timestamp("ns", "UTC")), "sym": sym, "px": px}
            )
        )
        del s
        gc.collect()
        partition = next((path / "weir").glob(f"**/{today}"))
        partition.rename(partition.with_name(day))
    s = km.local(path, manual=True)
    s.merge_history("md.ticks", "2026-11-01")
    return s


def due_by(time_ns: int) -> list[tuple]:
    """The oracle: every row timed at or before `time_ns`, as `(sym, n, total)` per symbol."""
    by_sym: dict[str, tuple[int, float]] = {}
    for day in DAYS.values():
        for ts, sym, px in day:
            if ts.timestamp() * 1e9 <= time_ns:
                n, total = by_sym.get(sym, (0, 0.0))
                by_sym[sym] = (n + 1, total + px)
    return sorted((sym, n, total) for sym, (n, total) in by_sym.items())


def totals(table) -> list[tuple]:
    t = pa.table(table)
    return sorted(zip(t["sym"].to_pylist(), t["n"].to_pylist(), t["total"].to_pylist()))


def test_a_replay_ticks_through_past_days_in_event_time_order(tmp_path):
    s = store_with_past_days(tmp_path)
    replay = s.replay("2026-11-01", "2026-11-02", step="1s")
    ticks = replay.live_table("md.ticks")
    by_sym = ticks.agg_by("sym", n="count(*)", total="sum(px)")
    times = []
    while (time := replay.step()) is not None:
        times.append(time)
        assert totals(by_sym) == due_by(time)
    # One cycle per second with rows, on whole seconds, across both days (one merged, one in
    # weir); a night skipped, not stepped through
    seconds = [datetime.fromtimestamp(t / 1e9, UTC) for t in times]
    assert seconds == [
        at("2026-11-01", 1),
        at("2026-11-01", 3),
        at("2026-11-02", 1),
        at("2026-11-02", 7),
    ]
    assert (replay.cycles, replay.rows) == (4, 7)


def test_a_deployments_code_runs_on_a_replay_as_a_backtest(tmp_path):
    s = store_with_past_days(tmp_path)

    def latest(db):
        """A deployment's function, as it runs live."""
        db.create_table("md.ticks", COLUMNS)
        return db.live_table("md.ticks").last_by("sym")

    replay = s.replay("2026-11-01", "2026-11-02")
    board = latest(replay)
    replay.run()
    got = pa.table(board)
    assert sorted(zip(got["sym"].to_pylist(), got["px"].to_pylist())) == [
        ("a", 5.0),
        ("b", 7.0),
        ("c", 6.0),
    ]


def test_a_replay_reads_the_store_and_never_writes_it(tmp_path):
    s = store_with_past_days(tmp_path)
    replay = s.replay("2026-11-01", "2026-11-02")
    with pytest.raises(km.KermodesError, match="does not publish"):
        replay.publish("md.board", replay.live_table("md.ticks"), key="sym")
    with pytest.raises(km.KermodesError, match="other columns"):
        replay.create_table("md.ticks", {"sym": km.string()})
    with pytest.raises(km.KermodesError, match="only stored tables"):
        replay.create_table("md.other", COLUMNS)
    today = datetime.now(UTC).date().isoformat()
    with pytest.raises(ValueError, match="finished dates"):
        s.replay("2026-11-01", today)
    with pytest.raises(ValueError, match="finished dates"):
        s.replay("2026-11-02", "2026-11-01")
