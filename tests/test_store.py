import gc
import subprocess
import sys

import kermodes as km
import pyarrow as pa
import pytest

COLUMNS = {"n": km.int64(), "sym": km.string()}


def rows(values: list[int]) -> pa.Table:
    return pa.table(
        {"n": pa.array(values, pa.int64()), "sym": [f"s{v}" for v in values]}
    )


def test_stored_tables_tick_and_survive_the_process(tmp_path):
    s = km.local(tmp_path, manual=True)
    s.create_table("md.ticks", COLUMNS)
    w = s.writer("md.ticks", source="feed")
    assert w.write(rows([1, 2])) and w.last_seq == 1
    live = s.live_table("md.ticks")
    assert w.write(rows([3]))
    # A resend of a sequence number already written changes nothing.
    assert not w.write(rows([99]), seq=1)
    s.step()
    assert pa.table(live)["n"].to_pylist() == [1, 2, 3]
    assert live.is_refreshing and s.tables == ["md.ticks"]
    with pytest.raises(km.KermodesError, match="rows arrive|does not accept"):
        live.append(rows([4]))

    del s, w, live
    gc.collect()
    s = km.local(tmp_path, manual=True)
    reopened = s.live_table("md.ticks")
    s.step()
    assert pa.table(reopened)["n"].to_pylist() == [1, 2, 3]
    w = s.writer("md.ticks", source="feed")
    assert w.last_seq == 2


def test_a_store_has_one_owner_and_clear_errors(tmp_path):
    s = km.local(tmp_path, manual=True)
    with pytest.raises(km.KermodesError, match="open in another process"):
        km.local(tmp_path)
    with pytest.raises(km.KermodesError, match="not a table name"):
        s.create_table("ticks", COLUMNS)
    with pytest.raises(km.KermodesError, match="no table md.nope"):
        s.live_table("md.nope")
    s.create_table("md.ticks", COLUMNS)
    with pytest.raises(km.KermodesError, match="already exists with columns"):
        s.create_table("md.ticks", {"n": km.float64()})
    first = s.writer("md.ticks")
    with pytest.raises(km.KermodesError, match="already has a writer"):
        s.writer("md.ticks")
    del first
    s.writer("md.ticks")
    with pytest.raises(km.KermodesError, match="keeps no tables"):
        km.local(manual=True).live_table("md.ticks")


_WRITE_UNTIL_KILLED = """
import sys
import pyarrow as pa
import kermodes as km

s = km.local(sys.argv[1], manual=True)
s.create_table("md.ticks", {"n": km.int64()})
w = s.writer("md.ticks", source="feed")
seq = (w.last_seq or 0) + 1
while True:
    w.write(pa.table({"n": pa.array([seq] * 100, pa.int64())}), seq=seq)
    print(seq, flush=True)  # acknowledged: durable
    seq += 1
"""


def test_a_killed_writer_loses_nothing_it_acknowledged(tmp_path):
    for _ in range(3):
        child = subprocess.Popen(
            [sys.executable, "-c", _WRITE_UNTIL_KILLED, str(tmp_path)],
            stdout=subprocess.PIPE,
            text=True,
        )
        assert child.stdout is not None
        acknowledged = 0
        while acknowledged < 30:
            acknowledged = int(child.stdout.readline())
        child.kill()  # SIGKILL: no clean shutdown, possibly mid-write
        child.wait()
        s = km.local(tmp_path, manual=True)
        table = s.live_table("md.ticks")
        s.step()
        n = pa.table(table)["n"].to_pylist()
        batches = n[::100]
        # Every batch from 1 up, whole, once each; at least everything acknowledged.
        assert batches == list(range(1, len(batches) + 1))
        assert len(n) == 100 * len(batches)
        assert len(batches) >= acknowledged
        del s, table
        gc.collect()
