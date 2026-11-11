import time

import kermodes as km
import pyarrow as pa

TICKS = {
    "sym": km.string(),
    "price": km.float64(),
    "size": km.int64(),
    "ts": km.timestamp(),
}


def test_a_websocket_feed_ticks_into_a_stored_table(tmp_path):
    feed = km.demo.ticker(rate=2_000, symbols=20)
    s = km.local(tmp_path, manual=True)
    s.create_table("md.ticks", TICKS)
    live = s.live_table("md.ticks")
    ingest = s.ingest_websocket(feed.url, "md.ticks", flush="20ms")

    deadline = time.monotonic() + 20
    while ingest.stats.rows < 500:
        assert time.monotonic() < deadline, ingest.stats
        time.sleep(0.01)
    ingest.stop()
    feed.stop()
    stats = ingest.stats
    assert (stats.skipped, stats.failed) == (0, False), stats

    s.step()
    rows = pa.table(live)
    assert rows.num_rows == stats.rows
    assert rows.column("ts").null_count == 0
    assert set(rows.column("sym").to_pylist()) <= {f"S{i:04}" for i in range(20)}
