"""bench/stream.py's stream target in Deephaven Community: cycle time, sustained rate, memory.

    uv run --no-project --python 3.12 --with deephaven-server --with numpy --with pyarrow \
        python bench/deephaven_stream.py [--seconds 60] [--rate N]

Needs Java 17+ on the path. The same random walk arrives in 100k-row batches through
`InputTable.add`, at `--rate` rows/s or (`--rate 0`) as fast as cycles finish; each cycle is
timed from `add` until the update graph has finished it (its exclusive lock), downstream
tables included. Converting a batch to a Deephaven table is the producer's work: untimed. The
pipeline matches stream.py. Memory is the JVM heap in use after a full collection, at the end
minus before the first batch.
"""

import argparse
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import data  # bench/ is not a package: found through sys.path above

BATCH = 100_000
MB = 1 << 20


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds", type=int, default=60)
    parser.add_argument(
        "--rate", type=int, default=1_000_000, help="rows/s; 0: unthrottled"
    )
    args = parser.parse_args()

    from deephaven_server import Server

    Server(port=10_000, jvm_args=["-Xmx16g"]).start()

    # Deephaven's modules import only once the server runs.
    import jpy
    from deephaven import agg, input_table
    from deephaven import dtypes as dht
    from deephaven.arrow import to_table
    from deephaven.update_graph import exclusive_lock

    runtime = jpy.get_type("java.lang.Runtime").getRuntime()
    system = jpy.get_type("java.lang.System")

    def heap_mb() -> float:
        for _ in range(3):
            system.gc()
        return (runtime.totalMemory() - runtime.freeMemory()) / MB

    batches = args.seconds * 1_000_000 // BATCH
    stream = data.ticks(batches * BATCH)
    before = heap_mb()

    ticks = input_table(
        {"sym": dht.string, "price": dht.double, "size": dht.int64, "ts": dht.Instant}
    )
    live = ticks.where("price > 100").update(
        ["notional = price * size", "signed = size * (price - 100)"]
    )
    latest = live.last_by("sym")
    stats = live.agg_by(
        [agg.count_("n"), agg.avg("avg=price"), agg.std("vol=price")], by="sym"
    )

    interval = BATCH / args.rate if args.rate else 0.0
    cycles, late = [], 0
    start = next_at = time.perf_counter()
    for index in range(batches):
        batch = to_table(stream.slice(index * BATCH, BATCH))
        if interval:
            wait = next_at - time.perf_counter()
            if wait > 0:
                time.sleep(wait)
            else:
                late += 1
            next_at += interval
        begin = time.perf_counter()
        ticks.add(batch)
        # add() returns once the input table has the rows; the lock waits for the rest of the
        # cycle, downstream tables included.
        with exclusive_lock(ticks):
            pass
        cycles.append((time.perf_counter() - begin) * 1000)
        del batch
    elapsed = time.perf_counter() - start
    growth = heap_mb() - before

    cycles.sort()
    print(
        f"{len(cycles)} cycles of {BATCH:,} rows, rate "
        f"{'unthrottled' if not args.rate else f'{args.rate:,}/s'}: "
        f"p50 {statistics.median(cycles):.1f} ms, p99 {cycles[int(len(cycles) * 0.99)]:.1f} ms, "
        f"max {cycles[-1]:.1f} ms; engine {batches * BATCH / (sum(cycles) / 1000):,.0f} rows/s, "
        f"wall {batches * BATCH / elapsed:,.0f} rows/s"
        f"{f' ({late} batches late)' if interval else ''}"
    )
    print(
        f"memory: heap grew {growth:,.0f} MB ({latest.size:,} latest rows, "
        f"{stats.size:,} groups)"
    )


if __name__ == "__main__":
    main()
