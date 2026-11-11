"""bench/ticking.py's ticking operations in Deephaven Community: cycle time and memory.

    uv run --no-project --python 3.12 --with deephaven-server --with numpy --with pyarrow \
        python bench/deephaven_ticking.py [--cycles 100] [--batch 100000]

Needs Java 17+ on the path. Each operation runs in a fresh JVM: an append-only input table
`ticks` (and, for the as-of join, `quotes`) gains one batch per cycle through `InputTable.add`,
timed until the update graph has finished the cycle, downstream tables included. The as-of
join's two adds are two update cycles; Ursid appends both and runs one. Each batch is converted to
a Deephaven table before timing. Memory is the JVM heap in use after a full
collection, after the last cycle minus before the first.
"""

import argparse
import json
import statistics
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import data  # bench/ is not a package: found through sys.path above

OPERATIONS = [
    "input only",
    "where + update",
    "last_by",
    "agg_by",
    "natural_join",
    "as_of_join",
    "merge + agg_by",
]
WARM = 10
MB = 1 << 20


def worker(op: str, cycles: int, batch: int) -> dict:
    from deephaven_server import Server

    Server(port=10_000, jvm_args=["-Xmx12g"]).start()

    # Deephaven's modules import only once the server runs.
    import jpy
    from deephaven import agg, input_table, merge
    from deephaven import dtypes as dht
    from deephaven.arrow import to_table
    from deephaven.update_graph import exclusive_lock

    runtime = jpy.get_type("java.lang.Runtime").getRuntime()
    system = jpy.get_type("java.lang.System")

    def heap_mb() -> float:
        for _ in range(3):
            system.gc()
        return (runtime.totalMemory() - runtime.freeMemory()) / MB

    rows = cycles * batch
    ticks_data, quotes_data = data.ticks(rows), data.quotes(rows)
    before = heap_mb()

    stamp = dht.Instant
    ticks = input_table(
        {"sym": dht.string, "price": dht.double, "size": dht.int64, "ts": stamp}
    )
    quotes = input_table({"sym": dht.string, "bid": dht.double, "ts": stamp})
    refs = input_table({"sym": dht.string, "sector": dht.string}, key_cols="sym")
    refs.add(to_table(data.refs()))
    output = {
        "input only": lambda: ticks,
        "where + update": lambda: ticks.where("price > 100").update(
            "notional = price * size"
        ),
        "last_by": lambda: ticks.last_by("sym"),
        "agg_by": lambda: ticks.agg_by(
            [agg.count_("n"), agg.avg("avg=price"), agg.std("vol=price")], by="sym"
        ),
        "natural_join": lambda: ticks.natural_join(refs, on="sym"),
        "as_of_join": lambda: ticks.aj(quotes, on=["sym", "ts"]),
        "merge + agg_by": lambda: merge([ticks, ticks.where("price > 100")]).agg_by(
            [agg.count_("n"), agg.avg("avg=price")], by="sym"
        ),
    }[op]()

    times = []
    for cycle in range(cycles):
        # Converting the batch is the producer's work, not the engine's: untimed, and dropped
        # before memory is measured.
        tick_batch = to_table(ticks_data.slice(cycle * batch, batch))
        quote_batch = (
            to_table(quotes_data.slice(cycle * batch, batch))
            if op == "as_of_join"
            else None
        )
        start = time.perf_counter()
        ticks.add(tick_batch)
        if quote_batch is not None:
            quotes.add(quote_batch)
        # add() returns once the input table has the rows; the lock waits for the rest of the
        # cycle, downstream tables included.
        with exclusive_lock(ticks):
            pass
        times.append((time.perf_counter() - start) * 1000)
        del tick_batch, quote_batch
    memory = heap_mb() - before
    timed = sorted(times[WARM:])
    return {
        "p50": statistics.median(timed),
        "p99": timed[min(len(timed) - 1, round(0.99 * (len(timed) - 1)))],
        "rows_per_s": cycles * batch / (sum(times) / 1000),
        "memory_mb": memory,
        "rows": output.size,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cycles", type=int, default=100)
    parser.add_argument("--batch", type=int, default=100_000)
    parser.add_argument("--worker", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker:
        print(json.dumps(worker(args.worker, args.cycles, args.batch)))
        return
    print(f"\n{args.cycles} cycles x {args.batch:,} ticks, {data.SYMBOLS:,} symbols")
    print(f"(percentiles skip the first {WARM} cycles: JIT warm-up)\n")
    print("| operation | engine | cycle p50 ms | p99 ms | rows/s | memory MB | rows |")
    print("|---|---|---|---|---|---|---|")
    for op in OPERATIONS:
        command = [
            sys.executable,
            __file__,
            "--cycles",
            str(args.cycles),
            "--batch",
            str(args.batch),
            "--worker",
            op,
        ]
        out = subprocess.run(command, capture_output=True, text=True, check=True)
        r = json.loads(out.stdout.strip().splitlines()[-1])
        print(
            f"| {op} | deephaven | {r['p50']:.1f} | {r['p99']:.1f} | {r['rows_per_s']:,.0f} "
            f"| {r['memory_mb']:.0f} | {r['rows']:,} |"
        )


if __name__ == "__main__":
    main()
