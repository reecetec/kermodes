"""Ticking operators in Ursid (compare bench/deephaven_ticking.py): cycle time and memory.

    uv run --no-sync --with maturin maturin develop --release -m crates/ursid-py/Cargo.toml
    uv run --no-sync --with numpy --with psutil python bench/ticking.py \
        [--cycles 100] [--batch 100000]

Each operation runs alone in its own process: an append-only `ticks` table (and, for the as-of
join, `quotes`) gains one batch per cycle from bench/data.py, and each cycle is timed from
`step()` to its return. `refs` is a keyed table loaded before the first cycle. Each batch is
copied before it is appended and the copy dropped, so the engine's retained memory shows as RSS
growth (Ursid otherwise keeps the producer's Arrow buffers without copying).
"""

import argparse
import gc
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
MB = 1 << 20


def worker(op: str, cycles: int, batch: int) -> dict:
    import psutil
    import pyarrow as pa
    import pyarrow.compute as pc
    import ursid as us

    rows = cycles * batch
    ticks_data, quotes_data = data.ticks(rows), data.quotes(rows)
    gc.collect()
    process = psutil.Process()
    before = process.memory_info().rss

    s = us.local(manual=True)
    stamp = us.timestamp()
    ticks = s.input_table(
        {"sym": us.string(), "price": us.float64(), "size": us.int64(), "ts": stamp}
    )
    quotes = s.input_table({"sym": us.string(), "bid": us.float64(), "ts": stamp})
    refs = s.input_table({"sym": us.string(), "sector": us.string()}, key="sym")
    refs.upsert(data.refs())
    s.step()
    output = {
        "input only": lambda: ticks,
        "where + update": lambda: ticks.where("price > 100").update(
            "notional = price * size"
        ),
        "last_by": lambda: ticks.last_by("sym"),
        "agg_by": lambda: ticks.agg_by(
            "sym", n="count(*)", avg="mean(price)", vol="std(price)"
        ),
        "natural_join": lambda: ticks.natural_join(refs, "sym"),
        "as_of_join": lambda: ticks.as_of_join(quotes, "sym", "ts"),
        "merge + agg_by": lambda: us.merge(ticks, ticks.where("price > 100")).agg_by(
            "sym", n="count(*)", avg="mean(price)"
        ),
    }[op]()

    def copy(table: pa.Table, start: int) -> pa.Table:
        return pc.take(table, pa.array(range(start, start + batch)))

    times = []
    for cycle in range(cycles):
        ticks.append(copy(ticks_data, cycle * batch))
        if op == "as_of_join":
            quotes.append(copy(quotes_data, cycle * batch))
        start = time.perf_counter()
        s.step()
        times.append((time.perf_counter() - start) * 1000)
    gc.collect()
    times.sort()
    return {
        "p50": statistics.median(times),
        "p99": times[min(len(times) - 1, round(0.99 * (len(times) - 1)))],
        "rows_per_s": cycles * batch / (sum(times) / 1000),
        "memory_mb": (process.memory_info().rss - before) / MB,
        "rows": len(output),
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
    print(f"\n{args.cycles} cycles x {args.batch:,} ticks, {data.SYMBOLS:,} symbols\n")
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
            f"| {op} | ursid | {r['p50']:.1f} | {r['p99']:.1f} | {r['rows_per_s']:,.0f} "
            f"| {r['memory_mb']:.0f} | {r['rows']:,} |"
        )


if __name__ == "__main__":
    main()
