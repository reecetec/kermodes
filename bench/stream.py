"""The engine.md stream target in Kermodes (compare bench/deephaven_stream.py).

    uv run --no-sync --with maturin maturin develop --release -m crates/kermodes-py/Cargo.toml
    uv run --no-sync --with numpy --with psutil python bench/stream.py [--seconds 60] [--rate N]

A random walk over 10k symbols (bench/data.py) arrives in 100k-row batches, at `--rate` rows/s
(1M: one batch every 100 ms), or with `--rate 0` as fast as cycles finish, which measures the
rate sustained. Each batch is copied before it is appended (untimed), so memory growth shows
what the engine keeps. Each cycle is one timed `step()`: a manual session paced as the clock
would be. The pipeline: where -> update (two expressions) -> last_by(sym) and agg_by(sym)
(count, mean, std). Target: p99 cycle < 50 ms, memory growing with the source data only.
"""

import argparse
import gc
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

    import kermodes as km
    import pyarrow as pa
    import pyarrow.compute as pc
    from memory import resident

    batches = args.seconds * 1_000_000 // BATCH
    stream = data.ticks(batches * BATCH)
    gc.collect()
    before = resident()

    s = km.local(manual=True)
    ticks = s.input_table(
        {
            "sym": km.string(),
            "price": km.float64(),
            "size": km.int64(),
            "ts": km.timestamp(),
        }
    )
    live = ticks.where("price > 100").update(
        "notional = price * size", "signed = size * (price - 100)"
    )
    latest = live.last_by("sym")
    stats = live.agg_by("sym", n="count(*)", avg="mean(price)", vol="std(price)")

    interval = BATCH / args.rate if args.rate else 0.0
    cycles, appended, late = [], 0, 0
    start = next_at = time.perf_counter()
    for index in range(batches):
        batch = pc.take(stream, pa.array(range(index * BATCH, (index + 1) * BATCH)))
        appended += batch.nbytes
        if interval:
            wait = next_at - time.perf_counter()
            if wait > 0:
                time.sleep(wait)
            else:
                late += 1
            next_at += interval
        ticks.append(batch)
        del batch
        begin = time.perf_counter()
        s.step()
        cycles.append((time.perf_counter() - begin) * 1000)
    elapsed = time.perf_counter() - start
    gc.collect()
    growth = (resident() - before) / MB

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
        f"memory: grew {growth:,.0f} MB for {appended / MB:,.0f} MB of appended data "
        f"({len(latest):,} latest rows, {len(stats):,} groups)"
    )


if __name__ == "__main__":
    main()
