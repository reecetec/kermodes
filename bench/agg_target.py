"""The engine.md static agg_by target: 100M rows, 10k int64 groups, sum/mean/std of a float64,
in Kermodes and Polars (target: Kermodes within 2x of Polars' group_by).

    uv run --no-sync --with numpy --with polars --with psutil python bench/agg_target.py [--rows N]

Each engine runs in its own process on the same seeded data: the best of three runs, and the
peak RSS above the loaded input during the first (sampled every 2 ms).
"""

import argparse
import json
import subprocess
import sys
import threading
import time

REPEATS = 3
MB = 1 << 20


def worker(engine: str, rows: int) -> dict:
    import numpy as np
    import pyarrow as pa
    from memory import resident

    rng = np.random.default_rng(7)
    table = pa.table(
        {"key": rng.integers(0, 10_000, rows), "value": rng.standard_normal(rows)}
    )
    if engine == "kermodes":
        import kermodes as km

        source = km.local(manual=True).static_table(table)

        def run():
            return source.agg_by(
                "key", total="sum(value)", avg="mean(value)", vol="std(value)"
            )
    else:
        import polars as pl

        frame = pl.from_arrow(table)

        def run():
            return frame.group_by("key").agg(
                total=pl.col("value").sum(),
                avg=pl.col("value").mean(),
                vol=pl.col("value").std(),
            )

    before = resident()
    peak, done = before, threading.Event()

    def sample() -> None:
        nonlocal peak
        while not done.is_set():
            peak = max(peak, resident())
            time.sleep(0.002)

    sampler = threading.Thread(target=sample)
    sampler.start()
    times = []
    for index in range(REPEATS):
        start = time.perf_counter()
        result = run()
        times.append((time.perf_counter() - start) * 1000)
        if index == 0:
            done.set()
            sampler.join()
        groups = len(result)
        del result
    return {"ms": min(times), "peak_mb": (peak - before) / MB, "groups": groups}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=100_000_000)
    parser.add_argument("--worker", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker:
        print(json.dumps(worker(args.worker, args.rows)))
        return
    results = {}
    for engine in ["kermodes", "polars"]:
        out = subprocess.run(
            [sys.executable, __file__, "--worker", engine, "--rows", str(args.rows)],
            capture_output=True,
            text=True,
            check=True,
        ).stdout
        results[engine] = json.loads(out)
        r = results[engine]
        print(
            f"{engine}: {r['ms']:.1f} ms, peak {r['peak_mb']:.0f} MB, {r['groups']:,} groups"
        )
    ratio = results["kermodes"]["ms"] / results["polars"]["ms"]
    print(f"Kermodes / Polars: {ratio:.2f}x (target: within 2x)")


if __name__ == "__main__":
    main()
