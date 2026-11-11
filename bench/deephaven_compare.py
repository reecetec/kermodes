"""bench/compare.py's static operations in Deephaven Community, on the same data.

    uv run --no-project --python 3.12 --with deephaven-server --with numpy --with pyarrow \
        python bench/deephaven_compare.py [--rows N]

Needs Java 17+ on the path. Each operation runs three times and the fastest is reported, with the
result's row count and checksum, which should match compare.py's. Deephaven's `where` and joins
keep row redirections into their inputs instead of copying rows; `update` and the aggregations
compute their columns.

Memory is the JVM heap in use after a full collection (the process's RSS mostly reflects the heap
the JVM reserved): `inputs` is what loading the tables adds, `retained` what the first run of an
operation adds while its result is alive. Peak use during an operation is not measured.
"""

import argparse
import sys
import time
from pathlib import Path

from deephaven_server import Server

sys.path.insert(0, str(Path(__file__).parent))

import data  # bench/ is not a package: found through sys.path above

REPEATS = 3
MB = 1 << 20


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=data.ROWS)
    args = parser.parse_args()

    # Without memoization, a repeated operation recomputes rather than returning its cached table.
    Server(
        port=10_000, jvm_args=["-Xmx12g", "-DQueryTable.memoizeResults=false"]
    ).start()

    # Deephaven's modules import only once the server runs.
    import jpy
    from deephaven import agg, merge
    from deephaven.arrow import to_arrow, to_table

    runtime = jpy.get_type("java.lang.Runtime").getRuntime()
    system = jpy.get_type("java.lang.System")

    def heap_mb() -> float:
        for _ in range(3):
            system.gc()
        return (runtime.totalMemory() - runtime.freeMemory()) / MB

    generated = (data.ticks(args.rows), data.quotes(args.rows), data.refs())
    empty = heap_mb()
    ticks, quotes, refs = (to_table(t) for t in generated)
    inputs = heap_mb() - empty

    ops = {
        "where + update": (
            lambda: ticks.where("price > 100").update("notional = price * size"),
            ("notional", "sum"),
        ),
        "last_by": (lambda: ticks.last_by("sym"), ("price", "sum")),
        "agg_by": (
            lambda: ticks.agg_by(
                [agg.count_("n"), agg.avg("avg=price"), agg.std("vol=price")], by="sym"
            ),
            ("n", "sum"),
        ),
        "natural_join": (
            lambda: ticks.natural_join(refs, on="sym"),
            ("sector", "count"),
        ),
        "as_of_join": (lambda: ticks.aj(quotes, on=["sym", "ts"]), ("bid", "sum")),
        "merge": (lambda: merge([ticks, ticks]), ("price", "sum")),
        "merge + agg_by": (
            lambda: merge([ticks, ticks]).agg_by(
                [agg.count_("n"), agg.avg("avg=price")], by="sym"
            ),
            ("n", "sum"),
        ),
    }

    print(
        f"\n{args.rows:,} ticks, {data.SYMBOLS:,} symbols; best of {REPEATS}; "
        f"inputs {inputs:.0f} MB\n"
    )
    print("| operation | engine | ms | retained MB | rows | checksum |")
    print("|---|---|---|---|---|---|")
    for op, (run, (column, how)) in ops.items():
        before = heap_mb()
        start = time.perf_counter()
        result = run()
        times = [time.perf_counter() - start]
        retained = heap_mb() - before
        reduce = agg.count_("c") if how == "count" else agg.sum_(f"c={column}")
        if how == "count":
            checked = result.where(f"!isNull({column})").agg_by([reduce])
        else:
            checked = result.agg_by([reduce])
        total = to_arrow(checked)["c"][0].as_py() or 0
        rows = result.size
        del result, checked
        for _ in range(REPEATS - 1):
            start = time.perf_counter()
            run()
            times.append(time.perf_counter() - start)
        print(
            f"| {op} | deephaven | {min(times) * 1000:.1f} | {retained:.0f} | {rows:,} "
            f"| {float(total):.6g} |"
        )


if __name__ == "__main__":
    main()
