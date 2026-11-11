"""Static operators in Ursid, DuckDB, and Polars on the same data (bench/data.py): time and memory.

    uv run --no-sync --with maturin maturin develop --release -m crates/ursid-py/Cargo.toml
    uv run --no-sync --with numpy --with duckdb --with polars --with psutil python bench/compare.py
    ... python bench/compare.py --threads 1 --rows 1000000

Use a release build of Ursid: `uv sync` installs a faster-to-build profile.

Each (engine, operation) runs in its own process, so memory figures do not mix engines:

- inputs: RSS the engine adds when it loads the tables (Polars and Ursid share the generated
  Arrow buffers; DuckDB copies them into its own storage);
- peak: the highest RSS seen during the operation's first run, above the RSS before it, sampled
  every 2 ms;
- retained: RSS held after the first run, with its result kept alive.

Times are the fastest of three runs. Every engine materializes its result (DuckDB into a
temporary table, so no rows become Python objects), and each result's row count and checksum
must agree across engines.
"""

import argparse
import gc
import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import data  # bench/ is not a package: found through sys.path above

REPEATS = 3
ENGINES = ["ursid", "duckdb", "polars"]
OPERATIONS = [
    "where + update",
    "last_by",
    "agg_by",
    "natural_join",
    "as_of_join",
    "merge",
    "merge + agg_by",
    "tail",
    "sort",
    "rolling",
    "select",
]
# Each result's row count plus a checksum: the sum (or non-null count) of one column.
CHECKS = {
    "where + update": ("notional", "sum"),
    "last_by": ("price", "sum"),
    "agg_by": ("n", "sum"),
    "natural_join": ("sector", "count"),
    "as_of_join": ("bid", "sum"),
    "merge": ("price", "sum"),
    "merge + agg_by": ("n", "sum"),
    "tail": ("price", "sum"),
    "sort": ("price", "sum"),
    "rolling": ("avg", "sum"),
    "select": ("notional", "sum"),
}
MB = 1 << 20


def operations(engine: str, ticks, quotes, refs):
    """The engine's tables and a function per operation returning its materialized result."""
    if engine == "ursid":
        import ursid as us

        s = us.local(manual=True)
        t, q, r = s.static_table(ticks), s.static_table(quotes), s.static_table(refs)
        return {
            "where + update": lambda: t.where("price > 100").update(
                "notional = price * size"
            ),
            "last_by": lambda: t.last_by("sym"),
            "agg_by": lambda: t.agg_by(
                "sym", n="count(*)", avg="mean(price)", vol="std(price)"
            ),
            "natural_join": lambda: t.natural_join(r, "sym"),
            "as_of_join": lambda: t.as_of_join(q, "sym", "ts"),
            "merge": lambda: us.merge(t, t),
            "merge + agg_by": lambda: us.merge(t, t).agg_by(
                "sym", n="count(*)", avg="mean(price)"
            ),
            "tail": lambda: t.tail(1000),
            "sort": lambda: t.sort("sym", "ts desc"),
            "select": lambda: t.select("sym", "notional = price * size"),
            "rolling": lambda: t.rolling(
                "sym", rows=100, avg="mean(price)", vol="std(price)"
            ),
        }
    if engine == "duckdb":
        import duckdb

        con = duckdb.connect()
        threads = os.environ.get("URSID_THREADS")
        if threads:
            con.execute(f"set threads={threads}")
        for name, table in [("ticks", ticks), ("quotes", quotes), ("refs", refs)]:
            con.register("source", table)
            con.execute(f"create table {name} as select * from source")
            con.unregister("source")
        sql = {
            "where + update": "select *, price * size as notional from ticks where price > 100",
            "last_by": "select sym, max_by(price, ts) as price, max_by(size, ts) as size, "
            "max(ts) as ts from ticks group by sym",
            "agg_by": "select sym, count(*) as n, avg(price) as avg, stddev_samp(price) as vol "
            "from ticks group by sym",
            "natural_join": "select t.*, r.sector from ticks t left join refs r on t.sym = r.sym",
            "as_of_join": "select t.*, q.bid from ticks t asof left join quotes q "
            "on t.sym = q.sym and t.ts >= q.ts",
            "merge": "select * from ticks union all select * from ticks",
            "merge + agg_by": "select sym, count(*) as n, avg(price) as avg from "
            "(select * from ticks union all select * from ticks) group by sym",
            "tail": "select * from ticks where rowid >= (select max(rowid) - 999 from ticks)",
            "sort": "select * from ticks order by sym, ts desc",
            "select": "select sym, price * size as notional from ticks",
            "rolling": "select *, avg(price) over w as avg, stddev_samp(price) over w as vol "
            "from ticks window w as (partition by sym order by rowid "
            "rows between 99 preceding and current row)",
        }

        def query(text: str):
            def run():
                con.execute(f"create or replace temp table result as {text}")
                return con

            return run

        return {op: query(text) for op, text in sql.items()}
    import polars as pl

    t, q, r = pl.from_arrow(ticks), pl.from_arrow(quotes), pl.from_arrow(refs)
    return {
        "where + update": lambda: (
            t.lazy()
            .filter(pl.col("price") > 100)
            .with_columns(notional=pl.col("price") * pl.col("size"))
            .collect()
        ),
        "last_by": lambda: t.group_by("sym").last(),
        "agg_by": lambda: t.group_by("sym").agg(
            n=pl.len(), avg=pl.col("price").mean(), vol=pl.col("price").std()
        ),
        "natural_join": lambda: t.join(r, on="sym", how="left"),
        "as_of_join": lambda: t.join_asof(q, on="ts", by="sym"),
        "merge": lambda: pl.concat([t, t]),
        "merge + agg_by": lambda: (
            pl.concat([t, t])
            .group_by("sym")
            .agg(n=pl.len(), avg=pl.col("price").mean())
        ),
        "tail": lambda: t.tail(1000),
        "sort": lambda: t.sort(["sym", "ts"], descending=[False, True]),
        "select": lambda: t.select("sym", notional=pl.col("price") * pl.col("size")),
        "rolling": lambda: t.with_columns(
            avg=pl.col("price").rolling_mean(100, min_samples=1).over("sym"),
            vol=pl.col("price").rolling_std(100, min_samples=1).over("sym"),
        ),
    }


def as_arrow(engine: str, result):
    import pyarrow as pa

    if engine == "duckdb":
        return result.execute("select * from result").to_arrow_table()
    if engine == "polars":
        return result.to_arrow()
    return pa.table(result)


def worker(engine: str, op: str, rows: int) -> dict:
    import pyarrow.compute as pc
    from memory import resident

    ticks, quotes, refs = data.ticks(rows), data.quotes(rows), data.refs()
    gc.collect()
    generated = resident()
    run = operations(engine, ticks, quotes, refs)[op]
    gc.collect()
    before = resident()

    peak, done = before, threading.Event()

    def sample() -> None:
        nonlocal peak
        while not done.is_set():
            peak = max(peak, resident())
            time.sleep(0.002)

    sampler = threading.Thread(target=sample)
    sampler.start()
    start = time.perf_counter()
    result = run()
    times = [time.perf_counter() - start]
    done.set()
    sampler.join()
    peak = max(peak, resident())
    gc.collect()
    retained = resident() - before

    column, how = CHECKS[op]
    values = as_arrow(engine, result)
    total = pc.count(values[column]) if how == "count" else pc.sum(values[column])
    checked = (values.num_rows, float(total.as_py() or 0))
    del result, values
    for _ in range(REPEATS - 1):
        start = time.perf_counter()
        run()
        times.append(time.perf_counter() - start)
    return {
        "ms": min(times) * 1000,
        "inputs_mb": (before - generated) / MB,
        "peak_mb": (peak - before) / MB,
        "retained_mb": retained / MB,
        "rows": checked[0],
        "checksum": checked[1],
    }


def close(a: float, b: float) -> bool:
    return abs(a - b) <= 1e-9 * max(abs(a), abs(b)) + 1e-6


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--threads", type=int, help="cores per engine (default: all)")
    parser.add_argument("--rows", type=int, default=data.ROWS)
    parser.add_argument(
        "--operation",
        action="append",
        choices=OPERATIONS,
        help="run only these (repeatable)",
    )
    parser.add_argument(
        "--worker", nargs=2, metavar=("ENGINE", "OPERATION"), help=argparse.SUPPRESS
    )
    args = parser.parse_args()
    if args.worker:
        print(json.dumps(worker(*args.worker, args.rows)))
        return

    operations_to_run = args.operation or OPERATIONS
    env = dict(os.environ)
    if args.threads:
        env["POLARS_MAX_THREADS"] = env["URSID_THREADS"] = str(args.threads)
    results = {}
    for op in operations_to_run:
        for engine in ENGINES:
            command = [
                sys.executable,
                __file__,
                "--rows",
                str(args.rows),
                "--worker",
                engine,
                op,
            ]
            out = subprocess.run(
                command, env=env, capture_output=True, text=True, check=True
            )
            results[op, engine] = json.loads(out.stdout.strip().splitlines()[-1])
        first = results[op, ENGINES[0]]
        for engine in ENGINES[1:]:
            other = results[op, engine]
            if other["rows"] != first["rows"] or not close(
                other["checksum"], first["checksum"]
            ):
                raise SystemExit(
                    f"{op}: {engine} disagrees with {ENGINES[0]}: {other} vs {first}"
                )

    threads = args.threads or os.cpu_count()
    print(
        f"\n{args.rows:,} ticks, {data.SYMBOLS:,} symbols, {threads} threads; best of {REPEATS}\n"
    )
    print(
        "| operation | engine | ms | inputs MB | peak MB | retained MB | rows | checksum |"
    )
    print("|---|---|---|---|---|---|---|---|")
    for op in operations_to_run:
        for engine in ENGINES:
            r = results[op, engine]
            print(
                f"| {op} | {engine} | {r['ms']:.1f} | {r['inputs_mb']:.0f} | {r['peak_mb']:.0f} "
                f"| {r['retained_mb']:.0f} | {r['rows']:,} | {r['checksum']:.6g} |"
            )


if __name__ == "__main__":
    main()
