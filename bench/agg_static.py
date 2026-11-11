"""Static `agg_by` in DuckDB and Polars, for comparison with `kermodes-bench ticks`.

    uv run --no-project --with numpy --with polars --with duckdb python bench/agg_static.py
    POLARS_MAX_THREADS=1 uv run ...   # Polars on one thread

10M rows over 10k string symbols (`S00000`...), grouped by symbol with count, mean, and sample
standard deviation of a price. Each query runs 7 times; the fastest is reported.
"""

import time

import duckdb
import numpy as np
import polars as pl

ROWS, SYMBOLS, REPEATS = 10_000_000, 10_000, 7
rng = np.random.default_rng(1)
names = np.array([f"S{i:05}" for i in range(SYMBOLS)], dtype=object)
frame = pl.DataFrame(
    {
        "sym": names[rng.integers(0, SYMBOLS, ROWS)],
        "price": 100 + rng.standard_normal(ROWS).cumsum() * 0.01,
    }
)


SQL = "select sym, count(*), avg(price), stddev_samp(price) from t group by sym"


def fastest(query, *args):
    best = float("inf")
    for _ in range(REPEATS):
        start = time.perf_counter()
        query(*args)
        best = min(best, time.perf_counter() - start)
    return best * 1000


def duckdb_query(con):
    con.execute(SQL).fetchall()


for threads in (1, 4, 10):
    con = duckdb.connect()
    con.execute(f"set threads={threads}")
    con.execute("create table t as select * from frame")
    print(f"duckdb, {threads} threads: {fastest(duckdb_query, con):.1f} ms")

aggregations = [
    pl.len().alias("n"),
    pl.col("price").mean().alias("avg"),
    pl.col("price").std().alias("vol"),
]
elapsed = fastest(lambda: frame.lazy().group_by("sym").agg(aggregations).collect())
print(f"polars, {pl.thread_pool_size()} threads: {elapsed:.1f} ms")
