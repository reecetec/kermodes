"""The `ursid-bench book` workload in Deephaven Community, for comparison.

    uv run --no-project --python 3.12 --with deephaven-server --with numpy \
        python bench/deephaven_book.py

Needs Java 17+ on the path. A keyed input table holds 1M orders over 100 symbols; each cycle
writes 100k changes (80% modify a live order's price and size, 10% delete one, 10% insert a new
one) through `InputTable.add` and `InputTable.delete`, timed together. The same writes run three
times: into the input table alone, then with an `agg_by(sym)` of count, sum, mean, and std (twice,
the second JIT-warm); the difference between the runs is the aggregation's cost, so these calls
wait for downstream tables. Cycle times exclude the first 10 cycles of each run.
"""

import time

from deephaven_server import Server

Server(port=10_000, jvm_args=["-Xmx8g"]).start()

# Deephaven's modules import only once the server runs.
import numpy as np
from deephaven import agg, input_table, new_table
from deephaven import dtypes as dht
from deephaven.column import double_col, long_col, string_col

ORDERS, SYMBOLS, WRITES, CYCLES, WARM = 1_000_000, 100, 100_000, 40, 10
rng = np.random.default_rng(7)
names = np.array([f"S{i:05}" for i in range(SYMBOLS)])
symbol_of = rng.integers(0, SYMBOLS, ORDERS + CYCLES * WRITES)


def orders(ids):
    ids = np.asarray(ids, dtype=np.int64)
    return new_table(
        [
            long_col("id", ids.tolist()),
            string_col("sym", names[symbol_of[ids]].tolist()),
            double_col(
                "price", (1 + rng.integers(0, 100_000, len(ids)) / 100).tolist()
            ),
            long_col("size", (rng.integers(0, 1000, len(ids)) + 1).tolist()),
        ]
    )


load = orders(np.arange(ORDERS))
live, next_id, plans = np.arange(ORDERS), ORDERS, []
for _ in range(CYCLES):
    kind = rng.integers(0, 10, WRITES)
    n_deleted, n_inserted = int((kind == 8).sum()), int((kind == 9).sum())
    at = rng.choice(len(live), n_deleted, replace=False)
    deleted = live[at]
    live = np.delete(live, at)
    modified = live[rng.integers(0, len(live), WRITES - n_deleted - n_inserted)]
    inserted = np.arange(next_id, next_id + n_inserted)
    next_id += n_inserted
    live = np.concatenate([live, inserted])
    plans.append(
        (
            orders(np.concatenate([modified, inserted])),
            new_table([long_col("id", deleted.tolist())]),
        )
    )

COLUMNS = {"id": dht.int64, "sym": dht.string, "price": dht.double, "size": dht.int64}
AGGREGATIONS = [
    agg.count_("n"),
    agg.sum_(["total=size"]),
    agg.avg(["avg=price"]),
    agg.std(["vol=price"]),
]


def run(name, aggregate):
    book = input_table(COLUMNS, key_cols="id")
    stats = book.agg_by(AGGREGATIONS, by="sym") if aggregate else None
    start = time.perf_counter()
    book.add(load)
    load_ms = (time.perf_counter() - start) * 1000
    cycles = []
    for upserts, deletes in plans:
        start = time.perf_counter()
        book.add(upserts)
        book.delete(deletes)
        cycles.append((time.perf_counter() - start) * 1000)
    timed = np.array(cycles[WARM:])
    groups = stats.size if stats is not None else 0
    print(
        f"{name}: load {load_ms:.0f} ms, cycle p50 {np.percentile(timed, 50):.1f} ms, "
        f"p99 {np.percentile(timed, 99):.1f} ms, live {book.size}, groups {groups}"
    )


run("input only", aggregate=False)
run("agg_by", aggregate=True)
run("agg_by, warm", aggregate=True)
