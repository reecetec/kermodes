"""A long-running session with constant live data: does its memory stay flat?

    uv run --no-sync --with numpy --with psutil python bench/soak.py [--minutes 20]

A keyed order book holds 1M orders over 100 symbols; each cycle upserts 100k random orders and
deletes 10k, through agg_by(sym), where -> update, and last_by(sym). Every 50 cycles a notebook-
style burst builds a static snapshot of the book, sorts it, rolls a window over it, and drops all
three. Live data stays the same size throughout, so memory that keeps growing is the
allocator's fragmentation, not data. Prints memory (bench/memory.py) every 30 s.
"""

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

ORDERS, SYMBOLS, UPSERTS, DELETES, BURST = 1_000_000, 100, 100_000, 10_000, 50
MB = 1 << 20


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--minutes", type=float, default=20)
    args = parser.parse_args()

    import numpy as np
    import pyarrow as pa
    import ursid as us
    from memory import resident

    rng = np.random.default_rng(3)
    names = np.array([f"S{i:03}" for i in range(SYMBOLS)], dtype=object)

    def orders(count: int) -> pa.Table:
        return pa.table(
            {
                "id": rng.integers(0, ORDERS, count),
                "sym": names[rng.integers(0, SYMBOLS, count)],
                "price": 100 + rng.standard_normal(count),
                "size": rng.integers(1, 1000, count),
            }
        )

    s = us.local(manual=True)
    book = s.input_table(
        {
            "id": us.int64(),
            "sym": us.string(),
            "price": us.float64(),
            "size": us.int64(),
        },
        key="id",
    )
    stats = book.agg_by(
        "sym", n="count(*)", total="sum(size)", avg="mean(price)", vol="std(price)"
    )
    rich = book.where("price > 100").update(
        "notional = price * size", "half = size / 2"
    )
    latest = rich.last_by("sym")
    book.upsert(
        pa.table({"id": np.arange(ORDERS), **orders(ORDERS).drop(["id"]).to_pydict()})
    )
    s.step()

    start = last_report = time.monotonic()
    cycles = 0
    print(f"t=0s: {resident() / MB:,.0f} MB, {len(book):,} orders", flush=True)
    while time.monotonic() - start < args.minutes * 60:
        book.upsert(orders(UPSERTS))
        book.delete(pa.table({"id": rng.integers(0, ORDERS, DELETES)}))
        s.step()
        cycles += 1
        if cycles % BURST == 0:
            snapshot = s.static_table(pa.table(book))
            ranked = snapshot.sort("sym", "price desc")
            rolled = ranked.rolling("sym", rows=100, avg="mean(price)")
            del snapshot, ranked, rolled
        if time.monotonic() - last_report >= 30:
            last_report = time.monotonic()
            print(
                f"t={last_report - start:.0f}s: {resident() / MB:,.0f} MB, {cycles} cycles, "
                f"{len(book):,} orders, {len(stats)} groups, {len(latest)} latest, "
                f"{s.table_count} tables",
                flush=True,
            )


if __name__ == "__main__":
    main()
