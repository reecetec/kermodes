"""Seeded data for the comparison scripts, so every engine sees the same rows.

Ticks and quotes are sorted by time (as they arrive); `price > 100` keeps about half of the ticks.
"""

import numpy as np
import pyarrow as pa

SYMBOLS = 10_000
ROWS = 10_000_000
DAY_NS = 86_400 * 10**9


def names(symbols: int = SYMBOLS) -> np.ndarray:
    return np.array([f"S{i:05}" for i in range(symbols)], dtype=object)


def _times(rng: np.random.Generator, rows: int) -> pa.Array:
    stamps = np.sort(rng.integers(0, DAY_NS, rows))
    return pa.array(stamps, pa.timestamp("ns", tz="UTC"))


def ticks(rows: int = ROWS, symbols: int = SYMBOLS, seed: int = 1) -> pa.Table:
    rng = np.random.default_rng(seed)
    return pa.table(
        {
            "sym": names(symbols)[rng.integers(0, symbols, rows)],
            "price": 100 + rng.standard_normal(rows).cumsum() * 0.01,
            "size": rng.integers(1, 1000, rows),
            "ts": _times(rng, rows),
        }
    )


def quotes(rows: int = ROWS, symbols: int = SYMBOLS, seed: int = 2) -> pa.Table:
    rng = np.random.default_rng(seed)
    return pa.table(
        {
            "sym": names(symbols)[rng.integers(0, symbols, rows)],
            "bid": 100 + rng.standard_normal(rows),
            "ts": _times(rng, rows),
        }
    )


def refs(symbols: int = SYMBOLS) -> pa.Table:
    return pa.table(
        {
            "sym": names(symbols),
            "sector": [f"sector{i % 11:02}" for i in range(symbols)],
        }
    )
