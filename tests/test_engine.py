import pyarrow as pa
import pytest
import ursid as us

COLUMNS = {"sym": us.string(), "price": us.float64(), "size": us.int64()}


def ticks(sym: list[str], price: list[float], size: list[int | None]) -> pa.Table:
    return pa.table(
        {
            "sym": pa.array(sym, pa.string()),
            "price": pa.array(price, pa.float64()),
            "size": pa.array(size, pa.int64()),
        }
    )


def test_derived_tables_tick_with_their_input():
    s = us.local(manual=True)
    src = s.input_table(COLUMNS)
    live = src.where("price > 0").update("notional = price * size")

    first = ticks(["a", "b", "c"], [1.5, -2.0, 3.0], [10, 20, None])
    second = ticks(["d"], [0.5], [4])
    src.append(first)
    assert len(live) == 0
    s.step()
    src.append(second)
    s.step()

    rows = first.to_pylist() + second.to_pylist()
    expected = [
        r | {"notional": None if r["size"] is None else r["price"] * r["size"]}
        for r in rows
        if r["price"] > 0
    ]
    assert pa.table(live).to_pylist() == expected
    assert live.is_refreshing


def test_static_tables_compute_without_cycles():
    s = us.local(manual=True)
    data = s.static_table(ticks(["a", "b"], [1.0, 2.0], [1, 2]))
    doubled = data.update("size = size * 2")
    assert not doubled.is_refreshing
    assert pa.table(doubled)["size"].to_pylist() == [2, 4]


def test_expression_errors_point_at_the_problem():
    src = us.local(manual=True).input_table(COLUMNS)
    with pytest.raises(us.UrsidError, match="did you mean `price`"):
        src.where("prise > 0")


def test_the_clock_runs_by_default_starts_once_and_stops_quietly():
    auto = us.local()
    assert auto.is_running
    auto.stop()
    assert not auto.is_running

    s = us.local(manual=True)
    assert not s.is_running
    with pytest.raises(ValueError, match="period"):
        s.start("fast")
    s.start("10ms")
    try:
        assert s.is_running
        with pytest.raises(us.UrsidError, match="already running"):
            s.start()
    finally:
        s.stop()
    assert not s.is_running
    s.stop()


def test_a_failed_table_raises_but_the_session_keeps_running():
    s = us.local(manual=True)
    src = s.input_table(COLUMNS)
    risky = src.update("big = size * 4611686018427387904")
    fine = src.where("size > 0")
    src.append(ticks(["a"], [1.0], [4]))
    s.step()
    with pytest.raises(us.UrsidError, match="Overflow"):
        len(risky)
    assert len(fine) == 1


def test_appends_must_match_the_table():
    src = us.local(manual=True).input_table(COLUMNS)
    wrong = pa.table({"sym": ["a"], "price": [1], "size": [1]})
    with pytest.raises(us.UrsidError, match="column `price` is int64"):
        src.append(wrong)


def test_keyed_tables_follow_upserts_and_deletes():
    s = us.local(manual=True)
    book = s.input_table(COLUMNS, key="sym")
    rich = book.where("price > 10")
    book.upsert(ticks(["a", "b"], [5.0, 20.0], [1, 2]))
    s.step()
    book.upsert(ticks(["a", "b"], [15.0, 8.0], [3, 4]))
    book.delete(pa.table({"sym": ["c"]}))
    s.step()
    assert pa.table(book).to_pylist() == [
        {"sym": "a", "price": 15.0, "size": 3},
        {"sym": "b", "price": 8.0, "size": 4},
    ]
    assert pa.table(rich)["sym"].to_pylist() == ["a"]


def test_last_by_and_agg_by_match_a_plain_python_recomputation():
    s = us.local(manual=True)
    src = s.input_table(COLUMNS)
    latest = src.last_by("sym")
    stats = src.agg_by(
        "sym", n="count(*)", total="sum(size)", avg="mean(price)", high="max(price)"
    )
    batches = [
        ticks(["a", "b", "a"], [1.0, 2.0, 3.0], [1, 2, 3]),
        ticks(["b", "c"], [5.0, 4.0], [None, 6]),
    ]
    for batch in batches:
        src.append(batch)
        s.step()

    rows = [r for b in batches for r in b.to_pylist()]
    by_sym: dict[str, list[dict]] = {}
    for r in rows:
        by_sym.setdefault(r["sym"], []).append(r)
    assert pa.table(latest).to_pylist() == [group[-1] for group in by_sym.values()]
    got = sorted(pa.table(stats).to_pylist(), key=lambda r: r["sym"])
    assert got == [
        {
            "sym": sym,
            "n": len(group),
            "total": sum(r["size"] for r in group if r["size"] is not None),
            "avg": sum(r["price"] for r in group) / len(group),
            "high": max(r["price"] for r in group),
        }
        for sym, group in by_sym.items()
    ]


def test_trades_pick_up_the_prevailing_quote_and_reference_data():
    s = us.local(manual=True)
    stamp = us.timestamp()
    trades = s.input_table({"sym": us.string(), "ts": stamp, "qty": us.int64()})
    quotes = s.input_table({"sym": us.string(), "ts": stamp, "bid": us.float64()})
    refs = s.input_table({"sym": us.string(), "sector": us.string()}, key="sym")
    enriched = trades.as_of_join(quotes, "sym", "ts").natural_join(refs, "sym")

    def at(*seconds: int) -> pa.Array:
        return pa.array([t * 10**9 for t in seconds], pa.timestamp("ns", tz="UTC"))

    quotes.append(
        pa.table({"sym": ["a", "a", "b"], "ts": at(1, 5, 2), "bid": [10.0, 11.0, 20.0]})
    )
    refs.upsert(pa.table({"sym": ["a"], "sector": ["tech"]}))
    trades.append(
        pa.table(
            {
                "sym": ["a", "a", "b", "c", "a"],
                "ts": at(0, 6, 3, 9, 4),
                "qty": [1, 2, 3, 4, 5],
            }
        )
    )
    s.step()
    rows = pa.table(enriched).select(["sym", "qty", "bid", "sector"]).to_pylist()
    assert sorted(rows, key=lambda r: r["qty"]) == [
        {"sym": "a", "qty": 1, "bid": None, "sector": "tech"},
        {"sym": "a", "qty": 2, "bid": 11.0, "sector": "tech"},
        {"sym": "b", "qty": 3, "bid": 20.0, "sector": None},
        {"sym": "c", "qty": 4, "bid": None, "sector": None},
        {"sym": "a", "qty": 5, "bid": 10.0, "sector": "tech"},
    ]

    # A later quote re-prices only the trades it now precedes; new reference data reaches all.
    quotes.append(pa.table({"sym": ["a"], "ts": at(3), "bid": [12.0]}))
    refs.upsert(pa.table({"sym": ["b"], "sector": ["energy"]}))
    s.step()
    rows = pa.table(enriched).select(["qty", "bid", "sector"]).to_pylist()
    assert sorted(rows, key=lambda r: r["qty"]) == [
        {"qty": 1, "bid": None, "sector": "tech"},
        {"qty": 2, "bid": 11.0, "sector": "tech"},
        {"qty": 3, "bid": 20.0, "sector": "energy"},
        {"qty": 4, "bid": None, "sector": None},
        {"qty": 5, "bid": 12.0, "sector": "tech"},
    ]


def test_merge_follows_its_tables_in_order_and_needs_matching_columns():
    s = us.local(manual=True)
    east, west = s.input_table(COLUMNS), s.input_table(COLUMNS)
    both = us.merge(east, west)
    east.append(ticks(["a"], [1.0], [1]))
    west.append(ticks(["b", "c"], [2.0, 3.0], [2, 3]))
    s.step()
    assert pa.table(both)["sym"].to_pylist() == ["a", "b", "c"]
    east.append(ticks(["d"], [4.0], [4]))
    s.step()
    assert pa.table(both)["sym"].to_pylist() == ["a", "d", "b", "c"]
    assert len(both) == 4 and both.is_refreshing

    other = s.input_table({"sym": us.string(), "price": us.float64()})
    with pytest.raises(us.UrsidError, match="same columns"):
        us.merge(east, other)


def test_head_and_tail_follow_the_ends_of_a_ticking_table():
    s = us.local(manual=True)
    src = s.input_table(COLUMNS)
    first, last = src.head(2), src.tail(2)
    src.append(ticks(["a", "b", "c"], [1.0, 2.0, 3.0], [1, 2, 3]))
    s.step()
    src.append(ticks(["d"], [4.0], [4]))
    s.step()
    assert pa.table(first)["sym"].to_pylist() == ["a", "b"]
    assert pa.table(last)["sym"].to_pylist() == ["c", "d"]
