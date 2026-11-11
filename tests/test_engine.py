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
    s = us.local()
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
    s = us.local()
    data = s.static_table(ticks(["a", "b"], [1.0, 2.0], [1, 2]))
    doubled = data.update("size = size * 2")
    assert not doubled.is_refreshing
    assert pa.table(doubled)["size"].to_pylist() == [2, 4]


def test_expression_errors_point_at_the_problem():
    src = us.local().input_table(COLUMNS)
    with pytest.raises(us.UrsidError, match="did you mean `price`"):
        src.where("prise > 0")


def test_a_failed_table_raises_but_the_session_keeps_running():
    s = us.local()
    src = s.input_table(COLUMNS)
    risky = src.update("big = size * 4611686018427387904")
    fine = src.where("size > 0")
    src.append(ticks(["a"], [1.0], [4]))
    s.step()
    with pytest.raises(us.UrsidError, match="Overflow"):
        len(risky)
    assert len(fine) == 1


def test_appends_must_match_the_table():
    src = us.local().input_table(COLUMNS)
    wrong = pa.table({"sym": ["a"], "price": [1], "size": [1]})
    with pytest.raises(us.UrsidError, match="column `price` is int64"):
        src.append(wrong)
