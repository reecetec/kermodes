import array

import pyarrow as pa
import ursid as us
from IPython.core.interactiveshell import InteractiveShell
from ursid._widget import MARGIN, TableView


def test_rerunning_a_cell_replaces_the_table_it_built():
    shell = InteractiveShell.instance()
    shell.run_cell(
        "import ursid as us\n"
        "s = us.local(manual=True)\n"
        "src = s.input_table({'sym': us.string(), 'size': us.int64()})"
    )
    for _ in range(3):
        # Displayed, so IPython's output history sees each table too.
        shell.run_cell("latest = src.last_by('sym')\nlatest", store_history=True)
    session = shell.user_ns["s"]
    assert session.table_count == 2, "the input and the newest last_by"

    # The history still reaches the table it shows while a name holds it, but doesn't keep it.
    assert shell.run_cell("len(Out[max(Out)])").result == 0
    shell.run_cell("del latest")
    assert session.table_count == 1


class _Recorded(TableView):
    """The viewer with an inline module instead of the built bundle, keeping what it sends."""

    _esm = "export default { render() {} }"

    def __init__(self, table: us.Table) -> None:
        self.sent: list[tuple[dict, pa.Table, list[int]]] = []
        super().__init__(table)

    def send(self, content: dict, buffers: list | None = None) -> None:
        assert buffers is not None
        rows = pa.ipc.open_stream(buffers[0]).read_all()
        keys = array.array("Q", buffers[1]).tolist()
        assert len(keys) == rows.num_rows
        self.sent.append((content, rows, keys))

    def ask(self, **content: object) -> tuple[dict, pa.Table, list[int]]:
        """Sends a message as the grid would and returns the viewer's answer."""
        self._receive(self, content, [])
        return self.sent[-1]


def test_the_viewer_sends_the_rows_in_view_and_follows_the_table():
    s = us.local(manual=True)
    src = s.input_table({"n": us.int64()})
    src.append(pa.table({"n": pa.array(range(1000), pa.int64())}))
    s.step()

    view = _Recorded(src)
    header, rows, _ = view.sent[-1]
    assert (header["start"], header["total"], view.row_count) == (0, 1000, 1000)
    assert rows["n"].to_pylist() == list(range(2 * MARGIN))

    # Scrolled to rows 600-619: those, with a margin either side.
    header, rows, _ = view.ask(type="viewport", top=600, bottom=619)
    assert header["start"] == 600 - MARGIN
    assert rows["n"].to_pylist() == list(range(600 - MARGIN, 620 + MARGIN))

    # A cycle that changes the table sends the same window again, with the new total.
    src.append(pa.table({"n": pa.array([-1], pa.int64())}))
    s.step()
    view.push(src)
    header, rows, _ = view.sent[-1]
    assert (header["total"], header["cycle"], view.row_count) == (
        1001,
        src._changed,
        1001,
    )
    assert view.status == f"ticking · updated in cycle {src._changed}"


def test_the_viewer_sorts_and_filters_in_the_engine_and_keeps_row_keys():
    s = us.local(manual=True)
    src = s.input_table({"sym": us.string(), "price": us.float64()}, key="sym")
    src.upsert(
        pa.table({"sym": ["aa", "AB", "b", "ca"], "price": [3.0, 1.0, 4.0, 2.0]})
    )
    s.step()
    view = _Recorded(src)
    _, _, keys = view.sent[-1]
    key_of = dict(zip(pa.table(src)["sym"].to_pylist(), keys, strict=True))

    # Contains, any case, then highest price first; keys still name the same rows.
    view.ask(type="filter", filters={"sym": "a", "price": ""})
    _, rows, keys = view.ask(type="sort", by=[{"column": "price", "descending": True}])
    assert rows["sym"].to_pylist() == ["aa", "ca", "AB"]
    assert keys == [key_of[sym] for sym in ["aa", "ca", "AB"]]
    assert (view.row_count, view.filters, view.filter_errors) == (3, {"sym": "a"}, {})

    # The view follows the table: a new price moves a row and keeps its key.
    src.upsert(pa.table({"sym": ["AB"], "price": [9.0]}))
    s.step()
    view.push(src)
    _, rows, keys = view.sent[-1]
    assert rows["sym"].to_pylist() == ["AB", "aa", "ca"]
    assert keys[0] == key_of["AB"]

    # An entry that is not a filter is reported and skipped; the others still apply.
    _, rows, _ = view.ask(type="filter", filters={"sym": "=b", "price": "> lots"})
    assert rows["sym"].to_pylist() == ["b"]
    assert view.filter_errors == {"price": "`lots` is not a number"}

    # Numbers compare; no filters and no sort show the table as it is.
    _, rows, _ = view.ask(type="filter", filters={"price": ">= 4"})
    assert rows["sym"].to_pylist() == ["AB", "b"]
    view.ask(type="filter", filters={})
    _, rows, _ = view.ask(type="sort", by=[])
    assert rows["sym"].to_pylist() == pa.table(src)["sym"].to_pylist()
