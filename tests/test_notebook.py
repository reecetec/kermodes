import array

import kermodes as km
import pyarrow as pa
from IPython.core.interactiveshell import InteractiveShell
from kermodes.notebook import TableView


def test_rerunning_a_cell_replaces_the_table_it_built():
    shell = InteractiveShell.instance()
    shell.run_cell(
        "import kermodes as km\n"
        "s = km.local(manual=True)\n"
        "src = s.input_table({'sym': km.string(), 'size': km.int64()})"
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
    """The widget with an inline module instead of the built bundle, keeping what it sends."""

    _esm = "export default { render() {} }"

    def __init__(self, table: km.Table) -> None:
        self.sent: list[tuple[dict, pa.Table, list[int]]] = []
        super().__init__(table)

    def send(self, content: dict, buffers: list | None = None) -> None:
        assert buffers is not None
        rows = pa.ipc.open_stream(buffers[0]).read_all()
        keys = array.array("Q", buffers[1]).tolist()
        assert len(keys) == rows.num_rows
        self.sent.append((content, rows, keys))

    def ask(self, **content: object) -> tuple[dict, pa.Table, list[int]]:
        """Sends a message as the grid would and returns the widget's answer."""
        self._receive(self, content, [])
        return self.sent[-1]


def test_the_widget_carries_frames_and_shows_the_viewers_state():
    s = km.local(manual=True)
    src = s.input_table({"sym": km.string(), "price": km.float64()}, key="sym")
    src.upsert(
        pa.table({"sym": ["aa", "AB", "b", "ca"], "price": [3.0, 1.0, 4.0, 2.0]})
    )
    s.step()
    view = _Recorded(src)
    header, rows, _ = view.sent[-1]
    assert (header["type"], header["start"], header["total"]) == ("rows", 0, 4)
    assert view.row_count == 4
    assert view.status == f"ticking · updated in cycle {src._changed}"

    # Sort and filter in the engine; the traits show what applies and what did not.
    view.ask(type="sort", by=[{"column": "price", "descending": True}])
    _, rows, _ = view.ask(type="filter", filters={"sym": "a", "price": "> lots"})
    assert rows["sym"].to_pylist() == ["aa", "ca", "AB"]
    assert view.sort == [{"column": "price", "descending": True}]
    assert view.filters == {"sym": "a", "price": "> lots"}
    assert view.filter_errors == {"price": "`lots` is not a number"}
    assert view.row_count == 3

    # A cycle's push carries the new rows; a fetch answers its id and leaves the window alone.
    src.upsert(pa.table({"sym": ["AB"], "price": [9.0]}))
    s.step()
    view.push()
    header, rows, _ = view.sent[-1]
    assert header["cycle"] == src._changed
    assert rows["sym"].to_pylist() == ["AB", "aa", "ca"]
    header, rows, _ = view.ask(type="fetch", id=3, start=1, count=1)
    assert (header["type"], header["id"], rows["sym"].to_pylist()) == (
        "fetched",
        3,
        ["aa"],
    )
    assert view.row_count == 3
