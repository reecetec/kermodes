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
        self.sent: list[tuple[dict, pa.Table]] = []
        super().__init__(table)

    def send(self, content: dict, buffers: list | None = None) -> None:
        assert buffers is not None
        self.sent.append((content, pa.ipc.open_stream(buffers[0]).read_all()))


def test_the_viewer_sends_the_rows_in_view_and_follows_the_table():
    s = us.local(manual=True)
    src = s.input_table({"n": us.int64()})
    src.append(pa.table({"n": pa.array(range(1000), pa.int64())}))
    s.step()

    view = _Recorded(src)
    header, rows = view.sent[-1]
    assert (header["start"], header["total"], view.row_count) == (0, 1000, 1000)
    assert rows["n"].to_pylist() == list(range(2 * MARGIN))

    # Scrolled to rows 600-619: those, with a margin either side.
    view._receive(view, {"type": "viewport", "top": 600, "bottom": 619}, [])
    header, rows = view.sent[-1]
    assert header["start"] == 600 - MARGIN
    assert rows["n"].to_pylist() == list(range(600 - MARGIN, 620 + MARGIN))

    # A cycle that changes the table sends the same window again, with the new total.
    src.append(pa.table({"n": pa.array([-1], pa.int64())}))
    s.step()
    view.push(src)
    header, rows = view.sent[-1]
    assert (header["total"], header["cycle"], view.row_count) == (
        1001,
        src._changed,
        1001,
    )
    assert view.status == f"ticking · updated in cycle {src._changed}"
