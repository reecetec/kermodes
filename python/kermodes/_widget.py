"""The table viewer in notebooks (architecture.md §15.3): an anywidget over the canvas grid.

The viewer's work (rows by position, sorting and quick filters in the engine, fetches) is the
engine's `Viewer`, shared with the web UI's server. This widget only carries its messages over
the notebook comm: grid messages in, frames out (a header and two buffers, the rows as Arrow IPC
and their keys), and the viewer's state as synced traits. Needs the `kermodes[notebook]` extra;
`kermodes._live` falls back to the HTML view without it.
"""

import threading
from pathlib import Path

import anywidget
import traitlets

from kermodes._native import Table, Viewer

BUNDLE = Path(__file__).parent / "static" / "widget.js"


class TableView(anywidget.AnyWidget):
    """The comm side of the viewer; subclasses supply the front end (`_esm`)."""

    columns = traitlets.List().tag(sync=True)
    row_count = traitlets.Int(0).tag(sync=True)
    status = traitlets.Unicode("").tag(sync=True)
    # What the grid shows: sort keys ({column, descending}), filter entries by column, and why
    # an entry was not applied.
    sort = traitlets.List().tag(sync=True)
    filters = traitlets.Dict().tag(sync=True)
    filter_errors = traitlets.Dict().tag(sync=True)

    def __init__(self, table: Table) -> None:
        super().__init__(columns=[{"name": n, "type": t} for n, t in table.columns])
        self._viewer = Viewer(table)
        # Cycles push from the refresher thread while requests arrive from the kernel; frames
        # go out in the order they were read.
        self._lock = threading.Lock()
        self.on_msg(self._receive)
        self.push()

    def _receive(self, _widget: object, content: dict, _buffers: list) -> None:
        with self._lock:
            self._deliver(self._viewer.receive(content))

    def push(self, _table: Table | None = None) -> None:
        """Sends the window the grid shows, as the table is now."""
        with self._lock:
            self._deliver(self._viewer.rows())

    def release(self) -> None:
        """Drops the sorted or filtered table: its table is gone, so the engine can free both."""
        self._viewer.release()

    def _deliver(self, frame: tuple[dict, bytes, bytes] | None) -> None:
        if frame is None:
            return
        header, rows, keys = frame
        if header["type"] == "rows":
            self.row_count = header["total"]
            self.status = self._viewer.status(header["cycle"])
            self.sort = [{"column": c, "descending": d} for c, d in self._viewer.sort]
            self.filters = self._viewer.filters
            self.filter_errors = self._viewer.filter_errors
        self.send(header, [rows, keys])


# anywidget reads the front end when a class is defined, so the class exists only where the
# bundle was built into the package.
Widget: type[TableView] | None = None
if BUNDLE.exists():

    class _Widget(TableView):
        _esm = BUNDLE

    Widget = _Widget
