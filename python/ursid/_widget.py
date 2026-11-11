"""The table viewer in notebooks (architecture.md §15.3): an anywidget over the canvas grid.

The grid asks for the rows it shows (a "viewport" message: first and last visible row); the
widget answers with those rows and some either side as one Arrow IPC buffer, read by position
in the engine, and pushes a fresh window after every cycle that changes the table. Needs the
`ursid[notebook]` extra; `ursid._live` falls back to the HTML view without it.
"""

import threading
import weakref
from pathlib import Path

import anywidget
import traitlets

from ursid._native import Table

BUNDLE = Path(__file__).parent / "static" / "widget.js"
# Rows sent beyond the visible ones on each side, so small scrolls need no round trip.
MARGIN = 100


class TableView(anywidget.AnyWidget):
    """The viewer's side of the protocol; subclasses supply the front end (`_esm`)."""

    columns = traitlets.List().tag(sync=True)
    row_count = traitlets.Int(0).tag(sync=True)
    status = traitlets.Unicode("").tag(sync=True)

    def __init__(self, table: Table) -> None:
        super().__init__(columns=[{"name": n, "type": t} for n, t in table.columns])
        # Weak, like every view: the table goes when the notebook lets go of it, and the grid
        # keeps showing its last rows.
        self._table = weakref.ref(table)
        self._window = (0, 2 * MARGIN)
        # Cycles push from the refresher thread while viewport requests arrive from the kernel.
        self._lock = threading.Lock()
        self.on_msg(self._receive)
        self.push(table)

    def _receive(self, _widget: object, content: dict, _buffers: list) -> None:
        if content.get("type") != "viewport":
            return
        top, bottom = int(content["top"]), int(content["bottom"])
        with self._lock:
            self._window = (max(0, top - MARGIN), max(0, bottom - top) + 1 + 2 * MARGIN)
        table = self._table()
        if table is not None:
            self.push(table)

    def push(self, table: Table) -> None:
        """Sends the current window of `table`'s rows."""
        with self._lock:
            start, count = self._window
            data, total = table._viewport(start, count)
            cycle = table._changed
            self.row_count = total
            self.status = (
                f"ticking · updated in cycle {cycle}"
                if table.is_refreshing
                else "static"
            )
            self.send(
                {"type": "rows", "start": start, "total": total, "cycle": cycle}, [data]
            )


# anywidget reads the front end when a class is defined, so the class exists only where the
# bundle was built into the package.
Viewer: type[TableView] | None = None
if BUNDLE.exists():

    class _Viewer(TableView):
        _esm = BUNDLE

    Viewer = _Viewer
