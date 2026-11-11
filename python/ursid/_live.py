"""Tables in notebooks.

Displaying a table shows its first rows as HTML. A ticking table's output is then re-rendered in
place (Jupyter's `update_display_data`), at most ten times a second and only after a cycle that
changed it. One background thread serves every live output in the process. This module imports
IPython, so only notebooks load it, through `Table._ipython_display_`.
"""

import threading
import time
from dataclasses import dataclass
from typing import Any

from IPython.display import display

from ursid._native import Table

ROWS = 20
INTERVAL = 0.1


@dataclass
class _View:
    table: Table
    # IPython's DisplayHandle, or the equivalent from a frontend's own kernel (Tern has one).
    handle: Any
    seen: int


_views: list[_View] = []
_lock = threading.Lock()
_refresher: threading.Thread | None = None


def _bundle(table: Table) -> dict[str, str]:
    return {"text/html": table._preview_html(ROWS), "text/plain": repr(table)}


def show(table: Table) -> None:
    seen = table._changed
    handle = display(_bundle(table), raw=True, display_id=True)
    if not table.is_refreshing or handle is None:
        return
    global _refresher
    with _lock:
        _views.append(_View(table, handle, seen))
        if _refresher is None:
            _refresher = threading.Thread(
                target=_refresh, name="ursid-live", daemon=True
            )
            _refresher.start()


def _refresh() -> None:
    while True:
        time.sleep(INTERVAL)
        with _lock:
            views = list(_views)
        for view in views:
            changed = view.table._changed
            if changed != view.seen:
                view.seen = changed
                view.handle.update(_bundle(view.table), raw=True)
