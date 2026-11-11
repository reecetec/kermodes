"""Tables in notebooks.

Displaying a table shows its first rows as HTML. A ticking table's output is then re-rendered in
place (Jupyter's `update_display_data`), at most ten times a second and only after a cycle that
changed it. One background thread serves every live output in the process. This module imports
IPython, so only notebooks load it, through `Table._ipython_display_`.

Neither the live outputs nor IPython's output history keep a table alive: re-running a cell
that rebinds a name releases the table it held. Views hold tables weakly, and after each cell
the history (`Out`, `_`, `__`, `___`, `_N`) keeps weak proxies to tables instead of the tables.
"""

import re
import threading
import time
import weakref
from dataclasses import dataclass
from typing import Any

from IPython import get_ipython
from IPython.display import display

from ursid._native import Table

ROWS = 20
INTERVAL = 0.1
# The names IPython gives cell results: `_`, `__`, `___`, and `_<n>`.
_HISTORY = re.compile(r"_{1,3}|_\d+")


@dataclass
class _View:
    table: weakref.ref[Table]
    # IPython's DisplayHandle, or the equivalent from a frontend's own kernel (Tern has one).
    handle: Any
    seen: int


_views: list[_View] = []
_lock = threading.Lock()
_refresher: threading.Thread | None = None
# Shells whose output history is watched (see `_unpin_history`).
_watched: "weakref.WeakSet[Any]" = weakref.WeakSet()


def _bundle(table: Table) -> dict[str, str]:
    return {"text/html": table._preview_html(ROWS), "text/plain": repr(table)}


def show(table: Table) -> None:
    shell = get_ipython()
    if shell is not None and shell not in _watched:
        shell.events.register("post_run_cell", _unpin_history)
        _watched.add(shell)
    seen = table._changed
    handle = display(_bundle(table), raw=True, display_id=True)
    if not table.is_refreshing or handle is None:
        return
    global _refresher
    with _lock:
        _views.append(_View(weakref.ref(table), handle, seen))
        if _refresher is None:
            _refresher = threading.Thread(
                target=_refresh, name="ursid-live", daemon=True
            )
            _refresher.start()


def _refresh() -> None:
    while True:
        time.sleep(INTERVAL)
        with _lock:
            # A released table's output stays as last rendered.
            _views[:] = [view for view in _views if view.table() is not None]
            views = list(_views)
        for view in views:
            table = view.table()
            if table is None:
                continue
            changed = table._changed
            if changed != view.seen:
                view.seen = changed
                view.handle.update(_bundle(table), raw=True)
            del table


def _weak(value: object) -> object:
    return weakref.proxy(value) if type(value) is Table else value


def _unpin_history(*_: object) -> None:
    """Swaps tables in IPython's output history for weak proxies."""
    shell = get_ipython()
    if shell is None:
        return
    history = shell.user_ns.get("Out")
    if isinstance(history, dict):
        for number, value in history.items():
            history[number] = _weak(value)
    # IPython pushes results into the user namespace and, as non-interactive names, into its
    # hidden namespace as well.
    for namespace in (shell.user_ns, shell.user_ns_hidden):
        for name, value in list(namespace.items()):
            if _HISTORY.fullmatch(name):
                namespace[name] = _weak(value)
    hook = shell.displayhook
    for name in ("_", "__", "___"):
        if hasattr(hook, name):
            setattr(hook, name, _weak(getattr(hook, name)))
