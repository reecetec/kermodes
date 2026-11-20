"""Tables in notebooks.

Displaying a table shows the table viewer (`kermodes.notebook`, with the `kermodes[notebook]` extra),
whose output also carries the table as text (`repr`), for an agent reading cell outputs. Where
widgets cannot run, the text alone. A ticking table's viewer then follows it, fresh rows pushed at
most ten times a second and only after a cycle that changed the table. One background thread
serves every live view in the process. This module imports IPython, so only notebooks load it,
through `Table._ipython_display_`.

Neither the live views nor IPython's output history keep a table alive: re-running a cell
that rebinds a name releases the table it held. Views hold tables weakly, and after each cell
the history (`Out`, `_`, `__`, `___`, `_N`) keeps weak proxies to tables instead of the tables.
"""

import importlib.util
import re
import threading
import time
import weakref
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from IPython import get_ipython
from IPython.display import display

from kermodes._native import Table

if TYPE_CHECKING:
    from kermodes.notebook import TableView

INTERVAL = 0.1
# The names IPython gives cell results: `_`, `__`, `___`, and `_<n>`.
_HISTORY = re.compile(r"_{1,3}|_\d+")


@dataclass
class _View:
    table: weakref.ref[Table]
    viewer: "TableView"
    seen: int


_views: list[_View] = []
_lock = threading.Lock()
_refresher: threading.Thread | None = None
# Shells whose output history is watched (see `_unpin_history`).
_watched: "weakref.WeakSet[Any]" = weakref.WeakSet()


def _viewer() -> "type[TableView] | None":
    """The table viewer's class where it can run: the `kermodes[notebook]` extra and its bundle are
    installed, and the kernel is IPython's, whose comms widgets talk over. Other kernels (Tern's
    own) get the text."""
    shell = get_ipython()
    if (
        getattr(shell, "kernel", None) is None
        or importlib.util.find_spec("anywidget") is None
    ):
        return None
    from kermodes.notebook import Widget

    return Widget


def show(table: Table) -> None:
    shell = get_ipython()
    if shell is not None and shell not in _watched:
        shell.events.register("post_run_cell", _unpin_history)
        _watched.add(shell)
    viewer_class = _viewer()
    if viewer_class is None:
        display({"text/plain": repr(table)}, raw=True)
        return
    seen = table._changed
    viewer = viewer_class(table)
    display(viewer)
    if not table.is_refreshing:
        return
    global _refresher
    with _lock:
        _views.append(_View(weakref.ref(table), viewer, seen))
        if _refresher is None:
            _refresher = threading.Thread(
                target=_refresh, name="kermodes-live", daemon=True
            )
            _refresher.start()


def _refresh() -> None:
    while True:
        time.sleep(INTERVAL)
        with _lock:
            # A released table's view stays as last shown.
            gone = [view for view in _views if view.table() is None]
            _views[:] = [view for view in _views if view.table() is not None]
            views = list(_views)
        for view in gone:
            view.viewer.release()
        for view in views:
            table = view.table()
            if table is None:
                continue
            changed = table._changed
            if changed != view.seen:
                view.seen = changed
                view.viewer.push()
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
