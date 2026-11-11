"""The table viewer in notebooks (architecture.md §15.3): an anywidget over the canvas grid.

The grid asks for the rows it shows (a "viewport" message: first and last visible row); the
widget answers with those rows and some either side, read by position in the engine, as an Arrow
IPC buffer plus their row keys, and pushes a fresh window after every cycle that changes the
table. Keys let the grid tell rows apart across pushes, as sorting moves them. Sorting
("sort" message) and quick filters ("filter" message) build a table from the shown one, `where`
then `sort`, which the widget owns and drops with its table. Needs the `ursid[notebook]` extra;
`ursid._live` falls back to the HTML view without it.
"""

import datetime
import re
import threading
import weakref
from pathlib import Path

import anywidget
import traitlets

from ursid._native import Table, UrsidError

BUNDLE = Path(__file__).parent / "static" / "widget.js"
# Rows sent beyond the visible ones on each side, so small scrolls need no round trip.
MARGIN = 100
# Most rows one "fetch" (a copy of a selection beyond the loaded window) returns.
FETCH_LIMIT = 100_000

_COMPARISON = re.compile(r"(>=|<=|!=|=|>|<)?\s*(.*)", re.DOTALL)
_NUMBER = re.compile(r"[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?")
_NUMBER_TYPES = {"int32", "int64", "float64"}


def quick_filter(column: str, type_name: str, text: str) -> str:
    """The `where` predicate for a quick-filter entry on `column`.

    Every type takes `null` and `!null`. Strings: `text` (contains, any case), `=text`,
    `!=text`, `!text` (does not contain). Numbers, dates, timestamps, and bools: a value,
    optionally after `=`, `!=`, `>`, `>=`, `<`, `<=`. Raises ValueError for other entries.
    """
    name = '"' + column.replace('"', '""') + '"'
    text = text.strip()
    if text.lower() == "null":
        return f"{name} IS NULL"
    if text.lower() == "!null":
        return f"{name} IS NOT NULL"
    if type_name == "utf8":
        if text.startswith("!="):
            return f"{name} <> {_string(text[2:])}"
        if text.startswith("="):
            return f"{name} = {_string(text[1:])}"
        if text.startswith("!"):
            return f"{name} NOT ILIKE {_string('%' + text[1:] + '%')}"
        return f"{name} ILIKE {_string('%' + text + '%')}"
    match = _COMPARISON.fullmatch(text)
    assert match is not None, "every text matches"
    op, value = match.group(1) or "=", match.group(2).strip()
    op = "<>" if op == "!=" else op
    if not value:
        raise ValueError("needs a value")
    if type_name in _NUMBER_TYPES:
        if not _NUMBER.fullmatch(value):
            raise ValueError(f"`{value}` is not a number")
        return f"{name} {op} {value}"
    if type_name == "bool":
        if value.lower() not in ("true", "false"):
            raise ValueError("takes true, false, null, or !null")
        return f"{name} {op} {value.lower()}"
    if type_name in ("date32", "timestamp"):
        try:
            if type_name == "date32":
                datetime.date.fromisoformat(value)
            else:
                datetime.datetime.fromisoformat(value)
        except ValueError:
            raise ValueError(f"`{value}` is not an ISO date or time") from None
        return f"{name} {op} {_string(value)}"
    raise ValueError(f"no quick filter for {type_name} columns")


def _string(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


class TableView(anywidget.AnyWidget):
    """The viewer's side of the protocol; subclasses supply the front end (`_esm`)."""

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
        # Weak, like every view: the table goes when the notebook lets go of it, and the grid
        # keeps showing its last rows.
        self._table = weakref.ref(table)
        self._types = dict(table.columns)
        # The sorted or filtered table shown instead, built from `_table`; None when neither.
        self._shown: Table | None = None
        self._window = (0, 2 * MARGIN)
        # Cycles push from the refresher thread while requests arrive from the kernel.
        self._lock = threading.Lock()
        self.on_msg(self._receive)
        self.push(table)

    def _receive(self, _widget: object, content: dict, _buffers: list) -> None:
        kind = content.get("type")
        if kind == "viewport":
            top, bottom = int(content["top"]), int(content["bottom"])
            with self._lock:
                self._window = (
                    max(0, top - MARGIN),
                    max(0, bottom - top) + 1 + 2 * MARGIN,
                )
        elif kind == "sort":
            by = [
                {"column": str(e["column"]), "descending": bool(e["descending"])}
                for e in content["by"]
            ]
            self._derive(by, dict(self.filters))
        elif kind == "filter":
            entries = {str(c): str(t) for c, t in content["filters"].items()}
            self._derive(
                list(self.sort), {c: t for c, t in entries.items() if t.strip()}
            )
        elif kind == "fetch":
            self._fetch(
                int(content["id"]), int(content["start"]), int(content["count"])
            )
            return
        else:
            return
        table = self._table()
        if table is not None:
            self.push(table)

    def _fetch(self, request: int, start: int, count: int) -> None:
        """Answers a "fetch" with exactly the rows asked for, from the table shown."""
        table = self._table()
        if table is None:
            return
        with self._lock:
            shown = self._shown if self._shown is not None else table
            data, keys, total = shown._viewport(start, min(count, FETCH_LIMIT))
            self.send(
                {"type": "fetched", "id": request, "start": start, "total": total},
                [data, keys],
            )

    def _derive(self, sort: list[dict], filters: dict[str, str]) -> None:
        table = self._table()
        if table is None:
            return
        errors: dict[str, str] = {}
        predicates = []
        for column, text in filters.items():
            if column not in self._types:
                errors[column] = "no such column"
                continue
            try:
                predicates.append(quick_filter(column, self._types[column], text))
            except ValueError as e:
                errors[column] = str(e)
        shown = table
        try:
            if predicates:
                shown = shown.where(" AND ".join(predicates))
            if sort:
                shown = shown.sort(
                    *(
                        f"{key['column']} desc" if key["descending"] else key["column"]
                        for key in sort
                    )
                )
        except UrsidError as e:
            shown = table
            errors = {column: str(e) for column in filters}
        with self._lock:
            self._shown = None if shown is table else shown
            self.sort, self.filters, self.filter_errors = sort, filters, errors

    def push(self, table: Table) -> None:
        """Sends the current window of `table`'s rows, sorted and filtered as the grid asks."""
        with self._lock:
            shown = self._shown if self._shown is not None else table
            start, count = self._window
            data, keys, total = shown._viewport(start, count)
            cycle = table._changed
            self.row_count = total
            self.status = (
                f"ticking · updated in cycle {cycle}"
                if table.is_refreshing
                else "static"
            )
            self.send(
                {"type": "rows", "start": start, "total": total, "cycle": cycle},
                [data, keys],
            )

    def release(self) -> None:
        """Drops the sorted or filtered table: its table is gone, so the engine can free both."""
        with self._lock:
            self._shown = None


# anywidget reads the front end when a class is defined, so the class exists only where the
# bundle was built into the package.
Viewer: type[TableView] | None = None
if BUNDLE.exists():

    class _Viewer(TableView):
        _esm = BUNDLE

    Viewer = _Viewer
