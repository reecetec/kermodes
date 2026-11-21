"""Kermodes: live and historical tables on Arrow."""

import os
from os import PathLike

from kermodes import demo
from kermodes._native import (
    HistoricalTable,
    KermodesError,
    Pending,
    Publication,
    Replay,
    Session,
    StoredTable,
    Table,
    Writer,
    merge,
)
from kermodes.project import app, deployment, ingester, websocket

__all__ = [
    "HistoricalTable",
    "KermodesError",
    "Pending",
    "Publication",
    "Replay",
    "Session",
    "StoredTable",
    "Table",
    "Writer",
    "app",
    "boolean",
    "connect",
    "date32",
    "demo",
    "deployment",
    "float64",
    "ingester",
    "int16",
    "int32",
    "int64",
    "merge",
    "string",
    "timestamp",
    "websocket",
]


def connect(
    target: str | PathLike[str] | None = None, *, manual: bool = False
) -> Session:
    """Start an engine inside this Python process, on the tables `target` names.

    - A server's URL (`grpc://host:port`, from `kermodes dev`, `kermodes weir`, or
      `Session.serve_store`): its tables. Many processes can connect to one server; it is the
      one writer of the store's files, and they are its clients. For a replicated weir, every
      replica (`grpc://a:8731,b:8731,c:8731`): writes go to whichever leads.
    - A directory: the store there, opened in this process (created if needed), like
      `duckdb.connect("x.duckdb")`: tables and the rows their writers write survive the
      process. One process opens a directory at a time.
    - `":memory:"`: no store; tables are this process's own.
    - Nothing: `$KERMODES_URL`, else the local `kermodes dev`.

    Cycles run on their own as soon as writes arrive. With `manual=True`, nothing runs until
    `step()` (tests, replay); `start()` and `stop()` switch between the two.
    """
    if target is None:
        from kermodes.cli import DEV_PORT

        target = os.environ.get("KERMODES_URL") or f"grpc://127.0.0.1:{DEV_PORT + 1}"
    target = os.fspath(target)
    if target == ":memory:":
        session = Session()
    elif "://" in target:
        session = Session(url=target)
    else:
        session = Session(target)
    if not manual:
        session.start()
    return session


type Was = str | list[str] | None


def _column(kind: str, was: Was) -> str:
    """A column's spec: its type, and the names it had before (`float64 was px`), so that
    `create_table` renames the column it was rather than adding one."""
    if was is None:
        return kind
    names = [was] if isinstance(was, str) else was
    return f"{kind} was {', '.join(names)}"


def boolean(*, was: Was = None) -> str:
    return _column("bool", was)


def int16(*, was: Was = None) -> str:
    return _column("int16", was)


def int32(*, was: Was = None) -> str:
    return _column("int32", was)


def int64(*, was: Was = None) -> str:
    return _column("int64", was)


def float64(*, was: Was = None) -> str:
    return _column("float64", was)


def string(*, was: Was = None) -> str:
    return _column("utf8", was)


def date32(*, was: Was = None) -> str:
    return _column("date32", was)


def timestamp(*, was: Was = None) -> str:
    """Nanoseconds since the epoch, UTC."""
    return _column("timestamp", was)
