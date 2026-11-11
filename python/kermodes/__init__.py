"""Kermodes: live and historical tables on Arrow."""

import os
from os import PathLike

from kermodes import demo
from kermodes._native import KermodesError, Session, Table, Writer, merge
from kermodes.project import deployment, ingester, websocket

__all__ = [
    "KermodesError",
    "Session",
    "Table",
    "Writer",
    "boolean",
    "connect",
    "date32",
    "demo",
    "deployment",
    "float64",
    "ingester",
    "int32",
    "int64",
    "local",
    "merge",
    "string",
    "timestamp",
    "websocket",
]


def local(path: str | PathLike[str] | None = None, *, manual: bool = False) -> Session:
    """Start an engine inside this Python process.

    With `path`, the session also opens the store in that directory (creating it if needed),
    like `duckdb.connect("x.duckdb")`: tables made with `create_table` and the rows their writers
    write survive the process. One process opens a directory at a time.

    Cycles run on their own as soon as writes arrive. With `manual=True`, nothing runs until
    `step()` (tests, replay); `start()` and `stop()` switch between the two.
    """
    session = Session(path)
    if not manual:
        session.start()
    return session


def connect(url: str | None = None, *, manual: bool = False) -> Session:
    """Start an engine inside this Python process that uses a server's tables.

    `url` is a store server's (`kermodes dev`, or `Session.serve_store`); by default
    `$KERMODES_URL`, else the local `kermodes dev`. Many processes can connect to one server:
    it is the one writer of the store's files, and they are its clients.
    """
    from kermodes.cli import DEV_PORT

    url = url or os.environ.get("KERMODES_URL") or f"grpc://127.0.0.1:{DEV_PORT + 1}"
    session = Session(url=url)
    if not manual:
        session.start()
    return session


def boolean() -> str:
    return "bool"


def int32() -> str:
    return "int32"


def int64() -> str:
    return "int64"


def float64() -> str:
    return "float64"


def string() -> str:
    return "utf8"


def date32() -> str:
    return "date32"


def timestamp() -> str:
    """Nanoseconds since the epoch, UTC."""
    return "timestamp"
