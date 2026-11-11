"""Kermodes: live and historical tables on Arrow."""

from os import PathLike

from kermodes._native import KermodesError, Session, Table, Writer, merge

__all__ = [
    "KermodesError",
    "Session",
    "Table",
    "Writer",
    "boolean",
    "date32",
    "float64",
    "int32",
    "int64",
    "local",
    "merge",
    "string",
    "timestamp",
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
