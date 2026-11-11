"""Ursid: live and historical tables on Arrow."""

from ursid._native import Session, Table, UrsidError

__all__ = [
    "Session",
    "Table",
    "UrsidError",
    "boolean",
    "date32",
    "float64",
    "int32",
    "int64",
    "local",
    "string",
    "timestamp",
]


def local(*, manual: bool = False) -> Session:
    """Start an engine inside this Python process.

    Cycles run on their own as soon as writes arrive. With `manual=True`, nothing runs until
    `step()` (tests, replay); `start()` and `stop()` switch between the two.
    """
    session = Session()
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
