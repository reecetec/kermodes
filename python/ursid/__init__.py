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


def local() -> Session:
    """Start an engine inside this Python process."""
    return Session()


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
