"""md.ticks: the demo ticker's feed (`kermodes demo ticker`), read by the built-in connector."""

import os

import kermodes as km

ticks = km.ingester(
    "ticks",
    target="md.ticks",
    source=km.websocket(os.environ.get("TICKER_URL", "ws://127.0.0.1:8740/")),
    columns={
        "sym": km.string(),
        "price": km.float64(),
        "size": km.int64(),
        "ts": km.timestamp(),
    },
)
