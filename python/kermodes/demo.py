"""Demo data sources: feeds to try Kermodes against."""

from kermodes import _native

Ticker = _native.Ticker


def ticker(
    port: int = 0, rate: int = 1_000, symbols: int = 100, host: str = "127.0.0.1"
) -> Ticker:
    """Serve a synthetic market-data feed over WebSocket on `host:port` (0 picks a free port).

    About `rate` ticks a second over `symbols` symbols, sent every 10 ms as JSON arrays of
    `{seq, sym, price, size, ts}`, the same ticks to every client, numbered by `seq`. Read it with
    `session.ingest_websocket(feed.url, "md.ticks")`, adding `seq="seq"` (and an int64 `seq`
    column) to write each tick once across handovers. The feed runs until `stop()` or until the
    object goes.
    """
    return _native.ticker(port=port, rate=rate, symbols=symbols, host=host)
