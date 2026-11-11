"""The `kermodes` command."""

import argparse
import itertools
import signal
import time
from collections.abc import Callable
from pathlib import Path

import kermodes as km

DEV_PORT = 8730
"""`kermodes dev`'s web UI port; its Flight server listens on the next one."""


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="kermodes", description="Live and historical tables."
    )
    commands = parser.add_subparsers(dest="command", required=True)
    dev = commands.add_parser(
        "dev",
        help="run Kermodes locally: a store, its Flight server, and the web UI",
        description=(
            "Opens the store in --data-dir and serves it: processes connect with "
            "km.connect() to create, write, and read tables, and the web UI shows every table."
        ),
    )
    dev.add_argument("--data-dir", type=Path, default=Path(".kermodes"))
    dev.add_argument("--host", default="127.0.0.1")
    dev.add_argument("--port", type=int, default=DEV_PORT, help="web UI port")
    dev.add_argument(
        "--flight-port", type=int, help="Flight port (default: --port + 1)"
    )

    weir = commands.add_parser(
        "weir",
        help="serve a store over Arrow Flight",
        description=(
            "Opens the store in --data-dir and serves it: writers and engines connect with "
            "km.connect(). The store's half of `kermodes dev`, for a cluster."
        ),
    )
    weir.add_argument("--data-dir", type=Path, required=True)
    weir.add_argument("--host", default="127.0.0.1")
    weir.add_argument("--port", type=int, default=DEV_PORT + 1)

    ui = commands.add_parser(
        "ui",
        help="serve every table of a store in the web UI",
        description=(
            "Connects to a store server (kermodes weir or dev), follows every table as it "
            "appears, and serves them in the web UI. Exits when it loses the store, so that "
            "its supervisor restarts it. The engine's half of `kermodes dev`, for a cluster."
        ),
    )
    ui.add_argument(
        "--server", help="the store server (default: $KERMODES_URL, else kermodes dev)"
    )
    ui.add_argument("--host", default="127.0.0.1")
    ui.add_argument("--port", type=int, default=DEV_PORT)

    ingest = commands.add_parser("ingest", help="run a built-in connector into a table")
    connectors = ingest.add_subparsers(dest="connector", required=True)
    websocket = connectors.add_parser(
        "websocket",
        help="read a WebSocket feed of JSON into a table",
        description=(
            "Each message is a JSON object or an array of them. Fields are matched to the "
            "table's columns by name; others are ignored, missing ones are NULL, and an "
            "unreadable record is skipped and counted."
        ),
    )
    websocket.add_argument("url", help="the feed, e.g. ws://127.0.0.1:8740/")
    websocket.add_argument("--table", required=True, help="namespace.table")
    websocket.add_argument("--source", default="websocket")
    websocket.add_argument(
        "--columns",
        help="create the table if needed, e.g. sym:utf8,price:float64,size:int64,ts:timestamp",
    )
    websocket.add_argument(
        "--server", help="the store server (default: $KERMODES_URL, else kermodes dev)"
    )
    websocket.add_argument("--flush", default="100ms")

    demo = commands.add_parser("demo", help="demo data sources")
    feeds = demo.add_subparsers(dest="feed", required=True)
    feed = feeds.add_parser(
        "ticker",
        help="serve a synthetic market-data feed over WebSocket",
        description=(
            "Random-walk prices as JSON arrays of {sym, price, size, ts} every 10 ms. Read it "
            "with: kermodes ingest websocket ws://HOST:PORT/ --table md.ticks --columns "
            "sym:utf8,price:float64,size:int64,ts:timestamp"
        ),
    )
    feed.add_argument("--host", default="127.0.0.1")
    feed.add_argument("--port", type=int, default=8740)
    feed.add_argument("--rate", type=int, default=1_000, help="ticks a second")
    feed.add_argument("--symbols", type=int, default=100)

    args = parser.parse_args(argv)
    # A supervisor stops a process with SIGTERM: shut down as on Ctrl-C.
    signal.signal(signal.SIGTERM, signal.default_int_handler)
    if args.command == "dev":
        run_dev(args.data_dir, args.host, args.port, args.flight_port or args.port + 1)
    elif args.command == "weir":
        run_weir(args.data_dir, args.host, args.port)
    elif args.command == "ui":
        run_ui(args.server, args.host, args.port)
    elif args.command == "ingest":
        run_websocket(
            args.url, args.table, args.source, args.columns, args.server, args.flush
        )
    elif args.command == "demo":
        run_ticker(args.host, args.port, args.rate, args.symbols)


def run_dev(data_dir: Path, host: str, port: int, flight_port: int) -> None:
    session = km.local(data_dir)
    flight = session.serve_store(port=flight_port, host=host)
    ui = session.serve(port=port, host=host)
    print(
        f"kermodes dev: {data_dir.resolve()}\n"
        f"  web UI   {ui.url}\n"
        f"  connect  km.connect({flight.url!r})",
        flush=True,
    )
    try:
        follow(session, ui.publish)
    except KeyboardInterrupt:
        pass


def run_weir(data_dir: Path, host: str, port: int) -> None:
    session = km.local(data_dir)
    flight = session.serve_store(port=port, host=host)
    print(f"kermodes weir: {data_dir.resolve()}\n  serving  {flight.url}", flush=True)
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        pass


def run_ui(server: str | None, host: str, port: int) -> None:
    session = km.connect(server)
    ui = session.serve(port=port, host=host)
    print(f"kermodes ui: {ui.url}", flush=True)
    try:
        follow(session, ui.publish)
    except KeyboardInterrupt:
        pass


def follow(session: km.Session, publish: Callable[[str, km.Table], None]) -> None:
    """Publishes every stored table, including those created later, until one fails.

    A live table fails when it loses the store it follows; the KermodesError that `len` raises
    then ends the process.
    """
    live: dict[str, km.Table] = {}
    while True:
        for name in session.tables:
            if name not in live:
                live[name] = session.live_table(name)
                publish(name, live[name])
        for table in live.values():
            len(table)
        time.sleep(1)


def run_websocket(
    url: str,
    table: str,
    source: str,
    columns: str | None,
    server: str | None,
    flush: str,
) -> None:
    session = km.connect(server)
    if columns:
        session.create_table(table, parse_columns(columns))
    ingest = session.ingest_websocket(url, table, source=source, flush=flush)
    print(f"reading {url} into {table} (source {source}); Ctrl-C to stop", flush=True)
    try:
        for second in itertools.count(1):
            time.sleep(1)
            stats = ingest.stats
            if stats.failed:
                # Its store went: exit, so a supervisor restarts it against the store.
                raise SystemExit(f"kermodes ingest: stopped: {stats.last_error}")
            if second % 5 == 0:
                print(
                    f"{stats.rows:,} rows from {stats.messages:,} messages; "
                    f"{stats.skipped:,} skipped, {stats.reconnects} reconnects"
                    + (f"; last error: {stats.last_error}" if stats.last_error else ""),
                    flush=True,
                )
    except KeyboardInterrupt:
        ingest.stop()


def parse_columns(text: str) -> dict[str, str]:
    """`name:type,...` as create_table's columns."""
    columns = {}
    for column in text.split(","):
        name, _, kind = column.partition(":")
        if not name.strip() or not kind.strip():
            raise SystemExit(f"--columns takes name:type pairs, not {column!r}")
        columns[name.strip()] = kind.strip()
    return columns


def run_ticker(host: str, port: int, rate: int, symbols: int) -> None:
    feed = km.demo.ticker(port=port, rate=rate, symbols=symbols, host=host)
    print(f"ticker: {feed.url} ({rate:,} ticks/s over {symbols} symbols)", flush=True)
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        feed.stop()


if __name__ == "__main__":
    main()
