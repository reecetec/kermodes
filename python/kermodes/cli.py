"""The `kermodes` command."""

import argparse
import time
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
    if args.command == "dev":
        run_dev(args.data_dir, args.host, args.port, args.flight_port or args.port + 1)
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
    # Every stored table is in the web UI, including those other processes create later.
    live: dict[str, km.Table] = {}
    try:
        while True:
            for name in session.tables:
                if name not in live:
                    live[name] = session.live_table(name)
                    ui.publish(name, live[name])
            time.sleep(1)
    except KeyboardInterrupt:
        pass


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
        while True:
            time.sleep(5)
            stats = ingest.stats
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
