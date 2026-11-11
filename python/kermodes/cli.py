"""The `kermodes` command."""

import argparse
import itertools
import json
import os
import signal
import tarfile
import time
import urllib.request
from collections.abc import Callable
from pathlib import Path

import kermodes as km
from kermodes import _native
from kermodes import project as projects
from kermodes.supervisor import Supervisor

DEV_PORT = 8730
"""`kermodes dev`'s web UI port; its Flight server listens on the next one."""


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="kermodes", description="Live and historical tables."
    )
    commands = parser.add_subparsers(dest="command", required=True)
    dev = commands.add_parser(
        "dev",
        help="run Kermodes locally: a store, its Flight server, the web UI, and a project",
        description=(
            "Opens the store in --data-dir and serves it: processes connect with "
            "km.connect() to create, write, and read tables, and the web UI shows every table. "
            "With a project (--project, or a kermodes.toml in the current directory), runs "
            "each of its resources and rolls them onto every change to its files."
        ),
    )
    dev.add_argument("--data-dir", type=Path, default=Path(".kermodes"))
    dev.add_argument("--host", default="127.0.0.1")
    dev.add_argument("--port", type=int, default=DEV_PORT, help="web UI port")
    dev.add_argument(
        "--flight-port", type=int, help="Flight port (default: --port + 1)"
    )
    dev.add_argument("--project", type=Path, help="the project to run")

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
    weir.add_argument(
        "--catalog",
        default=os.environ.get("KERMODES_CATALOG"),
        help="the catalog's Postgres URL, shared by every weir (default: $KERMODES_CATALOG; "
        "without one, SQLite in --data-dir)",
    )
    weir.add_argument(
        "--branch", default="prod", help="the catalog branch it serves (with --catalog)"
    )

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

    control = commands.add_parser(
        "control",
        help="run the controller: sync projects from git and run their resources",
        description="Syncs each configured project's mainline from git, discovers what each "
        "commit declares, and runs every resource as a Kubernetes Deployment rolled out by its "
        "update policy (architecture.md §11.3-11.4, §16.2). Secrets come from the environment: "
        "KERMODES_CATALOG (its Postgres), KERMODES_GIT_USERNAME and KERMODES_GIT_PASSWORD, "
        "KERMODES_WEBHOOK_SECRET; POD_NAMESPACE is where it runs.",
    )
    control.add_argument(
        "--config", type=Path, required=True, help="JSON: projects, image, weir, ..."
    )

    fetch = commands.add_parser(
        "fetch-snapshot",
        help="download a commit's code snapshot from the controller (run init containers)",
    )
    fetch.add_argument("url")
    fetch.add_argument("dest", type=Path)

    manifest = commands.add_parser(
        "manifest",
        help="print a project's resources as JSON",
        description="Discovers the project (its kermodes.toml and entry module) and prints its "
        "manifest: the resources the platform plans and deploys.",
    )
    manifest.add_argument("--project", type=Path, default=Path("."))

    run = commands.add_parser(
        "run",
        help="run one of a project's resources in this process",
        description="Runs an ingester or deployment from the working tree until it fails or is "
        "stopped, on a server's tables (--server, $KERMODES_URL, else kermodes dev) or a local "
        "store (--data-dir).",
    )
    run.add_argument("resource")
    run.add_argument("--project", type=Path, default=Path("."))
    where = run.add_mutually_exclusive_group()
    where.add_argument("--server", help="the store server")
    where.add_argument(
        "--data-dir", type=Path, help="a local store instead of a server"
    )
    run.add_argument(
        "--ready-file",
        type=Path,
        help="created once the run can take over from one it replaces (readiness probes)",
    )

    args = parser.parse_args(argv)
    # A supervisor stops a process with SIGTERM: shut down as on Ctrl-C.
    signal.signal(signal.SIGTERM, signal.default_int_handler)
    if args.command == "dev":
        project = args.project or (
            Path(".") if Path("kermodes.toml").exists() else None
        )
        run_dev(
            args.data_dir,
            args.host,
            args.port,
            args.flight_port or args.port + 1,
            project,
        )
    elif args.command == "weir":
        run_weir(args.data_dir, args.host, args.port, args.catalog, args.branch)
    elif args.command == "ui":
        run_ui(args.server, args.host, args.port)
    elif args.command == "ingest":
        run_websocket(
            args.url, args.table, args.source, args.columns, args.server, args.flush
        )
    elif args.command == "demo":
        run_ticker(args.host, args.port, args.rate, args.symbols)
    elif args.command == "manifest":
        print(json.dumps(projects.discover(args.project).manifest(), indent=2))
    elif args.command == "run":
        run_resource(
            args.project, args.resource, args.server, args.data_dir, args.ready_file
        )
    elif args.command == "control":
        run_control(args.config)
    elif args.command == "fetch-snapshot":
        fetch_snapshot(args.url, args.dest)


def run_control(config_path: Path) -> None:
    config = json.loads(config_path.read_text())
    for key, variable in [
        ("database", "KERMODES_CATALOG"),
        ("git_username", "KERMODES_GIT_USERNAME"),
        ("git_password", "KERMODES_GIT_PASSWORD"),
        ("webhook_secret", "KERMODES_WEBHOOK_SECRET"),
        ("namespace", "POD_NAMESPACE"),
    ]:
        if os.environ.get(variable):
            config[key] = os.environ[variable]
    print(
        f"kermodes control: {len(config.get('projects', []))} projects, "
        f"listening on {config.get('listen')}",
        flush=True,
    )
    _native.control(json.dumps(config))


def fetch_snapshot(url: str, dest: Path) -> None:
    """Extracts the tar at `url` into `dest`, refusing anything that would land outside it."""
    with (
        urllib.request.urlopen(url, timeout=60) as response,
        tarfile.open(fileobj=response, mode="r|") as tar,
    ):
        tar.extractall(dest, filter="data")


def run_resource(
    project: Path,
    name: str,
    server: str | None,
    data_dir: Path | None,
    ready: Path | None,
) -> None:
    resource = projects.discover(project).resource(name)
    db = km.local(data_dir) if data_dir else km.connect(server)
    print(f"running {name}; Ctrl-C to stop", flush=True)
    try:
        projects.run(resource, db, ready)
    except KeyboardInterrupt:
        pass


def run_dev(
    data_dir: Path, host: str, port: int, flight_port: int, project: Path | None
) -> None:
    session = km.local(data_dir)
    flight = session.serve_store(port=flight_port, host=host)
    ui = session.serve(port=port, host=host)
    print(
        f"kermodes dev: {data_dir.resolve()}\n"
        f"  web UI   {ui.url}\n"
        f"  connect  km.connect({flight.url!r})",
        flush=True,
    )
    supervisor = None
    try:
        supervisor = Supervisor(project, flight.url) if project else None
        follow(session, ui.publish, supervisor.tick if supervisor else None)
    except KeyboardInterrupt:
        pass
    finally:
        if supervisor:
            supervisor.stop()


def run_weir(
    data_dir: Path, host: str, port: int, catalog: str | None, branch: str
) -> None:
    if catalog:
        session = km.Session(data_dir, catalog=catalog, branch=branch)
        session.start()
        where = f"{data_dir.resolve()}, catalog branch {branch} in Postgres"
    else:
        session = km.local(data_dir)
        where = str(data_dir.resolve())
    flight = session.serve_store(port=port, host=host)
    print(f"kermodes weir: {where}\n  serving  {flight.url}", flush=True)
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


def follow(
    session: km.Session,
    publish: Callable[[str, km.Table], None],
    every_second: Callable[[], None] | None = None,
) -> None:
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
        if every_second:
            every_second()
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
