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
    args = parser.parse_args(argv)
    if args.command == "dev":
        run_dev(args.data_dir, args.host, args.port, args.flight_port or args.port + 1)


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


if __name__ == "__main__":
    main()
