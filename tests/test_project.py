import subprocess
import sys
import textwrap
import time

import kermodes as km
import pyarrow as pa
import pytest
from kermodes import project


def make_project(root, app: str, namespaces=("md",), entry="ticker.app"):
    root.mkdir(parents=True, exist_ok=True)
    (root / "kermodes.toml").write_text(
        textwrap.dedent(f"""
            [project]
            name = "ticker"
            entry = "{entry}"
            namespaces = {list(namespaces)!r}
        """)
    )
    package = root / "src" / entry.partition(".")[0]
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("")
    (package / f"{entry.partition('.')[2]}.py").write_text(textwrap.dedent(app))


def test_a_project_declares_ingesters_and_deployments(tmp_path):
    make_project(
        tmp_path,
        """
        import kermodes as km

        ticks = km.ingester(
            "ticks",
            target="md.ticks",
            source=km.websocket("ws://feed:8740/", flush="50ms"),
            columns={"sym": km.string(), "price": km.float64()},
        )

        @km.deployment(update="manual")
        def quote_poller(db):
            pass
        """,
    )
    manifest = project.discover(tmp_path).manifest()
    assert manifest["project"] == "ticker"
    assert manifest["resources"] == [
        {
            "name": "ticks",
            "update": "auto",
            "rollout": "rolling",
            "kind": "ingester",
            "target": "md.ticks",
            "columns": {"sym": "utf8", "price": "float64"},
            "source": {
                "kind": "websocket",
                "url": "ws://feed:8740/",
                "flush": "50ms",
                "max_rows": 10_000,
            },
        },
        {
            "name": "quote-poller",
            "update": "manual",
            "rollout": "rolling",
            "kind": "deployment",
            "function": "ticker.app:quote_poller",
        },
    ]


@pytest.mark.parametrize(
    "app",
    [
        # A table in a namespace the project does not own.
        'km.ingester("t", target="sports.odds", source=km.websocket("ws://x/"))',
        # Two resources with one name.
        "@km.deployment()\ndef a(db): pass\n@km.deployment(name='a')\ndef b(db): pass",
        # A policy that does not exist.
        "@km.deployment(update='sometimes')\ndef a(db): pass",
    ],
)
def test_declarations_the_platform_cannot_run_are_refused(tmp_path, app):
    make_project(tmp_path, "import kermodes as km\n" + app)
    with pytest.raises(km.KermodesError):
        project.discover(tmp_path)


def test_a_directory_without_kermodes_toml_is_not_a_project(tmp_path):
    with pytest.raises(km.KermodesError):
        project.discover(tmp_path)


def test_kermodes_run_runs_a_deployment_on_a_servers_tables(tmp_path):
    make_project(
        tmp_path / "proj",
        """
        import pyarrow as pa
        import kermodes as km

        @km.deployment()
        def poller(db):
            db.create_table("md.quotes", {"n": km.int64()})
            writer = db.writer("md.quotes", source="poller", wait=True)
            writer.write(pa.table({"n": pa.array([1, 2, 3], pa.int64())}))
        """,
    )
    owner = km.local(tmp_path / "store", manual=True)
    server = owner.serve_store()
    run = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "kermodes",
            "run",
            "poller",
            "--project",
            str(tmp_path / "proj"),
            "--server",
            server.url,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        reader = km.connect(server.url, manual=True)
        deadline = time.monotonic() + 30
        while "md.quotes" not in reader.tables:
            assert time.monotonic() < deadline and run.poll() is None, run.stderr
            time.sleep(0.05)
        live = reader.live_table("md.quotes")
        while len(live) < 3:
            assert time.monotonic() < deadline, len(live)
            reader.step()
            time.sleep(0.05)
        assert pa.table(live).column("n").to_pylist() == [1, 2, 3]
        # The deployment keeps running after its function returns, until it is stopped.
        assert run.poll() is None
    finally:
        run.terminate()
    assert run.wait(timeout=10) == 0


COUNTER = """
import threading
import time

import pyarrow as pa
import kermodes as km

VERSION = {version}


@km.deployment()
def counter(db):
    db.create_table("md.versions", {{"version": km.int64(), "epoch": km.int64()}})
    writer = db.writer("md.versions", source="counter", wait=True)

    def write():
        while True:
            row = {{"version": [VERSION], "epoch": [writer.epoch]}}
            writer.write(pa.table(row, schema=pa.schema([("version", pa.int64()), ("epoch", pa.int64())])))
            time.sleep(0.02)

    threading.Thread(target=write, daemon=True).start()
"""


def free_port() -> int:
    import socket

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_kermodes_dev_rolls_a_project_onto_each_change(tmp_path):
    root = tmp_path / "proj"
    make_project(root, COUNTER.format(version=1))
    port, flight = free_port(), free_port()
    dev = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "kermodes",
            "dev",
            "--project",
            str(root),
            "--data-dir",
            str(tmp_path / "store"),
            "--port",
            str(port),
            "--flight-port",
            str(flight),
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert dev.stdout is not None
        while "connect" not in dev.stdout.readline():
            assert dev.poll() is None
        reader = km.connect(f"grpc://127.0.0.1:{flight}", manual=True)

        def rows() -> list[tuple[int, int]]:
            reader.step()
            table = pa.table(live)
            return list(
                zip(
                    table.column("version").to_pylist(),
                    table.column("epoch").to_pylist(),
                )
            )

        deadline = time.monotonic() + 60
        while "md.versions" not in reader.tables:
            assert time.monotonic() < deadline and dev.poll() is None
            time.sleep(0.05)
        live = reader.live_table("md.versions")
        while len(rows()) < 10:
            assert time.monotonic() < deadline
            time.sleep(0.05)

        (root / "src" / "ticker" / "app.py").write_text(COUNTER.format(version=2))
        while not any(version == 2 for version, _ in rows()):
            assert time.monotonic() < deadline and dev.poll() is None
            time.sleep(0.05)
        time.sleep(0.5)

        # One writer at a time: version 1's rows under its lease, then version 2's under the
        # next, never interleaved.
        seen = rows()
        first = seen.index((2, 2))
        assert set(seen[:first]) == {(1, 1)}
        assert set(seen[first:]) == {(2, 2)}
    finally:
        dev.terminate()
        # Shown by pytest when the test fails.
        print(dev.communicate(timeout=30)[0])
    assert dev.returncode == 0
