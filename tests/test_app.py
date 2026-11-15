"""An app's process (`kermodes run <app>`), as the cluster runs it: its function's tables and its
built frontend, served under the app's path."""

import base64
import json
import os
import socket
import subprocess
import sys
import textwrap
import time
import urllib.request

import kermodes as km


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def first_message(port: int, path: str) -> dict:
    """The first message the server sends on a WebSocket at `path`: a bare-bones client, for
    the one text frame (unmasked, from a server) the test reads."""
    with socket.create_connection(("127.0.0.1", port), timeout=10) as s:
        key = base64.b64encode(os.urandom(16)).decode()
        s.sendall(
            f"GET {path} HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n".encode()
        )
        received = b""
        while b"\r\n\r\n" not in received:
            received += s.recv(4096)
        head, frame = received.split(b"\r\n\r\n", 1)
        assert head.startswith(b"HTTP/1.1 101"), head

        def take(n: int) -> bytes:
            nonlocal frame
            while len(frame) < n:
                frame += s.recv(65536)
            taken, frame = frame[:n], frame[n:]
            return taken

        first, length = take(2)
        assert first == 0x81, "a text frame"
        if length == 126:
            length = int.from_bytes(take(2))
        elif length == 127:
            length = int.from_bytes(take(8))
        return json.loads(take(length))


def test_an_app_serves_its_frontend_and_tables_under_its_path(tmp_path):
    project = tmp_path / "ticker"
    (project / "src" / "ticker").mkdir(parents=True)
    (project / "kermodes.toml").write_text(
        '[project]\nname = "ticker"\nentry = "ticker.resources"\nnamespaces = ["md"]\n'
    )
    (project / "src" / "ticker" / "__init__.py").write_text("")
    (project / "src" / "ticker" / "resources.py").write_text("")
    (project / "apps" / "market").mkdir(parents=True)
    (project / "apps" / "market" / "app.py").write_text(
        textwrap.dedent("""
            import kermodes as km

            @km.app()
            def market(db):
                ticks = db.live_table("md.ticks")
                return {"spreads": ticks.where("price > 0")}
        """)
    )
    (project / "apps" / "market" / "App.tsx").write_text("export default () => null;\n")
    built = tmp_path / "built"
    built.mkdir()
    (built / "index.html").write_text("<!doctype html>market")
    store = tmp_path / "store"
    km.local(store).create_table(
        "md.ticks", {"sym": km.string(), "price": km.float64()}
    )

    port = free_port()
    ready = tmp_path / "ready"
    app = subprocess.Popen(
        [sys.executable, "-m", "kermodes", "run", "market", "--project", str(project)]
        + ["--data-dir", str(store), "--ready-file", str(ready)]
        + ["--port", str(port), "--app-dir", str(built)],
    )
    try:
        deadline = time.monotonic() + 30
        while not ready.exists():
            assert app.poll() is None, "the app exited"
            assert time.monotonic() < deadline, "the app never became ready"
            time.sleep(0.05)
        page = urllib.request.urlopen(
            f"http://127.0.0.1:{port}/apps/ticker/market/", timeout=10
        )
        assert page.read() == b"<!doctype html>market"
        tables = first_message(port, "/apps/ticker/market/ws")
        assert tables["type"] == "tables"
        names = {t["name"] for t in tables["tables"]}
        # The function's table, and the platform's beside it.
        assert {"spreads", "md.ticks"} <= names
    finally:
        app.terminate()
        app.wait(timeout=15)
