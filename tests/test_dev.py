"""`kermodes dev` as people use it: one server, processes connecting to it, the server killed."""

import socket
import subprocess
import sys
import time

import kermodes as km
import pyarrow as pa
import pytest

_WRITE_UNTIL_THE_SERVER_GOES = """
import sys
import pyarrow as pa
import kermodes as km

s = km.connect(sys.argv[1], manual=True)
s.create_table("md.ticks", {"n": km.int64()})
w = s.writer("md.ticks", source="feed")
seq = (w.last_seq or 0) + 1
while True:
    w.write(pa.table({"n": pa.array([seq] * 100, pa.int64())}), seq=seq)
    print(seq, flush=True)  # acknowledged: durable on the server
    seq += 1
"""


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def start_dev(data_dir, port: int, flight_port: int) -> subprocess.Popen:
    dev = subprocess.Popen(
        [sys.executable, "-m", "kermodes", "dev", "--data-dir", str(data_dir)]
        + ["--port", str(port), "--flight-port", str(flight_port)],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert dev.stdout is not None
    while "connect" not in dev.stdout.readline():
        assert dev.poll() is None, "kermodes dev exited"
    return dev


def batches_of(session: km.Session, table: km.Table, count: int) -> list[int]:
    """The table's batch numbers once `count` batches have arrived (rows stream in)."""
    deadline = time.monotonic() + 30
    while True:
        session.step()
        n = pa.table(table)["n"].to_pylist()
        if len(n) >= 100 * count or time.monotonic() > deadline:
            assert len(n) % 100 == 0
            return n[::100]
        time.sleep(0.01)


def test_processes_share_kermodes_dev_and_a_killed_server_loses_nothing(tmp_path):
    port, flight_port = free_port(), free_port()
    url = f"grpc://127.0.0.1:{flight_port}"
    dev = start_dev(tmp_path, port, flight_port)
    writer = subprocess.Popen(
        [sys.executable, "-c", _WRITE_UNTIL_THE_SERVER_GOES, url],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    assert writer.stdout is not None
    acknowledged = 0
    while acknowledged < 30:
        acknowledged = int(writer.stdout.readline())

    # A third process reads the table while it ticks.
    reader = km.connect(url, manual=True)
    live = reader.live_table("md.ticks")
    assert batches_of(reader, live, 30)[:30] == list(range(1, 31))

    dev.kill()  # SIGKILL, mid-write
    dev.wait()
    writer.wait(timeout=30)  # its next write fails: the server is gone
    for line in writer.stdout:
        acknowledged = int(line)
    del live, reader

    dev = start_dev(tmp_path, port, flight_port)
    try:
        s = km.connect(url, manual=True)
        w = s.writer("md.ticks", source="feed")
        committed = w.last_seq
        assert committed is not None and committed >= acknowledged
        table = s.live_table("md.ticks")
        assert batches_of(s, table, committed) == list(range(1, committed + 1))
        # The writer resumes where the source left off.
        assert w.write(pa.table({"n": pa.array([committed + 1] * 100, pa.int64())}))
        assert batches_of(s, table, committed + 1) == list(range(1, committed + 2))
    finally:
        dev.kill()
        dev.wait()


def test_a_live_table_fails_when_its_store_goes(tmp_path):
    owner = km.local(tmp_path, manual=True)
    owner.create_table("md.ticks", {"n": km.int64()})
    server = owner.serve_store()
    reader = km.connect(server.url, manual=True)
    live = reader.live_table("md.ticks")
    derived = live.where("n > 0")
    reader.step()
    assert len(live) == 0

    server.stop()
    deadline = time.monotonic() + 10
    while True:
        reader.step()
        try:
            len(live)
        except km.KermodesError:
            break
        assert time.monotonic() < deadline, "the live table never noticed"
        time.sleep(0.01)
    with pytest.raises(km.KermodesError):
        len(derived)


def test_kermodes_ui_serves_on_while_its_store_is_away(tmp_path):
    store = km.local(tmp_path / "store")
    store.create_table("md.t", {"n": km.int64()})
    flight, port = free_port(), free_port()
    server = store.serve_store(port=flight)
    ui = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "kermodes",
            "ui",
            "--server",
            server.url,
            "--port",
            str(port),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    def say(what: str) -> None:
        assert ui.stdout is not None
        while what not in (line := ui.stdout.readline()):
            assert line, f"kermodes ui ended before saying {what!r}"

    def serving() -> bool:
        with socket.socket() as s:
            return s.connect_ex(("127.0.0.1", port)) == 0

    try:
        say("kermodes ui: http")
        assert serving()
        # The store goes (a restart, an upgrade): the page still loads.
        server.stop()
        say("the store is away")
        time.sleep(1.5)
        assert serving() and ui.poll() is None
        # It comes back on the same address, and so do its tables.
        server = store.serve_store(port=flight)
        say("the store is back")
        assert serving() and ui.poll() is None
    finally:
        ui.terminate()
        print(ui.communicate(timeout=30)[0])
        server.stop()
