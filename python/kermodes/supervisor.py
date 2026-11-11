"""Runs a project's resources locally (`kermodes dev --project`), as the cluster does
(architecture.md §11.2, §16.6): each resource in its own process, restarted when it dies, and
rolled on every change to the project's files. A roll starts the new run, waits until it is
ready, then stops the old one, so a writer hands its source over (§7.4) without a gap."""

import json
import os
import signal
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path

READY_TIMEOUT = 60.0
"""How long a new run may take to become ready before its roll fails and the old run stays."""

SETTLE = 0.3
"""How long the project's files must stay unchanged before a change is acted on."""

# Python trusts a cached .pyc while its source keeps the same size and mtime to the second, so an
# edit that keeps a file's length (`VERSION = 1` to `VERSION = 2`) within a second would run the
# old code. Project code is small: its processes compile it from source every time.
ENV = os.environ | {"PYTHONDONTWRITEBYTECODE": "1"}


@dataclass
class Run:
    process: subprocess.Popen
    ready: Path
    started: float = field(default_factory=time.monotonic)

    def is_ready(self) -> bool:
        return self.ready.exists()

    def stop(self) -> None:
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGTERM)
            try:
                self.process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
        self.ready.unlink(missing_ok=True)


class Supervisor:
    def __init__(self, project: Path, server: str) -> None:
        """Starts nothing: the first `tick` starts the project's resources, once the caller
        holds this supervisor and so can stop what it starts."""
        self.project = project.resolve()
        self.server = server
        self.runs: dict[str, Run] = {}
        self.specs: dict[str, dict] = {}
        self.files: dict[Path, float] = {}
        self.restarts: dict[str, float] = {}
        self._dir = Path(tempfile.mkdtemp(prefix="kermodes-dev-"))

    def tick(self) -> None:
        """Rolls everything if the project changed, and restarts runs that died."""
        files = self._files()
        if files != self.files:
            # A save can be several writes (truncate, then write): discovering between them
            # would load half a file. Wait until the project stops changing.
            while True:
                time.sleep(SETTLE)
                settled = self._files()
                if settled == files:
                    break
                files = settled
            self.files = files
            self.sync()
        for name, run in list(self.runs.items()):
            if run.process.poll() is not None and time.monotonic() >= self.restarts.get(
                name, 0
            ):
                print(
                    f"kermodes dev: {name} exited ({run.process.returncode}); restarting",
                    flush=True,
                )
                # Back off a crashing resource rather than spin on it.
                self.restarts[name] = time.monotonic() + 2
                self.runs[name] = self._start(name)

    def sync(self) -> None:
        """Discovers the project and rolls its resources onto the working tree."""
        manifest = self._manifest()
        if manifest is None:
            return
        specs = {spec["name"]: spec for spec in manifest["resources"]}
        for name in list(self.runs):
            if name not in specs:
                print(f"kermodes dev: {name} removed; stopping it", flush=True)
                self.runs.pop(name).stop()
        for name in specs:
            self._roll(name)
        self.specs = specs

    def stop(self) -> None:
        for run in self.runs.values():
            run.stop()
        self.runs.clear()

    def _roll(self, name: str) -> None:
        old = self.runs.get(name)
        new = self._start(name)
        deadline = time.monotonic() + READY_TIMEOUT
        while not new.is_ready():
            if new.process.poll() is not None or time.monotonic() > deadline:
                new.stop()
                if old is None:
                    print(f"kermodes dev: {name} failed to start", flush=True)
                else:
                    print(
                        f"kermodes dev: {name}'s new run failed; the old one keeps running",
                        flush=True,
                    )
                return
            time.sleep(0.05)
        self.runs[name] = new
        if old is not None:
            old.stop()
        print(f"kermodes dev: {name} {'rolled' if old else 'started'}", flush=True)

    def _start(self, name: str) -> Run:
        ready = self._dir / f"{name}-{time.monotonic_ns()}.ready"
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "kermodes",
                "run",
                name,
                "--project",
                str(self.project),
                "--server",
                self.server,
                "--ready-file",
                str(ready),
            ],
            env=ENV,
        )
        return Run(process, ready)

    def _manifest(self) -> dict | None:
        # Discovery imports the project's code: in its own process, never the dev server's.
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "kermodes",
                "manifest",
                "--project",
                str(self.project),
            ],
            capture_output=True,
            env=ENV,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            last = (result.stderr.strip().splitlines() or ["(no output)"])[-1]
            print(f"kermodes dev: cannot load the project: {last}", flush=True)
            return None
        return json.loads(result.stdout)

    def _files(self) -> dict[Path, float]:
        return {
            path: path.stat().st_mtime
            for pattern in ("kermodes.toml", "**/*.py")
            for path in self.project.glob(pattern)
            if "__pycache__" not in path.parts
        }
