"""Runs a project locally (`kermodes dev --project`), as the cluster does (architecture.md
§11.2, §16.6): each resource in its own process, restarted when it dies, and rolled on every
change to the project's Python. A roll starts the new run, waits until it is ready, then stops
the old one, so a writer hands its source over (§7.4) without a gap. Each app runs its own dev
server (Vite, with hot reload) behind the UI's server at `/apps/<project>/<name>/`, where it
has the address it has in the cluster."""

import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path

from kermodes import environment
from kermodes._native import KermodesError, Server

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


@dataclass
class App:
    """An app's dev server, and the manifests and lockfile it was installed from."""

    process: subprocess.Popen
    deps: dict[Path, float]

    def stop(self) -> None:
        # Its own process group: `bun run` starts Vite, which starts esbuild.
        if self.process.poll() is None:
            os.killpg(self.process.pid, signal.SIGTERM)
            try:
                self.process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                os.killpg(self.process.pid, signal.SIGKILL)
                self.process.wait()


class Supervisor:
    def __init__(self, project: Path, server: str, ui: Server | None = None) -> None:
        """Starts nothing: the first `tick` starts the project's resources and, behind `ui`,
        its apps, once the caller holds this supervisor and so can stop what it starts."""
        self.project = project.resolve()
        self.server = server
        self.ui = ui
        # The interpreter discovery and runs use: the project's environment's, once built.
        self.python = sys.executable
        self.runs: dict[str, Run] = {}
        self.specs: dict[str, dict] = {}
        self.apps: dict[str, App] = {}
        # Each app's directory, as the project declares it (`km.app(name, path=)`).
        self.app_dirs: dict[str, Path] = {}
        # Apps whose install failed, with the dependency files it failed on: tried again when
        # they change.
        self.failed: dict[str, dict[Path, float]] = {}
        self.name = ""
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
        for name, app in list(self.apps.items()):
            if app.deps != self._deps(name):
                print(
                    f"kermodes dev: app {name}'s dependencies changed; reinstalling",
                    flush=True,
                )
                self._restart_app(name)
            elif (
                app.process.poll() is not None
                and time.monotonic() >= self.restarts.get(f"app {name}", 0)
            ):
                print(f"kermodes dev: app {name} exited; restarting", flush=True)
                self.restarts[f"app {name}"] = time.monotonic() + 2
                self._restart_app(name)
        for name, deps in list(self.failed.items()):
            if deps != self._deps(name):
                del self.failed[name]
                self._start_app(name)

    def sync(self) -> None:
        """Discovers the project and rolls its resources onto the working tree."""
        if not self._environment():
            return
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
        self.name = manifest["project"]
        if self.ui is not None:
            self.app_dirs = {
                app["name"]: self.project / app["path"]
                for app in manifest.get("apps", [])
            }
            apps = set(self.app_dirs)
            for name in set(self.apps) - apps:
                print(f"kermodes dev: app {name} removed; stopping it", flush=True)
                self._stop_app(name)
            for name in sorted(apps - set(self.apps)):
                self._start_app(name)

    def stop(self) -> None:
        for run in self.runs.values():
            run.stop()
        self.runs.clear()
        for name in list(self.apps):
            self._stop_app(name)

    def _start_app(self, name: str) -> None:
        """Installs the app's dependencies and starts its dev server behind the UI's server."""
        assert self.ui is not None
        bun = shutil.which("bun")
        if bun is None:
            print(f"kermodes dev: app {name} needs Bun (https://bun.sh)", flush=True)
            return
        deps = self._deps(name)
        installed = subprocess.run(
            [bun, "install"],
            cwd=self._js_root(name),
            capture_output=True,
            text=True,
            check=False,
        )
        if installed.returncode != 0:
            last = (installed.stderr.strip().splitlines() or ["(no output)"])[-1]
            print(f"kermodes dev: app {name}: bun install failed: {last}", flush=True)
            self.failed[name] = deps
            return
        # A free port for Vite; it serves under the address the app has in the cluster.
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        base = f"/apps/{self.name}/{name}/"
        process = subprocess.Popen(
            [bun, "run", "dev", "--port", str(port), "--strictPort"]
            + ["--host", "127.0.0.1", "--base", base],
            cwd=self.app_dirs[name],
            start_new_session=True,
        )
        self.apps[name] = App(process, deps)
        self.ui.serve_app(self.name, name, f"http://127.0.0.1:{port}")
        print(
            f"kermodes dev: app {name} at {self.ui.url.rstrip('/')}{base}", flush=True
        )

    def _stop_app(self, name: str) -> None:
        app = self.apps.pop(name, None)
        if app is not None:
            app.stop()
        if self.ui is not None:
            self.ui.stop_app(self.name, name)

    def _restart_app(self, name: str) -> None:
        self._stop_app(name)
        self._start_app(name)

    def _js_root(self, name: str) -> Path:
        """Where the app's dependencies install: the project, when it is a workspace of its
        apps and shared packages, else the app's own directory."""
        return (
            self.project
            if (self.project / "package.json").exists()
            else self.app_dirs[name]
        )

    def _deps(self, name: str) -> dict[Path, float]:
        app = self.app_dirs[name]
        return {
            path: path.stat().st_mtime
            for path in {
                self._js_root(name) / "package.json",
                self._js_root(name) / "bun.lock",
                app / "package.json",
            }
            if path.exists()
        }

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
                self.python,
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
                self.python,
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

    def _environment(self) -> bool:
        """Builds the project's environment from its uv.lock, as the cluster does, unless it is
        built; discovery and runs then use its Python. Whether there is one to run on."""
        lock = environment.lock_of(self.project)
        if lock is None:
            self.python = sys.executable
            return True
        built = self.project / ".kermodes" / "environments" / lock[:16]
        if not built.exists():
            print("kermodes dev: building the environment from uv.lock", flush=True)
            # Built aside and moved into place, so a failed build never looks built.
            scratch = built.with_name(f"{built.name}.building")
            shutil.rmtree(scratch, ignore_errors=True)
            try:
                environment.build(self.project, scratch)
            except KermodesError as e:
                print(f"kermodes dev: the environment did not build: {e}", flush=True)
                return False
            scratch.rename(built)
        self.python = str(built / "bin" / "python")
        return True

    def _files(self) -> dict[Path, float]:
        """The project's Python, its kermodes.toml, and what its environment is built from. Apps
        reload themselves, and dependency directories are never walked."""
        watched = ("kermodes.toml", "uv.lock", "pyproject.toml")
        files = {}
        apps = set(self.app_dirs.values())
        for directory, subdirectories, names in os.walk(self.project):
            subdirectories[:] = [
                d
                for d in subdirectories
                if not d.startswith(".")
                and d not in ("node_modules", "__pycache__")
                and Path(directory, d) not in apps
            ]
            for name in names:
                if name.endswith(".py") or name in watched:
                    path = Path(directory, name)
                    files[path] = path.stat().st_mtime
        return files
