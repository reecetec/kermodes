"""Projects (architecture.md §11.1): a directory with `kermodes.toml` whose entry module declares
resources, which Kermodes runs: locally with `kermodes run` and `kermodes dev`, in the cluster
when a commit is synced.

Declarations::

    import kermodes as km

    ticks = km.ingester(
        "ticks",
        target="md.ticks",
        source=km.websocket("wss://feed.example/ticks"),
        columns={"sym": km.string(), "price": km.float64()},
    )

    @km.deployment(cpu=1, memory="2Gi")  # defaults: 0.5 CPU, 512Mi
    def poller(db):
        writer = db.writer("md.quotes", source="poller", wait=True)
        ...

    @km.app()  # its frontend in apps/market/
    def market(db):
        return {"last": db.live_table("md.ticks").last_by("sym")}

Discovery imports the entry module and returns a `Project`; its manifest is the JSON the platform
plans and deploys from (`kermodes manifest`). An app is a deployment with a frontend: its process
runs its function and serves the frontend and the tables it returns at
`/apps/<project>/<name>/` (architecture.md §15.2).
"""

import importlib
import re
import sys
import time
import tomllib
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from kermodes._native import KermodesError, Session, Table

UPDATES = ("auto", "manual", "on_start")
ROLLOUTS = ("rolling", "recreate")
Update = Literal["auto", "manual", "on_start"]
Rollout = Literal["rolling", "recreate"]

_NAME = re.compile(r"[a-z][a-z0-9_-]{0,62}")
# Kubernetes quantities: cores or millicores; bytes, or with a binary or decimal suffix.
_CPU = re.compile(r"(\d+(\.\d+)?|\d+m)")
_MEMORY = re.compile(r"\d+(Ki|Mi|Gi|Ti|k|M|G|T)?")
# What a resource gets when it does not say: its requests and limits both.
DEFAULT_CPU = "500m"
DEFAULT_MEMORY = "512Mi"


@dataclass(frozen=True)
class WebSocket:
    """A WebSocket feed of JSON objects, or arrays of them (architecture.md §6.2)."""

    url: str
    flush: str = "100ms"
    max_rows: int = 10_000
    # The integer column the feed numbers its records by: each is written once across handovers.
    seq: str | None = None


def websocket(
    url: str, *, flush: str = "100ms", max_rows: int = 10_000, seq: str | None = None
) -> WebSocket:
    """A WebSocket feed of JSON, for `ingester(source=...)`. With `seq`, the column the feed
    numbers its records by, a rolling update writes each record exactly once."""
    return WebSocket(url, flush, max_rows, seq)


@dataclass(frozen=True)
class Ingester:
    """A built-in connector reading `source` into the table `target`, as the source called
    after the resource."""

    name: str
    target: str
    source: WebSocket
    columns: dict[str, str] | None = None
    update: Update = "auto"
    rollout: Rollout = "rolling"
    cpu: str = DEFAULT_CPU
    memory: str = DEFAULT_MEMORY
    # Where it is declared: the file and line of the `ingester(...)` call.
    defined_at: tuple[str, int] | None = None


@dataclass(frozen=True)
class Deployment:
    """Python that runs as long as it is deployed: `function(db)` with a session on the
    platform's tables. The session keeps running after `function` returns."""

    name: str
    function: Callable[[Session], object]
    # The function's name in the entry module, which the manifest names it by.
    function_name: str
    update: Update = "auto"
    rollout: Rollout = "rolling"
    cpu: str = DEFAULT_CPU
    memory: str = DEFAULT_MEMORY
    # Where it is declared: the file and first line (its first decorator) of the function.
    defined_at: tuple[str, int] | None = None


@dataclass(frozen=True)
class App:
    """A deployment with a frontend: `function(db)` runs in its own process, which serves the
    TypeScript frontend in the project's directory `path` (one with a package.json whose `dev`
    and `build` scripts run Vite) and the tables the function returns, at
    `/apps/<project>/<name>/`."""

    name: str
    function: Callable[[Session], object]
    function_name: str
    path: str
    update: Update = "auto"
    rollout: Rollout = "rolling"
    cpu: str = DEFAULT_CPU
    memory: str = DEFAULT_MEMORY
    defined_at: tuple[str, int] | None = None


Resource = Ingester | Deployment | App


# What the module being discovered declares; `discover` empties it around each import.
_declared: list[Resource] = []


def app(
    name: str | None = None,
    *,
    path: str | None = None,
    update: Update = "auto",
    rollout: Rollout = "rolling",
    cpu: float | str = DEFAULT_CPU,
    memory: int | str = DEFAULT_MEMORY,
) -> Callable[[Callable[[Session], object]], Callable[[Session], object]]:
    """Declares the decorated `function(db)` as an app, named after it unless `name`, whose
    frontend is the project's directory `path` (`apps/<name>` by default). Its process serves
    the frontend and the tables the function returns (`{"name": table}`, or nothing), beside
    the platform's; `cpu` and `memory` as for a deployment."""

    def declare(function: Callable[[Session], object]) -> Callable[[Session], object]:
        function_name = getattr(function, "__name__", None)
        if function_name is None:
            raise KermodesError("@km.app() decorates a function")
        resource_name = name or function_name.replace("_", "-")
        code = getattr(function, "__code__", None)
        resource = App(
            resource_name,
            function,
            function_name,
            path or f"apps/{resource_name}",
            update,
            rollout,
            _cpu(resource_name, cpu),
            _memory(resource_name, memory),
            None if code is None else (code.co_filename, code.co_firstlineno),
        )
        _check_policy(resource)
        _declared.append(resource)
        return function

    return declare


def ingester(
    name: str,
    *,
    target: str,
    source: WebSocket,
    columns: dict[str, str] | None = None,
    update: Update = "auto",
    rollout: Rollout = "rolling",
    cpu: float | str = DEFAULT_CPU,
    memory: int | str = DEFAULT_MEMORY,
) -> Ingester:
    """Declares a built-in connector reading `source` into `target` (created with `columns`
    if it does not exist), with `cpu` and `memory` as its requests and limits."""
    caller = sys._getframe(1)
    resource = Ingester(
        name,
        target,
        source,
        columns,
        update,
        rollout,
        _cpu(name, cpu),
        _memory(name, memory),
        (caller.f_code.co_filename, caller.f_lineno),
    )
    _check_policy(resource)
    _declared.append(resource)
    return resource


def deployment(
    name: str | None = None,
    *,
    update: Update = "auto",
    rollout: Rollout = "rolling",
    cpu: float | str = DEFAULT_CPU,
    memory: int | str = DEFAULT_MEMORY,
) -> Callable[[Callable[[Session], object]], Callable[[Session], object]]:
    """Declares the decorated `function(db)` as a deployment, named after it unless `name`, with
    `cpu` (cores, or a quantity such as "500m") and `memory` (bytes, or a quantity such as
    "2Gi") as its requests and limits."""

    def declare(function: Callable[[Session], object]) -> Callable[[Session], object]:
        function_name = getattr(function, "__name__", None)
        if function_name is None:
            raise KermodesError("@km.deployment() decorates a function")
        resource_name = name or function_name.replace("_", "-")
        code = getattr(function, "__code__", None)
        resource = Deployment(
            resource_name,
            function,
            function_name,
            update,
            rollout,
            _cpu(resource_name, cpu),
            _memory(resource_name, memory),
            None if code is None else (code.co_filename, code.co_firstlineno),
        )
        _check_policy(resource)
        _declared.append(resource)
        return function

    return declare


@dataclass(frozen=True)
class Project:
    name: str
    root: Path
    entry: str
    namespaces: tuple[str, ...]
    mainline: str
    resources: dict[str, Resource] = field(default_factory=dict)

    def resource(self, name: str) -> Resource:
        try:
            return self.resources[name]
        except KeyError:
            known = ", ".join(sorted(self.resources)) or "none"
            raise KermodesError(
                f"{self.name} declares no resource `{name}` (it has: {known})"
            ) from None

    def app_path(self, name: str) -> str:
        """Where the app `name` is served: `/apps/<project>/<name>/`."""
        return f"/apps/{self.name}/{name}/"

    def manifest(self) -> dict:
        """The resolved declarations, as the platform plans and deploys them."""
        return {
            "project": self.name,
            "entry": self.entry,
            "namespaces": list(self.namespaces),
            "mainline": self.mainline,
            "resources": [
                _spec(r, self.entry, self.root) for r in self.resources.values()
            ],
        }


def discover(root: str | Path = ".") -> Project:
    """Loads the project in `root`: its `kermodes.toml`, then the resources its entry module
    declares."""
    root = Path(root).resolve()
    config_path = root / "kermodes.toml"
    if not config_path.exists():
        raise KermodesError(f"{root} has no kermodes.toml; `kermodes init` makes one")
    try:
        config = tomllib.loads(config_path.read_text())["project"]
        name, entry = config["name"], config["entry"]
    except (tomllib.TOMLDecodeError, KeyError) as e:
        raise KermodesError(
            f"{config_path}: needs [project] with name and entry ({e})"
        ) from None
    if not _NAME.fullmatch(name):
        raise KermodesError(
            f"{config_path}: project name `{name}` must match {_NAME.pattern}"
        )
    namespaces = tuple(config.get("namespaces", ()))
    mainline = config.get("mainline", "main")

    # Code lives in src/ when the project has one (architecture.md §11.1).
    path = str(root / "src") if (root / "src").is_dir() else str(root)
    sys.path.insert(0, path)
    _declared.clear()
    try:
        # Cached modules would not run their declarations again, and a cached package would
        # still import from wherever it was first found: the project's code is imported fresh.
        package = entry.partition(".")[0]
        for module in [
            m for m in sys.modules if m == package or m.startswith(package + ".")
        ]:
            del sys.modules[module]
        importlib.import_module(entry)
        declared = list(_declared)
    finally:
        _declared.clear()
        sys.path.remove(path)

    resources: dict[str, Resource] = {}
    for resource in declared:
        if not _NAME.fullmatch(resource.name):
            raise KermodesError(
                f"resource name `{resource.name}` must match {_NAME.pattern}"
            )
        # One name per row of the Deployments tab, whatever it is.
        if resource.name in resources:
            raise KermodesError(f"{name} declares `{resource.name}` twice")
        if isinstance(resource, App):
            directory = (root / resource.path).resolve()
            if (
                not directory.is_relative_to(root)
                or not (directory / "package.json").exists()
            ):
                raise KermodesError(
                    f"app `{resource.name}`'s path {resource.path} is not a directory of "
                    f"{name} with a package.json"
                )
        if isinstance(resource, Ingester):
            namespace = resource.target.partition(".")[0]
            if namespace not in namespaces:
                raise KermodesError(
                    f"ingester `{resource.name}` writes {resource.target}, but {name} owns "
                    f"only namespaces {list(namespaces)} (kermodes.toml)"
                )
        resources[resource.name] = resource
    return Project(name, root, entry, namespaces, mainline, resources)


@dataclass(frozen=True)
class Serving:
    """Where an app's process serves: its path (`/apps/<project>/<name>/`), address, and built
    frontend (none under `kermodes dev`, where its dev server serves the pages)."""

    path: str
    host: str = "0.0.0.0"
    port: int = 8080
    frontend: Path | None = None


def run(
    resource: Resource,
    db: Session,
    ready: Path | None = None,
    serving: Serving | None = None,
) -> None:
    """Runs `resource` in this process until it fails or the process is stopped: an ingester's
    connector, a deployment's function and then its session, or an app's function and then its
    server (`serving`), with the tables the function returned.

    `ready` is created once the run can take over from a run it replaces (a rolling update): an
    ingester once it is connected to its feed, holding its source's lease or still waiting for
    it; a deployment once its function starts, since the function may itself wait for a lease;
    an app once it serves.
    """
    if ready is not None:
        ready.unlink(missing_ok=True)
    if isinstance(resource, Ingester):
        if resource.columns:
            db.create_table(resource.target, resource.columns)
        source = resource.source
        connector = db.ingest_websocket(
            source.url,
            resource.target,
            source=resource.name,
            flush=source.flush,
            max_rows=source.max_rows,
            wait=True,
            seq=source.seq,
        )
        while True:
            stats = connector.stats
            if stats.failed:
                raise KermodesError(
                    f"ingester `{resource.name}` stopped: {stats.last_error}"
                )
            if ready is not None and stats.connected and not ready.exists():
                ready.touch()
            time.sleep(0.2)
    elif isinstance(resource, App):
        if serving is None:
            raise KermodesError(f"app `{resource.name}` needs where to serve")
        tables = resource.function(db) or {}
        if not isinstance(tables, dict) or not all(
            isinstance(k, str) and isinstance(v, Table) for k, v in tables.items()
        ):
            raise KermodesError(
                f"app `{resource.name}` returns its tables as {{name: table}}, or nothing"
            )
        server = db.serve(
            port=serving.port,
            host=serving.host,
            at=serving.path,
            app=serving.frontend,
        )
        for name, table in tables.items():
            server.publish(name, table)
        print(f"serving {resource.name} at {serving.path}", flush=True)
        if ready is not None:
            ready.touch()
        while True:
            time.sleep(3600)
    else:
        if ready is not None:
            ready.touch()
        resource.function(db)
        # Like a persistent query's script: what it built keeps ticking until it is stopped.
        while True:
            time.sleep(3600)


def _check_policy(resource: Resource) -> None:
    if resource.update not in UPDATES:
        raise KermodesError(f"`{resource.name}`: update must be one of {UPDATES}")
    if resource.rollout not in ROLLOUTS:
        raise KermodesError(f"`{resource.name}`: rollout must be one of {ROLLOUTS}")


def _cpu(name: str, cpu: float | str) -> str:
    """CPU as a Kubernetes quantity: cores as millicores, or a quantity as written."""
    if isinstance(cpu, int | float) and not isinstance(cpu, bool) and cpu > 0:
        return f"{round(cpu * 1000)}m"
    if isinstance(cpu, str) and _CPU.fullmatch(cpu):
        return cpu
    raise KermodesError(
        f'`{name}`: cpu must be cores (0.5) or a quantity ("500m"), not {cpu!r}'
    )


def _memory(name: str, memory: int | str) -> str:
    """Memory as a Kubernetes quantity: bytes as written, or a quantity such as "2Gi"."""
    if isinstance(memory, int) and not isinstance(memory, bool) and memory > 0:
        return str(memory)
    if isinstance(memory, str) and _MEMORY.fullmatch(memory):
        return memory
    raise KermodesError(
        f'`{name}`: memory must be bytes or a quantity ("512Mi", "2Gi"), not {memory!r}'
    )


def _defined_at(declared: Resource, root: Path) -> dict | None:
    """Where `declared` is, relative to the project, so the platform can link to it at any
    commit (§15.2)."""
    if declared.defined_at is None:
        return None
    file, line = declared.defined_at
    path = Path(file).resolve()
    if not path.is_relative_to(root):
        return None
    return {"file": path.relative_to(root).as_posix(), "line": line}


def _spec(resource: Resource, entry: str, root: Path) -> dict:
    spec: dict = {
        "name": resource.name,
        "update": resource.update,
        "rollout": resource.rollout,
        "cpu": resource.cpu,
        "memory": resource.memory,
    }
    if (at := _defined_at(resource, root)) is not None:
        spec["defined_at"] = at
    if isinstance(resource, Ingester):
        spec |= {
            "kind": "ingester",
            "target": resource.target,
            "columns": resource.columns,
            "source": {
                "kind": "websocket",
                "url": resource.source.url,
                "flush": resource.source.flush,
                "max_rows": resource.source.max_rows,
            },
        }
    elif isinstance(resource, App):
        spec |= {
            "kind": "app",
            "function": f"{entry}:{resource.function_name}",
            "path": resource.path,
        }
    else:
        spec |= {
            "kind": "deployment",
            "function": f"{entry}:{resource.function_name}",
        }
    return spec
