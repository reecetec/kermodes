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

    @km.deployment()
    def poller(db):
        writer = db.writer("md.quotes", source="poller", wait=True)
        ...

Discovery imports the entry module and returns a `Project`; its manifest is the JSON the platform
plans and deploys from (`kermodes manifest`).
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

from kermodes._native import KermodesError, Session

UPDATES = ("auto", "manual", "on_start")
ROLLOUTS = ("rolling", "recreate")
Update = Literal["auto", "manual", "on_start"]
Rollout = Literal["rolling", "recreate"]

_NAME = re.compile(r"[a-z][a-z0-9_-]{0,62}")


@dataclass(frozen=True)
class WebSocket:
    """A WebSocket feed of JSON objects, or arrays of them (architecture.md §6.2)."""

    url: str
    flush: str = "100ms"
    max_rows: int = 10_000


def websocket(url: str, *, flush: str = "100ms", max_rows: int = 10_000) -> WebSocket:
    """A WebSocket feed of JSON, for `ingester(source=...)`."""
    return WebSocket(url, flush, max_rows)


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


Resource = Ingester | Deployment

# Resources declared by the module being discovered; `discover` empties it around each import.
_declared: list[Resource] = []


def ingester(
    name: str,
    *,
    target: str,
    source: WebSocket,
    columns: dict[str, str] | None = None,
    update: Update = "auto",
    rollout: Rollout = "rolling",
) -> Ingester:
    """Declares a built-in connector reading `source` into `target` (created with `columns`
    if it does not exist)."""
    resource = Ingester(name, target, source, columns, update, rollout)
    _check_policy(resource)
    _declared.append(resource)
    return resource


def deployment(
    name: str | None = None, *, update: Update = "auto", rollout: Rollout = "rolling"
) -> Callable[[Callable[[Session], object]], Callable[[Session], object]]:
    """Declares the decorated `function(db)` as a deployment, named after it unless `name`."""

    def declare(function: Callable[[Session], object]) -> Callable[[Session], object]:
        function_name = getattr(function, "__name__", None)
        if function_name is None:
            raise KermodesError("@km.deployment() decorates a function")
        resource = Deployment(
            name or function_name.replace("_", "-"),
            function,
            function_name,
            update,
            rollout,
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

    def manifest(self) -> dict:
        """The resolved declarations, as the platform plans and deploys them."""
        return {
            "project": self.name,
            "entry": self.entry,
            "namespaces": list(self.namespaces),
            "mainline": self.mainline,
            "resources": [_spec(r, self.entry) for r in self.resources.values()],
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
        if resource.name in resources:
            raise KermodesError(f"{name} declares `{resource.name}` twice")
        if isinstance(resource, Ingester):
            namespace = resource.target.partition(".")[0]
            if namespace not in namespaces:
                raise KermodesError(
                    f"ingester `{resource.name}` writes {resource.target}, but {name} owns "
                    f"only namespaces {list(namespaces)} (kermodes.toml)"
                )
        resources[resource.name] = resource
    return Project(name, root, entry, namespaces, mainline, resources)


def run(resource: Resource, db: Session, ready: Path | None = None) -> None:
    """Runs `resource` in this process until it fails or the process is stopped: an ingester's
    connector, or a deployment's function and then its session.

    `ready` is created once the run can take over from a run it replaces (a rolling update): an
    ingester once it is connected to its feed, holding its source's lease or still waiting for
    it; a deployment once its function starts, since the function may itself wait for a lease.
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


def _spec(resource: Resource, entry: str) -> dict:
    spec: dict = {
        "name": resource.name,
        "update": resource.update,
        "rollout": resource.rollout,
    }
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
    else:
        spec |= {
            "kind": "deployment",
            "function": f"{entry}:{resource.function_name}",
        }
    return spec
