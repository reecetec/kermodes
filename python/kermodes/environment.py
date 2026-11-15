"""Ready-made environments (architecture.md §10.6): a project's locked dependencies, installed once
into a venv over the platform's own Python and Kermodes, which run on it unchanged. The cluster
builds one per lockfile and platform in a Job and pods unpack it; `kermodes dev` builds the same
locally, so a project runs on what it locked in both."""

import hashlib
import shutil
import subprocess
import sys
import sysconfig
import tarfile
import tempfile
import urllib.request
from pathlib import Path

from kermodes._native import KermodesError


def lock_of(project: Path) -> str | None:
    """What a project's environment is built from, as a hash: its `uv.lock` and
    `pyproject.toml`. None for a project with no lockfile, which runs on the platform alone."""
    lock = project / "uv.lock"
    if not lock.exists():
        return None
    digest = hashlib.sha256()
    for path in (lock, project / "pyproject.toml"):
        digest.update(path.read_bytes() if path.exists() else b"")
        digest.update(b"\0")
    return digest.hexdigest()


def build(project: Path, dest: Path) -> None:
    """Installs `project`'s locked dependencies into a new venv at `dest`, which sees this
    Python's packages (Kermodes among them, whether this Python is a venv or not) after its
    own. Kermodes itself is the platform's, so a lock that pins it is not installed over it.
    The venv's interpreter is a copy, not a link, so a tar of it unpacks anywhere without links
    out of it. Fails, saying why, if the dependencies do not install or Kermodes no longer
    imports over them."""
    uv = shutil.which("uv")
    if uv is None:
        raise KermodesError(
            "building an environment needs uv (https://docs.astral.sh/uv/)"
        )
    requirements = _run(
        [
            uv,
            "export",
            "--frozen",
            "--no-hashes",
            "--no-dev",
            "--no-emit-project",
            "--no-emit-package",
            "kermodes",
            "--format",
            "requirements-txt",
        ],
        cwd=project,
        doing="reading uv.lock",
    )
    _run(
        [sys.executable, "-m", "venv", "--copies", str(dest)],
        doing="creating the environment",
    )
    version = f"python{sys.version_info.major}.{sys.version_info.minor}"
    platform = {sysconfig.get_paths()[kind] for kind in ("purelib", "platlib")}
    # As site directories, so their own .pth files (an editable install's) are read too.
    (dest / "lib" / version / "site-packages" / "_kermodes_platform.pth").write_text(
        "".join(
            f"import site; site.addsitedir({path!r})\n" for path in sorted(platform)
        )
    )
    python = dest / "bin" / "python"
    if requirements.strip():
        listed = dest / "requirements.txt"
        listed.write_text(requirements)
        _run(
            [uv, "pip", "install", "--python", str(python), "-r", str(listed)],
            doing="installing the locked dependencies",
        )
    _run(
        [str(python), "-c", "import kermodes"],
        doing="importing Kermodes over the locked dependencies",
    )


def upload(dest: Path, url: str) -> None:
    """PUTs a tar of the environment at `dest` to `url` (a presigned URL for its one object),
    written first in the temporary directory: `dest` may sit on a read-only parent."""
    with tempfile.TemporaryDirectory() as scratch:
        archive = Path(scratch) / "environment.tar"
        with tarfile.open(archive, "w") as tar:
            tar.add(dest, arcname=".")
        with archive.open("rb") as body:
            request = urllib.request.Request(
                url,
                data=body,
                method="PUT",
                headers={"Content-Length": str(archive.stat().st_size)},
            )
            with urllib.request.urlopen(request, timeout=600):
                pass


def _run(command: list[str], *, doing: str, cwd: Path | None = None) -> str:
    result = subprocess.run(
        command, cwd=cwd, capture_output=True, text=True, check=False
    )
    if result.returncode != 0:
        tail = "\n".join(result.stderr.strip().splitlines()[-5:]) or "(no output)"
        raise KermodesError(f"{doing} failed:\n{tail}")
    return result.stdout
