import subprocess
import textwrap

import kermodes as km
import pytest
from kermodes import environment


def make_project(root, dependencies="[]"):
    root.mkdir(parents=True, exist_ok=True)
    (root / "pyproject.toml").write_text(
        textwrap.dedent(f"""
            [project]
            name = "desk"
            version = "0.0.0"
            requires-python = ">=3.12"
            dependencies = {dependencies}
        """)
    )
    subprocess.run(["uv", "lock", "--offline", "--quiet"], cwd=root, check=True)


def test_an_environment_runs_the_platforms_kermodes_under_the_projects_packages(
    tmp_path,
):
    project = tmp_path / "desk"
    make_project(project)
    lock = environment.lock_of(project)
    assert lock is not None and len(lock) == 64

    environment.build(project, tmp_path / "env")
    python = tmp_path / "env" / "bin" / "python"
    # Its own interpreter (a copy, so a tar of it unpacks anywhere), seeing Kermodes.
    assert not python.is_symlink()
    where = subprocess.run(
        [python, "-c", "import kermodes, sys; print(sys.prefix)"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert where.stdout.strip() == str(tmp_path / "env")

    # A change to the project's dependencies is a new lock; its code is not.
    (project / "src.py").write_text("print('hello')\n")
    assert environment.lock_of(project) == lock
    (project / "pyproject.toml").write_text(
        (project / "pyproject.toml").read_text() + "\n# a comment\n"
    )
    assert environment.lock_of(project) != lock


def test_a_project_without_a_lockfile_runs_on_the_platform_alone(tmp_path):
    assert environment.lock_of(tmp_path) is None
    with pytest.raises(km.KermodesError, match="reading uv.lock failed"):
        environment.build(tmp_path, tmp_path / "env")
