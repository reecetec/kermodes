#!/usr/bin/env python3
"""Relabel a release candidate's files with the release's version, changing nothing else.

    ci/relabel.py <release version> <candidate dir> <out dir>

A release is a mainline build that already passed and ran (ci/version): its wheels, npm tarballs,
and Helm chart are published again under the tag's version rather than rebuilt, so what users
install is what was tested. Only the version metadata changes: a wheel's `*.dist-info` name,
METADATA, and RECORD; an npm package's package.json (its version and its pins on the other
@kermodes packages); a chart's Chart.yaml. Every other file is copied byte for byte, and checked to
be after writing; any difference fails the release.
"""

import base64
import gzip
import hashlib
import io
import json
import re
import sys
import tarfile
import zipfile
from pathlib import Path

RELEASE = re.compile(r"^\d+\.\d+\.\d+$")


def digest(data: bytes) -> str:
    return (
        "sha256="
        + base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()
    )


def same_payload(
    before: dict[str, bytes], after: dict[str, bytes], label: set[str], name: str
):
    """Every file but the relabelled ones is present on both sides with the same bytes."""
    kept = {path for path in before if path not in label}
    if kept != {path for path in after if path not in label}:
        raise SystemExit(f"{name}: the release's files differ from the candidate's")
    for path in kept:
        if before[path] != after[path]:
            raise SystemExit(f"{name}: {path} differs from the candidate's")


def wheel(path: Path, version: str, out: Path) -> Path:
    with zipfile.ZipFile(path) as candidate:
        infos = candidate.infolist()
        files = {info.filename: candidate.read(info) for info in infos}
    dist_info = next(
        n.split("/")[0] for n in files if n.split("/")[0].endswith(".dist-info")
    )
    project, old = dist_info.removesuffix(".dist-info").rsplit("-", 1)
    renamed = f"{project}-{version}.dist-info"
    metadata = files[f"{dist_info}/METADATA"].decode()
    if f"\nVersion: {old}\n" not in metadata:
        raise SystemExit(f"{path.name}: METADATA does not say Version: {old}")

    released: dict[str, bytes] = {}
    for name, data in files.items():
        if name == f"{dist_info}/RECORD":
            continue
        if name == f"{dist_info}/METADATA":
            data = metadata.replace(
                f"\nVersion: {old}\n", f"\nVersion: {version}\n", 1
            ).encode()
        released[name.replace(dist_info + "/", renamed + "/", 1)] = data
    record = f"{renamed}/RECORD"
    lines = [f"{name},{digest(data)},{len(data)}" for name, data in released.items()]
    released[record] = ("\n".join([*lines, f"{record},,"]) + "\n").encode()

    distribution, _, tags = path.name.removesuffix(".whl").split("-", 2)
    target = out / f"{distribution}-{version}-{tags}.whl"
    with zipfile.ZipFile(target, "w", zipfile.ZIP_DEFLATED) as release:
        for info in infos:
            name = info.filename.replace(dist_info + "/", renamed + "/", 1)
            entry = zipfile.ZipInfo(name, info.date_time)
            entry.external_attr = info.external_attr
            entry.compress_type = zipfile.ZIP_DEFLATED
            release.writestr(entry, released[name])
    with zipfile.ZipFile(target) as written:
        after = {
            n.replace(renamed + "/", dist_info + "/", 1): written.read(n)
            for n in written.namelist()
        }
    same_payload(
        files, after, {f"{dist_info}/METADATA", f"{dist_info}/RECORD"}, target.name
    )
    return target


def tarball(path: Path) -> tuple[list[tarfile.TarInfo], dict[str, bytes]]:
    with tarfile.open(path, "r:gz") as candidate:
        members = candidate.getmembers()
        files = {}
        for member in members:
            extracted = candidate.extractfile(member) if member.isfile() else None
            if extracted is not None:
                files[member.name] = extracted.read()
    return members, files


def write_tarball(
    target: Path, members: list[tarfile.TarInfo], files: dict[str, bytes]
):
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w", format=tarfile.PAX_FORMAT) as release:
        for member in members:
            if member.name in files:
                member.size = len(files[member.name])
                release.addfile(member, io.BytesIO(files[member.name]))
            else:
                release.addfile(member)
    target.write_bytes(gzip.compress(buffer.getvalue(), mtime=0))


def npm(path: Path, version: str, out: Path) -> Path:
    members, files = tarball(path)
    manifest = json.loads(files["package/package.json"])
    old = manifest["version"]
    manifest["version"] = version
    for field in ("dependencies", "peerDependencies", "optionalDependencies"):
        for name, pinned in manifest.get(field, {}).items():
            if name.startswith("@kermodes/"):
                if pinned != old:
                    raise SystemExit(
                        f"{path.name}: {name} is pinned to {pinned}, not {old}"
                    )
                manifest[field][name] = version
    released = {
        **files,
        "package/package.json": (json.dumps(manifest, indent=2) + "\n").encode(),
    }
    target = out / (
        manifest["name"].removeprefix("@").replace("/", "-") + f"-{version}.tgz"
    )
    write_tarball(target, members, released)
    same_payload(files, tarball(target)[1], {"package/package.json"}, target.name)
    return target


def chart(path: Path, version: str, out: Path) -> Path:
    members, files = tarball(path)
    name = next(n for n in files if n.count("/") == 1 and n.endswith("/Chart.yaml"))
    lines = files[name].decode().splitlines(keepends=True)
    old = next(
        line.split(":", 1)[1].strip() for line in lines if line.startswith("version:")
    )
    relabelled = 0
    for i, line in enumerate(lines):
        key = line.split(":", 1)[0]
        if (
            key in ("version", "appVersion")
            and line.split(":", 1)[1].strip().strip("\"'") == old
        ):
            lines[i] = f"{key}: {version}\n"
            relabelled += 1
    if relabelled != 2:
        raise SystemExit(
            f"{path.name}: Chart.yaml's version and appVersion are not both {old}"
        )
    chart_name = name.split("/")[0]
    target = out / f"{chart_name}-{version}.tgz"
    write_tarball(target, members, {**files, name: "".join(lines).encode()})
    same_payload(files, tarball(target)[1], {name}, target.name)
    return target


def main():
    version, candidate, out = sys.argv[1], Path(sys.argv[2]), Path(sys.argv[3])
    if not RELEASE.match(version):
        raise SystemExit(f"a release version is MAJOR.MINOR.PATCH, not {version}")
    out.mkdir(parents=True, exist_ok=True)
    for path in sorted(candidate.rglob("*")):
        if path.suffix == ".whl":
            print(wheel(path, version, out).name)
        elif path.suffix == ".tgz":
            _, files = tarball(path)
            relabel = npm if "package/package.json" in files else chart
            print(relabel(path, version, out).name)


if __name__ == "__main__":
    main()
