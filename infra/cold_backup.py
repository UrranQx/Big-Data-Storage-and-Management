"""Холодная копия volumes, образов и исходников после штатной остановки.

Пишет в новый каталог, включая секреты восстановления; не удаляет и не запускает сервисы."""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import zipfile


PROJECT = "speedrun-lab1"
VOLUMES = (
    "postgres-data",
    "mongo-data",
    "garage-meta",
    "garage-data",
    "etl-state",
    "airflow-logs",
)
ROOT = Path(__file__).resolve().parents[1]

ARCHIVE = r"""
import gzip, hashlib, json, os, shutil, subprocess, tarfile, tempfile, time
from pathlib import Path

def sha(path):
    with path.open('rb') as handle:
        return hashlib.file_digest(handle, 'sha256').hexdigest()

for name in json.loads(os.environ['BACKUP_VOLUMES']):
    target = Path('/backup') / (name + '.tar.gz')
    verified = Path('/backup') / (name + '.verified.json')
    started = time.monotonic()
    if verified.exists():
        row = json.loads(verified.read_text())
        if sha(target) != row['sha256']:
            raise ValueError('Previously verified archive changed: ' + name)
        print(json.dumps(dict(reused=True, **row)), flush=True)
        continue
    if not target.exists():
        with target.open('xb') as output:
            tar = subprocess.Popen(['tar', '--numeric-owner', '-C', '/vol/' + name, '-cf', '-', '.'], stdout=subprocess.PIPE)
            try:
                with gzip.GzipFile(filename='', mode='wb', fileobj=output, compresslevel=1, mtime=0) as compressed:
                    while chunk := tar.stdout.read(4 * 1024**2):
                        compressed.write(chunk)
            finally:
                tar.stdout.close()
            if tar.wait():
                raise RuntimeError('tar failed: ' + name)
    # Tiny gzip/tar reads over a Windows bind are very slow. Copy the compressed
    # file once to Linux, then verify CRC, members and SHA there. No extraction.
    with tempfile.TemporaryDirectory() as temp:
        local = Path(temp) / target.name
        shutil.copyfile(target, local)
        with gzip.open(local, 'rb') as stream:
            while stream.read(4 * 1024**2):
                pass
        members = 0
        with tarfile.open(local, 'r:gz') as archive:
            for member in archive:
                parts = Path(member.name).parts
                if member.name.startswith('/') or '..' in parts:
                    raise ValueError('Unsafe archive member')
                members += 1
        row = dict(name=name, file=target.name, bytes=target.stat().st_size,
                   sha256=sha(local), members=members, gzip_crc_verified=True,
                   seconds=round(time.monotonic()-started, 2))
    verified.write_text(json.dumps(row, indent=2))
    print(json.dumps(row), flush=True)
"""


def command(args, **kwargs):
    return subprocess.run(args, cwd=ROOT, check=True, **kwargs)


def output(args):
    return command(args, capture_output=True, text=True).stdout


def checksum(path):
    result = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(4 * 1024**2):
            result.update(chunk)
    return result.hexdigest()


def source_files(root):
    """Include report CSV and local config; skip raw data, caches and exports."""
    root = Path(root)
    root_only = {
        ".git",
        "data",
        "exports",
        "archive",
        "logs",
        "volumes",
        "build",
        "dist",
        ".aws",
        ".codex",
        ".agents",
    }
    caches = {".venv", ".uv-cache", "__pycache__", ".pytest_cache", ".ruff_cache"}
    for directory, names, files in os.walk(root):
        directory = Path(directory)
        names[:] = sorted(
            name
            for name in names
            if name not in caches and not (directory == root and name in root_only)
        )
        for name in sorted(files):
            yield directory / name


def main():
    parser = argparse.ArgumentParser(
        description="Offline backup of this project's six volumes, images and local working tree.\n\nRun on the Docker host after a graceful batch pause and `docker compose stop`.\nNever deletes, restores or starts services. The destination must be a NEW folder.\nLocal secrets are included for recovery; this backup must not be published."
    )
    parser.add_argument("--destination", required=True, type=Path)
    parser.add_argument(
        "--evidence",
        required=True,
        type=Path,
        help="Frozen baseline JSON captured before stopping services",
    )
    args = parser.parse_args()
    destination = args.destination
    if not destination.is_absolute() or destination.exists():
        parser.error(
            "Use a new absolute destination directory; existing backups are never overwritten"
        )
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination = destination.resolve()
    if destination.is_relative_to(ROOT):
        parser.error(
            "Use a different disk/folder outside the project for this preservation backup"
        )
    baseline = json.loads(args.evidence.read_text(encoding="utf-8-sig"))
    if not baseline.get("safe_to_stop"):
        parser.error(
            "Baseline must confirm stopped drivers/controllers and zero active DAG runs/tasks"
        )
    running = output(
        [
            "docker",
            "ps",
            "--filter",
            "label=com.docker.compose.project=" + PROJECT,
            "--format",
            "{{.ID}}",
        ]
    ).strip()
    if running:
        parser.error(
            "Stop all project services first; copying live databases is not supported"
        )
    containers = output(["docker", "compose", "ps", "-aq"]).split()
    if not containers:
        parser.error("Keep the stopped project containers for image/mount provenance")
    metadata = json.loads(output(["docker", "inspect", *containers]))
    mounts = {m["Name"] for c in metadata for m in c["Mounts"] if m["Type"] == "volume"}
    volumes = {PROJECT + "_" + name for name in VOLUMES}
    # Mongo's optional anonymous /data/configdb is empty in this compose setup;
    # refuse any additional nonempty/unknown volume rather than silently omitting it.
    unknown = mounts - volumes
    for volume in sorted(unknown):
        probe = [
            "docker",
            "run",
            "--rm",
            "--user",
            "0",
            "--entrypoint",
            "python",
            "--mount",
            f"type=volume,src={volume},dst=/extra,readonly",
            "speedrun-lab1-airflow:local",
            "-c",
            "from pathlib import Path; print(len(list(Path('/extra').rglob('*'))))",
        ]
        if int(output(probe).strip()):
            parser.error(
                "Unexpected nonempty volume needs explicit preservation: " + volume
            )
    for name in sorted(volumes):
        item = json.loads(output(["docker", "volume", "inspect", name]))[0]
        if item.get("Labels", {}).get("com.docker.compose.project") != PROJECT:
            parser.error("Volume ownership mismatch: " + name)
    free = shutil.disk_usage(destination.parent).free
    if free < 40 * 1024**3:
        parser.error("Require at least 40 GiB free on backup destination")
    destination.mkdir()
    shutil.copyfile(args.evidence, destination / "baseline.json")
    images = sorted({c["Image"] for c in metadata})
    manifest = {
        "format": "speedrun-lab1-cold-backup-v1",
        "complete": False,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "destination": str(destination),
        "contains_local_secrets": True,
        "git_head": output(["git", "rev-parse", "HEAD"]).strip(),
        "services": [
            {
                "service": c["Config"]["Labels"].get("com.docker.compose.service"),
                "image_id": c["Image"],
                "image_reference": c["Config"]["Image"],
            }
            for c in metadata
        ],
        "ignored_empty_volumes": sorted(unknown),
        "restore_verified": False,
    }
    manifest_path = destination / "manifest.json"

    def save():
        manifest_path.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    save()
    archive_args = [
        "docker",
        "run",
        "--rm",
        "--user",
        "0",
        "--entrypoint",
        "python",
        "--mount",
        f"type=bind,src={destination},dst=/backup",
        "--env",
        "BACKUP_VOLUMES=" + json.dumps(VOLUMES),
    ]
    for name in VOLUMES:
        archive_args += [
            "--mount",
            f"type=volume,src={PROJECT}_{name},dst=/vol/{name},readonly",
        ]
    command([*archive_args, "speedrun-lab1-airflow:local", "-c", ARCHIVE])
    manifest["volumes"] = [
        json.loads((destination / (n + ".verified.json")).read_text()) for n in VOLUMES
    ]
    save()
    # Include uncommitted/untracked working code and local recovery credentials.
    source_zip = destination / "project-and-local-config.zip"
    with zipfile.ZipFile(source_zip, "x", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in source_files(ROOT):
            archive.write(path, path.relative_to(ROOT).as_posix())
    with zipfile.ZipFile(source_zip) as archive:
        if archive.testzip() is not None:
            raise ValueError("Source ZIP CRC failed")
        for required in (".env", "infra/garage.local.toml", "compose.yaml"):
            if required not in archive.namelist():
                raise ValueError("Missing recovery configuration: " + required)
    manifest["project_zip"] = {
        "file": source_zip.name,
        "bytes": source_zip.stat().st_size,
        "sha256": checksum(source_zip),
    }
    bundle = destination / "repository.bundle"
    command(
        ["git", "bundle", "create", str(bundle), "--all"],
        capture_output=True,
        text=True,
    )
    manifest["git_bundle"] = {
        "file": bundle.name,
        "bytes": bundle.stat().st_size,
        "sha256": checksum(bundle),
    }
    save()
    print("Saving four existing Docker images for offline recovery", flush=True)
    image_archive = destination / "docker-images.tar"
    command(["docker", "image", "save", "--output", str(image_archive), *images])
    manifest["images_archive"] = {
        "file": image_archive.name,
        "bytes": image_archive.stat().st_size,
        "sha256": checksum(image_archive),
    }
    manifest.update(complete=True, finished_at=datetime.now(timezone.utc).isoformat())
    save()
    print(
        json.dumps(
            {
                "complete": True,
                "destination": str(destination),
                "total_bytes": sum(
                    p.stat().st_size for p in destination.iterdir() if p.is_file()
                ),
            }
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
