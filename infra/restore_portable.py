"""Восстанавливает приватный комплект на ноутбуке в НОВЫЙ каталог и volumes.

Проверяет SHA, загружает локальные образы, запускает реальную проверку копий,
затем Airflow на localhost. Очередь источника и DAG автоматически не запускаются.
"""

import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import sys
import time
from urllib.request import urlopen
import zipfile


def sha(path):
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def safe_member(name):
    """Accept only relative project paths, including Windows extraction rules."""
    value = name.replace("\\", "/")
    parts = PurePosixPath(value).parts
    if (
        not parts
        or value.startswith("/")
        or ".." in parts
        or any(":" in p for p in parts)
    ):
        raise ValueError("Unsafe source ZIP member")
    return value


def run(args, root=None, **kwargs):
    return subprocess.run(args, cwd=root, check=True, **kwargs)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backup", type=Path, required=True)
    parser.add_argument("--destination", type=Path, required=True)
    args = parser.parse_args()
    backup = args.backup.resolve(strict=True)
    destination = args.destination.resolve()
    manifest = json.loads((backup / "manifest.json").read_text(encoding="utf-8"))
    if destination.exists():
        parser.error(
            "Destination must be a NEW directory; existing projects are never overwritten"
        )
    if not manifest.get("complete"):
        parser.error("The package is not complete")
    for field in ("project_zip", "git_bundle", "images_archive"):
        item = manifest[field]
        path = backup / safe_member(item["file"])
        if sha(path) != item["sha256"]:
            raise ValueError("SHA mismatch: " + item["file"])
    project_zip = backup / manifest["project_zip"]["file"]
    with zipfile.ZipFile(project_zip) as archive:
        for item in archive.infolist():
            safe_member(item.filename)
        if archive.testzip() is not None:
            raise ValueError("Source ZIP CRC differs")
    if shutil.which("git"):
        run(
            [
                "git",
                "clone",
                "--branch",
                "main",
                str(backup / manifest["git_bundle"]["file"]),
                str(destination),
            ]
        )
    else:
        destination.mkdir(parents=True)
    with zipfile.ZipFile(project_zip) as archive:
        archive.extractall(destination)
    run(
        [
            "docker",
            "image",
            "load",
            "--input",
            str(backup / manifest["images_archive"]["file"]),
        ]
    )
    run(
        [
            sys.executable,
            "infra/verify_cold_backup.py",
            "--backup",
            str(backup),
            "--keep-verified-volumes",
        ],
        destination,
    )
    override = backup / "recovered-volumes.compose.yaml"
    compose = ["docker", "compose", "-f", "compose.yaml", "-f", str(override)]
    run(compose + ["up", "-d", "--no-build", "--pull", "never"], destination)
    url = "http://127.0.0.1:8080/api/v2/monitor/health"
    for attempt in range(60):
        try:
            with urlopen(url, timeout=5) as response:
                if response.status == 200:
                    break
        except OSError:
            time.sleep(2)
    else:
        raise RuntimeError(
            "Airflow did not become healthy; inspect docker compose logs"
        )
    proof = {
        "project": str(destination),
        "override": str(override),
        "airflow": "http://127.0.0.1:8080",
        "data_restore_verified": True,
        "source_queue_resumed": False,
        "DAGs_remain_paused": True,
    }
    (destination / "exports").mkdir(exist_ok=True)
    (destination / "exports/portable-launch.json").write_text(
        json.dumps(proof, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(proof))


if __name__ == "__main__":
    main()
