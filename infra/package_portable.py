"""Собирает приватный комплект для ноутбука из проверенной холодной копии.

Данные остаются на момент baseline; исходники и четыре образа берутся после
внедрения рефакторинга. Новая папка обязательна. Копия содержит локальные секреты.
"""

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import zipfile

from cold_backup import checksum, source_files


ROOT = Path(__file__).resolve().parents[1]


def run(args, **kwargs):
    return subprocess.run(args, cwd=ROOT, check=True, **kwargs)


def output(args):
    return run(args, capture_output=True, text=True).stdout


def copy_immutable(source, target):
    """Hard links share unchanged archives on one disk; cross-disk copies work too."""
    if target.exists():
        raise FileExistsError(target)
    try:
        os.link(source, target)
    except OSError:
        shutil.copyfile(source, target)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backup", type=Path, required=True)
    parser.add_argument("--destination", type=Path, required=True)
    args = parser.parse_args()
    backup = args.backup.resolve(strict=True)
    destination = args.destination.resolve()
    manifest = json.loads((backup / "manifest.json").read_text(encoding="utf-8"))
    if not manifest.get("complete"):
        parser.error("Use a completed, checksummed cold backup")
    if destination.exists() or destination.is_relative_to(ROOT):
        parser.error("Use a NEW directory outside the repository")
    destination.mkdir(parents=True)
    manifest.update(
        complete=False,
        restore_verified=False,
        data_restore_verified=manifest.get("restore_verified", False),
        destination=str(destination),
        preservation_backup=str(backup),
        packaged_at=datetime.now(timezone.utc).isoformat(),
    )
    for row in manifest["volumes"]:
        source = backup / row["file"]
        if checksum(source) != row["sha256"]:
            raise ValueError("Archive changed: " + row["file"])
        copy_immutable(source, destination / row["file"])
    for name in ("baseline.json", "restore-verification.json"):
        if (backup / name).exists():
            shutil.copyfile(backup / name, destination / name)
    project = destination / "project-and-local-config.zip"
    with zipfile.ZipFile(project, "x", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in source_files(ROOT):
            archive.write(path, path.relative_to(ROOT).as_posix())
    with zipfile.ZipFile(project) as archive:
        if archive.testzip() is not None:
            raise ValueError("Source ZIP CRC differs")
    bundle = destination / "repository.bundle"
    run(["git", "bundle", "create", str(bundle), "--all"], capture_output=True)
    containers = output(["docker", "compose", "ps", "-aq"]).split()
    metadata = json.loads(output(["docker", "inspect", *containers]))
    app_image = json.loads(
        output(["docker", "image", "inspect", "speedrun-lab1-airflow:local"])
    )[0]["Id"]
    manifest["services"] = [
        {
            "service": c["Config"]["Labels"]["com.docker.compose.service"],
            "image_id": c["Image"],
            "image_reference": c["Config"]["Image"],
        }
        for c in metadata
    ]
    for service in manifest["services"]:
        if service["service"] in ("init", "scheduler", "api-server", "dag-processor"):
            service["image_id"] = app_image
    image_archive = destination / "docker-images.tar"
    run(
        [
            "docker",
            "image",
            "save",
            "--output",
            str(image_archive),
            *sorted({c["image_id"] for c in manifest["services"]}),
        ]
    )
    for field, path in (
        ("project_zip", project),
        ("git_bundle", bundle),
        ("images_archive", image_archive),
    ):
        manifest[field] = {
            "file": path.name,
            "bytes": path.stat().st_size,
            "sha256": checksum(path),
        }
    manifest["git_head"] = output(["git", "rev-parse", "HEAD"]).strip()
    manifest["complete"] = True
    (destination / "manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
    )
    shutil.copyfile(
        ROOT / "infra/restore_portable.py", destination / "restore_portable.py"
    )
    if os.name == "nt":
        # The producer uses uv's standalone Python. Check the copied runtime
        # before shipping it, so the laptop only needs Docker Desktop.
        runtime = destination / "python"
        shutil.copytree(
            Path(sys.base_prefix), runtime, ignore=shutil.ignore_patterns("__pycache__")
        )
        run(
            [
                str(runtime / "python.exe"),
                "-I",
                "-c",
                "import hashlib,json,subprocess,zipfile; print('Portable Python OK')",
            ]
        )
        manifest["python_runtime"] = {
            "executable": "python/python.exe",
            "version": sys.version,
        }
        (destination / "START.cmd").write_text(
            '@echo off\nset "TARGET=%USERPROFILE%\\ETL_demo"\n'
            'if not "%~1"=="" set "TARGET=%~1"\n'
            '"%~dp0python\\python.exe" -I "%~dp0restore_portable.py" --backup "%~dp0." --destination "%TARGET%"\n'
            "pause\n",
            encoding="utf-8",
        )
        (destination / "manifest.json").write_text(
            json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
        )
    (destination / "START_HERE.txt").write_text(
        "PRIVATE: contains .env and local credentials. Share only with trusted teammates.\n"
        "Windows: copy this whole directory to a local disk (100 GiB free recommended).\n"
        "Docker Desktop must be running in Linux-container mode. Windows Python is included.\n"
        "Windows: double-click START.cmd, or: START.cmd C:\\ETL_demo\n"
        "Git is optional for the demo; repository.bundle allows later offline cloning.\n"
        "Run: python restore_portable.py --backup . --destination C:\\ETL_demo\n"
        "Use a NEW destination directory. Restores to NEW volumes and keeps all DAGs paused.\n"
        "Airflow: http://127.0.0.1:8080 ; login admin; password is in destination .env.\n"
        "Existing data/UI work offline. Fresh source collection requires internet.\n"
        "Instructions: docs/LAPTOP_DEMO.md inside the restored project.\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "complete": True,
                "destination": str(destination),
                "git_head": manifest["git_head"],
                "bytes": sum(
                    p.stat().st_size for p in destination.rglob("*") if p.is_file()
                ),
            }
        )
    )


if __name__ == "__main__":
    main()
