"""Перенос ODS/control PostgreSQL без секретов и без Docker volumes.

Python 3.11+, Docker. Export/restore never starts the scheduler or source client.
Binary dumps pass through files/pipes, never PowerShell text redirection.
"""

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess
import time
from uuid import uuid4
import zipfile

ROOT = Path(__file__).resolve().parents[1]
DATABASES = {"speedrun_ods": "speedrun_ods", "speedrun_control": "speedrun_control"}
PG_IMAGE = "postgres:16@sha256:ca0bd484cb98bf4b24eb1010e73fb3fcbd6714d240fbc1a10eea5b7dbecb641d"


def run(args, **kwargs):
    return subprocess.run(args, check=True, **kwargs)


def output(args):
    return run(args, stdout=subprocess.PIPE).stdout.decode().strip()


def sql(container, database, statement):
    return output(
        [
            "docker",
            "exec",
            container,
            "psql",
            "-XAt",
            "-v",
            "ON_ERROR_STOP=1",
            "-U",
            "postgres",
            "-d",
            database,
            "-c",
            statement,
        ]
    )


def sha256(path):
    with Path(path).open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def inventory(container):
    """SHA-256 of every row, ordered by row hash; also checks extra/missing tables."""
    result = {}
    for database in DATABASES:
        tables = sql(
            container,
            database,
            """SELECT json_build_array(n.nspname,c.relname)::text
            FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE c.relkind='r' AND n.nspname NOT LIKE 'pg_%'
            AND n.nspname!='information_schema' ORDER BY n.nspname,c.relname""",
        ).splitlines()
        result[database] = {}
        for table in tables:
            schema, name = json.loads(table)
            qualified = (
                '"' + schema.replace('"', '""') + '"."' + name.replace('"', '""') + '"'
            )
            query = f"COPY (SELECT checksum FROM (SELECT encode(sha256(convert_to(row_to_json(t)::text,'UTF8')),'hex') AS checksum FROM {qualified} t) hashes ORDER BY checksum COLLATE \"C\") TO STDOUT"
            process = subprocess.Popen(
                [
                    "docker",
                    "exec",
                    container,
                    "psql",
                    "-Xq",
                    "-v",
                    "ON_ERROR_STOP=1",
                    "-U",
                    "postgres",
                    "-d",
                    database,
                    "-c",
                    query,
                ],
                stdout=subprocess.PIPE,
            )
            checksum, count = hashlib.sha256(), 0
            for chunk in iter(lambda: process.stdout.read(1024 * 1024), b""):
                checksum.update(chunk)
                count += chunk.count(b"\n")
            process.stdout.close()
            if process.wait() != 0:
                raise RuntimeError("Database inventory failed")
            result[database][schema + "." + name] = {
                "count": count,
                "sha256": checksum.hexdigest(),
            }
            print(f"Inventory: {database}.{schema}.{name}: {count}", flush=True)
    return result


def get_container(compose):
    container = output(["docker", "compose", *compose, "ps", "-q", "postgres"])
    if not container or "\n" in container:
        raise RuntimeError("Exactly one running Compose PostgreSQL service is required")
    return container


def restore_dumps(container, directory):
    """Refuses nonempty target databases; does not drop or overwrite data."""
    for database in DATABASES:
        count = sql(
            container,
            database,
            """SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname!='information_schema' AND c.relkind IN ('r','v','m','S')""",
        )
        if count != "0":
            raise RuntimeError(f"Refusing restore into nonempty database: {database}")
    for database, owner in DATABASES.items():
        with (directory / (database + ".dump")).open("rb") as handle:
            run(
                [
                    "docker",
                    "exec",
                    "-i",
                    container,
                    "pg_restore",
                    "--exit-on-error",
                    "--no-owner",
                    "--no-acl",
                    "--single-transaction",
                    "-U",
                    owner,
                    "-d",
                    database,
                ],
                stdin=handle,
            )


def check_files(directory):
    manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    for name, info in manifest["files"].items():
        if Path(name).name != name:
            raise ValueError("Invalid archive member name")
        path = directory / name
        if path.stat().st_size != info["bytes"] or sha256(path) != info["sha256"]:
            raise RuntimeError("Transfer file checksum differs: " + name)
    return manifest


def verify(directory):
    manifest = check_files(directory)
    name = "speedrun-transfer-check-" + uuid4().hex[:12]
    label = "etl.transfer_check=" + name
    run(
        [
            "docker",
            "run",
            "-d",
            "--name",
            name,
            "--label",
            label,
            "--network",
            "none",
            "-e",
            "POSTGRES_HOST_AUTH_METHOD=trust",
            PG_IMAGE,
        ],
        stdout=subprocess.DEVNULL,
    )
    try:
        for _ in range(120):
            ready = (
                subprocess.run(
                    [
                        "docker",
                        "exec",
                        name,
                        "pg_isready",
                        "-h",
                        "127.0.0.1",
                        "-U",
                        "postgres",
                    ],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                ).returncode
                == 0
            )
            if ready:
                break
            time.sleep(1)
        else:
            raise RuntimeError("Restore verification PostgreSQL did not start")
        for database, owner in DATABASES.items():
            sql(name, "postgres", f"CREATE ROLE {owner} LOGIN")
            sql(name, "postgres", f"CREATE DATABASE {database} OWNER {owner}")
        restore_dumps(name, directory)
        actual = inventory(name)
        if actual != manifest["inventory"]:
            raise RuntimeError("Restored database content differs from source")
        evidence = {
            "verified_at": datetime.now(timezone.utc).isoformat(),
            "status": "all_rows_equal",
            "network": "none",
            "inventory": actual,
            "manifest_sha256": sha256(directory / "manifest.json"),
        }
        (directory / "verified.json").write_text(
            json.dumps(evidence, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print("Restore verified: all rows of both databases match", flush=True)
    finally:
        actual_label = output(
            [
                "docker",
                "inspect",
                "--format",
                '{{index .Config.Labels "etl.transfer_check"}}',
                name,
            ]
        )
        if actual_label != name:
            raise RuntimeError(
                "Unexpected verification container label; refusing removal"
            )
        run(["docker", "rm", "-fv", name], stdout=subprocess.DEVNULL)


def export(directory, compose):
    directory.mkdir(parents=True, exist_ok=False)
    container = get_container(compose)
    active = sql(
        container,
        "airflow",
        "SELECT count(*) FROM dag_run WHERE state IN ('running','queued')",
    )
    if active != "0":
        raise RuntimeError("Pause ETL and finish active DAGs before transfer")
    files = {}
    baseline = inventory(container)
    for database in DATABASES:
        path = directory / (database + ".dump")
        with path.open("wb") as handle:
            run(
                [
                    "docker",
                    "exec",
                    container,
                    "pg_dump",
                    "-U",
                    "postgres",
                    "-Fc",
                    "--no-acl",
                    database,
                ],
                stdout=handle,
            )
        files[path.name] = {"bytes": path.stat().st_size, "sha256": sha256(path)}
        print(f"Exported {database}: {path.stat().st_size} bytes", flush=True)
    if inventory(container) != baseline:
        raise RuntimeError("Source changed during export; transfer is not sealed")
    manifest = {
        "format": "speedrun-postgres-transfer-v1",
        "postgres_major": 16,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "files": files,
        "inventory": baseline,
        "contains_secrets": False,
        "source_collection_resumed": False,
    }
    (directory / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def pack(directory):
    manifest = check_files(directory)
    evidence = json.loads((directory / "verified.json").read_text(encoding="utf-8"))
    if (
        evidence["manifest_sha256"] != sha256(directory / "manifest.json")
        or evidence["inventory"] != manifest["inventory"]
    ):
        raise RuntimeError("Transfer must be verified before packaging")
    for filename in (
        "RESTORE.ps1",
        "RESTORE.cmd",
        "TRANSFER_README.md",
        "compose.transfer.yaml",
        "postgres-init.sh",
    ):
        source = ROOT / "infra" / filename
        (directory / filename).write_bytes(source.read_bytes())
    destination = directory.with_suffix(".zip")
    tracked = output(["git", "-C", str(ROOT), "ls-files"]).splitlines()
    added = output(
        ["git", "-C", str(ROOT), "ls-files", "--others", "--exclude-standard"]
    ).splitlines()
    source_paths = sorted(
        set(
            tracked
            + [p for p in added if p.startswith(("infra/", "src/", "tests/", "docs/"))]
        )
    )
    source_paths = [
        p
        for p in source_paths
        if (ROOT / p).is_file()
        and (not p.startswith((".git/", "exports/", ".env")) or p == ".env.example")
    ]
    with zipfile.ZipFile(
        destination, "x", compression=zipfile.ZIP_STORED, allowZip64=True
    ) as archive:
        members = [database + ".dump" for database in DATABASES] + [
            "manifest.json",
            "verified.json",
            "RESTORE.ps1",
            "RESTORE.cmd",
            "TRANSFER_README.md",
            "compose.transfer.yaml",
            "postgres-init.sh",
        ]
        for filename in members:
            archive.write(directory / filename, filename)
        for path in source_paths:
            archive.write(ROOT / path, "project/" + path)
    destination.with_suffix(".zip.sha256").write_text(
        sha256(destination) + "  " + destination.name + "\n", encoding="ascii"
    )
    print("Ready for Google Drive:", destination, flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("export", "verify", "pack", "restore"))
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--compose-file", type=Path)
    parser.add_argument("--env-file", type=Path)
    args = parser.parse_args()
    directory = args.directory.resolve()
    compose = (
        ["--env-file", str(args.env_file.resolve())] if args.env_file else []
    ) + (["-f", str(args.compose_file.resolve())] if args.compose_file else [])
    if args.command == "export":
        export(directory, compose)
    elif args.command == "verify":
        verify(directory)
    elif args.command == "pack":
        pack(directory)
    else:
        manifest = check_files(directory)
        container = get_container(compose)
        restore_dumps(container, directory)
        if inventory(container) != manifest["inventory"]:
            raise RuntimeError("Restored content differs from manifest")
        print("Database transfer restored and verified")


if __name__ == "__main__":
    main()
