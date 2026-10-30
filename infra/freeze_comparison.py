"""Копирует завершённые ODS-пакеты для сравнения, не изменяя исходный volume.

SQLite backup создаёт согласованные копии. В эксперимент переносятся четыре
пакета каждой игры и их доказательства, без HTTP-журнала, секретов и очереди.
"""

import argparse
import hashlib
import os
from pathlib import Path
import sqlite3

from speedrun_etl.entity_ods import EntityPackage
from speedrun_etl.jsonutil import atomic_write, digest, read, utcnow


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--target", type=Path, required=True)
    parser.add_argument("--experiment-id", required=True)
    parser.add_argument("--collections", nargs="+", required=True)
    args = parser.parse_args()
    if args.source.resolve() == args.target.resolve():
        raise ValueError("Source and isolated state must differ")
    if (args.target / "frozen-inputs.json").exists():
        raise ValueError("Frozen input already exists; do not overwrite it")
    args.target.mkdir(parents=True, exist_ok=True)
    if any(args.target.iterdir()):
        raise ValueError("Target must be a new empty comparison volume")
    manifest = {
        "created_at": utcnow(),
        "collections": args.collections,
        "snapshots": {},
        "source_HTTP": 0,
        "production_writes": 0,
    }
    for collection in args.collections:
        for kind in ("games_metadata", "runs", "wr_history", "players"):
            sid = collection + "-" + kind
            source = args.source / "snapshots" / sid
            ref, proof = read(source / "extracted.json"), read(source / "verified.json")
            if proof["state"] != "ready" or proof["digest"] != ref["digest"]:
                raise ValueError("Input is not ready: " + sid)
            if set(proof["stores"]) != {"postgres", "mongo", "garage"}:
                raise ValueError("Missing source store confirmation: " + sid)
            if any(digest(v) != ref["digest"] for v in proof["stores"].values()):
                raise ValueError("Unequal source store summaries: " + sid)
            target = args.target / "snapshots" / sid
            package_root = target / "entity-package"
            package_root.mkdir(parents=True, exist_ok=False)
            source_db = source / "entity-package" / "entities.sqlite3"
            target_db = package_root / "entities.sqlite3"
            with sqlite3.connect(f"file:{source_db}?mode=ro", uri=True) as before:
                with sqlite3.connect(target_db) as after:
                    before.backup(after)
            package = EntityPackage(package_root, role=ref["role"])
            summary = package.summary()
            package.db.close()
            if digest(summary) != ref["digest"]:
                raise ValueError("Copied package differs: " + sid)
            ref["package_uri"] = str(package_root)
            atomic_write(target / "extracted.json", ref)
            atomic_write(target / "verified.json", proof)
            for name in ("config.json",):
                atomic_write(target / name, read(source / name))
            with target_db.open("rb") as handle:
                checksum = hashlib.file_digest(handle, "sha256").hexdigest()
            manifest["snapshots"][sid] = {
                "digest": ref["digest"],
                "format_version": ref["format_version"],
                "role": ref["role"],
                "sqlite_sha256": checksum,
                "sqlite_bytes": target_db.stat().st_size,
                "counts": summary,
                "verified": proof,
            }
            print("Frozen", sid, target_db.stat().st_size, flush=True)
    atomic_write(args.target / "frozen-inputs.json", manifest)
    atomic_write(
        args.target / "isolated-experiment.json",
        {
            "experiment_id": args.experiment_id,
            "production_access": False,
            "created_at": utcnow(),
            "input_digest": digest(manifest),
        },
    )
    if os.geteuid() == 0:
        for directory, _, files in os.walk(args.target):
            os.chown(directory, 50000, 0)
            for name in files:
                os.chown(Path(directory) / name, 50000, 0)
    print("Frozen snapshots:", len(manifest["snapshots"]), flush=True)


if __name__ == "__main__":
    main()
