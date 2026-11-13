"""Удаляет только подтверждённые копии ODS после завершения цепочки игры.

Состав пакета хранится сжатым в control PG. Перед очисткой перечитывает все
записи PostgreSQL и сравнивает digest. Failed/незавершённые цепочки не трогает.
"""

import argparse
from contextlib import contextmanager
import gzip
import hashlib
import os
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory

from .connections import pg_connect
from .entity_ods import DATASETS, EntityPackage
from .entity_storage import ready, validate_namespace
from .jsonutil import atomic_write, digest, dumps, file_lock, loads, read, utcnow
from .model import kinds
from .settings import stores


def init_archive():
    with pg_connect(True) as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS staging_archives (
            namespace text NOT NULL, snapshot_id text NOT NULL, role text NOT NULL,
            package_digest text NOT NULL, summary jsonb NOT NULL,
            record_keys bytea NOT NULL, metadata jsonb NOT NULL,
            verified_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY(namespace,snapshot_id,role))""")


def archive_info(reference):
    with pg_connect(True) as conn:
        row = conn.execute(
            """SELECT package_digest,summary,record_keys,metadata
            FROM staging_archives WHERE namespace=%s AND snapshot_id=%s AND role=%s""",
            [
                reference.get("namespace", "entity_v2"),
                reference["snapshot_id"],
                reference["role"],
            ],
        ).fetchone()
    if row is None or row[0] != reference["digest"]:
        raise RuntimeError("No exact PostgreSQL staging archive for snapshot")
    return {
        "digest": row[0],
        "summary": row[1],
        "keys": loads(gzip.decompress(row[2])),
        "metadata": row[3],
    }


def pg_records(namespace, dataset, keys):
    from psycopg import sql

    table = sql.Identifier(validate_namespace(namespace), dataset)
    with pg_connect() as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        for offset in range(0, len(keys), 500):
            batch = keys[offset : offset + 500]
            rows = dict(
                conn.execute(
                    sql.SQL("SELECT key,document FROM {} WHERE key=ANY(%s)").format(
                        table
                    ),
                    [batch],
                )
            )
            if set(rows) != set(batch):
                raise RuntimeError("PostgreSQL archive is missing records")
            for key in batch:
                yield rows[key]


def check_archive(reference, info):
    actual = {}
    for dataset in DATASETS:
        checksum, count = hashlib.sha256(b"["), 0
        for row in pg_records(
            reference.get("namespace", "entity_v2"), dataset, info["keys"][dataset]
        ):
            if count:
                checksum.update(b",")
            checksum.update(dumps([row["key"], digest(row)]).encode())
            count += 1
        checksum.update(b"]")
        actual[dataset] = {"count": count, "digest": checksum.hexdigest()}
    if actual != info["summary"] or digest(actual) != reference["digest"]:
        raise RuntimeError("PostgreSQL archive content differs from the sealed package")
    return actual


@contextmanager
def open_package(reference):
    """Читает staging либо временно восстанавливает точный пакет из PostgreSQL."""
    root = Path(reference["package_uri"])
    if (root / "entities.sqlite3").is_file():
        package = EntityPackage(root, role=reference["role"])
        try:
            yield package
        finally:
            package.db.close()
        return
    info = archive_info(reference)
    with TemporaryDirectory(prefix="speedrun-replay-") as temporary:
        package = EntityPackage(temporary, role=reference["role"])
        try:
            for dataset in DATASETS:
                for row in pg_records(
                    reference.get("namespace", "entity_v2"),
                    dataset,
                    info["keys"][dataset],
                ):
                    package.put(dataset, row)
                package.db.commit()
            if digest(package.summary()) != reference["digest"]:
                raise RuntimeError("Restored PostgreSQL package differs")
            yield package
        finally:
            package.db.close()


def manifest(root):
    root = Path(root)
    if (root / "retired.json").exists():
        return read(root / "retired.json")["manifest"]
    database = root / "entity-package" / "entities.sqlite3"
    if database.exists():
        package = EntityPackage(database.parent)
        try:
            return next(package.records("manifests"))
        finally:
            package.db.close()
    reference = read(root / "extracted.json")
    with open_package(reference) as package:
        return next(package.records("manifests"))


def within(path, parent):
    if Path(path).is_symlink():
        raise ValueError("Symlinks are not permitted in staging cleanup")
    path, parent = Path(path).resolve(), Path(parent).resolve()
    if path == parent or not path.is_relative_to(parent) or path.is_symlink():
        raise ValueError(
            "Staging target must be strictly inside the snapshots directory"
        )
    return path


def seal(root, snapshots):
    """Проверяет все байты логических записей до любого удаления."""
    from psycopg.types.json import Jsonb

    root = within(root, snapshots)
    reference = read(root / "extracted.json")
    if reference["snapshot_id"] != root.name:
        raise ValueError("Snapshot identity differs from its directory")
    if within(reference["package_uri"], root) != root / "entity-package":
        raise ValueError("Unexpected package path")
    namespace = reference.get("namespace", "entity_v2")
    ready(root.name, namespace, reference["role"], reference["digest"])
    if (root / "retired.json").exists():
        # A retry after interrupted deletion must still prove the archive exists.
        info = archive_info(reference)
        check_archive(reference, info)
        return reference, info
    with open_package(reference) as package:
        summary = package.summary()
        if digest(summary) != reference["digest"]:
            raise RuntimeError("Local package differs from its receipt")
        keys = {
            dataset: [r["key"] for r in package.records(dataset)]
            for dataset in DATASETS
        }
        metadata = {
            "manifest": next(package.records("manifests")),
            "config": read(root / "config.json"),
        }
    info = {"keys": keys, "summary": summary, "metadata": metadata}
    check_archive(reference, info)
    compressed = gzip.compress(dumps(keys).encode(), mtime=0)
    with pg_connect(True) as conn:
        existing = conn.execute(
            """SELECT package_digest FROM staging_archives
            WHERE namespace=%s AND snapshot_id=%s AND role=%s""",
            [namespace, root.name, reference["role"]],
        ).fetchone()
        if existing and existing[0] != reference["digest"]:
            raise RuntimeError("Immutable staging archive conflict")
        conn.execute(
            """INSERT INTO staging_archives
            (namespace,snapshot_id,role,package_digest,summary,record_keys,metadata)
            VALUES(%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
            [
                namespace,
                root.name,
                reference["role"],
                reference["digest"],
                Jsonb(summary, dumps=dumps),
                compressed,
                Jsonb(metadata, dumps=dumps),
            ],
        )
    # Read back the committed archive, not the in-memory inventory.
    committed = archive_info(reference)
    if (
        committed["keys"] != keys
        or committed["summary"] != summary
        or committed["metadata"] != metadata
    ):
        raise RuntimeError("Staging archive readback differs")
    return reference, committed


def cleanup_collection(state, collection, apply=False):
    """Каталог/рейтинг и partial runs остаются на диске; очередь не возобновляется."""
    state = Path(state).resolve()
    snapshots = state / "snapshots"
    if (
        not collection
        or Path(collection).name != collection
        or collection in (".", "..")
    ):
        raise ValueError("Invalid collection ID")
    roots = []
    meta = within(snapshots / (collection + "-games_metadata"), snapshots)
    if not (meta / "config.json").exists():
        return {"collection": collection, "skipped": "no_metadata"}
    conf = read(meta / "config.json")
    if conf["scope"] in ("catalog", "ranking"):
        return {"collection": collection, "skipped": "shared_selection_input"}
    expected = ("games_metadata",) if conf.get("metadata_only") else kinds(conf)
    for kind in expected:
        root = within(snapshots / (collection + "-" + kind), snapshots)
        if not (root / "verified.json").is_file():
            return {"collection": collection, "skipped": "unfinished_chain"}
        roots.append(root)
    if "runs" in expected:
        result = manifest(snapshots / (collection + "-runs")).get("source_result") or {}
        not_applicable = (
            result.get("coverage") == "unavailable"
            and result.get("diagnostic") == "no_full_game_categories_in_source_metadata"
            and result.get("partitions") == []
            and result.get("wr") == []
        )
        if (
            (
                result.get("coverage") != "verified_public_partitions"
                and not not_applicable
            )
            or result.get("stop_reason")
            or result.get("intentional_limit")
        ):
            return {"collection": collection, "skipped": "partial_runs"}
    targets = []
    for root in roots:
        for name in ("requests", "entity-package"):
            path = root / name
            if path.exists():
                within(path, root)
                if any(p.is_symlink() for p in path.rglob("*")):
                    raise ValueError("Symlinks are not permitted in staging cleanup")
                targets.append(path)
    size = sum(
        p.stat().st_size for target in targets for p in target.rglob("*") if p.is_file()
    )
    result = {
        "collection": collection,
        "snapshots": len(roots),
        "reclaimable_bytes": size,
        "applied": apply,
    }
    if not apply:
        return result
    if stores() != ("postgres",):
        raise RuntimeError("Cleanup is enabled only for PostgreSQL mode")
    with file_lock(state / "staging-cleanup.lock"):
        init_archive()
        # Seal the whole chain before removing any part of it.
        sealed = [(root, *seal(root, snapshots)) for root in roots]
        for root, reference, info in sealed:
            atomic_write(
                root / "retired.json",
                {
                    "snapshot_id": root.name,
                    "digest": reference["digest"],
                    "summary": info["summary"],
                    **info["metadata"],
                    "retired_at": utcnow(),
                    "storage": "postgresql",
                    "policy": "completed_game_verified_readback",
                },
            )
        for path in targets:
            shutil.rmtree(within(path, snapshots))
    return result


def auto_cleanup(state, collection):
    if os.environ.get("ETL_STAGING_CLEANUP", "1") == "1" and stores() == ("postgres",):
        return cleanup_collection(state, collection, apply=True)
    return {"collection": collection, "skipped": "disabled"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--state",
        type=Path,
        default=os.environ.get("ETL_STATE"),
        required="ETL_STATE" not in os.environ,
    )
    parser.add_argument("--collection")
    parser.add_argument("--all-completed", action="store_true")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    if bool(args.collection) == bool(args.all_completed):
        parser.error("Choose --collection or --all-completed")
    if args.apply:
        import psycopg

        dsn = os.environ.get("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")
        if not dsn:
            raise RuntimeError(
                "Offline cleanup requires Airflow DB configuration to check for active DAGs"
            )
        with psycopg.connect(
            dsn.replace("postgresql+psycopg2://", "postgresql://")
        ) as conn:
            if conn.execute(
                "SELECT count(*) FROM dag_run WHERE state IN ('running','queued')"
            ).fetchone()[0]:
                raise RuntimeError(
                    "Offline cleanup requires all DAG chains to finish first"
                )
    collections = (
        [args.collection]
        if args.collection
        else sorted(
            p.name[: -len("-games_metadata")]
            for p in (args.state / "snapshots").glob("*-games_metadata")
        )
    )
    for collection in collections:
        print(dumps(cleanup_collection(args.state, collection, args.apply)), flush=True)


if __name__ == "__main__":
    main()
