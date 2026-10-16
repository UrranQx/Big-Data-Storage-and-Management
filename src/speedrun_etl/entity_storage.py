"""Concrete entity-v2 loaders: batched DB operations and immutable S3 chunks."""
import gzip
import hashlib
import os
from pathlib import Path
import re
import time

from .entity_ods import DATASETS, EntityPackage
from .jsonutil import digest, dumps, loads, atomic_write
from .storage import pg_connect, Mongo, S3


def validate_namespace(value):
    if value != "entity_v2" and not re.fullmatch(r"test_[a-z0-9_]{1,40}", value):
        raise ValueError("Explicit entity_v2 or test_* namespace required")
    return value


def batches(records):
    batch, size = [], 0
    for row in records:
        raw = dumps(row)
        if batch and (len(batch) >= 500 or size + len(raw) > 4 * 1024**2):
            yield batch
            batch, size = [], 0
        batch.append((row, raw))
        size += len(raw)
    if batch:
        yield batch


def init_entities(namespace="entity_v2"):
    from psycopg import sql
    namespace = validate_namespace(namespace)
    with pg_connect() as conn:
        conn.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(namespace)))
        for dataset in DATASETS:
            table = sql.Identifier(namespace, dataset)
            conn.execute(sql.SQL("""CREATE TABLE IF NOT EXISTS {} (
                key text PRIMARY KEY, source_id text, projection text, game_id text,
                category_id text, status integer, document jsonb NOT NULL)""").format(table))
            conn.execute(sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} (source_id,projection)").format(
                sql.Identifier(dataset + "_source"), table))
            if dataset in ("runs", "wr_history"):
                conn.execute(sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} (game_id,category_id,status)").format(
                    sql.Identifier(dataset + "_scope"), table))
    with Mongo() as store:
        db = store.client[namespace]
        for dataset in DATASETS:
            db[dataset].create_index([("source_id", 1), ("projection", 1)])
        db.runs.create_index([("game_id", 1), ("category_id", 1), ("status", 1)])
    with pg_connect(True) as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS entity_receipts (
            namespace text NOT NULL,snapshot_id text NOT NULL,role text NOT NULL,store text NOT NULL,
            package_digest text NOT NULL,summary jsonb NOT NULL,verified_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY(namespace,snapshot_id,role,store))""")


def attrs(row):
    return [row.get(k) for k in ("source_id", "projection", "game_id", "category_id", "status")]


def shard(row):
    return hashlib.sha256(str(row.get("source_id", row["key"])).encode()).hexdigest()[0]


def make_chunks(package, dataset):
    buffers = {}
    for row in package.records(dataset):
        part = shard(row)
        raw = dumps(row).encode() + b"\n"
        buf, count = buffers.get(part, (bytearray(), 0))
        if buf and len(buf) + len(raw) > 4 * 1024**2:
            yield part, gzip.compress(bytes(buf), mtime=0), count
            buf, count = bytearray(), 0
        buf.extend(raw)
        buffers[part] = buf, count + 1
    for part, (buf, count) in sorted(buffers.items()):
        yield part, gzip.compress(bytes(buf), mtime=0), count


def immutable_object(store, key, raw):
    from botocore.exceptions import ClientError
    checksum = hashlib.sha256(raw).hexdigest()
    try:
        head = store.client.head_object(Bucket=store.bucket, Key=key)
    except ClientError as error:
        if error.response["Error"]["Code"] not in ("404", "NoSuchKey"):
            raise
        store.client.put_object(Bucket=store.bucket, Key=key, Body=raw,
                                Metadata={"sha256": checksum})
    else:
        if head["ContentLength"] != len(raw) or head["Metadata"].get("sha256") != checksum:
            raise ValueError("Immutable S3 chunk conflict")


def load(name, package, snapshot_id, namespace="entity_v2", role="all", fail_after=None):
    """Read back once, compare every logical record, then persist one receipt."""
    from psycopg import sql
    from psycopg.types.json import Jsonb
    from pymongo import UpdateOne
    namespace = validate_namespace(namespace)
    expected_summary = package.summary()
    started = time.perf_counter()
    n = 0
    if name == "postgres":
        with pg_connect() as conn:
            for dataset in DATASETS:
                table = sql.Identifier(namespace, dataset)
                query = sql.SQL("""INSERT INTO {}(key,source_id,projection,game_id,category_id,status,document)
                    VALUES(%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(key) DO NOTHING""").format(table)
                for batch in batches(package.records(dataset)):
                    with conn.cursor() as cursor:
                        cursor.executemany(query, [(r["key"], *attrs(r), Jsonb(r, dumps=dumps)) for r, raw in batch])
                    conn.commit()
                    n += len(batch)
                    if fail_after and n >= fail_after:
                        raise RuntimeError("Injected entity-v2 partial load")
                for batch in batches(package.records(dataset)):
                    rows = conn.execute(sql.SQL("SELECT key,document FROM {} WHERE key=ANY(%s)").format(table),
                                        [[r["key"] for r, raw in batch]]).fetchall()
                    actual = {key: dumps(row) for key, row in rows}
                    if actual != {r["key"]: raw for r, raw in batch}:
                        raise ValueError("Postgres entity readback differs")
    elif name == "mongo":
        with Mongo() as store:
            from pymongo.write_concern import WriteConcern
            db = store.client.get_database(namespace, write_concern=WriteConcern(w=1, j=True))
            for dataset in DATASETS:
                for batch in batches(package.records(dataset)):
                    db[dataset].bulk_write([UpdateOne({"_id": r["key"]}, {"$setOnInsert": {
                        "document": r, **dict(zip(("source_id", "projection", "game_id", "category_id", "status"), attrs(r)))
                    }}, upsert=True) for r, raw in batch], ordered=False)
                    n += len(batch)
                    if fail_after and n >= fail_after:
                        raise RuntimeError("Injected entity-v2 partial load")
                for batch in batches(package.records(dataset)):
                    rows = db[dataset].find({"_id": {"$in": [r["key"] for r, raw in batch]}}, {"document": 1})
                    actual = {r["_id"]: dumps(r["document"]) for r in rows}
                    if actual != {r["key"]: raw for r, raw in batch}:
                        raise ValueError("Mongo entity readback differs")
    elif name == "garage":
        chunks = []
        prefix = namespace + "/entity-v2/"
        with S3() as store:
            for dataset in DATASETS:
                for part, raw, count in make_chunks(package, dataset):
                    checksum = hashlib.sha256(raw).hexdigest()
                    key = prefix + f"chunks/{dataset}/{part}/{checksum}.jsonl.gz"
                    immutable_object(store, key, raw)
                    chunks.append({"dataset": dataset, "shard": part, "key": key,
                                   "sha256": checksum, "count": count})
                    n += count
                    if fail_after and n >= fail_after:
                        raise RuntimeError("Injected entity-v2 partial load")
            # Store readback hashes in a small local temporary SQLite index, so
            # validation has bounded memory even for a larger input package.
            import sqlite3
            from tempfile import TemporaryDirectory
            with TemporaryDirectory() as temp:
                db = sqlite3.connect(str(Path(temp) / "readback.sqlite3"))
                db.execute("CREATE TABLE actual(dataset text,key text,checksum text,PRIMARY KEY(dataset,key)) WITHOUT ROWID")
                for chunk in chunks:
                    response = store.client.get_object(Bucket=store.bucket, Key=chunk["key"])
                    with response["Body"] as stream:
                        raw = stream.read()
                    if hashlib.sha256(raw).hexdigest() != chunk["sha256"]:
                        raise ValueError("S3 chunk checksum differs")
                    count = 0
                    for line in gzip.decompress(raw).splitlines():
                        row = loads(line)
                        db.execute("INSERT INTO actual VALUES(?,?,?)", (chunk["dataset"], row["key"], digest(row)))
                        count += 1
                    if count != chunk["count"]:
                        raise ValueError("S3 chunk count differs")
                for dataset in DATASETS:
                    expected = expected_summary[dataset]
                    checksum, count = hashlib.sha256(b"["), 0
                    for key, value in db.execute("SELECT key,checksum FROM actual WHERE dataset=? ORDER BY key", (dataset,)):
                        if count:
                            checksum.update(b",")
                        checksum.update(dumps([key, value]).encode())
                        count += 1
                    checksum.update(b"]")
                    if {"count": count, "digest": checksum.hexdigest()} != expected:
                        raise ValueError("S3 logical record set differs")
                db.close()
            index = {"format_version": "entity-v2", "snapshot_id": snapshot_id, "role": role,
                     "summary": expected_summary, "chunks": chunks}
            key = prefix + "snapshots/" + snapshot_id + "/" + role + "/" + digest(expected_summary) + ".json"
            raw = dumps(index).encode()
            immutable_object(store, key, raw)
            response = store.client.get_object(Bucket=store.bucket, Key=key)
            with response["Body"] as stream:
                if stream.read() != raw:
                    raise ValueError("S3 snapshot index differs")
    else:
        raise ValueError("Unknown entity store")
    result = {"namespace": namespace, "snapshot_id": snapshot_id, "role": role, "store": name,
              "digest": digest(expected_summary), "summary": expected_summary,
              "seconds": round(time.perf_counter() - started, 3)}
    with pg_connect(True) as conn:
        conn.execute("""INSERT INTO entity_receipts(namespace,snapshot_id,role,store,package_digest,summary)
            VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT(namespace,snapshot_id,role,store)
            DO UPDATE SET package_digest=excluded.package_digest,summary=excluded.summary,verified_at=now()""",
            [namespace, snapshot_id, role, name, result["digest"], Jsonb(expected_summary, dumps=dumps)])
    return result


def ready(snapshot_id, namespace="entity_v2", role="all", expected=None):
    with pg_connect(True) as conn:
        rows = conn.execute("""SELECT store,package_digest,summary FROM entity_receipts
            WHERE namespace=%s AND snapshot_id=%s AND role=%s""", [namespace, snapshot_id, role]).fetchall()
    if {r[0] for r in rows} != {"postgres", "mongo", "garage"} or len({r[1] for r in rows}) != 1:
        raise RuntimeError("Entity snapshot not acknowledged by all three stores")
    if expected and {r[1] for r in rows} != {expected}:
        raise RuntimeError("Entity receipt belongs to another package")
    return {"digest": rows[0][1], "stores": {r[0]: r[2] for r in rows}}


def read_entity(name, dataset, source_id, snapshot_id, summary, namespace="entity_v2", role="all"):
    """Versions/history belonging to this exact input snapshot in every store."""
    if dataset not in DATASETS:
        raise ValueError(dataset)
    namespace = validate_namespace(namespace)
    check_snapshot = snapshot_id[:-len("-players")] + "-runs" if snapshot_id.endswith("-players") else snapshot_id
    if name == "postgres":
        from psycopg import sql
        with pg_connect() as conn:
            checks = [r[0] for r in conn.execute(sql.SQL("SELECT document FROM {} WHERE source_id=%s AND document->>'snapshot_id'=%s").format(
                sql.Identifier(namespace, "checks")), [source_id, check_snapshot])]
            if snapshot_id.endswith("-players"):
                checks = [c for c in checks if c["dataset"] == "players"]
            if dataset == "checks":
                return sorted(checks, key=lambda r:r["key"])
            keys = list({s["version_key"] for c in checks if c["dataset"] == dataset for s in c["states"]})
            return [r[0] for r in conn.execute(sql.SQL("SELECT document FROM {} WHERE key=ANY(%s) ORDER BY key").format(
                sql.Identifier(namespace, dataset)), [keys])]
    if name == "mongo":
        with Mongo() as store:
            db = store.client[namespace]
            checks = [r["document"] for r in db.checks.find({"source_id": source_id, "document.snapshot_id": check_snapshot})]
            if snapshot_id.endswith("-players"):
                checks = [c for c in checks if c["dataset"] == "players"]
            if dataset == "checks":
                return sorted(checks, key=lambda r:r["key"])
            keys = list({s["version_key"] for c in checks if c["dataset"] == dataset for s in c["states"]})
            return [r["document"] for r in db[dataset].find({"_id": {"$in": keys}}).sort("_id", 1)]
    with S3() as store:
        key = namespace + "/entity-v2/snapshots/" + snapshot_id + "/" + role + "/" + digest(summary) + ".json"
        response = store.client.get_object(Bucket=store.bucket, Key=key)
        with response["Body"] as stream:
            index = loads(stream.read())
        result = []
        part = shard({"key": source_id, "source_id": source_id})
        for chunk in index["chunks"]:
            if chunk["dataset"] != dataset or chunk["shard"] != part:
                continue
            response = store.client.get_object(Bucket=store.bucket, Key=chunk["key"])
            with response["Body"] as stream:
                raw = stream.read()
            if hashlib.sha256(raw).hexdigest() != chunk["sha256"]:
                raise ValueError("S3 lookup chunk differs")
            result.extend(r for r in map(loads, gzip.decompress(raw).splitlines()) if r.get("source_id") == source_id)
        return sorted(result, key=lambda r: r["key"])


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--package", required=True)
    parser.add_argument("--namespace", required=True)
    args = parser.parse_args()
    package = EntityPackage(args.package)
    manifest = next(package.records("manifests"))
    sid = manifest["snapshot_id"]
    init_entities(args.namespace)
    results = []
    for repeat in range(2):
        for name in ("postgres", "mongo", "garage"):
            result = load(name, package, sid, args.namespace)
            results.append(result)
            print(dumps({"repeat": repeat + 1, **result}), flush=True)
    evidence = {**ready(sid, args.namespace, expected=digest(package.summary())), "loads": results}
    for dataset in ("runs", "players", "wr_history", "checks"):
        first = next(package.records(dataset), None)
        if first and first.get("source_id"):
            reads = [read_entity(name, dataset, first["source_id"], sid, package.summary(), args.namespace)
                     for name in ("postgres", "mongo", "garage")]
            if len({digest(r) for r in reads}) != 1 or first not in reads[0]:
                raise ValueError("Entity/history read differs between stores")
    atomic_write(Path(args.package) / "stores_verified.json", evidence)
    package.db.close()
    print("Three stores and replay verified", flush=True)


if __name__ == "__main__":
    main()
