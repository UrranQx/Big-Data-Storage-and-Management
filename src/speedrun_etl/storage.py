"""Прежние загрузчики PostgreSQL, MongoDB и S3 для локального ODS-пакета.

Записывают immutable-данные и проверяют чтение перед подтверждением загрузки."""

from contextlib import contextmanager
import os
import re
from pathlib import Path

from .jsonutil import digest, dumps, loads
from .ods import SETS, restore, s3_key


def namespace():
    name = os.environ.get("ETL_NAMESPACE", "")
    if name and not re.fullmatch(r"test_[a-z0-9_]{1,40}", name):
        raise ValueError("Only explicit test_* namespaces are supported")
    return name


def pg_connect(control=False):
    import psycopg
    from psycopg.types.json import set_json_loads

    conn = psycopg.connect(
        host=os.environ.get("PGHOST", "postgres"),
        dbname="speedrun_control"
        if control
        else os.environ.get("PGDATABASE", "speedrun_ods"),
        user="speedrun_control"
        if control
        else os.environ.get("PGUSER", "speedrun_ods"),
        password=os.environ["CONTROL_DB_PASSWORD" if control else "PGPASSWORD"],
    )
    set_json_loads(loads, conn)
    if not control and namespace():
        from psycopg import sql

        conn.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                sql.Identifier(namespace())
            )
        )
        conn.execute(
            sql.SQL("SET search_path TO {}").format(sql.Identifier(namespace()))
        )
    return conn


def init_stores():
    with pg_connect() as conn:
        conn.execute(Path("/opt/project/sql/ods.sql").read_text())
    with pg_connect(True) as conn:
        conn.execute(Path("/opt/project/sql/control.sql").read_text())
    with Mongo() as mongo:
        mongo.db.observations.create_index(
            [
                ("source_system", 1),
                ("entity_type", 1),
                ("source_id", 1),
                ("observed_at", 1),
            ]
        )
        mongo.db.observations.create_index(
            [
                ("snapshot_id", 1),
                ("context.game_id", 1),
                ("context.category_id", 1),
                ("context.status", 1),
            ]
        )
        mongo.db.responses.create_index("snapshot_id")
    with S3() as s3:
        s3.client.head_bucket(Bucket=s3.bucket)


class Postgres:
    def __enter__(self):
        self.conn = pg_connect()
        return self

    def __exit__(self, *args):
        self.conn.close()

    def put(self, kind, item):
        from psycopg import sql
        from psycopg.types.json import Jsonb

        row = {"key": item["key"], "document": Jsonb(item, dumps=dumps)}
        if kind == "responses":
            row.update(
                {
                    k: item[k]
                    for k in (
                        "snapshot_id",
                        "requested_at",
                        "fetched_at",
                        "http_status",
                    )
                }
            )
        if kind == "observations":
            row.update(
                {
                    k: item[k]
                    for k in (
                        "source_system",
                        "entity_type",
                        "source_id",
                        "snapshot_id",
                        "response_id",
                        "payload",
                        "observed_at",
                    )
                }
            )
            row.update(
                {
                    k: item["context"].get(k)
                    for k in ("game_id", "category_id", "status")
                }
            )
        self.conn.execute(
            sql.SQL(
                "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT (key) DO NOTHING"
            ).format(
                sql.Identifier(kind),
                sql.SQL(",").join(map(sql.Identifier, row)),
                sql.SQL(",").join(sql.Placeholder() for _ in row),
            ),
            list(row.values()),
        )

    def flush(self):
        self.conn.commit()

    def get(self, kind, key, reference=None):
        from psycopg import sql

        row = self.conn.execute(
            sql.SQL("SELECT document FROM {} WHERE key=%s").format(
                sql.Identifier(kind)
            ),
            [key],
        ).fetchone()
        if not row:
            raise KeyError(key)
        return row[0]

    def history(self, entity_type, source_id):
        return [
            row[0]
            for row in self.conn.execute(
                "SELECT document FROM observations WHERE source_system='speedruncom' AND entity_type=%s AND source_id=%s ORDER BY observed_at,key",
                [entity_type, source_id],
            )
        ]


class Mongo:
    def __enter__(self):
        from pymongo import MongoClient
        from pymongo.write_concern import WriteConcern

        self.client = MongoClient(
            "mongodb://mongo:27017",
            username="ods",
            password=os.environ["MONGO_PASSWORD"],
            serverSelectionTimeoutMS=10000,
        )
        self.db = self.client.get_database(
            namespace() or "speedrun_ods", write_concern=WriteConcern(w=1, j=True)
        )
        return self

    def __exit__(self, *args):
        self.client.close()

    def put(self, kind, item):
        self.db[kind].update_one(
            {"_id": item["key"]}, {"$setOnInsert": item}, upsert=True
        )

    def put_many(self, kind, items):
        from pymongo import UpdateOne

        self.db[kind].bulk_write(
            [
                UpdateOne({"_id": item["key"]}, {"$setOnInsert": item}, upsert=True)
                for item in items
            ],
            ordered=False,
        )

    def flush(self):
        pass

    def get(self, kind, key, reference=None):
        item = self.db[kind].find_one({"_id": key})
        if item is None:
            raise KeyError(key)
        item.pop("_id")
        return item

    def history(self, entity_type, source_id):
        return [
            {k: v for k, v in row.items() if k != "_id"}
            for row in self.db.observations.find(
                {
                    "source_system": "speedruncom",
                    "entity_type": entity_type,
                    "source_id": source_id,
                }
            ).sort([("observed_at", 1), ("key", 1)])
        ]


class S3:
    def __enter__(self):
        import boto3
        from botocore.config import Config

        self.bucket = "speedrun-ods"
        self.prefix = (namespace() + "/") if namespace() else ""
        self.client = boto3.client(
            "s3",
            endpoint_url="http://garage:3900",
            region_name="garage",
            aws_access_key_id=os.environ["S3_ACCESS_KEY"],
            aws_secret_access_key=os.environ["S3_SECRET_KEY"],
            config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 3}),
        )
        return self

    def __exit__(self, *args):
        self.client.close()

    def put(self, kind, item):
        from botocore.exceptions import ClientError

        key = self.prefix + s3_key(kind, item)
        try:
            previous = self.get(kind, item["key"], item)
        except ClientError as error:
            if error.response["Error"]["Code"] not in ("NoSuchKey", "404"):
                raise
        else:
            if digest(previous) != digest(item):
                raise ValueError("S3 immutable key conflict")
            return
        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=dumps(item).encode(),
            ContentType="application/json",
        )

    def flush(self):
        pass

    def get(self, kind, key, reference=None):
        location = self.prefix + s3_key(kind, reference or {"key": key})
        response = self.client.get_object(Bucket=self.bucket, Key=location)
        with response["Body"] as body:
            return loads(body.read())

    def history(self, entity_type, source_id):
        from urllib.parse import quote

        prefix = (
            self.prefix
            + "observations/speedruncom/"
            + quote(entity_type, safe="")
            + "/"
            + quote(source_id, safe="")
            + "/"
        )
        result = []
        for page in self.client.get_paginator("list_objects_v2").paginate(
            Bucket=self.bucket, Prefix=prefix
        ):
            for item in page.get("Contents", []):
                response = self.client.get_object(Bucket=self.bucket, Key=item["Key"])
                with response["Body"] as body:
                    result.append(loads(body.read()))
        return sorted(result, key=lambda row: (row["observed_at"], row["key"]))


STORES = {"postgres": Postgres, "mongo": Mongo, "garage": S3}


def verify_store(store, package):
    from .ods import LISTS, OBJECTS

    referenced_responses = set()
    actual = {}
    for kind in SETS:
        pairs = []
        import hashlib

        checksum, count = hashlib.sha256(b"["), 0
        for expected in package.records(kind):
            row = store.get(kind, expected["key"], expected)
            if dumps(row) != dumps(expected):
                raise ValueError(f"Readback mismatch: {kind}/{expected['key']}")
            pair = [row["key"], digest(row)]
            if package.db is not None:
                if count:
                    checksum.update(b",")
                checksum.update(dumps(pair).encode())
            else:
                pairs.append(pair)
            count += 1
            if kind == "observations":
                referenced_responses.add(row["response_id"])
        checksum.update(b"]")
        actual[kind] = {
            "count": count,
            "digest": checksum.hexdigest()
            if package.db is not None
            else digest(sorted(pairs)),
        }
    if actual != package.summary():
        raise ValueError("Package digest differs")
    # Every stored document above equals its local immutable counterpart. Restore
    # from those checked values rather than re-fetching repeated player payloads.
    for response in package.records("responses"):
        restore(package.get, response, resolver=package.resolve)
    linked = 0
    for response_id in referenced_responses:
        response = (
            package.get("responses", response_id)
            if package.contains("responses", response_id)
            else store.get("responses", response_id)
        )
        for ref in response["references"]:
            path = ref["path"]
            entity_type = ({**LISTS, **OBJECTS}).get(path[0]) if path else None
            if entity_type is None:
                continue
            key = digest([response_id, entity_type, path])
            if not package.contains("observations", key):
                continue  # profile package is a subset of its original responses
            observation = package.get("observations", key)
            if (
                observation["response_id"],
                observation["path"],
                observation["payload"],
            ) != (response_id, path, ref["payload"]):
                raise ValueError(
                    "Observation does not link to its original response fragment"
                )
            linked += 1
    if linked != actual["observations"]["count"]:
        raise ValueError("Unresolved observation references")
    return actual


def load_package(name, package, snapshot_id, fail_after=None):
    with STORES[name]() as store:
        n = 0
        for kind in SETS:
            if name == "mongo" and fail_after is None:
                batch = []
                for item in package.records(kind):
                    batch.append(item)
                    if len(batch) == 100:
                        store.put_many(kind, batch)
                        batch.clear()
                if batch:
                    store.put_many(kind, batch)
                store.flush()
                continue
            for item in package.records(kind):
                store.put(kind, item)
                n += 1
                if n % 100 == 0:
                    store.flush()
                if fail_after is not None and n == fail_after:
                    store.flush()
                    raise RuntimeError("Injected partial load failure")
            store.flush()
        summary = verify_store(store, package)
    from psycopg.types.json import Jsonb

    with pg_connect(True) as conn:
        conn.execute(
            """INSERT INTO receipts(snapshot_id,store,package_digest,summary) VALUES (%s,%s,%s,%s)
            ON CONFLICT(snapshot_id,store) DO UPDATE SET package_digest=excluded.package_digest,
            summary=excluded.summary, verified_at=now()""",
            [snapshot_id, name, digest(summary), Jsonb(summary, dumps=dumps)],
        )
    return {"store": name, "digest": digest(summary), "summary": summary}


def require_ready(snapshot_id):
    with pg_connect(True) as conn:
        rows = conn.execute(
            "SELECT store,package_digest FROM receipts WHERE snapshot_id=%s",
            [snapshot_id],
        ).fetchall()
        status = conn.execute(
            "SELECT state FROM snapshot_status WHERE snapshot_id=%s", [snapshot_id]
        ).fetchone()
    if (
        {r[0] for r in rows} != set(STORES)
        or len({r[1] for r in rows}) != 1
        or not status
        or status[0] != "ready"
    ):
        raise RuntimeError("The exact input snapshot is not ready in all stores")


def set_status(snapshot_id, state, details):
    from psycopg.types.json import Jsonb

    with pg_connect(True) as conn:
        conn.execute(
            """INSERT INTO snapshot_status(snapshot_id,state,details) VALUES(%s,%s,%s)
            ON CONFLICT(snapshot_id) DO UPDATE SET state=excluded.state,details=excluded.details,updated_at=now()""",
            [snapshot_id, state, Jsonb(details, dumps=dumps)],
        )
