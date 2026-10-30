"""Фиксированные опыты запросов, индексов и DDL для ЛР1.

Работают только в test_wl_* на синтетических копиях, не меняют production ODS."""

import argparse
from collections import Counter, defaultdict
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal, localcontext
import csv
import gzip
import hashlib
import os
from pathlib import Path
import re
import sqlite3
import time

from .benchmark import collections_busy
from .capacity import check_capacity
from .entity_ods import DATASETS, EntityPackage
from .entity_storage import batches, make_chunks, ready
from .jsonutil import atomic_write, digest, dumps, file_lock, loads, read, utcnow
from .storage import Mongo, S3, pg_connect
from .experiment_inputs import verify_ref

DATA = ("runs", "players", "games")
RUN_COLUMNS = (
    "key",
    "game_id",
    "category_id",
    "status",
    "source_month",
    "time_ns",
    "participant_keys",
    "properties",
    "raw_json",
    "provenance",
)
PLAYER_COLUMNS = ("key", "game_id", "profile_id", "name", "area_id", "raw_json")
OPS = (
    "batch_100",
    "scope_top_100",
    "json_filter",
    "player_runs",
    "monthly_counts",
    "cumulative_counts",
    "join_1000",
    "offset_page",
    "keyset_page",
)


def scratch(namespace):
    if not re.fullmatch(r"test_wl_[a-z0-9_]{1,36}", namespace):
        raise ValueError("Only an explicit test_wl_* namespace is allowed")
    return namespace


def duration_ns(value):
    if not isinstance(value, str) or not re.fullmatch(r"\d+(?:\.\d+)?s", value):
        return None
    with localcontext() as ctx:
        ctx.prec = max(28, len(value) + 12)
        number = Decimal(value[:-1]) * 10**9
    return (
        int(number)
        if number == number.to_integral_value() and number <= 2**63 - 1
        else None
    )


def project_run(row, raw, seen, snapshot, context=None):
    gid = row["game_id"]
    date = raw.get("performedAt")
    month = None
    if isinstance(date, str):
        try:
            datetime.fromisoformat(date.replace("Z", "+00:00"))
            month = date[:7]  # Source calendar month, no invented missing date.
        except ValueError:
            pass
    return {
        "key": gid + ":" + row["source_id"],
        "game_id": gid,
        "category_id": row.get("category_id"),
        "status": row.get("status"),
        "source_month": month,
        "time_ns": duration_ns(raw.get("time")),
        "participant_keys": [gid + ":" + p for p in row.get("player_ids", [])],
        "properties": {
            "platform_id": raw.get("platformId"),
            "emulator": raw.get("emulator"),
            "obsolete": context.get("values", {}).get("obsolete")
            if context
            else raw.get("obsolete"),
            "value_ids": raw.get("valueIds", []),
        },
        "raw_json": dumps(raw),
        "provenance": {
            "snapshot": snapshot,
            "version_key": row["key"],
            "projection": row["projection"],
            "observed_at": seen,
            **({"unfiltered_context": context} if context else {}),
        },
    }


def prepare(state, collections, root, sizes):
    """Select one observed version per game/ID for a scratch experiment, not DDS."""
    candidates = sqlite3.connect(root / "candidates.sqlite3")
    candidates.execute(
        "CREATE TABLE candidate(dataset text,key text,seen text,rank text,document text,PRIMARY KEY(dataset,key)) WITHOUT ROWID"
    )
    sources = {}

    def put(dataset, row, seen, rank=""):
        candidates.execute(
            """INSERT INTO candidate VALUES(?,?,?,?,?) ON CONFLICT(dataset,key)
            DO UPDATE SET seen=excluded.seen,rank=excluded.rank,document=excluded.document
            WHERE (excluded.seen,excluded.rank)>(candidate.seen,candidate.rank)""",
            [dataset, row["key"], seen, rank, dumps(row)],
        )

    for collection in collections:
        refs = {
            k: read(state / "snapshots" / (collection + "-" + k) / "extracted.json")
            for k in ("games_metadata", "runs", "players")
        }
        gid = None
        for kind, ref in refs.items():
            verify_ref(state, ref)
            sources[ref["snapshot_id"]] = ref["digest"]
            p = EntityPackage(ref["package_uri"], role=ref["role"])
            if digest(p.summary()) != ref["digest"]:
                raise ValueError("Local package differs from its verified digest")
            try:
                if kind == "games_metadata":
                    detail = next(
                        r
                        for r in p.records("games")
                        if r["projection"] == "game_detail"
                    )
                    raw = p.decode(detail["raw"])["game"]
                    gid = detail["source_id"]
                    put(
                        "games",
                        {"key": gid, "name": raw.get("name"), "raw_json": dumps(raw)},
                        "",
                        "",
                    )
                    continue
                dataset = "runs" if kind == "runs" else "players"
                check_documents = {
                    c["last_seen"]["version_key"]: c
                    for c in p.records("checks")
                    if c["dataset"] == dataset
                }
                checks = {
                    key: c["last_seen"]["observed_at"]
                    for key, c in check_documents.items()
                }
                for row in p.records(dataset):
                    if row["key"] not in checks or (
                        dataset == "runs"
                        and not (
                            row["projection"].startswith("leaderboard_run")
                            or row["projection"] == "run_core"
                        )
                    ):
                        continue
                    raw = p.decode(row["raw"])
                    seen = checks[row["key"]]
                    if dataset == "runs":
                        context = (
                            check_documents[row["key"]]
                            .get("unfiltered_leaderboard", {})
                            .get("last_seen")
                        )
                        value = project_run(row, raw, seen, ref["snapshot_id"], context)
                    else:
                        value = {
                            "key": gid + ":" + row["source_id"],
                            "game_id": gid,
                            "profile_id": row["source_id"],
                            "name": raw.get("name"),
                            "area_id": raw.get("areaId"),
                            "raw_json": dumps(raw),
                        }
                    put(dataset, value, seen, row["projection"])
            finally:
                p.db.close()
        candidates.commit()
        print("Prepared source", collection, flush=True)
    keys = [
        r[0]
        for r in candidates.execute("SELECT key FROM candidate WHERE dataset='runs'")
    ]
    keys.sort(key=digest)
    if len(keys) < max(sizes):
        raise ValueError(
            f"Need {max(sizes)} observed run IDs; only {len(keys)} available"
        )
    inputs = []
    for size in sizes:
        p = EntityPackage(root / ("input_" + str(size)))
        participants = set()
        games = set()
        for key in keys[:size]:
            row = loads(
                candidates.execute(
                    "SELECT document FROM candidate WHERE dataset='runs' AND key=?",
                    [key],
                ).fetchone()[0]
            )
            p.put("runs", row)
            participants.update(row["participant_keys"])
            games.add(row["game_id"])
        missing = []
        for key in sorted(participants):
            found = candidates.execute(
                "SELECT document FROM candidate WHERE dataset='players' AND key=?",
                [key],
            ).fetchone()
            if found:
                p.put("players", loads(found[0]))
            else:
                missing.append(key)
        for key in sorted(games):
            p.put(
                "games",
                loads(
                    candidates.execute(
                        "SELECT document FROM candidate WHERE dataset='games' AND key=?",
                        [key],
                    ).fetchone()[0]
                ),
            )
        p.db.commit()
        summary = p.summary()
        metadata = {
            "format_version": "lab1-workload-v1",
            "size": size,
            "digest": digest(summary),
            "counts": {d: summary[d]["count"] for d in DATA},
            "source_snapshots": sources,
            "missing_profile_keys": missing,
            "source_HTTP": 0,
            "selection": "Nested hash-ID subsets. Latest observed version per game/ID across leaderboard projections; not official current standings.",
            "profile_scope": "game plus source ID, no cross-game guest/name merging",
            "time_ns_unparsed": sum(r["time_ns"] is None for r in p.records("runs")),
        }
        atomic_write(p.root / "input.json", metadata)
        inputs.append(p.root)
        p.db.close()
    candidates.close()
    return inputs


def parameters(runs):
    ordered = sorted(runs, key=lambda r: digest(r["key"]))
    lexical = sorted(r["key"] for r in runs)
    scopes = Counter(
        (r["game_id"], r["category_id"], r["status"])
        for r in runs
        if r["time_ns"] is not None
    )
    scope = min(scopes, key=lambda s: (-scopes[s], s))
    platforms = Counter(
        r["properties"]["platform_id"] for r in runs if r["properties"]["platform_id"]
    )
    platform = min(platforms, key=lambda p: (platforms[p], p))
    players = Counter(p for r in runs for p in set(r["participant_keys"]))
    player = min(players, key=lambda p: (-players[p], p))
    offset = len(runs) // 2
    return {
        "ids": [r["key"] for r in ordered[:100]],
        "join_ids": [r["key"] for r in ordered[:1000]],
        "update_ids": [r["key"] for r in ordered[:1000]],
        "scope": list(scope),
        "platform": platform,
        "player": player,
        "offset": offset,
        "cursor": lexical[offset - 1],
    }


def local_query(runs, profiles, operation, params):
    if operation == "batch_100":
        ids = set(params["ids"])
        return sorted([r["key"], r["raw_json"]] for r in runs if r["key"] in ids)
    if operation == "scope_top_100":
        rows = [
            r
            for r in runs
            if [r[k] for k in ("game_id", "category_id", "status")] == params["scope"]
            and r["time_ns"] is not None
        ]
        return [
            [r["key"], r["time_ns"]]
            for r in sorted(rows, key=lambda r: (r["time_ns"], r["key"]))[:100]
        ]
    if operation == "json_filter":
        return sorted(
            r["key"]
            for r in runs
            if r["properties"]["platform_id"] == params["platform"]
        )
    if operation == "player_runs":
        return sorted(
            r["key"] for r in runs if params["player"] in r["participant_keys"]
        )
    if operation in ("monthly_counts", "cumulative_counts"):
        counts = Counter(
            (r["game_id"], r["status"], r["source_month"] or "")
            for r in runs
            if operation == "monthly_counts" or r["source_month"] is not None
        )
        totals = Counter()
        result = []
        for (g, status, month), n in sorted(counts.items()):
            totals[g, status] += n
            result.append(
                [g, status, month, n]
                + ([totals[g, status]] if operation == "cumulative_counts" else [])
            )
        return result
    if operation == "join_1000":
        ids = set(params["join_ids"])
        result = []
        for r in sorted(runs, key=lambda r: r["key"]):
            if r["key"] in ids:
                result.append(
                    [
                        r["key"],
                        [
                            [p, profiles[p]["name"], profiles[p]["area_id"]]
                            for p in sorted(set(r["participant_keys"]))
                            if p in profiles
                        ],
                    ]
                )
        return result
    keys = sorted(r["key"] for r in runs)
    if operation == "offset_page":
        return keys[params["offset"] : params["offset"] + 100]
    if operation == "keyset_page":
        return [k for k in keys if k > params["cursor"]][:100]
    raise ValueError(operation)


@contextmanager
def connect(name, namespace, *, existing=False):
    scratch(namespace)
    h = {"name": name, "namespace": namespace, "io": Counter()}
    if name == "postgres":
        with pg_connect() as conn:
            h["conn"] = conn
            conn.execute("SET statement_timeout = 120000")
            yield h
    elif name == "mongo":
        from pymongo.write_concern import WriteConcern

        with Mongo() as store:
            h["db"] = store.client.get_database(
                namespace, write_concern=WriteConcern(w=1, j=True)
            )
            h["client"] = store.client
            yield h
    elif name == "garage":
        with S3() as store:
            h["s3"] = store
            if existing:
                h["index"] = loads(s3_get(h, namespace + "/workloads/base/index.json"))
            yield h
    else:
        raise ValueError(name)


def table(h, name):
    from psycopg import sql

    return sql.Identifier(scratch(h["namespace"]), name)


def s3_get(h, key):
    response = h["s3"].client.get_object(Bucket=h["s3"].bucket, Key=key)
    with response["Body"] as stream:
        raw = stream.read()
    h["io"]["get_calls"] += 1
    h["io"]["read_bytes"] += len(raw)
    return raw


def s3_put(h, key, raw):
    if not key.startswith(scratch(h["namespace"]) + "/workloads/"):
        raise ValueError("Write outside workload namespace")
    response = h["s3"].client.put_object(Bucket=h["s3"].bucket, Key=key, Body=raw)
    h["io"]["put_calls"] += 1
    h["io"]["written_bytes"] += len(raw)
    return response


def iter_s3_rows(h, dataset):
    """Читает блоки последовательно; не держит весь набор в памяти клиента."""
    for chunk in h["index"]["chunks"]:
        if chunk["dataset"] != dataset:
            continue
        raw = s3_get(h, chunk["key"])
        if hashlib.sha256(raw).hexdigest() != chunk["sha256"]:
            raise ValueError("Changed workload chunk")
        yield from (loads(line) for line in gzip.decompress(raw).splitlines())


def s3_rows(h, dataset):
    """Материализация для полной обратной сверки, вне пользовательских чтений."""
    return list(iter_s3_rows(h, dataset))


def setup(h):
    from psycopg import sql

    if h["name"] == "postgres":
        c = h["conn"]
        c.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(h["namespace"])))
        c.execute(
            sql.SQL(
                "CREATE TABLE {} (key text PRIMARY KEY,game_id text,category_id text,status int,source_month text,time_ns bigint,participant_keys text[],properties jsonb,raw_json text,provenance jsonb)"
            ).format(table(h, "runs"))
        )
        c.execute(
            sql.SQL(
                "CREATE TABLE {} (key text PRIMARY KEY,game_id text,profile_id text,name text,area_id text,raw_json text)"
            ).format(table(h, "players"))
        )
        c.execute(
            sql.SQL(
                "CREATE TABLE {} (key text PRIMARY KEY,name text,raw_json text)"
            ).format(table(h, "games"))
        )
        c.commit()
    elif h["name"] == "mongo":
        if h["db"].list_collection_names():
            raise ValueError("Workload namespace already exists")
        for dataset in DATA:
            h["db"].create_collection(dataset)
    else:
        found = h["s3"].client.list_objects_v2(
            Bucket=h["s3"].bucket, Prefix=h["namespace"] + "/workloads/", MaxKeys=1
        )
        if found.get("KeyCount"):
            raise ValueError("Workload prefix already exists")


def load_native(h, package):
    from psycopg import sql
    from psycopg.types.json import Jsonb

    if h["name"] == "postgres":
        for dataset in DATA:
            columns = (
                RUN_COLUMNS
                if dataset == "runs"
                else PLAYER_COLUMNS
                if dataset == "players"
                else ("key", "name", "raw_json")
            )
            statement = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                table(h, dataset),
                sql.SQL(",").join(map(sql.Identifier, columns)),
                sql.SQL(",").join(sql.Placeholder() for _ in columns),
            )
            with h["conn"].cursor() as cursor:
                for batch in batches(package.records(dataset)):
                    cursor.executemany(
                        statement,
                        [
                            [
                                Jsonb(r[k], dumps=dumps)
                                if k in ("properties", "provenance")
                                else r[k]
                                for k in columns
                            ]
                            for r, raw in batch
                        ],
                    )
        h["conn"].commit()
    elif h["name"] == "mongo":
        for dataset in DATA:
            for batch in batches(package.records(dataset)):
                h["db"][dataset].insert_many(
                    [{"_id": r["key"], **r} for r, raw in batch], ordered=False
                )
    else:
        chunks = []
        for dataset in DATA:
            for part, raw, count in make_chunks(package, dataset):
                checksum = hashlib.sha256(raw).hexdigest()
                key = f"{h['namespace']}/workloads/base/{dataset}/{part}/{checksum}.jsonl.gz"
                s3_put(h, key, raw)
                chunks.append(
                    {
                        "dataset": dataset,
                        "key": key,
                        "sha256": checksum,
                        "count": count,
                        "bytes": len(raw),
                    }
                )
        h["index"] = {"chunks": chunks}
        s3_put(
            h, h["namespace"] + "/workloads/base/index.json", dumps(h["index"]).encode()
        )


def all_rows(h, dataset, *, relation=None):
    if h["name"] == "garage":
        return sorted(s3_rows(h, dataset), key=lambda r: r["key"])
    if h["name"] == "mongo":
        return [
            {k: v for k, v in r.items() if k != "_id"}
            for r in h["db"][relation or dataset].find().sort("_id", 1)
        ]
    from psycopg import sql

    columns = (
        RUN_COLUMNS
        if dataset == "runs"
        else PLAYER_COLUMNS
        if dataset == "players"
        else ("key", "name", "raw_json")
    )
    rows = h["conn"].execute(
        sql.SQL("SELECT {} FROM {} ORDER BY key").format(
            sql.SQL(",").join(map(sql.Identifier, columns)),
            table(h, relation or dataset),
        )
    )
    return [dict(zip(columns, r)) for r in rows]


def indexes(h):
    if h["name"] == "garage":
        return  # No secondary query index, do not call this a server index.
    if h["name"] == "mongo":
        h["db"].runs.create_index(
            [
                ("game_id", 1),
                ("category_id", 1),
                ("status", 1),
                ("time_ns", 1),
                ("key", 1),
            ]
        )
        h["db"].runs.create_index([("properties.platform_id", 1)])
        h["db"].runs.create_index([("participant_keys", 1)])
    else:
        from psycopg import sql

        c = h["conn"]
        t = table(h, "runs")
        c.execute(
            sql.SQL(
                "CREATE INDEX ON {} (game_id,category_id,status,time_ns,key)"
            ).format(t)
        )
        c.execute(
            sql.SQL("CREATE INDEX ON {} USING gin (properties jsonb_path_ops)").format(
                t
            )
        )
        c.execute(sql.SQL("CREATE INDEX ON {} USING gin (participant_keys)").format(t))
        c.commit()


def analyze(h):
    if h["name"] == "postgres":
        from psycopg import sql

        for dataset in DATA:
            h["conn"].execute(sql.SQL("ANALYZE {}").format(table(h, dataset)))
        h["conn"].commit()


def pg_statement(h, operation, p):
    from psycopg import sql

    statements = {
        "batch_100": (
            "SELECT key,raw_json FROM {} WHERE key=ANY(%s) ORDER BY key",
            [p["ids"]],
        ),
        "scope_top_100": (
            "SELECT key,time_ns FROM {} WHERE game_id=%s AND category_id=%s AND status=%s AND time_ns>=0 ORDER BY time_ns,key LIMIT 100",
            p["scope"],
        ),
        "json_filter": (
            "SELECT key FROM {} WHERE properties @> %s::jsonb ORDER BY key",
            [dumps({"platform_id": p["platform"]})],
        ),
        "player_runs": (
            "SELECT key FROM {} WHERE participant_keys @> %s::text[] ORDER BY key",
            [[p["player"]]],
        ),
        "monthly_counts": (
            "SELECT game_id,status,COALESCE(source_month,''),count(*) FROM {} GROUP BY 1,2,3 ORDER BY 1,2,3",
            [],
        ),
        "cumulative_counts": (
            "WITH m AS (SELECT game_id,status,source_month,count(*) AS n FROM {} WHERE source_month IS NOT NULL GROUP BY 1,2,3) SELECT game_id,status,source_month,n,sum(n) OVER(PARTITION BY game_id,status ORDER BY source_month) FROM m ORDER BY 1,2,3",
            [],
        ),
        "offset_page": (
            "SELECT key FROM {} ORDER BY key OFFSET %s LIMIT 100",
            [p["offset"]],
        ),
        "keyset_page": (
            "SELECT key FROM {} WHERE key>%s ORDER BY key LIMIT 100",
            [p["cursor"]],
        ),
    }
    if operation == "join_1000":
        return sql.SQL(
            "SELECT r.key,COALESCE((SELECT jsonb_agg(jsonb_build_array(p.key,p.name,p.area_id) ORDER BY p.key) FROM {} p WHERE p.key=ANY(r.participant_keys)),'[]'::jsonb) FROM {} r WHERE r.key=ANY(%s) ORDER BY r.key"
        ).format(table(h, "players"), table(h, "runs")), [p["join_ids"]]
    text, args = statements[operation]
    return sql.SQL(text).format(table(h, "runs")), args


def mongo_pipeline(operation, p):
    if operation == "scope_top_100":
        return [
            {
                "$match": dict(zip(("game_id", "category_id", "status"), p["scope"]))
                | {"time_ns": {"$gte": 0}}
            },
            {"$sort": {"time_ns": 1, "key": 1}},
            {"$limit": 100},
            {"$project": {"key": 1, "time_ns": 1}},
        ]
    if operation in ("monthly_counts", "cumulative_counts"):
        stages = (
            [{"$match": {"source_month": {"$ne": None}}}]
            if operation == "cumulative_counts"
            else []
        )
        stages += [
            {
                "$group": {
                    "_id": {
                        "g": "$game_id",
                        "s": "$status",
                        "m": {"$ifNull": ["$source_month", ""]},
                    },
                    "n": {"$sum": 1},
                }
            }
        ]
        if operation == "cumulative_counts":
            stages += [
                {
                    "$setWindowFields": {
                        "partitionBy": {"g": "$_id.g", "s": "$_id.s"},
                        "sortBy": {"_id.m": 1},
                        "output": {
                            "cumulative": {
                                "$sum": "$n",
                                "window": {"documents": ["unbounded", "current"]},
                            }
                        },
                    }
                }
            ]
        return stages + [{"$sort": {"_id.g": 1, "_id.s": 1, "_id.m": 1}}]
    if operation == "join_1000":
        return [
            {"$match": {"_id": {"$in": p["join_ids"]}}},
            {"$sort": {"_id": 1}},
            {
                "$lookup": {
                    "from": "players",
                    "localField": "participant_keys",
                    "foreignField": "_id",
                    "as": "profiles",
                }
            },
            {
                "$project": {
                    "key": 1,
                    "profiles._id": 1,
                    "profiles.name": 1,
                    "profiles.area_id": 1,
                }
            },
        ]
    return None


def s3_query(h, operation, p):
    """Выполняет те же девять запросов потоково, без внешнего SQL-движка.

    Все нужные блоки по-прежнему читаются клиентом. В памяти остаются только
    результаты, ключи/агрегаты или выбранные участники JOIN; raw_json всего
    корпуса одновременно не материализуется. Эквивалентность сверяет драйвер.
    """
    import heapq

    runs = iter_s3_rows(h, "runs")
    if operation == "batch_100":
        ids = set(p["ids"])
        return sorted([r["key"], r["raw_json"]] for r in runs if r["key"] in ids)
    if operation == "scope_top_100":
        selected = (
            (r["time_ns"], r["key"])
            for r in runs
            if [r[k] for k in ("game_id", "category_id", "status")] == p["scope"]
            and r["time_ns"] is not None
        )
        return [[key, elapsed] for elapsed, key in heapq.nsmallest(100, selected)]
    if operation == "json_filter":
        return sorted(
            r["key"] for r in runs if r["properties"]["platform_id"] == p["platform"]
        )
    if operation == "player_runs":
        return sorted(r["key"] for r in runs if p["player"] in r["participant_keys"])
    if operation in ("monthly_counts", "cumulative_counts"):
        counts = Counter(
            (r["game_id"], r["status"], r["source_month"] or "")
            for r in runs
            if operation == "monthly_counts" or r["source_month"] is not None
        )
        totals = Counter()
        result = []
        for (game, status, month), count in sorted(counts.items()):
            totals[game, status] += count
            result.append(
                [game, status, month, count]
                + ([totals[game, status]] if operation == "cumulative_counts" else [])
            )
        return result
    if operation == "join_1000":
        ids = set(p["join_ids"])
        selected = sorted(
            (r["key"], sorted(set(r["participant_keys"])))
            for r in runs
            if r["key"] in ids
        )
        needed = {
            participant for _, participants in selected for participant in participants
        }
        profiles = {
            r["key"]: [r["key"], r["name"], r["area_id"]]
            for r in iter_s3_rows(h, "players")
            if r["key"] in needed
        }
        return [
            [
                key,
                [
                    profiles[participant]
                    for participant in participants
                    if participant in profiles
                ],
            ]
            for key, participants in selected
        ]
    if operation == "offset_page":
        return sorted(r["key"] for r in runs)[p["offset"] : p["offset"] + 100]
    if operation == "keyset_page":
        return heapq.nsmallest(100, (r["key"] for r in runs if r["key"] > p["cursor"]))
    raise ValueError(operation)


def query(h, operation, p):
    if h["name"] == "garage":
        return s3_query(h, operation, p)
    if h["name"] == "postgres":
        statement, args = pg_statement(h, operation, p)
        values = [list(r) for r in h["conn"].execute(statement, args)]
        if operation in ("json_filter", "player_runs", "offset_page", "keyset_page"):
            return [r[0] for r in values]
        if operation == "cumulative_counts":
            for r in values:
                r[-1] = int(r[-1])
        return values
    collection = h["db"].runs
    pipeline = mongo_pipeline(operation, p)
    if pipeline:
        values = list(
            collection.aggregate(pipeline, maxTimeMS=120000, allowDiskUse=True)
        )
        if operation == "scope_top_100":
            return [[r["key"], r["time_ns"]] for r in values]
        if operation == "join_1000":
            return [
                [
                    r["key"],
                    sorted(
                        [[x["_id"], x["name"], x["area_id"]] for x in r["profiles"]]
                    ),
                ]
                for r in values
            ]
        return [
            [r["_id"]["g"], r["_id"]["s"], r["_id"]["m"], r["n"]]
            + ([r["cumulative"]] if operation == "cumulative_counts" else [])
            for r in values
        ]
    criteria = (
        {"_id": {"$in": p["ids"]}}
        if operation == "batch_100"
        else {"properties.platform_id": p["platform"]}
        if operation == "json_filter"
        else {"participant_keys": p["player"]}
        if operation == "player_runs"
        else {"_id": {"$gt": p["cursor"]}}
        if operation == "keyset_page"
        else {}
    )
    cursor = collection.find(
        criteria, {"raw_json": 1} if operation == "batch_100" else {"_id": 1}
    ).sort("_id", 1)
    if operation == "offset_page":
        cursor = cursor.skip(p["offset"]).limit(100)
    if operation == "keyset_page":
        cursor = cursor.limit(100)
    return (
        [[r["_id"], r["raw_json"]] for r in cursor]
        if operation == "batch_100"
        else [r["_id"] for r in cursor]
    )


def size_info(h):
    if h["name"] == "postgres":
        data = index = 0
        for d in DATA:
            relation = f'"{h["namespace"]}"."{d}"'
            a, b = (
                h["conn"]
                .execute(
                    "SELECT pg_table_size(%s::regclass),pg_indexes_size(%s::regclass)",
                    [relation, relation],
                )
                .fetchone()
            )
            data += a
            index += b
        return {
            "data_bytes": data,
            "index_bytes": index,
            "reported_bytes": data + index,
        }
    if h["name"] == "mongo":
        h["client"].admin.command({"fsync": 1})
        d = h["db"].command("dbStats")
        return {
            "data_bytes": d["dataSize"],
            "index_bytes": d["indexSize"],
            "reported_bytes": d["storageSize"] + d["indexSize"],
        }
    values = list(
        h["s3"]
        .client.get_paginator("list_objects_v2")
        .paginate(Bucket=h["s3"].bucket, Prefix=h["namespace"] + "/workloads/base/")
    )
    size = sum(o["Size"] for page in values for o in page.get("Contents", []))
    return {"data_bytes": size, "reported_bytes": size}


def plans(h, p):
    result = {}
    for operation in ("scope_top_100", "json_filter", "player_runs", "join_1000"):
        if h["name"] == "postgres":
            from psycopg import sql

            statement, args = pg_statement(h, operation, p)
            result[operation] = (
                h["conn"]
                .execute(
                    sql.SQL("EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) ") + statement, args
                )
                .fetchone()[0]
            )
        elif h["name"] == "mongo":
            pipeline = mongo_pipeline(operation, p)
            command = (
                {"aggregate": "runs", "pipeline": pipeline, "cursor": {}}
                if pipeline
                else {
                    "find": "runs",
                    "filter": {"properties.platform_id": p["platform"]}
                    if operation == "json_filter"
                    else {"participant_keys": p["player"]},
                    "sort": {"_id": 1},
                    "projection": {"_id": 1},
                }
            )
            result[operation] = h["db"].command(
                {"explain": command, "verbosity": "executionStats"}
            )
    return result


def clone_runs(h):
    if h["name"] == "postgres":
        from psycopg import sql

        h["conn"].execute(
            sql.SQL("CREATE TABLE {} AS TABLE {}").format(
                table(h, "clone_a"), table(h, "runs")
            )
        )
        h["conn"].commit()
    elif h["name"] == "mongo":
        list(h["db"].runs.aggregate([{"$out": "clone_a"}], maxTimeMS=120000))
    else:
        chunks = []
        for i, c in enumerate(h["index"]["chunks"]):
            if c["dataset"] != "runs":
                continue
            key = f"{h['namespace']}/workloads/clones/a/{i}.jsonl.gz"
            h["s3"].client.copy_object(
                Bucket=h["s3"].bucket,
                Key=key,
                CopySource={"Bucket": h["s3"].bucket, "Key": c["key"]},
            )
            h["io"]["copy_calls"] += 1
            h["io"]["copied_bytes"] += c["bytes"]
            chunks.append({**c, "key": key})
        h["clone_index"] = {"chunks": chunks}
        s3_put(
            h,
            h["namespace"] + "/workloads/clones/a/index.json",
            dumps(h["clone_index"]).encode(),
        )


def alias_snapshot(h, suffix="readonly"):
    """A read-only snapshot alias reuses immutable chunks; not a mutable table copy."""
    if h["name"] != "garage" or not re.fullmatch(r"[a-z0-9_]+", suffix):
        raise ValueError("Garage alias only")
    key = h["namespace"] + "/workloads/aliases/" + suffix + ".json"
    s3_put(h, key, dumps(h["index"]).encode())
    return key


def rename_clone(h):
    if h["name"] == "postgres":
        from psycopg import sql

        h["conn"].execute(
            sql.SQL("ALTER TABLE {} RENAME TO clone_b").format(table(h, "clone_a"))
        )
        h["conn"].commit()
    elif h["name"] == "mongo":
        h["db"].clone_a.rename("clone_b")
    else:
        previous = h["clone_index"]
        chunks = []
        for c in previous["chunks"]:
            key = c["key"].replace("/clones/a/", "/clones/b/")
            h["s3"].client.copy_object(
                Bucket=h["s3"].bucket,
                Key=key,
                CopySource={"Bucket": h["s3"].bucket, "Key": c["key"]},
            )
            h["io"]["copy_calls"] += 1
            h["io"]["copied_bytes"] += c["bytes"]
            chunks.append({**c, "key": key})
        h["clone_index"] = {"chunks": chunks}
        s3_put(
            h,
            h["namespace"] + "/workloads/clones/b/index.json",
            dumps(h["clone_index"]).encode(),
        )
        # Only redundant copies made by clone_runs in this exact scratch prefix.
        keys = [c["key"] for c in previous["chunks"]] + [
            h["namespace"] + "/workloads/clones/a/index.json"
        ]
        assert all(
            k.startswith(scratch(h["namespace"]) + "/workloads/clones/a/") for k in keys
        )
        for pos in range(0, len(keys), 1000):
            h["s3"].client.delete_objects(
                Bucket=h["s3"].bucket,
                Delete={"Objects": [{"Key": k} for k in keys[pos : pos + 1000]]},
            )
            h["io"]["delete_calls"] += 1


def verify_clone(h, expected):
    if h["name"] == "garage":
        original = h["index"]
        h["index"] = h["clone_index"]
        try:
            actual = all_rows(h, "runs")
        finally:
            h["index"] = original
    else:
        actual = all_rows(h, "runs", relation="clone_b")
    if digest(actual) != expected:
        raise ValueError("Clone/rename changed data")


def cache_counts(h, p):
    if h["name"] == "postgres":
        from psycopg import sql

        statement, args = pg_statement(h, "monthly_counts", p)
        h["conn"].execute(
            sql.SQL(
                "CREATE MATERIALIZED VIEW {} (game_id,status,source_month,n) AS "
            ).format(table(h, "cached_counts"))
            + statement,
            args,
        )
        h["conn"].commit()
    elif h["name"] == "mongo":
        list(
            h["db"].runs.aggregate(
                mongo_pipeline("monthly_counts", p) + [{"$out": "cached_counts"}],
                maxTimeMS=120000,
            )
        )
    else:
        s3_put(
            h,
            h["namespace"] + "/workloads/cached_counts.json",
            dumps(query(h, "monthly_counts", p)).encode(),
        )


def cached_counts(h):
    if h["name"] == "postgres":
        from psycopg import sql

        return [
            list(r)
            for r in h["conn"].execute(
                sql.SQL("SELECT * FROM {} ORDER BY 1,2,3").format(
                    table(h, "cached_counts")
                )
            )
        ]
    if h["name"] == "mongo":
        return [
            [r["_id"]["g"], r["_id"]["s"], r["_id"]["m"], r["n"]]
            for r in h["db"]
            .cached_counts.find()
            .sort([("_id.g", 1), ("_id.s", 1), ("_id.m", 1)])
        ]
    return loads(s3_get(h, h["namespace"] + "/workloads/cached_counts.json"))


def backfill(h, ids):
    if h["name"] == "postgres":
        from psycopg import sql

        h["conn"].execute(
            sql.SQL("UPDATE {} SET scratch_tag='reviewed' WHERE key=ANY(%s)").format(
                table(h, "runs")
            ),
            [ids],
        )
        h["conn"].commit()
    elif h["name"] == "mongo":
        h["db"].runs.update_many(
            {"_id": {"$in": ids}}, {"$set": {"scratch_tag": "reviewed"}}
        )
    else:
        chosen = set(ids)
        chunks = []
        for c in h["index"]["chunks"]:
            if c["dataset"] != "runs":
                chunks.append(c)
                continue
            raw = s3_get(h, c["key"])
            assert hashlib.sha256(raw).hexdigest() == c["sha256"]
            rows = [loads(line) for line in gzip.decompress(raw).splitlines()]
            changed = False
            for r in rows:
                if r["key"] in chosen:
                    r["scratch_tag"] = "reviewed"
                    changed = True
            if changed:
                body = gzip.compress(
                    b"".join(dumps(r).encode() + b"\n" for r in rows), mtime=0
                )
                checksum = hashlib.sha256(body).hexdigest()
                key = f"{h['namespace']}/workloads/mutations/{checksum}.jsonl.gz"
                s3_put(h, key, body)
                chunks.append({**c, "key": key, "sha256": checksum, "bytes": len(body)})
            else:
                chunks.append(c)
        h["index"] = {"chunks": chunks}
        s3_put(
            h,
            h["namespace"] + "/workloads/mutations/after_backfill.json",
            dumps(h["index"]).encode(),
        )


def verify_backfill(h, ids, expected):
    if h["name"] == "postgres":
        from psycopg import sql

        keys = {
            r[0]
            for r in h["conn"].execute(
                sql.SQL("SELECT key FROM {} WHERE scratch_tag='reviewed'").format(
                    table(h, "runs")
                )
            )
        }
        rows = all_rows(h, "runs")
    else:
        rows = all_rows(h, "runs")
        keys = {r["key"] for r in rows if r.pop("scratch_tag", None) == "reviewed"}
    if keys != set(ids) or digest(rows) != expected:
        raise ValueError("Backfill changed other fields/records")


def append_rows(h, rows):
    if h["name"] == "postgres":
        from psycopg import sql
        from psycopg.types.json import Jsonb

        statement = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            table(h, "runs"),
            sql.SQL(",").join(map(sql.Identifier, RUN_COLUMNS)),
            sql.SQL(",").join(sql.Placeholder() for _ in RUN_COLUMNS),
        )
        with h["conn"].cursor() as c:
            c.executemany(
                statement,
                [
                    [
                        Jsonb(r[k], dumps=dumps)
                        if k in ("properties", "provenance")
                        else r[k]
                        for k in RUN_COLUMNS
                    ]
                    for r in rows
                ],
            )
        h["conn"].commit()
    elif h["name"] == "mongo":
        h["db"].runs.insert_many([{"_id": r["key"], **r} for r in rows])
    else:
        chunks = []
        for pos, batch in enumerate(batches(rows)):
            raw = gzip.compress(
                b"".join(raw.encode() + b"\n" for r, raw in batch), mtime=0
            )
            checksum = hashlib.sha256(raw).hexdigest()
            key = f"{h['namespace']}/workloads/append/{checksum}.jsonl.gz"
            s3_put(h, key, raw)
            chunks.append(
                {
                    "dataset": "runs",
                    "key": key,
                    "sha256": checksum,
                    "count": len(batch),
                    "bytes": len(raw),
                }
            )
        h["appended_chunks"] = chunks
        h["index"] = {"chunks": h["index"]["chunks"] + chunks}
        s3_put(
            h,
            h["namespace"] + "/workloads/mutations/after_append.json",
            dumps(h["index"]).encode(),
        )


def delete_appended(h, ids):
    if not all(k.startswith("benchmark-new:") for k in ids):
        raise ValueError("Only newly inserted benchmark records may be removed")
    if h["name"] == "postgres":
        from psycopg import sql

        h["conn"].execute(
            sql.SQL("DELETE FROM {} WHERE key=ANY(%s)").format(table(h, "runs")), [ids]
        )
        h["conn"].commit()
    elif h["name"] == "mongo":
        h["db"].runs.delete_many({"_id": {"$in": ids}})
    else:
        keys = {c["key"] for c in h["appended_chunks"]}
        h["index"] = {
            "chunks": [c for c in h["index"]["chunks"] if c["key"] not in keys]
        }
        # Logical removal; previous manifest/chunks stay available as an archive.
        s3_put(
            h,
            h["namespace"] + "/workloads/mutations/after_delete.json",
            dumps(h["index"]).encode(),
        )


def capabilities(h, game_id):
    """Deliberate invalid writes/failures only in this newly created probe dataset."""
    results = {}
    name = h["name"]
    if name == "postgres":
        from psycopg import sql, IntegrityError

        c = h["conn"]
        c.commit()
        c.execute(
            sql.SQL(
                "CREATE TABLE {} (key text PRIMARY KEY,ref text REFERENCES {}(key),counter int CHECK(counter>=0))"
            ).format(table(h, "probe"), table(h, "games"))
        )
        c.execute(
            sql.SQL("INSERT INTO {} VALUES (%s,%s,0),(%s,%s,0)").format(
                table(h, "probe")
            ),
            ["a", game_id, "b", game_id],
        )
        c.commit()
        for label, values in [
            ("duplicate_key", ["a", game_id, 0]),
            ("foreign_key", ["bad", "missing-game", 0]),
            ("check_constraint", ["bad", game_id, -1]),
        ]:
            try:
                c.execute(
                    sql.SQL("INSERT INTO {} VALUES (%s,%s,%s)").format(
                        table(h, "probe")
                    ),
                    values,
                )
                c.commit()
                results[label] = "accepted"
            except IntegrityError as error:
                c.rollback()
                results[label] = {"rejected": True, "code": error.sqlstate}
        try:
            with c.transaction():
                c.execute(
                    sql.SQL("UPDATE {} SET counter=1 WHERE key='a'").format(
                        table(h, "probe")
                    )
                )
                raise RuntimeError("Injected failure before second write")
        except RuntimeError:
            pass
        results["multi_record_rollback"] = [
            list(r)
            for r in c.execute(
                sql.SQL("SELECT key,counter FROM {} ORDER BY key").format(
                    table(h, "probe")
                )
            )
        ]
        c.commit()
        results["schema_optional_field"] = (
            "ALTER TABLE ADD COLUMN; benchmarked separately"
        )
    elif name == "mongo":
        from pymongo.errors import PyMongoError

        db = h["db"]
        db.create_collection(
            "probe",
            validator={
                "$jsonSchema": {
                    "bsonType": "object",
                    "required": ["ref", "counter"],
                    "properties": {
                        "ref": {"bsonType": "string"},
                        "counter": {"bsonType": "int", "minimum": 0},
                    },
                }
            },
        )
        db.probe.insert_many(
            [
                {"_id": "a", "ref": game_id, "counter": 0},
                {"_id": "b", "ref": game_id, "counter": 0},
            ]
        )
        for label, row in [
            ("duplicate_key", {"_id": "a", "ref": game_id, "counter": 0}),
            ("foreign_key", {"_id": "bad", "ref": "missing-game", "counter": 0}),
            ("check_constraint", {"_id": "invalid", "ref": game_id, "counter": -1}),
        ]:
            try:
                db.probe.insert_one(row)
                results[label] = "accepted"
            except PyMongoError as error:
                results[label] = {
                    "rejected": True,
                    "code": getattr(error, "code", None),
                }
        try:
            with h["client"].start_session() as session:
                with session.start_transaction():
                    db.probe.update_one(
                        {"_id": "a"}, {"$inc": {"counter": 1}}, session=session
                    )
            results["multi_record_transaction"] = "supported"
        except PyMongoError as error:
            results["multi_record_transaction"] = {
                "supported": False,
                "code": getattr(error, "code", None),
            }
        db.probe.update_one(
            {"_id": "a"}, {"$set": {"counter": 1}}
        )  # Injected stop before b.
        results["nontransactional_partial_write"] = [
            [r["_id"], r["counter"]]
            for r in db.probe.find({"_id": {"$in": ["a", "b"]}}).sort("_id", 1)
        ]
        results["schema_optional_field"] = (
            "No DDL needed; validator can enforce fields, not cross-collection FK"
        )
    else:
        from botocore.exceptions import ClientError

        base = h["namespace"] + "/workloads/probe/"
        for key in ("a", "b"):
            s3_put(h, base + key, b'{"counter":0}')
        first = h["s3"].client.head_object(Bucket=h["s3"].bucket, Key=base + "a")[
            "ETag"
        ]
        s3_put(h, base + "a", b'{"counter":1}')  # Injected stop before b.
        changed = h["s3"].client.head_object(Bucket=h["s3"].bucket, Key=base + "a")[
            "ETag"
        ]
        assert first != changed
        results["multi_object_partial_write"] = [
            loads(s3_get(h, base + k)) for k in ("a", "b")
        ]
        sent = []

        def capture(request, **kwargs):
            values = {}
            for field in ("If-Match", "If-None-Match"):
                value = request.headers.get(field)
                if value is not None:
                    values[field] = (
                        value.decode("ascii") if isinstance(value, bytes) else value
                    )
            if values:
                sent.append(values)

        h["s3"].client.meta.events.register("before-sign.s3.PutObject", capture)
        try:
            response = h["s3"].client.put_object(
                Bucket=h["s3"].bucket,
                Key=base + "a",
                Body=b'{"counter":2}',
                IfMatch=first,
            )
            results["stale_etag_write"] = {
                "rejected": False,
                "status": response["ResponseMetadata"]["HTTPStatusCode"],
            }
        except ClientError as error:
            results["stale_etag_write"] = {
                "rejected": True,
                "status": error.response["ResponseMetadata"]["HTTPStatusCode"],
                "code": error.response["Error"]["Code"],
            }
        results["stale_etag_write"].update(
            initial_etag=first,
            current_etag_before_write=changed,
            body_after_write=loads(s3_get(h, base + "a")),
        )
        try:
            response = h["s3"].client.put_object(
                Bucket=h["s3"].bucket,
                Key=base + "b",
                Body=b'{"counter":9}',
                IfNoneMatch="*",
            )
            results["create_only_existing_object"] = {
                "rejected": False,
                "status": response["ResponseMetadata"]["HTTPStatusCode"],
            }
        except ClientError as error:
            results["create_only_existing_object"] = {
                "rejected": True,
                "status": error.response["ResponseMetadata"]["HTTPStatusCode"],
                "code": error.response["Error"]["Code"],
            }
        results["create_only_existing_object"]["body_after_write"] = loads(
            s3_get(h, base + "b")
        )
        results["actual_put_precondition_headers"] = sent
        s3_put(h, base + "schema-invalid", b'{"ref":"missing-game","counter":-1}')
        results["schema_invalid_object_accepted"] = loads(
            s3_get(h, base + "schema-invalid")
        )
        results["schema_optional_field"] = "Client encoding; no server table schema"
    return results


def concurrent_reads(name, namespace, p, expected):
    from concurrent.futures import ThreadPoolExecutor
    from contextlib import ExitStack
    import math
    import statistics

    with ExitStack() as stack:
        handles = [
            stack.enter_context(connect(name, namespace, existing=True))
            for _ in range(4)
        ]
        for h in handles:
            if digest(query(h, "batch_100", p)) != expected:
                raise ValueError("Concurrent warmup differs")
            h["io"] = Counter()

        def work(h):
            values = []
            latencies = []
            for _ in range(5):
                started = time.perf_counter()
                values.append(query(h, "batch_100", p))
                latencies.append(time.perf_counter() - started)
            return values, latencies

        with ThreadPoolExecutor(max_workers=4) as pool:
            started = time.perf_counter()
            futures = [pool.submit(work, h) for h in handles]
            results = [f.result() for f in futures]
            wall = time.perf_counter() - started
        for values, _ in results:
            if any(digest(v) != expected for v in values):
                raise ValueError("Concurrent read differs")
        latencies = [t for _, times in results for t in times]
        return {
            "workers": 4,
            "calls": 20,
            "wall_seconds": wall,
            "latency_seconds": latencies,
            "p50_seconds": statistics.median(latencies),
            "p95_seconds": sorted(latencies)[math.ceil(0.95 * len(latencies)) - 1],
            "calls_per_second": 20 / wall,
            "io": dict(sum((h["io"] for h in handles), Counter())),
            "note": "Small 20-call workload, not a saturation or production tail-latency guarantee; connections/index and warmup excluded.",
        }


def production_signature(state):
    """Read-only counters/inventory guard, not a full production content audit."""
    from psycopg import sql

    with pg_connect() as c:
        postgres = {
            d: c.execute(
                sql.SQL("SELECT count(*) FROM {}").format(
                    sql.Identifier("entity_v2", d)
                )
            ).fetchone()[0]
            for d in DATASETS
        }
        postgres_v3 = {
            d: (
                c.execute(
                    sql.SQL("SELECT count(*) FROM {}").format(
                        sql.Identifier("entity_v3", d)
                    )
                ).fetchone()[0]
                if c.execute("SELECT to_regclass(%s)", ["entity_v3." + d]).fetchone()[0]
                else None
            )
            for d in DATASETS
        }
    with Mongo() as store:
        mongo = {d: store.client.entity_v2[d].count_documents({}) for d in DATASETS}
        mongo_v3 = {d: store.client.entity_v3[d].count_documents({}) for d in DATASETS}
    with S3() as store:
        objects = sorted(
            [o["Key"], o["Size"], o["ETag"]]
            for prefix in ("entity_v2/entity-v2/", "entity_v3/entity-v3/")
            for page in store.client.get_paginator("list_objects_v2").paginate(
                Bucket=store.bucket, Prefix=prefix
            )
            for o in page.get("Contents", [])
        )
    return {
        "postgres_counts": postgres,
        "mongo_counts": mongo,
        "postgres_v3_counts": postgres_v3,
        "mongo_v3_counts": mongo_v3,
        "s3_object_inventory_digest": digest(objects),
        "s3_objects": len(objects),
        "source_budgets": {
            p.parent.name: read(p)["used"]
            for p in (state / "collections").glob("*/budget.json")
        },
    }


def pg_copy(h, package, *, merge=False):
    from psycopg import sql
    from psycopg.types.json import Jsonb

    for dataset in DATA:
        columns = (
            RUN_COLUMNS
            if dataset == "runs"
            else PLAYER_COLUMNS
            if dataset == "players"
            else ("key", "name", "raw_json")
        )
        target = table(h, "stage_" + dataset) if merge else table(h, dataset)
        if merge:
            h["conn"].execute(
                sql.SQL("CREATE TABLE {} (LIKE {})").format(target, table(h, dataset))
            )
        statement = sql.SQL("COPY {} ({}) FROM STDIN").format(
            target, sql.SQL(",").join(map(sql.Identifier, columns))
        )
        with h["conn"].cursor() as cursor, cursor.copy(statement) as copy:
            for row in package.records(dataset):
                copy.write_row(
                    [
                        Jsonb(row[k], dumps=dumps)
                        if k in ("properties", "provenance")
                        else row[k]
                        for k in columns
                    ]
                )
        if merge:
            h["conn"].execute(
                sql.SQL(
                    "INSERT INTO {} SELECT * FROM {} ON CONFLICT(key) DO NOTHING"
                ).format(table(h, dataset), target)
            )
            h["conn"].execute(sql.SQL("DROP TABLE {}").format(target))
    h["conn"].commit()


def bulk_trials(state, input_root):
    input_root = input_root.resolve()
    if not input_root.is_relative_to((state / "benchmarks").resolve()):
        raise ValueError("Use an existing workload input inside ETL_STATE/benchmarks")
    metadata = read(input_root / "input.json")
    if metadata["format_version"] != "lab1-workload-v1":
        raise ValueError("Expected scratch workload input")
    if collections_busy(state):
        raise RuntimeError("Finish other collection work before measuring")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    root = state / "benchmarks" / ("pg_bulk_" + stamp)
    root.mkdir()
    print("Bulk experiment:", root, flush=True)
    with file_lock(state / "source.lock"):
        if collections_busy(state):
            raise RuntimeError("Collection started during preflight")
        before = production_signature(state)
        package = EntityPackage(input_root)
        expected = {d: digest(list(package.records(d))) for d in DATA}
        rows = sum(metadata["counts"].values())
        fields = [
            "repeat",
            "method",
            "operation",
            "seconds",
            "records",
            "namespace",
            "input_digest",
        ]
        with (root / "measurements.csv").open("w", newline="") as output:
            writer = csv.DictWriter(output, fieldnames=fields)
            writer.writeheader()
            for repeat in range(3):
                for method in (
                    ("executemany", "copy")
                    if repeat % 2 == 0
                    else ("copy", "executemany")
                ):
                    check_capacity(state)
                    ns = f"test_wl_copy_{stamp}_{repeat}_{method}"
                    with connect("postgres", ns) as h:
                        setup(h)
                        for operation in ("initial_load", "idempotent_copy_merge"):
                            started = time.perf_counter()
                            if operation == "idempotent_copy_merge":
                                pg_copy(h, package, merge=True)
                            elif method == "copy":
                                pg_copy(h, package)
                            else:
                                load_native(h, package)
                            seconds = time.perf_counter() - started
                            for dataset in DATA:
                                if digest(all_rows(h, dataset)) != expected[dataset]:
                                    raise ValueError("Bulk COPY/replay changed data")
                            writer.writerow(
                                {
                                    "repeat": repeat + 1,
                                    "method": method,
                                    "operation": operation,
                                    "seconds": seconds,
                                    "records": rows,
                                    "namespace": ns,
                                    "input_digest": metadata["digest"],
                                }
                            )
                            output.flush()
                            print(
                                repeat + 1,
                                method,
                                operation,
                                round(seconds, 4),
                                flush=True,
                            )
        package.db.close()
        after = production_signature(state)
        if before != after:
            raise ValueError("Production counters/inventory or source budgets changed")
        atomic_write(
            root / "completed.json",
            {
                "at": utcnow(),
                "input": metadata,
                "repeats": 3,
                "source_HTTP": 0,
                "production_counters_and_S3_inventory_unchanged": True,
                "timing": "Acknowledged write/commit; complete content readback excluded and verified after every operation.",
            },
        )
    return root


def main():
    parser = argparse.ArgumentParser(
        description="Fixed Lab 1 query/DDL experiments in test_wl_*; never changes production ODS."
    )
    inputs = parser.add_mutually_exclusive_group(required=True)
    inputs.add_argument("--collections", nargs="+")
    inputs.add_argument(
        "--pg-bulk-input",
        type=Path,
        help="Additional COPY/INSERT/replay trials on a completed workload input",
    )
    parser.add_argument("--sizes", nargs="+", type=int, default=[1000, 10000, 100000])
    parser.add_argument("--log", action="store_true")
    args = parser.parse_args()
    if any(
        not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", c) for c in args.collections or []
    ) or any(not 100 <= n <= 200000 for n in args.sizes):
        parser.error("Invalid collections/sizes")
    state = Path(os.environ["ETL_STATE"])
    check_capacity(state)
    if args.pg_bulk_input:
        bulk_trials(state, args.pg_bulk_input)
        return
    if collections_busy(state):
        raise RuntimeError("Finish collection drivers/DAGs before measuring")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    root = state / "benchmarks" / ("workloads_" + stamp)
    root.mkdir(parents=True, exist_ok=False)
    atomic_write(
        root / "config.json",
        {
            "collections": args.collections,
            "sizes": sorted(set(args.sizes)),
            "repeats": 3,
            "format_version": "lab1-workload-v1",
            "s3_read_strategy": "streaming_query",
            "source_HTTP": 0,
        },
    )
    atomic_write(root / "status.json", {"phase": "preparing", "at": utcnow()})
    print("Workload experiment:", root, flush=True)
    if args.log:
        import sys

        sys.stdout = sys.stderr = (root / "driver.log").open("a", buffering=1)
    try:
        with file_lock(state / "source.lock"):
            if collections_busy(state):
                raise RuntimeError("Collection started during preflight")
            before = production_signature(state)
            atomic_write(root / "production_before.json", before)
            inputs = prepare(state, args.collections, root, sorted(set(args.sizes)))
            fields = [
                "size",
                "repeat",
                "store",
                "namespace",
                "phase",
                "operation",
                "pass",
                "seconds",
                "count",
                "digest",
                "get_calls",
                "read_bytes",
                "put_calls",
                "written_bytes",
                "copy_calls",
                "copied_bytes",
                "delete_calls",
                "data_bytes",
                "index_bytes",
                "reported_bytes",
            ]
            with (root / "measurements.csv").open("w", newline="") as output:
                writer = csv.DictWriter(output, fieldnames=fields)
                writer.writeheader()
                for input_root in inputs:
                    package = EntityPackage(input_root)
                    metadata = read(input_root / "input.json")
                    size = metadata["size"]
                    runs = list(package.records("runs"))
                    profiles = {r["key"]: r for r in package.records("players")}
                    p = parameters(runs)
                    expected = {
                        op: digest(local_query(runs, profiles, op, p)) for op in OPS
                    }
                    original = {d: digest(list(package.records(d))) for d in DATA}
                    atomic_write(input_root / "parameters.json", p)
                    for repeat in range(3):
                        order = ["postgres", "mongo", "garage"]
                        order = order[repeat:] + order[:repeat]
                        ns = f"test_wl_{stamp}_{size}_{repeat}"
                        for name in order:
                            check_capacity(state)
                            if collections_busy(state):
                                raise RuntimeError(
                                    "Collection started during experiment"
                                )
                            with connect(name, ns) as h:
                                setup(h)
                                base = {
                                    "size": size,
                                    "repeat": repeat + 1,
                                    "store": name,
                                    "namespace": ns,
                                }

                                def measure(
                                    operation,
                                    fn,
                                    *,
                                    phase="table_ops",
                                    pass_no=0,
                                    checksum=None,
                                ):
                                    check_capacity(state)
                                    h["io"] = Counter()
                                    started = time.perf_counter()
                                    value = fn()
                                    elapsed = time.perf_counter() - started
                                    actual = digest(value) if value is not None else ""
                                    if checksum is not None and actual != checksum:
                                        raise ValueError(
                                            f"{name} {operation}: result differs"
                                        )
                                    writer.writerow(
                                        {
                                            **base,
                                            "phase": phase,
                                            "operation": operation,
                                            "pass": pass_no,
                                            "seconds": elapsed,
                                            "count": len(value)
                                            if isinstance(value, list)
                                            else "",
                                            "digest": actual,
                                            **h["io"],
                                        }
                                    )
                                    output.flush()
                                    atomic_write(
                                        root / "status.json",
                                        {
                                            "phase": "running",
                                            **base,
                                            "operation": operation,
                                            "at": utcnow(),
                                        },
                                    )
                                    print(
                                        size,
                                        repeat + 1,
                                        name,
                                        phase,
                                        operation,
                                        round(elapsed, 4),
                                        flush=True,
                                    )
                                    return value

                                measure(
                                    "load_native",
                                    lambda: load_native(h, package),
                                    phase="setup",
                                )
                                for dataset in DATA:
                                    if (
                                        digest(all_rows(h, dataset))
                                        != original[dataset]
                                    ):
                                        raise ValueError("Native load changed input")
                                analyze(h)
                                for phase in ("pk_only", "indexed"):
                                    if phase == "indexed":
                                        measure(
                                            "build_indexes",
                                            lambda: indexes(h),
                                            phase="setup",
                                        )
                                        analyze(h)
                                    writer.writerow(
                                        {
                                            **base,
                                            "phase": phase,
                                            "operation": "size",
                                            **size_info(h),
                                        }
                                    )
                                    output.flush()
                                    for pass_no in range(1, 4):
                                        for op in OPS:
                                            measure(
                                                op,
                                                lambda op=op: query(h, op, p),
                                                phase=phase,
                                                pass_no=pass_no,
                                                checksum=expected[op],
                                            )
                                    if name != "garage":
                                        atomic_write(
                                            root
                                            / f"plans_{size}_{repeat}_{name}_{phase}.json",
                                            plans(h, p),
                                        )
                                concurrent = concurrent_reads(
                                    name, ns, p, expected["batch_100"]
                                )
                                atomic_write(
                                    root / f"concurrent_{size}_{repeat}_{name}.json",
                                    concurrent,
                                )
                                writer.writerow(
                                    {
                                        **base,
                                        "phase": "indexed",
                                        "operation": "concurrent_4x5",
                                        "seconds": concurrent["wall_seconds"],
                                        "count": concurrent["calls"],
                                        **concurrent["io"],
                                    }
                                )
                                output.flush()
                                if name == "garage":
                                    alias = measure(
                                        "readonly_manifest_alias",
                                        lambda: alias_snapshot(h),
                                    )
                                    if loads(s3_get(h, alias)) != h["index"]:
                                        raise ValueError(
                                            "Read-only snapshot alias differs"
                                        )
                                measure("clone_table", lambda: clone_runs(h))
                                measure("rename_clone", lambda: rename_clone(h))
                                verify_clone(h, original["runs"])
                                measure("cache_build", lambda: cache_counts(h, p))
                                for n in range(1, 4):
                                    measure(
                                        "cached_counts",
                                        lambda: cached_counts(h),
                                        pass_no=n,
                                        checksum=expected["monthly_counts"],
                                    )
                                if name == "postgres":
                                    from psycopg import sql

                                    def add_column():
                                        h["conn"].execute(
                                            sql.SQL(
                                                "ALTER TABLE {} ADD COLUMN scratch_tag text"
                                            ).format(table(h, "runs"))
                                        )
                                        h["conn"].commit()

                                    measure("add_optional_column", add_column)
                                measure(
                                    "update_1000", lambda: backfill(h, p["update_ids"])
                                )
                                verify_backfill(h, p["update_ids"], original["runs"])
                                added = [
                                    {**r, "key": "benchmark-new:" + r["key"]}
                                    for r in runs[:100]
                                ]
                                measure("append_100", lambda: append_rows(h, added))
                                after = all_rows(h, "runs")
                                assert len(after) == size + len(added)
                                assert {
                                    r["key"]: r["raw_json"]
                                    for r in after
                                    if r["key"].startswith("benchmark-new:")
                                } == {r["key"]: r["raw_json"] for r in added}
                                measure(
                                    "delete_100",
                                    lambda: delete_appended(
                                        h, [r["key"] for r in added]
                                    ),
                                )
                                verify_backfill(h, p["update_ids"], original["runs"])
                                atomic_write(
                                    root / f"capabilities_{size}_{repeat}_{name}.json",
                                    capabilities(h, runs[0]["game_id"]),
                                )
                    package.db.close()
                    atomic_write(
                        root / f"completed_{size}.json",
                        {
                            "input": metadata,
                            "query_digests": expected,
                            "repeats": 3,
                            "source_HTTP": 0,
                        },
                    )
            after = production_signature(state)
            atomic_write(root / "production_after.json", after)
            if before != after:
                raise ValueError(
                    "Production counters/inventory or source HTTP budgets changed during experiment"
                )
            atomic_write(
                root / "completed.json",
                {
                    "at": utcnow(),
                    "source_HTTP": 0,
                    "production_counters_and_S3_inventory_unchanged": True,
                    "sizes": sorted(set(args.sizes)),
                    "repeats": 3,
                    "queries_per_phase": list(OPS),
                    "namespaces_retained": True,
                    "format": "lab1-workload-v1; mutable scratch projections, not production ODS or DDS",
                },
            )
            atomic_write(root / "status.json", {"phase": "finished", "at": utcnow()})
    except Exception as error:
        atomic_write(
            root / "status.json",
            {
                "phase": "failed",
                "type": type(error).__name__,
                "detail": str(error)[:300],
                "at": utcnow(),
            },
        )
        raise
    print("Workload experiment complete:", root, flush=True)


if __name__ == "__main__":
    main()
