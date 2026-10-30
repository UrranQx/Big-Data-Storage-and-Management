"""Сравнивает полные аналитические конвейеры на шести связанных наборах.

Работает только в test_wl_an_* изолированного стенда. Никаких source HTTP,
production DDS или официального рейтинга. SQL/Mongo считают сервером,
Garage использует потоковое чтение и Python; SQLite — независимый эталон.
"""

import argparse
from collections import Counter, defaultdict
import csv
import gzip
import hashlib
import heapq
from itertools import chain
import os
from pathlib import Path
import sqlite3
import threading
import time

from .analytics_input import prepare, rows
from .jsonutil import atomic_write, dumps, loads, read, utcnow
from .storage_workloads import connect, table, s3_get, s3_put


SCHEMA = {
    "games": [("key", "text"), ("name", "text")],
    "categories": [
        ("key", "text"),
        ("game_id", "text"),
        ("source_id", "text"),
        ("name", "text"),
    ],
    "boards": [("key", "text"), ("category_key", "text"), ("scope_json", "text")],
    "players": [
        ("key", "text"),
        ("name", "text"),
        ("area", "text"),
        ("resolved", "boolean"),
    ],
    "runs": [
        ("key", "text"),
        ("board_id", "text"),
        ("status", "bigint"),
        ("month", "text"),
        ("time_ns", "bigint"),
        ("eligible", "boolean"),
    ],
    "run_players": [("key", "text"), ("run_id", "text"), ("player_id", "text")],
    "activity": [
        ("key", "text"),
        ("game_id", "text"),
        ("category_key", "text"),
        ("status", "bigint"),
        ("month", "text"),
        ("n_runs", "bigint"),
        ("n_players", "bigint"),
    ],
    "geography": [
        ("key", "text"),
        ("game_id", "text"),
        ("game_name", "text"),
        ("area", "text"),
        ("n_runs", "bigint"),
        ("n_players", "bigint"),
    ],
    "best": [
        ("key", "text"),
        ("board_id", "text"),
        ("player_id", "text"),
        ("time_ns", "bigint"),
    ],
    "ranked": [
        ("key", "text"),
        ("board_id", "text"),
        ("player_id", "text"),
        ("time_ns", "bigint"),
        ("place", "bigint"),
        ("seq", "bigint"),
        ("n_players", "bigint"),
    ],
    "features": [
        ("key", "text"),
        ("n_players", "bigint"),
        ("best_ns", "bigint"),
        ("median_ns", "bigint"),
        ("top_ns", "bigint"),
        ("weight", "bigint"),
    ],
    "scores": [("key", "text"), ("score", "bigint"), ("boards", "bigint")],
    "rating": [
        ("key", "text"),
        ("score", "bigint"),
        ("boards", "bigint"),
        ("place", "bigint"),
        ("name", "text"),
        ("area", "text"),
    ],
}
BASE = list(SCHEMA)[:6]
PIPELINE = list(SCHEMA)[6:]


def canonical(dataset, value):
    return {
        key: None
        if value.get(key) is None
        else bool(value[key])
        if kind == "boolean"
        else int(value[key])
        if kind == "bigint"
        else value[key]
        for key, kind in SCHEMA[dataset.removesuffix("_full")]
    }


def scan(h, dataset):
    if h["name"] == "postgres":
        from psycopg import sql

        with h["conn"].cursor(name="analytics_scan") as cursor:
            cursor.itersize = 2000
            cursor.execute(
                sql.SQL('SELECT * FROM {} ORDER BY key COLLATE "C"').format(
                    table(h, dataset)
                )
            )
            for value in cursor:
                yield canonical(
                    dataset,
                    dict(
                        zip(
                            (k for k, _ in SCHEMA[dataset.removesuffix("_full")]), value
                        )
                    ),
                )
    elif h["name"] == "mongo":
        for value in h["db"][dataset].find().sort("_id", 1):
            yield canonical(dataset, value)
    else:
        for key in h["index"].get(dataset, []):
            for line in gzip.decompress(s3_get(h, key)).splitlines():
                yield canonical(dataset, loads(line))


def write(h, dataset, values):
    """Replace only one scratch relation. PG/Mongo acknowledgements precede return."""
    if h["name"] == "postgres":
        from psycopg import sql

        h["conn"].execute(sql.SQL("DROP TABLE IF EXISTS {}").format(table(h, dataset)))
        fields = sql.SQL(",").join(
            sql.SQL("{} " + typ).format(sql.Identifier(key))
            for key, typ in SCHEMA[dataset.removesuffix("_full")]
        )
        h["conn"].execute(
            sql.SQL("CREATE TABLE {} ({}, PRIMARY KEY(key))").format(
                table(h, dataset), fields
            )
        )
        with (
            h["conn"]
            .cursor()
            .copy(sql.SQL("COPY {} FROM STDIN").format(table(h, dataset))) as copy
        ):
            for row in values:
                copy.write_row(
                    [row.get(k) for k, _ in SCHEMA[dataset.removesuffix("_full")]]
                )
        h["conn"].commit()
    elif h["name"] == "mongo":
        h["db"][dataset].drop()
        batch = []
        for row in values:
            batch.append({"_id": row["key"], **row})
            if len(batch) == 2000:
                h["db"][dataset].insert_many(batch, ordered=False)
                batch = []
        if batch:
            h["db"][dataset].insert_many(batch, ordered=False)
        if not h["db"][dataset].count_documents({}, limit=1):
            if dataset not in h["db"].list_collection_names():
                h["db"].create_collection(dataset)
    else:
        chunks = []
        buffer = bytearray()

        def flush():
            if not buffer:
                return
            body = gzip.compress(bytes(buffer), mtime=0)
            key = f"{h['namespace']}/workloads/analytics/{dataset}/{hashlib.sha256(body).hexdigest()}.jsonl.gz"
            s3_put(h, key, body)
            chunks.append(key)
            buffer.clear()

        for value in values:
            line = dumps(value).encode() + b"\n"
            if len(buffer) + len(line) > 4 * 1024**2:
                flush()
            buffer.extend(line)
        flush()
        h["index"][dataset] = chunks
        s3_put(
            h,
            f"{h['namespace']}/workloads/analytics/index.json",
            dumps(h["index"]).encode(),
        )


def sql_query(op, suffix="", board_filter=False, player_filter=False, sqlite=False):
    """Concrete pipeline SQL; suffix is internal, never supplied by the CLI."""
    best, ranked, features, scores = [
        '"' + v + suffix + '"' for v in ("best", "ranked", "features", "scores")
    ]
    # All identifiers are selected from the module's fixed SCHEMA.
    cond = "AND r.board_id IN ({boards})" if board_filter else ""
    player = "WHERE r.player_id IN ({players})" if player_filter else ""
    q = {
        "activity": """SELECT c.game_id||':'||c.key||':'||COALESCE(CAST(r.status AS TEXT),'unknown')||':'||COALESCE(r.month,'unknown') AS key,c.game_id,c.key AS category_key,COALESCE(r.status,-1) AS status,COALESCE(r.month,'unknown') AS month,count(DISTINCT r.key) AS n_runs,count(DISTINCT CASE WHEN p.resolved THEN p.key END) AS n_players FROM runs r JOIN boards b ON b.key=r.board_id JOIN categories c ON c.key=b.category_key LEFT JOIN run_players rp ON rp.run_id=r.key LEFT JOIN players p ON p.key=rp.player_id GROUP BY c.game_id,c.key,r.status,r.month""",
        "geography": """SELECT g.key||':'||COALESCE(p.area,'unknown') AS key,g.key AS game_id,g.name AS game_name,COALESCE(p.area,'unknown') AS area,count(DISTINCT r.key) AS n_runs,count(DISTINCT CASE WHEN p.resolved THEN p.key END) AS n_players FROM runs r JOIN boards b ON b.key=r.board_id JOIN categories c ON c.key=b.category_key JOIN games g ON g.key=c.game_id LEFT JOIN run_players rp ON rp.run_id=r.key LEFT JOIN players p ON p.key=rp.player_id GROUP BY g.key,g.name,p.area""",
        "best": f"SELECT r.board_id||':'||rp.player_id AS key,r.board_id,rp.player_id,min(r.time_ns) AS time_ns FROM runs r JOIN run_players rp ON rp.run_id=r.key JOIN players p ON p.key=rp.player_id WHERE r.eligible AND p.resolved {cond} GROUP BY r.board_id,rp.player_id",
        "ranked": f"SELECT key,board_id,player_id,time_ns,rank() OVER(PARTITION BY board_id ORDER BY time_ns) AS place,row_number() OVER(PARTITION BY board_id ORDER BY time_ns,player_id) AS seq,count(*) OVER(PARTITION BY board_id) AS n_players FROM {best}",
        "features": f"""WITH f AS (SELECT board_id AS key,max(n_players) AS n_players,min(time_ns) AS best_ns,max(CASE WHEN seq=(n_players+1)/2 THEN time_ns END) AS median_ns,max(CASE WHEN seq<=100 THEN time_ns END) AS top_ns FROM {ranked} GROUP BY board_id) SELECT key,n_players,best_ns,median_ns,top_ns,(CASE WHEN n_players>100 THEN 3 WHEN n_players>20 THEN 2 ELSE 1 END)*(CASE WHEN n_players<2 THEN 1 WHEN (top_ns-best_ns)*100.0<=best_ns THEN 3 WHEN (top_ns-best_ns)*10.0<=best_ns THEN 2 ELSE 1 END)*(CASE WHEN median_ns<60000000000 THEN 1 WHEN median_ns<1800000000000 THEN 2 ELSE 3 END) AS weight FROM f""",
        "scores": f"SELECT r.player_id AS key,sum((1000000*f.weight)/r.place) AS score,count(*) AS boards FROM {ranked} r JOIN {features} f ON f.key=r.board_id {player} GROUP BY r.player_id",
        "rating": f"SELECT s.key,s.score,s.boards,rank() OVER(ORDER BY s.score DESC) AS place,p.name,p.area FROM {scores} s JOIN players p ON p.key=s.key",
    }[op]
    return q


def mongo_pipeline(op, suffix="", boards=None, players=None):
    best, ranked, features, scores = [
        v + suffix for v in ("best", "ranked", "features", "scores")
    ]

    def lookup(local, foreign, asname):
        return [
            {
                "$lookup": {
                    "from": foreign,
                    "localField": local,
                    "foreignField": "_id",
                    "as": asname,
                }
            },
            {"$unwind": {"path": "$" + asname, "preserveNullAndEmptyArrays": True}},
        ]

    if op in ("activity", "geography"):
        p = lookup("board_id", "boards", "b") + lookup(
            "b.category_key", "categories", "c"
        )
        if op == "geography":
            p += lookup("c.game_id", "games", "g")
        p += [
            {
                "$lookup": {
                    "from": "run_players",
                    "localField": "key",
                    "foreignField": "run_id",
                    "as": "rp",
                }
            },
            {"$unwind": {"path": "$rp", "preserveNullAndEmptyArrays": True}},
        ] + lookup("rp.player_id", "players", "p")
        group = (
            {
                "game": "$c.game_id",
                "category": "$c.key",
                "status": {"$ifNull": ["$status", -1]},
                "month": {"$ifNull": ["$month", "unknown"]},
            }
            if op == "activity"
            else {
                "game": "$g.key",
                "name": "$g.name",
                "area": {"$ifNull": ["$p.area", "unknown"]},
            }
        )
        p += [
            {
                "$group": {
                    "_id": group,
                    "runs": {"$addToSet": "$key"},
                    "players": {
                        "$addToSet": {"$cond": ["$p.resolved", "$p.key", None]}
                    },
                }
            }
        ]
        project = {
            "_id": 0,
            "n_runs": {"$size": "$runs"},
            "n_players": {"$size": {"$setDifference": ["$players", [None]]}},
            "game_id": "$_id.game",
        }
        if op == "activity":
            project.update(
                key={
                    "$concat": [
                        "$_id.game",
                        ":",
                        "$_id.category",
                        ":",
                        {
                            "$cond": [
                                {"$eq": ["$_id.status", -1]},
                                "unknown",
                                {"$toString": "$_id.status"},
                            ]
                        },
                        ":",
                        "$_id.month",
                    ]
                },
                category_key="$_id.category",
                status="$_id.status",
                month="$_id.month",
            )
        else:
            project.update(
                key={"$concat": ["$_id.game", ":", "$_id.area"]},
                game_name="$_id.name",
                area="$_id.area",
            )
        return "runs", p + [{"$project": project}]
    if op == "best":
        match = {"eligible": True}
        if boards is not None:
            match["board_id"] = {"$in": boards}
        p = (
            [
                {"$match": match},
                {
                    "$lookup": {
                        "from": "run_players",
                        "localField": "key",
                        "foreignField": "run_id",
                        "as": "rp",
                    }
                },
                {"$unwind": "$rp"},
            ]
            + lookup("rp.player_id", "players", "p")
            + [
                {"$match": {"p.resolved": True}},
                {
                    "$group": {
                        "_id": {"board": "$board_id", "player": "$rp.player_id"},
                        "time_ns": {"$min": "$time_ns"},
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "key": {"$concat": ["$_id.board", ":", "$_id.player"]},
                        "board_id": "$_id.board",
                        "player_id": "$_id.player",
                        "time_ns": 1,
                    }
                },
            ]
        )
        return "runs", p
    if op == "ranked":
        return best, [
            {
                "$setWindowFields": {
                    "partitionBy": "$board_id",
                    "sortBy": {"time_ns": 1},
                    "output": {
                        "place": {"$rank": {}},
                        "n_players": {
                            "$count": {},
                            "window": {"documents": ["unbounded", "unbounded"]},
                        },
                    },
                }
            },
            {"$set": {"_order": {"time": "$time_ns", "player": "$player_id"}}},
            {
                "$setWindowFields": {
                    "partitionBy": "$board_id",
                    "sortBy": {"_order": 1},
                    "output": {"seq": {"$documentNumber": {}}},
                }
            },
            {"$project": {"_id": 0, **{k: 1 for k, _ in SCHEMA["ranked"]}}},
        ]
    if op == "features":
        return ranked, [
            {
                "$group": {
                    "_id": "$board_id",
                    "n_players": {"$max": "$n_players"},
                    "best_ns": {"$min": "$time_ns"},
                    "median_ns": {
                        "$max": {
                            "$cond": [
                                {
                                    "$eq": [
                                        "$seq",
                                        {"$ceil": {"$divide": ["$n_players", 2]}},
                                    ]
                                },
                                "$time_ns",
                                None,
                            ]
                        }
                    },
                    "top_ns": {
                        "$max": {"$cond": [{"$lte": ["$seq", 100]}, "$time_ns", None]}
                    },
                }
            },
            {
                "$set": {
                    "weight": {
                        "$multiply": [
                            {
                                "$switch": {
                                    "branches": [
                                        {
                                            "case": {"$gt": ["$n_players", 100]},
                                            "then": 3,
                                        },
                                        {
                                            "case": {"$gt": ["$n_players", 20]},
                                            "then": 2,
                                        },
                                    ],
                                    "default": 1,
                                }
                            },
                            {
                                "$cond": [
                                    {"$lt": ["$n_players", 2]},
                                    1,
                                    {
                                        "$switch": {
                                            "branches": [
                                                {
                                                    "case": {
                                                        "$lte": [
                                                            {
                                                                "$multiply": [
                                                                    {
                                                                        "$subtract": [
                                                                            "$top_ns",
                                                                            "$best_ns",
                                                                        ]
                                                                    },
                                                                    100,
                                                                ]
                                                            },
                                                            "$best_ns",
                                                        ]
                                                    },
                                                    "then": 3,
                                                },
                                                {
                                                    "case": {
                                                        "$lte": [
                                                            {
                                                                "$multiply": [
                                                                    {
                                                                        "$subtract": [
                                                                            "$top_ns",
                                                                            "$best_ns",
                                                                        ]
                                                                    },
                                                                    10,
                                                                ]
                                                            },
                                                            "$best_ns",
                                                        ]
                                                    },
                                                    "then": 2,
                                                },
                                            ],
                                            "default": 1,
                                        }
                                    },
                                ]
                            },
                            {
                                "$switch": {
                                    "branches": [
                                        {
                                            "case": {
                                                "$lt": ["$median_ns", 60000000000]
                                            },
                                            "then": 1,
                                        },
                                        {
                                            "case": {
                                                "$lt": ["$median_ns", 1800000000000]
                                            },
                                            "then": 2,
                                        },
                                    ],
                                    "default": 3,
                                }
                            },
                        ]
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "key": "$_id",
                    **{k: 1 for k, _ in SCHEMA["features"] if k != "key"},
                }
            },
        ]
    if op == "scores":
        p = [] if players is None else [{"$match": {"player_id": {"$in": players}}}]
        p += lookup("board_id", features, "f") + [
            {
                "$group": {
                    "_id": "$player_id",
                    "score": {
                        "$sum": {
                            "$floor": {
                                "$divide": [
                                    {"$multiply": [1000000, "$f.weight"]},
                                    "$place",
                                ]
                            }
                        }
                    },
                    "boards": {"$sum": 1},
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "key": "$_id",
                    "score": {"$toLong": "$score"},
                    "boards": 1,
                }
            },
        ]
        return ranked, p
    if op == "rating":
        return scores, [
            {
                "$setWindowFields": {
                    "sortBy": {"score": -1},
                    "output": {"place": {"$rank": {}}},
                }
            }
        ] + lookup("key", "players", "p") + [
            {
                "$project": {
                    "_id": 0,
                    "key": 1,
                    "score": 1,
                    "boards": 1,
                    "place": 1,
                    "name": "$p.name",
                    "area": "$p.area",
                }
            }
        ]
    raise ValueError(op)


def client_query(h, op, boards=None, players=None):
    """Concrete S3 algorithms: no SQL engine or full raw-response materialization."""
    if op in ("activity", "geography"):
        boards = {r["key"]: r for r in scan(h, "boards")}
        categories = {r["key"]: r for r in scan(h, "categories")}
        games = {r["key"]: r for r in scan(h, "games")}
        profiles = {r["key"]: r for r in scan(h, "players")}
        memberships = defaultdict(list)
        for r in scan(h, "run_players"):
            memberships[r["run_id"]].append(r["player_id"])
        groups = {}
        for r in scan(h, "runs"):
            category = categories[boards[r["board_id"]]["category_key"]]
            gid = category["game_id"]
            participants = memberships.get(r["key"], [None])
            for pid in participants:
                p = profiles.get(pid, {})
                key = (
                    (
                        gid,
                        category["key"],
                        r["status"] if r["status"] is not None else -1,
                        r["month"] or "unknown",
                    )
                    if op == "activity"
                    else (gid, games[gid]["name"], p.get("area") or "unknown")
                )
                a, b = groups.setdefault(key, (set(), set()))
                a.add(r["key"])
                if p.get("resolved"):
                    b.add(pid)
        result = []
        for key, (rr, pp) in groups.items():
            if op == "activity":
                gid, cid, status, month = key
                result.append(
                    {
                        "key": ":".join(
                            [
                                gid,
                                cid,
                                str(status) if status != -1 else "unknown",
                                month,
                            ]
                        ),
                        "game_id": gid,
                        "category_key": cid,
                        "status": status,
                        "month": month,
                        "n_runs": len(rr),
                        "n_players": len(pp),
                    }
                )
            else:
                gid, name, area = key
                result.append(
                    {
                        "key": gid + ":" + area,
                        "game_id": gid,
                        "game_name": name,
                        "area": area,
                        "n_runs": len(rr),
                        "n_players": len(pp),
                    }
                )
        return result
    if op == "best":
        valid = {r["key"] for r in scan(h, "players") if r["resolved"]}
        eligible = {
            r["key"]: (r["board_id"], r["time_ns"])
            for r in scan(h, "runs")
            if r["eligible"] and (boards is None or r["board_id"] in boards)
        }
        best = {}
        for r in scan(h, "run_players"):
            if r["run_id"] in eligible and r["player_id"] in valid:
                board, ns = eligible[r["run_id"]]
                key = (board, r["player_id"])
                best[key] = min(best.get(key, ns), ns)
        return [
            {"key": b + ":" + p, "board_id": b, "player_id": p, "time_ns": ns}
            for (b, p), ns in best.items()
        ]
    if op == "ranked":
        grouped = defaultdict(list)
        for r in scan(h, "best"):
            if boards is None or r["board_id"] in boards:
                grouped[r["board_id"]].append(r)
        result = []
        for group in grouped.values():
            group.sort(key=lambda r: (r["time_ns"], r["player_id"]))
            last = None
            place = 0
            for i, r in enumerate(group, 1):
                if r["time_ns"] != last:
                    place = i
                    last = r["time_ns"]
                result.append({**r, "place": place, "seq": i, "n_players": len(group)})
        return result
    if op == "features":
        grouped = {}
        for r in scan(h, "ranked"):
            if boards is not None and r["board_id"] not in boards:
                continue
            f = grouped.setdefault(
                r["board_id"],
                {
                    "key": r["board_id"],
                    "n_players": r["n_players"],
                    "best_ns": r["time_ns"],
                    "median_ns": None,
                    "top_ns": 0,
                },
            )
            f["best_ns"] = min(f["best_ns"], r["time_ns"])
            if r["seq"] == (r["n_players"] + 1) // 2:
                f["median_ns"] = r["time_ns"]
            if r["seq"] <= 100:
                f["top_ns"] = max(f["top_ns"], r["time_ns"])
        for f in grouped.values():
            n = f["n_players"]
            gap = f["top_ns"] - f["best_ns"]
            density = (
                1
                if n < 2
                else 3
                if gap * 100 <= f["best_ns"]
                else 2
                if gap * 10 <= f["best_ns"]
                else 1
            )
            f["weight"] = (
                (3 if n > 100 else 2 if n > 20 else 1)
                * density
                * (
                    1
                    if f["median_ns"] < 60000000000
                    else 2
                    if f["median_ns"] < 1800000000000
                    else 3
                )
            )
        return list(grouped.values())
    if op == "scores":
        features = {r["key"]: r for r in scan(h, "features")}
        scores = {}
        for r in scan(h, "ranked"):
            if players is not None and r["player_id"] not in players:
                continue
            score = scores.setdefault(
                r["player_id"], {"key": r["player_id"], "score": 0, "boards": 0}
            )
            score["score"] += 1000000 * features[r["board_id"]]["weight"] // r["place"]
            score["boards"] += 1
        return list(scores.values())
    if op == "rating":
        profiles = {r["key"]: r for r in scan(h, "players")}
        values = sorted(scan(h, "scores"), key=lambda r: (-r["score"], r["key"]))
        last = None
        place = 0
        for i, r in enumerate(values, 1):
            if r["score"] != last:
                place = i
                last = r["score"]
            r.update(
                place=place,
                name=profiles[r["key"]]["name"],
                area=profiles[r["key"]]["area"],
            )
        return values
    raise ValueError(op)


def materialize(h, op, suffix=""):
    if h["name"] == "postgres":
        from psycopg import sql

        text = sql_query(op, suffix)
        h["conn"].execute(
            sql.SQL("DROP TABLE IF EXISTS {}").format(table(h, op + suffix))
        )
        h["conn"].execute(
            sql.SQL("CREATE TABLE {} AS " + text).format(table(h, op + suffix))
        )
        h["conn"].commit()
    elif h["name"] == "mongo":
        dataset, pipeline = mongo_pipeline(op, suffix)
        # Explicit _id makes readback order and repeated materialization stable.
        list(
            h["db"][dataset].aggregate(
                pipeline + [{"$set": {"_id": "$key"}}, {"$out": op + suffix}],
                allowDiskUse=True,
            )
        )
    else:
        view = {**h, "index": dict(h["index"])}
        for relation in ("best", "ranked", "features", "scores"):
            if suffix and relation + suffix in h["index"]:
                view["index"][relation] = h["index"][relation + suffix]
        write(h, op + suffix, client_query(view, op))


def output_digest(h, dataset):
    """Readback outside timers; SQLite orders S3 rows with bounded client memory."""
    db = sqlite3.connect("")
    db.execute("CREATE TABLE output(key TEXT PRIMARY KEY,document TEXT)")
    for value in scan(h, dataset):
        db.execute("INSERT INTO output VALUES(?,?)", [value["key"], dumps(value)])
    checksum = hashlib.sha256()
    count = 0
    for (value,) in db.execute("SELECT document FROM output ORDER BY key"):
        checksum.update(value.encode() + b"\n")
        count += 1
    db.close()
    return {"count": count, "digest": checksum.hexdigest()}


def oracle(input_db):
    """Independent SQLite execution, never used to answer the S3 benchmark."""
    conn = sqlite3.connect("")
    for dataset in BASE:
        fields = ",".join('"' + key + '" ' + kind for key, kind in SCHEMA[dataset])
        conn.execute("CREATE TABLE " + dataset + " (" + fields + ")")
        cols = [k for k, _ in SCHEMA[dataset]]
        conn.executemany(
            "INSERT INTO " + dataset + " VALUES(" + ",".join("?" for _ in cols) + ")",
            ([r.get(k) for k in cols] for r in rows(input_db, dataset)),
        )
    result = {}
    for op in PIPELINE:
        conn.execute("CREATE TABLE " + op + " AS " + sql_query(op, sqlite=True))
        checksum = hashlib.sha256()
        count = 0
        for row in conn.execute("SELECT * FROM " + op + " ORDER BY key"):
            checksum.update(
                dumps(
                    canonical(op, dict(zip((k for k, _ in SCHEMA[op]), row)))
                ).encode()
                + b"\n"
            )
            count += 1
        result[op] = {"count": count, "digest": checksum.hexdigest()}
    conn.close()
    return result


def add_indexes(h):
    targets = {
        "runs": [("board_id",), ("eligible", "board_id")],
        "run_players": [("run_id",), ("player_id",)],
        "boards": [("category_key",)],
        "categories": [("game_id",)],
    }
    for dataset, groups in targets.items():
        for columns in groups:
            if h["name"] == "postgres":
                from psycopg import sql

                h["conn"].execute(
                    sql.SQL("CREATE INDEX ON {} ({})").format(
                        table(h, dataset),
                        sql.SQL(",").join(map(sql.Identifier, columns)),
                    )
                )
            elif h["name"] == "mongo":
                h["db"][dataset].create_index([(k, 1) for k in columns])
    if h["name"] == "postgres":
        h["conn"].commit()
        for dataset in BASE:
            h["conn"].execute('ANALYZE "' + h["namespace"] + '"."' + dataset + '"')
        h["conn"].commit()


class Monitor:
    """Sample client RSS; CPU time excludes database processes."""

    def __enter__(self):
        self.peak = 0
        self.stop = threading.Event()
        self.cpu = time.process_time()

        def sample():
            while not self.stop.is_set():
                for line in Path("/proc/self/status").read_text().splitlines():
                    if line.startswith("VmRSS:"):
                        self.peak = max(self.peak, int(line.split()[1]) * 1024)
                self.stop.wait(0.05)

        self.thread = threading.Thread(target=sample, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, *args):
        self.stop.set()
        self.thread.join()
        self.cpu = time.process_time() - self.cpu


def initial(h):
    if h["name"] == "postgres":
        h["conn"].execute('CREATE SCHEMA "' + h["namespace"] + '"')
        h["conn"].execute('SET search_path TO "' + h["namespace"] + '"')
        h["conn"].commit()
    elif h["name"] == "mongo":
        if h["db"].list_collection_names():
            raise ValueError("Namespace already exists")
    else:
        if (
            h["s3"]
            .client.list_objects_v2(
                Bucket=h["s3"].bucket, Prefix=h["namespace"] + "/", MaxKeys=1
            )
            .get("KeyCount")
        ):
            raise ValueError("Prefix exists")
        h["index"] = {}


def patch_plan(input_db):
    candidates = (r for r in rows(input_db, "runs") if r["eligible"])
    boards = set()
    changes = {}
    for r in candidates:
        if r["board_id"] not in boards and len(boards) >= 8:
            continue
        boards.add(r["board_id"])
        changes[r["key"]] = {
            **r,
            "eligible": len(changes) % 3 != 0,
            "time_ns": max(1, r["time_ns"] - 1000000),
        }
        if len(changes) >= 128:
            break
    players = []
    for r in rows(input_db, "players"):
        if r["resolved"]:
            players.append({**r, "area": "benchmark-test-region"})
            if len(players) == 5:
                break
    return {
        "runs": changes,
        "players": {r["key"]: r for r in players},
        "boards": sorted(boards),
        "synthetic": True,
    }


def apply_patch_to_store(h, patch):
    for dataset in ("runs", "players"):
        values = patch[dataset]
        if h["name"] == "postgres":
            from psycopg import sql

            columns = [k for k, _ in SCHEMA[dataset] if k != "key"]
            q = sql.SQL("UPDATE {} SET {} WHERE key=%s").format(
                table(h, dataset),
                sql.SQL(",").join(
                    sql.SQL("{}=%s").format(sql.Identifier(k)) for k in columns
                ),
            )
            with h["conn"].cursor() as cursor:
                cursor.executemany(
                    q, [[r[k] for k in columns] + [r["key"]] for r in values.values()]
                )
            h["conn"].commit()
        elif h["name"] == "mongo":
            from pymongo import ReplaceOne

            if values:
                h["db"][dataset].bulk_write(
                    [
                        ReplaceOne({"_id": key}, {"_id": key, **row})
                        for key, row in values.items()
                    ]
                )
        else:
            write(h, dataset, (values.get(r["key"], r) for r in scan(h, dataset)))


def partial(h, op, boards, players=None):
    field = "player_id" if op == "scores" else "key" if op == "features" else "board_id"
    keys = players if op == "scores" else boards
    if not keys:
        return
    if h["name"] == "postgres":
        from psycopg import sql

        q = sql_query(op, board_filter=op == "best", player_filter=op == "scores")
        values = list(keys)
        marks = ",".join(["%s"] * len(values))
        if op == "best":
            q = q.replace("{boards}", marks)
        elif op == "ranked":
            q += " WHERE board_id IN (" + marks + ")"
        elif op == "features":
            q = q.replace(
                'FROM "ranked" GROUP BY',
                'FROM "ranked" WHERE board_id IN (' + marks + ") GROUP BY",
            )
        elif op == "scores":
            q = q.replace("{players}", marks)
        h["conn"].execute(
            sql.SQL("DELETE FROM {} WHERE {}=ANY(%s)").format(
                table(h, op), sql.Identifier("key" if op == "scores" else field)
            ),
            [values],
        )
        h["conn"].execute(sql.SQL("INSERT INTO {} " + q).format(table(h, op)), values)
        h["conn"].commit()
    elif h["name"] == "mongo":
        dataset, p = mongo_pipeline(
            op,
            boards=list(boards) if op == "best" else None,
            players=list(players) if op == "scores" else None,
        )
        if op in ("ranked", "features"):
            p = [{"$match": {"board_id": {"$in": list(boards)}}}] + p
        h["db"][op].delete_many(
            {("key" if op == "scores" else field): {"$in": list(keys)}}
        )
        list(
            h["db"][dataset].aggregate(
                p
                + [
                    {"$set": {"_id": "$key"}},
                    {
                        "$merge": {
                            "into": op,
                            "on": "_id",
                            "whenMatched": "replace",
                            "whenNotMatched": "insert",
                        }
                    },
                ],
                allowDiskUse=True,
            )
        )
    else:
        fresh = client_query(
            h,
            op,
            boards=set(boards),
            players=set(players) if players is not None else None,
        )
        member = "key" if op in ("features", "scores") else "board_id"
        keep = (r for r in scan(h, op) if r[member] not in keys)
        write(h, op, chain(keep, fresh))


def incremental(h, patch):
    boards = set(patch["boards"])
    affected = {r["player_id"] for r in scan(h, "best") if r["board_id"] in boards}
    partial(h, "best", boards)
    affected.update(r["player_id"] for r in scan(h, "best") if r["board_id"] in boards)
    partial(h, "ranked", boards)
    partial(h, "features", boards)
    partial(h, "scores", boards, affected)
    # A changed score can move any global place: ranking the resulting totals
    # remains global even when only a few boards needed their scores recomputed.
    materialize(h, "rating")


def top(h):
    if h["name"] == "postgres":
        from psycopg import sql

        return [
            list(row)
            for row in h["conn"].execute(
                sql.SQL(
                    "SELECT key,score,place FROM {} ORDER BY place,key LIMIT 20"
                ).format(table(h, "rating"))
            )
        ]
    if h["name"] == "mongo":
        return [
            [r["key"], r["score"], r["place"]]
            for r in h["db"].rating.find().sort([("place", 1), ("key", 1)]).limit(20)
        ]
    return loads(s3_get(h, h["namespace"] + "/workloads/analytics/top20.json"))


def publish(h):
    if h["name"] == "postgres":
        from psycopg import sql

        h["conn"].execute(
            sql.SQL("CREATE INDEX ON {} (place,key)").format(table(h, "rating"))
        )
        h["conn"].commit()
    elif h["name"] == "mongo":
        h["db"].rating.create_index([("place", 1), ("key", 1)])
    else:
        values = heapq.nsmallest(
            20, scan(h, "rating"), key=lambda r: (r["place"], r["key"])
        )
        s3_put(
            h,
            h["namespace"] + "/workloads/analytics/top20.json",
            dumps([[r["key"], r["score"], r["place"]] for r in values]).encode(),
        )


def save_plans(h, target):
    if h["name"] == "garage":
        return
    result = {}
    for op in ("geography", "best", "ranked", "features", "scores", "rating"):
        if h["name"] == "postgres":
            result[op] = (
                h["conn"]
                .execute("EXPLAIN (FORMAT JSON) " + sql_query(op))
                .fetchone()[0]
            )
        else:
            source, pipeline = mongo_pipeline(op)
            result[op] = h["db"].command(
                "explain",
                {"aggregate": source, "pipeline": pipeline, "cursor": {}},
                verbosity="queryPlanner",
            )
    # Plans contain BSON timestamps/Int64 wrappers; this export is diagnostic.
    import json

    target.write_text(
        json.dumps(result, default=str, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def run(root, sizes, repeats):
    frozen = read(root / "frozen" / "frozen.json")
    with (root / "frozen/source.sqlite3").open("rb") as handle:
        if hashlib.file_digest(handle, "sha256").hexdigest() != frozen["sqlite_sha256"]:
            raise ValueError("Frozen input changed")
    maximum = int(frozen["counts"]["runs"])
    sizes = sorted(set([*sizes, maximum]))
    if maximum < max(sizes):
        raise ValueError("Requested size exceeds frozen maximum")
    stamp = time.strftime("%Y%m%d%H%M%S", time.gmtime())
    output = root / ("results_" + stamp)
    output.mkdir()
    atomic_write(
        output / "config.json",
        {
            "at": utcnow(),
            "sizes": sizes,
            "repeats": repeats,
            "frozen": frozen,
            "source_HTTP": 0,
            "normalization": "Common client transformation measured once per size, followed by concrete native six-relation load. Not a native server JSON-normalization benchmark.",
        },
    )
    fields = [
        "size",
        "repeat",
        "store",
        "operation",
        "seconds",
        "client_cpu_seconds",
        "sampled_client_peak_rss_bytes",
        "count",
        "digest",
        "get_calls",
        "read_bytes",
        "put_calls",
        "written_bytes",
    ]
    with (output / "measurements.csv").open(
        "w", encoding="utf-8", newline=""
    ) as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for size in sizes:
            folder = root / ("input_v2_" + str(size))
            metadata = (
                read(folder / "input.json")
                if (folder / "input.json").exists()
                else prepare(root / "frozen" / "source.sqlite3", folder, size)
            )
            db = sqlite3.connect(folder / "input.sqlite3")
            expected = oracle(db)
            atomic_write(output / ("input_" + str(size) + ".json"), metadata)
            atomic_write(output / ("reference_" + str(size) + ".json"), expected)
            patch = patch_plan(db)
            atomic_write(output / ("patch_" + str(size) + ".json"), patch)
            for repeat in range(repeats):
                order = ["postgres", "mongo", "garage"]
                order = order[repeat % 3 :] + order[: repeat % 3]
                for name in order:
                    namespace = f"test_wl_an_{stamp}_{size}_{repeat}"
                    with connect(name, namespace) as h:
                        initial(h)

                        def measure(op, fn, expected_result=None, dataset=None):
                            atomic_write(
                                output / "status.json",
                                {
                                    "phase": "running",
                                    "size": size,
                                    "repeat": repeat + 1,
                                    "store": name,
                                    "operation": op,
                                    "at": utcnow(),
                                },
                            )
                            h["io"] = Counter()
                            with Monitor() as resources:
                                started = time.perf_counter()
                                value = fn()
                                seconds = time.perf_counter() - started
                            io = dict(h["io"])
                            proof = output_digest(h, dataset) if dataset else None
                            if expected_result is not None and proof != expected_result:
                                raise ValueError(
                                    f"{name} {op} differs: {proof} vs {expected_result}"
                                )
                            writer.writerow(
                                {
                                    "size": size,
                                    "repeat": repeat + 1,
                                    "store": name,
                                    "operation": op,
                                    "seconds": seconds,
                                    "client_cpu_seconds": resources.cpu,
                                    "sampled_client_peak_rss_bytes": resources.peak,
                                    **(proof or {}),
                                    **io,
                                }
                            )
                            handle.flush()
                            print(
                                size,
                                repeat + 1,
                                name,
                                op,
                                round(seconds, 3),
                                flush=True,
                            )
                            return value

                        def load():
                            for dataset in BASE:
                                write(h, dataset, rows(db, dataset))

                        measure("normalization_load", load)
                        for dataset in BASE:
                            if output_digest(h, dataset) != {
                                "count": metadata["counts"][dataset],
                                "digest": metadata["digests"][dataset],
                            }:
                                raise ValueError("Normalized input differs")
                        measure("indexes", lambda: add_indexes(h))
                        for op in PIPELINE:
                            measure(
                                op, lambda op=op: materialize(h, op), expected[op], op
                            )
                        measure("publish_views", lambda: publish(h))
                        for p in range(3):
                            measure("cached_top20", lambda: top(h))
                        if repeat == 0:
                            save_plans(h, output / f"plans_{size}_{name}.json")
                        measure(
                            "synthetic_patch", lambda: apply_patch_to_store(h, patch)
                        )

                        def full():
                            for op in (
                                "best",
                                "ranked",
                                "features",
                                "scores",
                                "rating",
                            ):
                                materialize(h, op, "_full")

                        measure("full_rating_refresh", full, dataset="rating_full")
                        full_proof = output_digest(h, "rating_full")
                        measure(
                            "incremental_rating_refresh",
                            lambda: incremental(h, patch),
                            full_proof,
                            "rating",
                        )
                        atomic_write(
                            output / f"verified_{size}_{repeat}_{name}.json",
                            {
                                "base": expected,
                                "after_delta": full_proof,
                                "all_input_relations_verified": True,
                            },
                        )
            db.close()
    atomic_write(
        output / "completed.json",
        {
            "at": utcnow(),
            "sizes": sizes,
            "repeats": repeats,
            "source_HTTP": 0,
            "input_frozen_sha256": frozen["sqlite_sha256"],
            "all_native_and_client_results_equal": True,
        },
    )
    atomic_write(output / "status.json", {"phase": "complete", "at": utcnow()})
    print("COMPLETE", output, flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--sizes", type=int, nargs="+", default=[1000, 10000, 100000])
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()
    if os.environ.get("ETL_EXPERIMENT_ID") != "analytics-20261030":
        parser.error("Explicit isolated experiment environment required")
    if not (args.root / "isolated.json").exists():
        parser.error("Missing isolated stand marker")
    marker = read(args.root / "isolated.json")
    if (
        marker.get("experiment_id") != "analytics-20261030"
        or marker.get("production_access") is not False
    ):
        parser.error("Invalid isolated stand marker")
    if not 1 <= args.repeats <= 3 or any(size <= 0 for size in args.sizes):
        parser.error("Use positive sizes and one to three repeats")
    run(args.root, args.sizes, args.repeats)


if __name__ == "__main__":
    main()
