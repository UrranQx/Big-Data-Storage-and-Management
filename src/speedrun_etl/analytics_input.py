"""Фиксирует завершённые ODS-снимки и готовит связанные тестовые наборы.

Источник читается в read-only транзакции; HTTP и загрузчиков здесь нет.
SQLite используется для воспроизводимого входа и эталона, не как движок S3.
"""

import argparse
import hashlib
import os
from pathlib import Path
import sqlite3
import time

from .entity_ods import EntityPackage
from .jsonutil import atomic_write, digest, dumps, loads, utcnow
from .storage import pg_connect
from .storage_workloads import duration_ns


def database(path):
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS records(dataset TEXT,key TEXT,rank TEXT,document TEXT,PRIMARY KEY(dataset,key)) WITHOUT ROWID"
    )
    return conn


def put(conn, dataset, key, value):
    conn.execute(
        "INSERT INTO records VALUES(?,?,?,?)", [dataset, key, digest(key), dumps(value)]
    )


def rows(conn, dataset):
    for row in conn.execute(
        "SELECT document FROM records WHERE dataset=? ORDER BY key", [dataset]
    ):
        yield loads(row[0])


def select_time(raw, game):
    """Explicit experimental timer rule; never combine different fields in a group."""
    preferred = {0: "time", 1: "timeWithLoads", 2: "igt"}.get(
        game.get("defaultTimer"), "time"
    )
    for field in dict.fromkeys([preferred, "time", "igt", "timeWithLoads"]):
        value = duration_ns(raw.get(field))
        if value is not None:
            return field, value, field != preferred
    return preferred, None, False


class SourceDecoder:
    decode = EntityPackage.decode

    def __init__(self, conn):
        self.conn = conn

    def get(self, dataset, key):
        if dataset != "blocks":
            raise ValueError(dataset)
        return self.conn.execute(
            "SELECT document FROM entity_v3.blocks WHERE key=%s", [key]
        ).fetchone()[0]


def freeze(target, batch):
    os.environ.setdefault("PGPASSWORD", os.environ.get("ODS_DB_PASSWORD", ""))
    target.mkdir(parents=True, exist_ok=False)
    started = time.monotonic()
    with pg_connect(True) as control:
        control.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        games = control.execute(
            "SELECT game_id,collection_id,state FROM entity_batch_queue WHERE batch_id=%s AND state IN ('complete','partial','not_applicable') ORDER BY position",
            [batch],
        ).fetchall()
        if not games:
            raise ValueError("No finished game chains")
        ids = [
            c + "-" + kind
            for _, c, _ in games
            for kind in ("games_metadata", "runs", "wr_history", "players")
        ]
        receipts = control.execute(
            "SELECT snapshot_id,role,store,package_digest,summary FROM entity_receipts WHERE namespace='entity_v3' AND snapshot_id=ANY(%s)",
            [ids],
        ).fetchall()
    proof = {}
    for sid, role, store, checksum, summary in receipts:
        if digest(summary) != checksum:
            raise ValueError("Receipt digest mismatch")
        proof.setdefault(sid, {})[store] = checksum
    if set(proof) != set(ids) or any(
        set(v) != {"postgres", "mongo", "garage"} or len(set(v.values())) != 1
        for v in proof.values()
    ):
        raise ValueError("Missing three-store source confirmation")
    out = database(target / "source.sqlite3")
    run_sids = [c + "-runs" for _, c, _ in games]
    with pg_connect() as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        decode = SourceDecoder(conn)
        for dataset, projections in [("runs", ["run_core"]), ("players", None)]:
            snapshots = (
                run_sids
                if dataset == "runs"
                else [c + "-" + k for _, c, _ in games for k in ("runs", "wr_history")]
            )
            query = (
                """WITH selected AS (
                SELECT DISTINCT ON(source_id) source_id,document AS check_doc
                FROM entity_v3.checks WHERE document->>'dataset'=%s
                AND document->>'snapshot_id'=ANY(%s)
                AND (%s::text[] IS NULL OR projection=ANY(%s))
                ORDER BY source_id,document->'last_seen'->>'observed_at' DESC,key DESC)
                SELECT e.document,s.check_doc FROM selected s JOIN entity_v3."""
                + dataset
                + " e ON e.key=s.check_doc->'last_seen'->>'version_key' ORDER BY e.source_id"
            )
            count = 0
            with conn.cursor(name="analytics_" + dataset) as cursor:
                cursor.itersize = 2000
                cursor.execute(query, [dataset, snapshots, projections, projections])
                for entity, check in cursor:
                    raw = decode.decode(entity["raw"])
                    put(
                        out,
                        dataset,
                        entity["source_id"],
                        {
                            "id": entity["source_id"],
                            "raw": raw,
                            "seen": check["last_seen"],
                            "context": check.get("unfiltered_leaderboard", {}).get(
                                "last_seen"
                            ),
                        },
                    )
                    count += 1
                    if count % 10000 == 0:
                        out.commit()
                        print("Frozen", dataset, count, flush=True)
            out.commit()
            print("Frozen", dataset, count, flush=True)
        for gid, _, _ in games:
            row = conn.execute(
                "SELECT document FROM entity_v3.games WHERE source_id=%s AND projection='game_detail' ORDER BY key LIMIT 1",
                [gid],
            ).fetchone()
            if row:
                put(out, "games", gid, {"id": gid, "raw": decode.decode(row[0]["raw"])})
        out.commit()
    counts = dict(out.execute("SELECT dataset,count(*) FROM records GROUP BY dataset"))
    out.close()
    with (target / "source.sqlite3").open("rb") as handle:
        checksum = hashlib.file_digest(handle, "sha256").hexdigest()
    atomic_write(
        target / "frozen.json",
        {
            "at": utcnow(),
            "batch": batch,
            "counts": counts,
            "sqlite_sha256": checksum,
            "source_HTTP_by_exporter": 0,
            "games": [list(row) for row in games],
            "receipts": proof,
            "seconds": time.monotonic() - started,
        },
    )


def prepare(source, target, size):
    """Common client transformation; native storage cost is measured separately."""
    started = time.monotonic()
    target.mkdir(parents=True, exist_ok=False)
    src = sqlite3.connect(f"file:{source.resolve().as_posix()}?mode=ro", uri=True)
    out = database(target / "input.sqlite3")
    profiles = {r["id"]: r for r in rows(src, "players")}
    metadata = {r["id"]: r["raw"] for r in rows(src, "games")}
    used_players, boards, categories, used_games = {}, {}, {}, set()
    diagnostics = {
        "missing_time": 0,
        "unknown_obsolete": 0,
        "unresolved_participants": 0,
        "alternate_timer": 0,
    }
    selected = src.execute(
        "SELECT document FROM records WHERE dataset='runs' ORDER BY rank,key LIMIT ?",
        [size],
    )
    count = 0
    for (document,) in selected:
        item = loads(document)
        raw = item["raw"]
        gid, cid, rid = raw["gameId"], raw.get("categoryId"), item["id"]
        category = gid + ":" + str(cid)
        category_raw = next(
            (c for c in metadata[gid].get("categories", []) if c.get("id") == cid), {}
        )
        categories[category] = {
            "key": category,
            "game_id": gid,
            "source_id": cid,
            "name": category_raw.get("name"),
        }
        timer_field, ns, alternate = select_time(raw, metadata[gid].get("game", {}))
        diagnostics["alternate_timer"] += alternate
        scope = [
            gid,
            cid,
            sorted(raw.get("valueIds", [])),
            raw.get("platformId"),
            raw.get("emulator"),
            metadata[gid].get("game", {}).get("defaultTimer"),
            timer_field,
        ]
        board = digest(scope)
        boards[board] = {
            "key": board,
            "category_key": category,
            "scope_json": dumps(scope),
        }
        used_games.add(gid)
        obsolete = (item.get("context") or {}).get("values", {}).get("obsolete")
        diagnostics["missing_time"] += ns is None
        diagnostics["unknown_obsolete"] += obsolete is None
        month = str(raw.get("performedAt") or "")[:7] or None
        put(
            out,
            "runs",
            rid,
            {
                "key": rid,
                "board_id": board,
                "status": raw.get("verified"),
                "month": month,
                "time_ns": ns,
                "eligible": raw.get("verified") == 1
                and obsolete is False
                and ns is not None
                and ns > 0,
            },
        )
        for position, pid in enumerate(raw.get("playerIds", [])):
            if pid in profiles:
                p = profiles[pid]["raw"]
                key = pid
                player = {
                    "key": key,
                    "name": p.get("name"),
                    "area": p.get("areaId") or None,
                    "resolved": True,
                }
            else:
                key = "unresolved:" + rid + ":" + str(position)
                player = {"key": key, "name": None, "area": None, "resolved": False}
                diagnostics["unresolved_participants"] += 1
            used_players[key] = player
            put(
                out,
                "run_players",
                rid + ":" + str(position),
                {"key": rid + ":" + str(position), "run_id": rid, "player_id": key},
            )
        count += 1
        if count % 10000 == 0:
            out.commit()
    if count != size:
        raise ValueError("Frozen input has fewer runs than requested")
    for dataset, values in [
        ("players", used_players.values()),
        ("boards", boards.values()),
        ("categories", categories.values()),
        (
            "games",
            (
                {"key": gid, "name": metadata[gid]["game"].get("name")}
                for gid in sorted(used_games)
            ),
        ),
    ]:
        for value in values:
            put(out, dataset, value["key"], value)
    out.commit()
    counts, hashes = {}, {}
    for dataset, n in out.execute(
        "SELECT dataset,count(*) FROM records GROUP BY dataset"
    ).fetchall():
        counts[dataset] = n
        h = hashlib.sha256()
        for value in rows(out, dataset):
            h.update(dumps(value).encode() + b"\n")
        hashes[dataset] = h.hexdigest()
    out.close()
    src.close()
    result = {
        "size": size,
        "counts": counts,
        "digests": hashes,
        "diagnostics": diagnostics,
        "normalization_seconds": time.monotonic() - started,
        "format_version": "analytics-relations-v2",
        "scope": "Technical comparison group: all valueIds, platform, emulator, game default timer and actual timer field. Prefer declared default, then an available time/igt/timeWithLoads; fallback is counted, never merged across fields. May split official boards. Experimental ranks only.",
        "team_policy": "Full experimental points to each resolved participant; one best per participant/group.",
        "source_HTTP": 0,
    }
    atomic_write(target / "input.json", result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--freeze", type=Path)
    parser.add_argument("--batch", default="lab1-popular500-20261030")
    args = parser.parse_args()
    if not args.freeze:
        parser.error("--freeze required")
    freeze(args.freeze, args.batch)


if __name__ == "__main__":
    main()
