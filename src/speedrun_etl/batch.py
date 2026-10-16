"""Portable, resumable Airflow queue: fresh directory plus top N or all games."""
import argparse
from datetime import datetime, timezone
import os
from pathlib import Path
import re
import sys
import time
from urllib.error import HTTPError

from .cli import api
from .collect import validate_conf
from .entity_ods import EntityPackage
from .entity_storage import ready
from .jsonutil import atomic_write, dumps, file_lock, read, utcnow
from .storage import pg_connect


def token():
    return api("POST", "/auth/token", {"username": "admin", "password": os.environ["AIRFLOW_ADMIN_PASSWORD"]})["access_token"]


def parent_package(state, snapshot):
    root = Path(state) / "snapshots" / snapshot
    ref = read(root / "extracted.json")
    ready(snapshot, role=ref["role"], expected=ref["digest"])
    return EntityPackage(ref["package_uri"])


def validate_selection(state, conf):
    parent = conf.get("catalog_snapshot_id") or conf["selection_snapshot_id"]
    package = parent_package(state, parent)
    try:
        if conf["scope"] == "ranking":
            manifest = next(package.records("manifests"))
            if manifest["input_config"]["scope"] != "catalog":
                raise ValueError("Ranking requires a directory snapshot")
            return
        manifest = next(package.records("manifests"))
        result = manifest.get("source_result") or {}
        selected = result.get("selected")
        if selected is not None:
            match = any(g["id"] == conf["game_id"] and g["url"] == conf["game"] for g in selected)
        else:
            rows = package.db.execute("SELECT document FROM records WHERE dataset='games' AND json_extract(document,'$.source_id')=?", [conf["game_id"]])
            from .jsonutil import loads
            match = any(package.decode(loads(r[0])["raw"]).get("url") == conf["game"] for r in rows)
        if not match:
            raise ValueError("Game ID/URL absent from the exact verified selection snapshot")
    finally:
        package.db.close()


def start(collection, conf):
    conf = validate_conf({"collection_id": collection, **conf})
    auth = token()
    try:
        api("POST", "/api/v2/dags/games_metadata/dagRuns", {
            "dag_run_id": collection + "-games_metadata", "logical_date": utcnow(), "conf": conf}, auth)
    except HTTPError as error:
        if error.code != 409:
            raise
        existing = api("GET", f"/api/v2/dags/games_metadata/dagRuns/{collection}-games_metadata", token=auth)
        if validate_conf(existing["conf"]) != conf:
            raise ValueError("Existing DAG run has another scope")


def wait(collection, kinds):
    previous = None
    while True:
        states = {}
        auth = token()
        for kind in kinds:
            try:
                states[kind] = api("GET", f"/api/v2/dags/{kind}/dagRuns/{collection}-{kind}", token=auth)["state"]
            except HTTPError as error:
                if error.code != 404:
                    raise
                states[kind] = "not_created"
        if states != previous:
            print(utcnow(), collection, states, flush=True)
            previous = states
        if "failed" in states.values():
            raise RuntimeError("Queue paused after failed DAG; inspect and resume its checkpoint before continuing")
        if all(v == "success" for v in states.values()):
            for kind in kinds:
                ready(collection + "-" + kind, role="runs" if kind == "runs" else "all")
            return
        time.sleep(15)


def selection(state, snapshot, all_games):
    package = parent_package(state, snapshot)
    try:
        if not all_games:
            return next(package.records("manifests"))["source_result"]["selected"]
        games = {}
        for check in package.records("checks"):
            if check["dataset"] != "games" or check["projection"] != "game_card":
                continue
            row = package.get("games", check["last_seen"]["version_key"])
            raw = package.decode(row["raw"])
            if raw.get("type") == "game" and raw.get("url"):
                games[raw["id"]] = {k: raw.get(k) for k in ("id", "url", "name", "type")}
        return [games[key] for key in sorted(games)]
    finally:
        package.db.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("run", "status"))
    parser.add_argument("--collection", required=True)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--top", type=int, default=10)
    mode.add_argument("--all", action="store_true")
    parser.add_argument("--game-request-budget", type=int, default=5000)
    parser.add_argument("--hours", type=float, default=8, help="Stop starting new games after this time; finish the active game")
    parser.add_argument("--log", action="store_true")
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,40}", args.collection) or not 1 <= args.top <= 100 or args.hours <= 0:
        parser.error("Invalid batch ID, top count or session duration")
    if not 100 <= args.game_request_budget <= 10000:
        parser.error("Per-game budget must be 100..10000")
    with pg_connect(True) as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS entity_batch_queue (
            batch_id text NOT NULL,position integer NOT NULL,game_id text NOT NULL,collection_id text NOT NULL,
            state text NOT NULL DEFAULT 'pending',details jsonb NOT NULL,updated_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY(batch_id,game_id))""")
    if args.command == "status":
        with pg_connect(True) as conn:
            rows = conn.execute("SELECT position,game_id,collection_id,state,details FROM entity_batch_queue WHERE batch_id=%s ORDER BY position", [args.collection]).fetchall()
        for row in rows:
            print(dumps(dict(zip(("position", "game_id", "collection_id", "state", "details"), row))))
        return
    state = Path(os.environ["ETL_STATE"])
    root = state / "batches" / args.collection
    root.mkdir(parents=True, exist_ok=True)
    if args.log:
        sys.stdout = sys.stderr = (root / "driver.log").open("a", buffering=1)
    conf = {"mode": "all" if args.all else "top", "top": args.top if not args.all else None,
            "game_request_budget": args.game_request_budget}
    with file_lock(root / "driver.lock"):
        path = root / "config.json"
        if path.exists() and read(path) != conf:
            raise ValueError("Batch scope cannot change; use the same options or a new collection ID")
        atomic_write(path, conf)
        session_end = time.monotonic() + args.hours * 3600
        catalog = args.collection + "-catalog"
        start(catalog, {"scope": "catalog", "request_budget": 650})
        wait(catalog, ("games_metadata",))
        parent = catalog + "-games_metadata"
        if not args.all:
            ranking = args.collection + "-ranking"
            start(ranking, {"scope": "ranking", "catalog_snapshot_id": parent, "top_n": args.top})
            wait(ranking, ("games_metadata",))
            parent = ranking + "-games_metadata"
        games = selection(state, parent, args.all)
        if not games or (not args.all and len(games) != args.top):
            raise ValueError("Verified selection does not contain the requested number of games")
        from psycopg.types.json import Jsonb
        with pg_connect(True) as conn:
            with conn.cursor() as cursor:
                cursor.executemany("""INSERT INTO entity_batch_queue(batch_id,position,game_id,collection_id,details)
                    VALUES(%s,%s,%s,%s,%s) ON CONFLICT(batch_id,game_id) DO NOTHING""",
                    [(args.collection, i, g["id"], args.collection + "-g-" + g["id"], Jsonb(g)) for i,g in enumerate(games, 1)])
        for position, game in enumerate(games, 1):
            with pg_connect(True) as conn:
                collection, status = conn.execute("SELECT collection_id,state FROM entity_batch_queue WHERE batch_id=%s AND game_id=%s", [args.collection,game["id"]]).fetchone()
            if status == "complete":
                continue
            if time.monotonic() >= session_end:
                print("Session time reached; queue preserved. Run the same command to continue.", flush=True)
                return
            print(f"Batch: game={position}/{len(games)} name={game['name']} id={game['id']}", flush=True)
            with pg_connect(True) as conn:
                conn.execute("UPDATE entity_batch_queue SET state='running',updated_at=now() WHERE batch_id=%s AND game_id=%s", [args.collection,game["id"]])
            try:
                start(collection, {"scope": "game", "game": game["url"], "game_id": game["id"],
                    "selection_snapshot_id": parent, "request_budget": args.game_request_budget})
                wait(collection, ("games_metadata", "runs", "players"))
                package = EntityPackage(state / "snapshots" / (collection + "-runs") / "entity-package")
                result = next(package.records("manifests"))["source_result"]
                package.db.close()
                status = "partial" if result.get("stop_reason") or any(
                    p.get("coverage") != "verified_public_partitions" for p in result.get("partitions", [])) else "complete"
            except Exception:
                with pg_connect(True) as conn:
                    conn.execute("UPDATE entity_batch_queue SET state='failed',updated_at=now() WHERE batch_id=%s AND game_id=%s", [args.collection,game["id"]])
                raise
            with pg_connect(True) as conn:
                conn.execute("UPDATE entity_batch_queue SET state=%s,updated_at=now() WHERE batch_id=%s AND game_id=%s", [status,args.collection,game["id"]])
            if status == "partial":
                print("Per-game budget reached; partial data retained, queue paused for review", flush=True)
                return
        print("Batch complete; source coverage limitations remain in each manifest", flush=True)


if __name__ == "__main__":
    main()
