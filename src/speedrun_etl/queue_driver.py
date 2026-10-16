"""Bounded top-five Airflow driver, resumable via control DB and deterministic IDs."""
import argparse
from datetime import datetime, timezone
import os
from pathlib import Path
import shutil
import sys
import time
from urllib.error import HTTPError

from .cli import api
from .jsonutil import atomic_write, file_lock, utcnow, read
from .ods import Package
from .storage import pg_connect, require_ready

STOP_NEW = datetime.fromisoformat("2026-10-16T20:00:00+00:00")


def manifest(state, snapshot):
    return next(Package(state / "snapshots" / snapshot / "package").records("manifests"))


def token():
    return api("POST", "/auth/token", {"username": "admin", "password": os.environ["AIRFLOW_ADMIN_PASSWORD"]})["access_token"]


def start(collection, conf):
    try:
        api("POST", "/api/v2/dags/games_metadata/dagRuns", {
            "dag_run_id": collection + "-games_metadata", "logical_date": utcnow(),
            "conf": {"collection_id": collection, **conf}}, token())
    except HTTPError as error:
        if error.code != 409:
            raise


def wait(collection, kinds):
    previous = None
    while True:
        auth = token()
        states = {}
        for kind in kinds:
            try:
                response = api("GET", f"/api/v2/dags/{kind}/dagRuns/{collection}-{kind}", token=auth)
                states[kind] = response["state"]
            except HTTPError as error:
                if error.code != 404:
                    raise
                states[kind] = "not_created"
        if states != previous:
            print(utcnow(), collection, states, flush=True)
            previous = states
        if "failed" in states.values():
            raise RuntimeError(f"A DAG failed in {collection}; queue paused, inspect task logs")
        if all(value == "success" for value in states.values()):
            for kind in kinds:
                require_ready(collection + "-" + kind)
            return
        time.sleep(30)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="catalog-20261016-001")
    parser.add_argument("--minecraft", default="minecraft-20261016-001")
    parser.add_argument("--queue", default="top5-20261016-001")
    parser.add_argument("--log", action="store_true")
    args = parser.parse_args()
    import re
    if not all(re.fullmatch(r"[a-zA-Z0-9_-]{1,80}", value) for value in (args.catalog, args.minecraft, args.queue)):
        parser.error("Invalid collection ID")
    state = Path(os.environ["ETL_STATE"])
    root = state / "queues" / args.queue
    root.mkdir(parents=True, exist_ok=True)
    if args.log:
        sys.stdout = sys.stderr = (root / "driver.log").open("a", buffering=1)
    with file_lock(root / "driver.lock"):
        atomic_write(root / "process.json", {"pid": os.getpid(), "started_at": utcnow()})
        with pg_connect(True) as conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS collection_queue (
                queue_id text NOT NULL, position integer NOT NULL, game_id text NOT NULL,
                collection_id text NOT NULL, state text NOT NULL, details jsonb NOT NULL,
                updated_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(queue_id,game_id))""")
        wait(args.catalog, ("games_metadata",))
        wait(args.minecraft, ("games_metadata", "runs", "players"))
        runs = manifest(state, args.minecraft + "-runs")["result"]
        if runs.get("stop_reason") or not runs.get("partitions") or any(
            p.get("coverage") != "verified_public_partitions" for p in runs["partitions"]
        ):
            raise RuntimeError("Minecraft partition checks are incomplete; top-five launch is paused")
        ranking = args.queue + "-ranking"
        if datetime.now(timezone.utc) >= STOP_NEW:
            print("Time reserved for benchmark/report; ranking not started", flush=True)
            return
        start(ranking, {"scope": "ranking", "catalog_snapshot_id": args.catalog + "-games_metadata"})
        wait(ranking, ("games_metadata",))
        selected = manifest(state, ranking + "-games_metadata")["result"]["top5"]
        from psycopg.types.json import Jsonb
        for position, game in enumerate(selected, 1):
            collection = args.minecraft if game["id"] == "j1npme6p" else args.queue + "-" + game["id"]
            with pg_connect(True) as conn:
                conn.execute("""INSERT INTO collection_queue(queue_id,position,game_id,collection_id,state,details)
                    VALUES(%s,%s,%s,%s,'pending',%s) ON CONFLICT(queue_id,game_id) DO NOTHING""",
                    [args.queue, position, game["id"], collection, Jsonb(game)])
        for game in selected:
            with pg_connect(True) as conn:
                collection, status = conn.execute("SELECT collection_id,state FROM collection_queue WHERE queue_id=%s AND game_id=%s",
                                                 [args.queue, game["id"]]).fetchone()
            if status == "complete":
                continue
            if collection != args.minecraft:
                disk = shutil.disk_usage(state)
                host_path = state / "host_capacity.json"
                host = read(host_path) if host_path.exists() else None
                if host is None or host["free_bytes"] < max(10 * 1024**3, host["total_bytes"] * 0.2):
                    print("Queue paused: Docker host drive fails the agreed 10 GiB / 20% reserve; "
                          "catalog, Minecraft and ranking are retained. Refresh host_capacity.json before resuming.", flush=True)
                    return
                if datetime.now(timezone.utc) >= STOP_NEW or disk.free < max(10 * 1024**3, disk.total * 0.2):
                    print("Queue paused for time/disk reserve", flush=True)
                    return
                start(collection, {"scope": "ranked_game", "game": game["url"], "game_id": game["id"],
                                   "ranking_snapshot_id": ranking + "-games_metadata"})
            with pg_connect(True) as conn:
                conn.execute("UPDATE collection_queue SET state='running',updated_at=now() WHERE queue_id=%s AND game_id=%s",
                             [args.queue, game["id"]])
            try:
                wait(collection, ("games_metadata", "runs", "players"))
                result = manifest(state, collection + "-runs")["result"]
                status = "partial" if result.get("stop_reason") else "complete"
            except Exception:
                with pg_connect(True) as conn:
                    conn.execute("UPDATE collection_queue SET state='failed',updated_at=now() WHERE queue_id=%s AND game_id=%s",
                                 [args.queue, game["id"]])
                raise
            with pg_connect(True) as conn:
                conn.execute("UPDATE collection_queue SET state=%s,updated_at=now() WHERE queue_id=%s AND game_id=%s",
                             [status, args.queue, game["id"]])
            if status == "partial":
                print("Time/request budget reached; queue paused for review", flush=True)
                return
        print("Top-five queue complete", flush=True)


if __name__ == "__main__":
    main()
