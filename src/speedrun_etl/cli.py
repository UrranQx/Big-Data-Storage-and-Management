"""Local demonstration commands, executed inside the Airflow container."""
import argparse
import json
import os
from pathlib import Path
from urllib.request import Request, urlopen
from datetime import datetime, timezone

from .collect import location, snapshot_id, validate_conf
from .jsonutil import digest, dumps, read


def api(method, path, body=None, token=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    request = Request("http://api-server:8080" + path, data=json.dumps(body).encode() if body is not None else None, method=method, headers=headers)
    with urlopen(request, timeout=60) as response:
        return json.load(response)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["start-e2e", "start-half-life", "start-minecraft", "start-catalog", "status", "evidence", "replay", "history"])
    parser.add_argument("--collection", required=True)
    parser.add_argument("--request-budget", type=int)
    parser.add_argument("--entity", default="run")
    parser.add_argument("--id")
    args = parser.parse_args()
    start_conf = {
        "start-half-life": {"scope": "full_game", "game": "half-life", "request_budget": args.request_budget or 300},
        "start-minecraft": {"scope": "full_game", "game": "mc", "request_budget": args.request_budget or 1500},
        "start-catalog": {"scope": "catalog", "request_budget": args.request_budget or 650},
    }
    conf = validate_conf({"collection_id": args.collection,
                          **start_conf.get(args.command, {})})
    state = os.environ["ETL_STATE"]
    if args.command.startswith("start-") or args.command == "status":
        if args.command.startswith("start-"):
            import shutil
            disk = shutil.disk_usage(state)
            if disk.free < max(10 * 1024**3, disk.total * 0.2):
                raise RuntimeError("Insufficient free staging volume space: collection paused")
        token = api("POST", "/auth/token", {"username": "admin", "password": os.environ["AIRFLOW_ADMIN_PASSWORD"]})["access_token"]
        if args.command.startswith("start-"):
            sid = snapshot_id(conf, "games_metadata")
            response = api("POST", "/api/v2/dags/games_metadata/dagRuns", {"dag_run_id": sid,
                "logical_date": datetime.now(timezone.utc).isoformat(), "conf": conf}, token)
            print(json.dumps({k: response.get(k) for k in ("dag_id", "dag_run_id", "state")}))
        else:
            for kind in ("games_metadata", "runs", "players"):
                response = api("GET", f"/api/v2/dags/{kind}/dagRuns?limit=10&order_by=-start_date", token=token)
                print(json.dumps({"dag": kind, "runs": [{k: r.get(k) for k in ("dag_run_id", "state", "start_date", "end_date")} for r in response["dag_runs"]]}))
    elif args.command == "evidence":
        from .ods import Package
        result = {"collection_id": args.collection, "budget": read(Path(state) / "collections" / args.collection / "budget.json"), "snapshots": {}}
        for kind in ("games_metadata", "runs", "players"):
            root = location(state, conf, kind)
            if not (root / "config.json").exists():
                continue
            result["snapshots"][kind] = {"verification": read(root / "verified.json"),
                "manifest": list(Package(root / "package").records("manifests"))}
        print(dumps(result))
    elif args.command == "replay":
        from .ods import Package
        from .storage import load_package
        for kind in ("games_metadata", "runs", "players"):
            if not (location(state, conf, kind) / "config.json").exists():
                continue
            for store in ("postgres", "mongo", "garage"):
                result = load_package(store, Package(location(state, conf, kind) / "package"), snapshot_id(conf, kind))
                print(dumps({"snapshot": snapshot_id(conf, kind), "store": store, "digest": result["digest"]}))
    else:
        from .storage import STORES
        if not args.id:
            parser.error("history requires --id")
        for name, cls in STORES.items():
            with cls() as store:
                history = store.history(args.entity, args.id)
            print(dumps({"store": name, "count": len(history), "digest": digest(history), "history": history}))


if __name__ == "__main__":
    main()
