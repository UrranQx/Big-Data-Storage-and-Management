"""Local demonstration commands, executed inside the Airflow container."""
import argparse
import json
import os
from pathlib import Path
from urllib.request import Request, urlopen
from datetime import datetime, timezone

from .collect import location, snapshot_id, validate_conf
from .jsonutil import digest, dumps, read
from .model import V2, V3, version, namespace, kinds, dag_run_id


def api(method, path, body=None, token=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    request = Request("http://api-server:8080" + path, data=json.dumps(body).encode() if body is not None else None, method=method, headers=headers)
    with urlopen(request, timeout=60) as response:
        return json.load(response)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["start-game", "start-e2e", "start-half-life", "start-minecraft", "start-catalog", "status", "evidence", "replay", "history"])
    parser.add_argument("--collection", required=True)
    parser.add_argument("--request-budget", type=int)
    parser.add_argument("--entity", default="run")
    parser.add_argument("--id")
    identity = parser.add_mutually_exclusive_group()
    identity.add_argument("--game", help="Exact speedrun.com game URL slug; start-game only")
    identity.add_argument("--game-id", help="Exact source game ID; start-game only")
    parser.add_argument("--metadata-only", action="store_true", help="Collect only the selected game card/metadata in three ODS")
    strategy = parser.add_mutually_exclusive_group()
    strategy.add_argument("--runs-strategy", choices=("standard", "date-large"), default="standard")
    strategy.add_argument('--accelerate-dates',action='store_true',help='Opt in to date subdivision; off by default')
    parser.add_argument('--data-model',choices=(V2,V3),default=V3)
    parser.add_argument('--smoke',action='store_true',help='Explicit test: first full-game category, at most two pages per status, one WR page')
    parser.add_argument('--smoke-category-id',help='Explicit category within a smoke test; must exist in fresh metadata')
    args = parser.parse_args()
    if args.accelerate_dates:args.runs_strategy='date-large'
    start_conf = {
        "start-half-life": {"scope": "full_game", "game": "half-life", "request_budget": args.request_budget or 300},
        "start-minecraft": {"scope": "full_game", "game": "mc", "request_budget": args.request_budget or 1500},
        "start-catalog": {"scope": "catalog", "request_budget": args.request_budget or 650},
    }
    if args.command == "start-game":
        if not (args.game or args.game_id):
            parser.error("start-game requires --game or --game-id")
        start_conf["start-game"]={"scope":"single_game", "request_budget":args.request_budget or (30 if args.metadata_only else 2500),
            **({"game":args.game} if args.game else {"game_id":args.game_id}),"metadata_only":args.metadata_only,
            "run_partition_strategy":"date_large_v1" if args.runs_strategy=="date-large" else "standard"}
        if args.smoke:start_conf['start-game']['smoke']=True
        if args.smoke_category_id:start_conf['start-game']['smoke_category_id']=args.smoke_category_id
    elif args.game or args.game_id or args.metadata_only or args.runs_strategy != "standard" or args.smoke or args.smoke_category_id:
        parser.error("Game identity, metadata-only and runs-strategy options require start-game")
    state = os.environ["ETL_STATE"]
    saved_conf = Path(state)/'snapshots'/(args.collection+'-games_metadata')/'config.json'
    conf = (read(saved_conf) if not args.command.startswith('start-') and saved_conf.exists() else
            validate_conf({"collection_id":args.collection,**start_conf.get(args.command,{}),
                           **({'data_model':args.data_model} if args.command.startswith('start-') else {})}))
    if args.command.startswith("start-") or args.command == "status":
        if args.command.startswith("start-"):
            from .capacity import check_capacity
            check_capacity(state)
        token = api("POST", "/auth/token", {"username": "admin", "password": os.environ["AIRFLOW_ADMIN_PASSWORD"]})["access_token"]
        if args.command.startswith("start-"):
            from .batch import start
            start(args.collection,conf)
            print(json.dumps({'dag_id':'games_metadata','dag_run_id':dag_run_id(conf,'games_metadata'),'state':'submitted_or_existing'}))
        else:
            for kind in kinds(conf):
                response = api("GET", f"/api/v2/dags/{kind}/dagRuns?limit=10&order_by=-start_date", token=token)
                print(json.dumps({"dag": kind, "runs": [{k: r.get(k) for k in ("dag_run_id", "state", "start_date", "end_date")} for r in response["dag_runs"]]}))
    elif args.command == "evidence":
        result = {"collection_id": args.collection, "budget": read(Path(state) / "collections" / args.collection / "budget.json"), "snapshots": {}}
        for kind in kinds(conf):
            root = location(state, conf, kind)
            if not (root / "config.json").exists():
                continue
            reference = read(root / "extracted.json")
            from .entity_ods import EntityPackage
            package = EntityPackage(reference["package_uri"], role=reference["role"])
            result["snapshots"][kind] = {"verification": read(root / "verified.json"),
                "manifest": list(package.records("manifests"))}
            package.db.close()
        print(dumps(result))
    elif args.command == "replay":
        from .entity_ods import EntityPackage
        from .entity_storage import load
        for kind in kinds(conf):
            if not (location(state, conf, kind) / "config.json").exists():
                continue
            reference = read(location(state, conf, kind) / "extracted.json")
            package = EntityPackage(reference["package_uri"], role=reference["role"])
            for store in ("postgres", "mongo", "garage"):
                result = load(store, package, reference["snapshot_id"], namespace=reference.get('namespace','entity_v2'),role=reference["role"])
                print(dumps({"snapshot": snapshot_id(conf, kind), "store": store, "digest": result["digest"]}))
            package.db.close()
    else:
        from .entity_storage import read_entity
        from .entity_ods import EntityPackage
        if not args.id:
            parser.error("history requires --id")
        dataset = {"run": "runs", "player": "players", "game": "games", "wr_history": "wr_history"}.get(args.entity)
        if not dataset:
            parser.error("Unknown entity type")
        kind = ("players" if dataset == "players" else "games_metadata" if dataset == "games" else
                'wr_history' if dataset=='wr_history' and version(conf)==V3 else "runs")
        reference = read(location(state, conf, kind) / "extracted.json")
        package = EntityPackage(reference["package_uri"], role=reference["role"])
        summary = package.summary()
        package.db.close()
        for name in ("postgres", "mongo", "garage"):
            options = {'namespace':reference.get('namespace','entity_v2'),'role':reference['role'],'format_version':reference['format_version']}
            rows = read_entity(name, dataset, args.id, reference["snapshot_id"], summary, **options)
            checks = read_entity(name, "checks", args.id, reference["snapshot_id"], summary, **options)
            value = {"versions": rows, "checks": checks}
            print(dumps({"store": name, "digest": digest(value), **value}))


if __name__ == "__main__":
    main()
