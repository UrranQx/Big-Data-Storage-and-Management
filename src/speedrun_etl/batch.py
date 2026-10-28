"""Portable, resumable Airflow queue: fresh directory plus top N or all games."""
import argparse
from datetime import datetime, timezone
import os
from pathlib import Path
import re
import shutil
import sys
import time
from urllib.error import HTTPError

from .cli import api
from .collect import validate_conf
from .capacity import check_capacity
from .entity_ods import EntityPackage
from .entity_storage import ready
from .jsonutil import atomic_write, digest, dumps, file_lock, read, utcnow
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
    check_capacity(os.environ["ETL_STATE"])
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
    last_progress = 0
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
        if time.monotonic() - last_progress >= 60:
            from .progress import progress, render_collection
            print(render_collection(progress(collection)), flush=True)
            last_progress = time.monotonic()
        if "failed" in states.values():
            raise RuntimeError("Queue paused after failed DAG; inspect and resume its checkpoint before continuing")
        if all(v == "success" for v in states.values()):
            for kind in kinds:
                ready(collection + "-" + kind, role="runs" if kind == "runs" else "all")
            return
        time.sleep(15)


def save_status(root, phase, **details):
    atomic_write(root / "status.json", {"phase": phase, "updated_at": utcnow(), **details})


def reserve_session(state, root, baseline, collection, maximum, limit):
    """Conservative reservation: never begin a chain that could exceed the cap."""
    if collection:
        baseline.setdefault(collection,0)
    names = set(baseline) | ({collection} if collection else set())
    current = {}
    for name in names:
        path = state / 'collections' / name / 'budget.json'
        current[name] = read(path)['used'] if path.exists() else 0
    used = sum(current[name] - baseline.get(name, 0) for name in names)
    reserve = max(0, maximum-current.get(collection, 0)) if collection else 0
    atomic_write(root / 'session-budget.json', {'limit':limit,'used':used,'reserved_next':reserve,
        'next_collection_id':collection,'policy':'reserve_remaining_per_game_budget','updated_at':utcnow()})
    if used+reserve > limit:
        save_status(root,'session_paused',reason='HTTP session budget: insufficient reserve for the next chain',
                    http_used=used,http_limit=limit,reserved_next=reserve)
        return False
    return True


def result_state(result):
    if result.get("coverage") == "unavailable":
        if (result.get("diagnostic") == "no_full_game_categories_in_source_metadata"
                and result.get("partitions") == [] and result.get("wr") == []
                and not result.get("stop_reason") and not result.get("intentional_limit")):
            return "not_applicable"
        return "unavailable"
    partitions = result.get("partitions", [])
    return "partial" if result.get("stop_reason") or not partitions or any(
        p.get("coverage") != "verified_public_partitions" for p in partitions) else "complete"


def drift_retry_partitions(result):
    """Only known live-pagination gaps may be deferred after all stores are ready."""
    if result.get("stop_reason") or result.get("intentional_limit") or not result.get("partitions"):
        return None
    pending = []
    known_errors = {"Pagination changed within a section", "Boundary IDs changed on resume",
                    "Boundary IDs changed at end of section", "Repeated IDs across pages"}
    for section in result["partitions"]:
        if section.get("coverage") == "verified_public_partitions":
            continue
        if section.get("coverage") != "partial" or section.get("intentional_limit"):
            return None
        diagnostic = section.get("diagnostic")
        if diagnostic == "pagination_unstable_after_one_retry":
            errors = section.get("errors", [])
            if not errors or any(e.get("error") not in known_errors for e in errors):
                return None
        elif diagnostic == "pagination_changed_during_traversal":
            if not any(section.get("drift", {}).get(k) for k in (
                    "pagination_changed", "boundary_changed", "tail_shrank", "repeated_ids_across_pages")):
                return None
        else:
            return None
        if not section.get("category_id") or section.get("status_code") not in (0, 1, 2):
            return None
        pending.append(section)
    return pending or None


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
            from .ranking import game_classification
            if game_classification(raw) == "in_scope" and raw.get("url"):
                games[raw["id"]] = {k: raw.get(k) for k in ("id", "url", "name", "type", "runCount")}
        return [games[key] for key in sorted(games)]
    finally:
        package.db.close()


def counter_band(count):
    count = int(count)
    if count < 0:
        raise ValueError('Negative directory run count')
    return '0-100' if count <= 100 else '101-1000' if count <= 1000 else '1001-10000' if count <= 10000 else '>10000'


def stratified_selection(games, size):
    """Equal hash-selected samples in four counter bands; counters are not coverage."""
    groups = {name: [] for name in ('0-100', '101-1000', '1001-10000', '>10000')}
    for game in games:
        band = counter_band(game['runCount'])
        groups[band].append({**game, 'sampling_band': band})
    return [game for group in groups.values() for game in sorted(group, key=lambda g: digest(g['id']))[:size//4]]


def validate_catalog_input(state, snapshot, require_fresh=True):
    package = parent_package(state, snapshot)
    try:
        manifest = next(package.records('manifests'))
        if (manifest.get('input_config', {}).get('scope') != 'catalog'
                or manifest.get('source_result', {}).get('coverage') != 'verified_public_partitions'):
            raise ValueError('A verified complete directory snapshot is required')
        fetched = [datetime.fromisoformat(r['metadata']['fetched_at']) for r in package.records('responses')
                   if r['metadata']['endpoint'] == 'GetGameList' and r['metadata']['http_status'] == 200]
        if not fetched:
            raise ValueError('Catalog has no successful source timestamps')
        age = (datetime.now(timezone.utc)-max(fetched)).total_seconds()
        if require_fresh and not -300 <= age <= 7*86400:
            raise ValueError('New queue requires a catalog observed within seven days')
    finally:
        package.db.close()


def reconcile_selection(batch_id, games, parent, state, refresh=False):
    """Keep started chains immutable; retire only reviewed pending exclusions."""
    from psycopg.types.json import Jsonb
    selected = {g["id"]: g for g in games}
    with pg_connect(True) as conn:
        previous = conn.execute("SELECT game_id,collection_id,state,details FROM entity_batch_queue WHERE batch_id=%s", [batch_id]).fetchall()
        active_ids = {r[0] for r in previous if r[2] != "excluded"}
        if previous and active_ids != set(selected) and not refresh:
            raise ValueError("Selection changed; inspect new ranking, then resume with --refresh-selection")
        for game_id, collection, status, details in previous:
            if game_id not in selected:
                if status not in ("pending", "excluded"):
                    raise ValueError("Cannot remove a started game from the queue")
                conn.execute("UPDATE entity_batch_queue SET state='excluded',updated_at=now() WHERE batch_id=%s AND game_id=%s", [batch_id, game_id])
            elif status in ("running", "failed", "partial", "unavailable"):
                path = Path(state) / "snapshots" / (collection + "-games_metadata") / "config.json"
                saved = read(path) if path.exists() else None
                if not saved or saved.get("game_id") != game_id or saved.get("game") != selected[game_id]["url"]:
                    raise ValueError("Started game identity/config requires review")
                details = {**details, "selection_snapshot_id": saved["selection_snapshot_id"]}
                conn.execute("UPDATE entity_batch_queue SET details=%s WHERE batch_id=%s AND game_id=%s", [Jsonb(details), batch_id, game_id])
        with conn.cursor() as cursor:
            cursor.executemany("""INSERT INTO entity_batch_queue(batch_id,position,game_id,collection_id,details)
                VALUES(%s,%s,%s,%s,%s) ON CONFLICT(batch_id,game_id) DO UPDATE SET
                position=excluded.position,
                details=CASE WHEN entity_batch_queue.state IN ('pending','excluded') THEN excluded.details ELSE entity_batch_queue.details END,
                state=CASE WHEN entity_batch_queue.state='excluded' THEN 'pending' ELSE entity_batch_queue.state END""",
                [(batch_id, i, g["id"], batch_id + "-g-" + g["id"], Jsonb({**g, "selection_snapshot_id": parent})) for i,g in enumerate(games, 1)])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("run", "status"))
    parser.add_argument("--collection", required=True)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--top", type=int, default=10)
    mode.add_argument("--all", action="store_true")
    mode.add_argument("--sample", type=int, choices=(4, 8, 12, 16, 20), help="Stratified resource pilot, up to five games per counter band")
    parser.add_argument("--catalog-snapshot", help="Explicit verified input catalog; do not fetch the directory again")
    parser.add_argument("--game-request-budget", type=int, default=5000)
    parser.add_argument('--session-request-budget',type=int,default=9000,
                        help='Maximum new HTTP attempts per driver session; reserves the remaining game budget before starting')
    parser.add_argument("--hours", type=float, default=8, help="Stop starting new games after this time; finish the active game")
    parser.add_argument("--log", action="store_true")
    parser.add_argument("--refresh-selection", action="store_true", help="Reviewed repair of pending selection; never replace a started chain")
    parser.add_argument("--continue-on-drift", action="store_true",
                        help="Keep known pagination gaps in the retry list and continue after all three stores acknowledge them")
    parser.add_argument("--runs-strategy", choices=("standard", "date-large"),
                        help="Explicit strategy for a new batch; date-large splits large runner lists by dates")
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,40}", args.collection) or not 1 <= args.top <= 100 or args.hours <= 0:
        parser.error("Invalid batch ID, top count or session duration")
    if not 100 <= args.game_request_budget <= 10000:
        parser.error("Per-game budget must be 100..10000")
    if args.session_request_budget < args.game_request_budget:
        parser.error('Session HTTP budget must be at least the per-game budget')
    if args.catalog_snapshot and not re.fullmatch(r'[A-Za-z0-9_-]{1,110}-games_metadata', args.catalog_snapshot):
        parser.error('Invalid catalog snapshot ID')
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
    conf = {"mode": "sample" if args.sample else "all" if args.all else "top", "top": args.top if not (args.all or args.sample) else None,
            "game_request_budget": args.game_request_budget}
    if args.sample:
        conf['sample_size'] = args.sample
    if args.catalog_snapshot:
        conf['catalog_snapshot_id'] = args.catalog_snapshot
    elif (root / 'config.json').exists() and read(root / 'config.json').get('catalog_snapshot_id'):
        conf['catalog_snapshot_id'] = read(root / 'config.json')['catalog_snapshot_id']
    if args.runs_strategy:
        conf["run_partition_strategy"] = "date_large_v1" if args.runs_strategy == "date-large" else "standard"
    elif (root / "config.json").exists():
        previous = read(root / "config.json")
        if "run_partition_strategy" in previous:
            conf["run_partition_strategy"] = previous["run_partition_strategy"]
    with file_lock(root / "driver.lock"):
        try:
            run_queue(args, state, root, conf)
        except Exception as error:
            previous = read(root / "status.json") if (root / "status.json").exists() else {}
            save_status(root, "failed", collection_id=previous.get("collection_id"),
                        error=type(error).__name__ + ": " + str(error)[:300])
            raise


def run_queue(args, state, root, conf):
    check_capacity(state, require_host=args.all)
    path = root / "config.json"
    if path.exists() and read(path) != conf:
        raise ValueError("Batch scope cannot change; use the same options or a new collection ID")
    atomic_write(path, conf)
    atomic_write(root / "runtime-policy.json", {"continue_on_drift": args.continue_on_drift,
                                               'session_request_budget':args.session_request_budget,
                                               "updated_at": utcnow()})
    session_end = time.monotonic() + args.hours * 3600
    with pg_connect(True) as conn:
        queued = {r[0] for r in conn.execute('SELECT collection_id FROM entity_batch_queue WHERE batch_id=%s',[args.collection])}
    baseline = {p.parent.name:read(p)['used'] for p in (state/'collections').glob('*/budget.json')
                if p.parent.name.startswith(args.collection+'-') or p.parent.name in queued}
    if conf.get('catalog_snapshot_id'):
        parent = conf['catalog_snapshot_id']
        validate_catalog_input(state, parent, require_fresh=not (root/'selection.json').exists())
        save_status(root, 'catalog_reused', input_snapshot_id=parent)
        print('Verified catalog reused:', parent, flush=True)
    else:
        catalog = args.collection + "-catalog"
        save_status(root, "catalog", collection_id=catalog)
        if not reserve_session(state,root,baseline,catalog,650,args.session_request_budget):return
        start(catalog, {"scope": "catalog", "request_budget": 650})
        wait(catalog, ("games_metadata",))
        parent = catalog + "-games_metadata"
    if conf['mode'] == 'top':
        ranking = args.collection + "-ranking-v2"
        save_status(root, "ranking", collection_id=ranking)
        if not reserve_session(state,root,baseline,ranking,500,args.session_request_budget):return
        start(ranking, {"scope": "ranking", "catalog_snapshot_id": parent, "top_n": args.top,
                        "ranking_policy": "exclude_ce_v1"})
        wait(ranking, ("games_metadata",))
        parent = ranking + "-games_metadata"
    games = selection(state, parent, conf['mode'] != 'top')
    if conf['mode'] == 'sample':
        games = stratified_selection(games, args.sample)
    if not games or (conf['mode'] == 'top' and len(games) != args.top):
        raise ValueError("Verified selection does not contain the requested number of games")
    reconcile_selection(args.collection, games, parent, state, refresh=args.refresh_selection)
    atomic_write(root / "selection.json", {"snapshot_id": parent, "policy": "stratified_run_count_v1" if args.sample else "exclude_ce_v1", "game_ids": [g["id"] for g in games]})
    for position, game in enumerate(games, 1):
        with pg_connect(True) as conn:
            collection, status, details = conn.execute("SELECT collection_id,state,details FROM entity_batch_queue WHERE batch_id=%s AND game_id=%s", [args.collection,game["id"]]).fetchone()
        if status in ("complete", "not_applicable"):
            continue
        if time.monotonic() >= session_end:
            save_status(root, "session_paused")
            print("Session time reached; queue preserved. Run the same command to continue.", flush=True)
            return
        save_status(root, "game", collection_id=collection, position=position, total=len(games), game=game["name"])
        print(f"Batch: game={position}/{len(games)} name={game['name']} id={game['id']}", flush=True)
        reuse_partial = status == "partial" and args.continue_on_drift
        if not reuse_partial:
            if not reserve_session(state,root,baseline,collection,args.game_request_budget,args.session_request_budget):return
            with pg_connect(True) as conn:
                conn.execute("""UPDATE entity_batch_queue SET state='running',updated_at=now(),
                    details=details || jsonb_build_object('first_started_at',COALESCE(details->>'first_started_at',%s))
                    WHERE batch_id=%s AND game_id=%s""", [utcnow(),args.collection,game["id"]])
        try:
            if not reuse_partial:
                start(collection, {"scope": "game", "game": game["url"], "game_id": game["id"],
                    "selection_snapshot_id": details["selection_snapshot_id"], "request_budget": args.game_request_budget,
                    **({"run_partition_strategy":conf["run_partition_strategy"]} if "run_partition_strategy" in conf else {})})
            wait(collection, ("games_metadata", "runs", "players"))
            package = EntityPackage(state / "snapshots" / (collection + "-runs") / "entity-package")
            result = next(package.records("manifests"))["source_result"]
            package.db.close()
            status = result_state(result)
            retry = drift_retry_partitions(result) if status == "partial" else None
        except Exception:
            with pg_connect(True) as conn:
                conn.execute("UPDATE entity_batch_queue SET state='failed',updated_at=now() WHERE batch_id=%s AND game_id=%s", [args.collection,game["id"]])
            raise
        with pg_connect(True) as conn:
            from psycopg.types.json import Jsonb
            retry_details = {"coverage_retry": {"state": "pending_review",
                "input_snapshot_id": collection + "-runs", "partitions": retry,
                "recorded_at": utcnow()}} if retry else {}
            conn.execute("UPDATE entity_batch_queue SET state=%s,details=details || %s,updated_at=now() WHERE batch_id=%s AND game_id=%s",
                         [status,Jsonb(retry_details),args.collection,game["id"]])
        if status != "complete":
            if status == "not_applicable":
                print("Game has no full-game categories; scope result retained in three stores, continuing", flush=True)
                continue
            if args.continue_on_drift and retry:
                print(f"Game coverage=partial; {len(retry)} drifting sections retained for retry; "
                      "three stores acknowledged, continuing to the next game", flush=True)
                continue
            save_status(root, "coverage_paused", collection_id=collection, coverage=status,
                reason=result.get("stop_reason") or result.get("diagnostic") or "Review partition coverage")
            print(f"Game coverage={status}; data retained, queue paused for review", flush=True)
            return
    with pg_connect(True) as conn:
        totals = dict(conn.execute("SELECT state,count(*) FROM entity_batch_queue WHERE batch_id=%s AND state!='excluded' GROUP BY state",
                                   [args.collection]).fetchall())
    if not reserve_session(state,root,baseline,None,0,args.session_request_budget):return
    phase = "complete" if sum(totals.get(s, 0) for s in ("complete", "not_applicable")) == len(games) else "complete_with_gaps"
    save_status(root, phase, total=len(games), states=totals)
    print(f"Batch traversal finished: {totals}; source gaps remain in coverage_retry and manifests", flush=True)


if __name__ == "__main__":
    main()
