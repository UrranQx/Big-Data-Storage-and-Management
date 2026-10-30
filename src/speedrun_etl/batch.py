"""Portable, resumable Airflow queue: fresh directory plus top N or all games."""
import argparse
from contextlib import ExitStack, redirect_stderr, redirect_stdout
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
from .model import V2, V3, version, namespace, kinds as pipeline_kinds, role, dag_run_id


def token():
    return api("POST", "/auth/token", {"username": "admin", "password": os.environ["AIRFLOW_ADMIN_PASSWORD"]})["access_token"]


def parent_package(state, snapshot):
    root = Path(state) / "snapshots" / snapshot
    ref = read(root / "extracted.json")
    ready(snapshot, namespace=ref.get('namespace','entity_v2'), role=ref["role"], expected=ref["digest"])
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
    run_id = dag_run_id(conf,'games_metadata')
    if version(conf)==V3:
        path = Path(os.environ['ETL_STATE'])/'collections'/collection/'launch.json'
        if path.exists() and read(path)!=conf:
            raise ValueError('Existing launch has another identity or scope')
        atomic_write(path,conf)
    auth = token()
    try:
        api("POST", "/api/v2/dags/games_metadata/dagRuns", {
            "dag_run_id": run_id, "logical_date": utcnow(), "conf": conf}, auth)
    except HTTPError as error:
        if error.code != 409:
            raise
        existing = api("GET", f"/api/v2/dags/games_metadata/dagRuns/{run_id}", token=auth)
        if validate_conf(existing["conf"]) != conf:
            raise ValueError("Existing DAG run has another scope")


def wait(collection, kinds):
    state = Path(os.environ['ETL_STATE'])
    launch = state/'collections'/collection/'launch.json'
    conf = read(launch) if launch.exists() else {'collection_id':collection}
    previous = None
    last_progress = 0
    while True:
        states = {}
        auth = token()
        for kind in kinds:
            game_conf = conf
            config_path = state/'snapshots'/(collection+'-'+kind)/'config.json'
            # Direct single-game runs learn their source name/ID in metadata.
            if config_path.exists():game_conf = read(config_path)
            elif kind != 'games_metadata' and version(conf)==V3:
                meta_ref = state/'snapshots'/(collection+'-games_metadata')/'extracted.json'
                if meta_ref.exists():
                    package = EntityPackage(read(meta_ref)['package_uri'])
                    try:
                        result = next(package.records('manifests')).get('source_result') or {}
                        if result.get('game_id'):
                            game_conf = {**conf,'display_game_id':result['game_id']}
                            game_conf.setdefault('game_label',result.get('game_name') or conf.get('game') or 'game')
                    finally:
                        package.db.close()
            try:
                states[kind] = api("GET", f"/api/v2/dags/{kind}/dagRuns/{dag_run_id(game_conf,kind)}", token=auth)["state"]
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
            results = {}
            for kind in kinds:
                ref_path = state/'snapshots'/(collection+'-'+kind)/'extracted.json'
                ref = read(ref_path) if ref_path.exists() else {}
                ready(collection + "-" + kind, namespace=ref.get('namespace',namespace(conf)), role=role(kind))
                if kind == 'wr_history':
                    package = EntityPackage(ref['package_uri'])
                    try:
                        results['wr_history'] = next(package.records('manifests')).get('source_result') or {}
                    finally:
                        package.db.close()
            return results
        time.sleep(15)


def save_status(root, phase, **details):
    atomic_write(root / "status.json", {"phase": phase, "updated_at": utcnow(), **details})


def set_pause(root, requested):
    """Independent of driver.lock: a running driver must be able to receive this."""
    with file_lock(root / "control.lock"):
        path = root / "control.json"
        control = read(path) if path.exists() else {}
        if requested != control.get("pause_requested", False):
            control.update(pause_requested=requested, updated_at=utcnow())
            control["requested_at" if requested else "resumed_at"] = control["updated_at"]
            atomic_write(path, control)


def begin_stage(root, phase=None, **details):
    """Atomically admit one whole chain, or acknowledge a durable pause request."""
    with file_lock(root / "control.lock"):
        path = root / "control.json"
        if path.exists() and read(path).get("pause_requested"):
            previous = read(root / "status.json") if (root / "status.json").exists() else {}
            save_status(root, "paused", collection_id=previous.get("collection_id"),
                        after_phase=previous.get("phase"), reason="User requested pause at a safe stage boundary")
            print("Paused at a safe boundary; no next chain started. Use batch resume with the same collection ID.", flush=True)
            return False
        if phase:
            save_status(root, phase, **details)
        return True


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


def ordered_selection(games, conf):
    """Rank the exact catalog snapshot, then freeze a bounded selection."""
    if conf.get('game_order') == 'run-count-desc':
        def rank(game):
            count = game.get('runCount')
            if not (type(count) is int or isinstance(count, str) and re.fullmatch(r'[0-9]+', count)):
                raise ValueError('Popularity ordering requires an explicit integer catalog runCount')
            if int(count) < 0:
                raise ValueError('Negative catalog runCount')
            return (-int(count), game['id'])
        games = sorted(games, key=rank)
    elif conf.get('game_order') == 'source-id':
        games = sorted(games, key=lambda game: game['id'])
    if conf.get('game_limit'):
        if len(games) < conf['game_limit']:
            raise ValueError('Catalog contains fewer in-scope games than the requested limit')
        games = games[:conf['game_limit']]
    return games


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
    parser.add_argument("command", choices=("run", "status", "pause", "resume"))
    parser.add_argument("--collection", required=True)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--top", type=int)
    mode.add_argument("--all", action="store_true")
    mode.add_argument("--sample", type=int, choices=(4, 8, 12, 16, 20), help="Stratified resource pilot, up to five games per counter band")
    parser.add_argument("--catalog-snapshot", help="Explicit verified input catalog; do not fetch the directory again")
    parser.add_argument('--order', choices=('source-id', 'run-count-desc'), help='Ordering of --all from the exact catalog snapshot')
    parser.add_argument('--limit-games', type=int, help='Bound --all to this many ordered games (1..100000)')
    parser.add_argument('--data-model', choices=(V2,V3), help='New batches use entity-v3; old saved batches keep their contract')
    parser.add_argument("--game-request-budget", type=int)
    parser.add_argument('--session-request-budget',type=int,
                        help='Maximum new HTTP attempts per driver session; reserves the remaining game budget before starting')
    parser.add_argument("--hours", type=float, help="Stop starting new games after this time; finish the active game (default: 8)")
    parser.add_argument("--log", action="store_true")
    parser.add_argument("--refresh-selection", action="store_true", help="Reviewed repair of pending selection; never replace a started chain")
    parser.add_argument("--continue-on-drift", action="store_true", default=None,
                        help="Keep known pagination gaps in the retry list and continue after all three stores acknowledge them")
    strategy = parser.add_mutually_exclusive_group()
    strategy.add_argument("--runs-strategy", choices=("standard", "date-large"),
                        help="Explicit strategy for a new batch; date-large splits large runner lists by dates")
    strategy.add_argument('--accelerate-dates',action='store_true',
                          help='Opt in to date subdivision; default is standard pagination without date filters')
    args = parser.parse_args()
    if args.accelerate_dates:args.runs_strategy='date-large'
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,40}", args.collection):
        parser.error("Invalid batch ID")
    state = Path(os.environ["ETL_STATE"])
    root = state / "batches" / args.collection
    saved = read(root / "config.json") if (root / "config.json").exists() else None
    if args.command in ("pause", "resume") and saved is None:
        parser.error("Unknown batch: no saved configuration; use run to create it")
    if args.command == "pause":
        set_pause(root, True)
        print("Pause requested. The admitted catalog/ranking or game chain may finish; no next stage will start.", flush=True)
        return
    if args.command == "resume":
        if any((args.all, args.top is not None, args.sample, args.catalog_snapshot,
                args.game_request_budget is not None, args.runs_strategy, args.refresh_selection,
                args.order, args.limit_games is not None, args.data_model)):
            parser.error("resume uses the saved scope and strategy; only session options may be supplied")
        conf = saved
        args.all = conf["mode"] == "all"
        args.top = conf.get("top") or 10
        args.sample = conf.get("sample_size") if conf["mode"] == "sample" else None
        args.game_request_budget = conf["game_request_budget"]
        policy = read(root / "runtime-policy.json") if (root / "runtime-policy.json").exists() else {}
    else:
        policy = {}
    args.top = 10 if args.top is None else args.top
    args.game_request_budget = 5000 if args.game_request_budget is None else args.game_request_budget
    args.hours = float(policy.get("hours", 8)) if args.hours is None else args.hours
    args.session_request_budget = policy.get("session_request_budget", 9000) if args.session_request_budget is None else args.session_request_budget
    args.continue_on_drift = policy.get("continue_on_drift", False) if args.continue_on_drift is None else args.continue_on_drift
    if not 1 <= args.top <= 100 or args.hours <= 0:
        parser.error("Invalid top count or session duration")
    if not 100 <= args.game_request_budget <= 10000:
        parser.error("Per-game budget must be 100..10000")
    if args.session_request_budget < args.game_request_budget:
        parser.error('Session HTTP budget must be at least the per-game budget')
    if args.order is not None or args.limit_games is not None:
        if not args.all:
            parser.error('--order and --limit-games require --all, not --top or --sample')
        if args.limit_games is not None and not 1 <= args.limit_games <= 100000:
            parser.error('--limit-games must be 1..100000')
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
    root.mkdir(parents=True, exist_ok=True)
    if args.command != "resume":
        conf = {"mode": "sample" if args.sample else "all" if args.all else "top", "top": args.top if not (args.all or args.sample) else None,
                "game_request_budget": args.game_request_budget}
        if args.data_model is not None:
            conf['data_model'] = args.data_model
        elif saved and 'data_model' in saved:
            conf['data_model'] = saved['data_model']
        elif not saved:
            conf['data_model'] = V3
        if args.all and not saved and args.order is None:
            args.order = 'run-count-desc'
        if args.sample:
            conf['sample_size'] = args.sample
        if args.catalog_snapshot:
            conf['catalog_snapshot_id'] = args.catalog_snapshot
        elif saved and saved.get('catalog_snapshot_id'):
            conf['catalog_snapshot_id'] = saved['catalog_snapshot_id']
        if args.runs_strategy:
            conf["run_partition_strategy"] = "date_large_v1" if args.runs_strategy == "date-large" else "standard"
        elif saved and "run_partition_strategy" in saved:
            conf["run_partition_strategy"] = saved["run_partition_strategy"]
        elif not saved:
            conf['run_partition_strategy'] = 'standard'
        for key, value in (('game_order', args.order), ('game_limit', args.limit_games)):
            if value is not None:
                conf[key] = value
            elif saved and key in saved:
                conf[key] = saved[key]
    with ExitStack() as stack:
        try:
            stack.enter_context(file_lock(root / "driver.lock", blocking=False))
        except BlockingIOError:
            print("Driver already running; no second driver started and pause request unchanged. "
                  "For resume, wait until it stops at the pause boundary, then retry.", flush=True)
            return
        if args.log:
            log = stack.enter_context((root / "driver.log").open("a", buffering=1))
            stack.enter_context(redirect_stdout(log))
            stack.enter_context(redirect_stderr(log))
        if args.command == "resume":
            set_pause(root, False)
        try:
            run_queue(args, state, root, conf)
        except Exception as error:
            previous = read(root / "status.json") if (root / "status.json").exists() else {}
            save_status(root, "failed", collection_id=previous.get("collection_id"),
                        error=type(error).__name__ + ": " + str(error)[:300])
            raise


def run_queue(args, state, root, conf):
    path = root / "config.json"
    if path.exists() and read(path) != conf:
        raise ValueError("Batch scope cannot change; use the same options or a new collection ID")
    atomic_write(path, conf)
    if not begin_stage(root):
        return
    atomic_write(root / "runtime-policy.json", {"continue_on_drift": args.continue_on_drift,
                                               'session_request_budget':args.session_request_budget,
                                               "hours": args.hours,
                                               "updated_at": utcnow()})
    check_capacity(state, require_host=args.all)
    session_end = time.monotonic() + args.hours * 3600
    with pg_connect(True) as conn:
        queued = {r[0] for r in conn.execute('SELECT collection_id FROM entity_batch_queue WHERE batch_id=%s',[args.collection])}
    baseline = {p.parent.name:read(p)['used'] for p in (state/'collections').glob('*/budget.json')
                if p.parent.name.startswith(args.collection+'-') or p.parent.name in queued}
    if conf.get('catalog_snapshot_id'):
        parent = conf['catalog_snapshot_id']
        validate_catalog_input(state, parent, require_fresh=not (root/'selection.json').exists())
        if not begin_stage(root, 'catalog_reused', input_snapshot_id=parent):return
        print('Verified catalog reused:', parent, flush=True)
    else:
        catalog = args.collection + "-catalog"
        if not reserve_session(state,root,baseline,catalog,650,args.session_request_budget):return
        if not begin_stage(root, "catalog", collection_id=catalog):return
        start(catalog, {"scope": "catalog", "request_budget": 650,
                        **({'data_model':conf['data_model']} if 'data_model' in conf else {})})
        wait(catalog, ("games_metadata",))
        parent = catalog + "-games_metadata"
    if conf['mode'] == 'top':
        ranking = args.collection + "-ranking-v2"
        if not reserve_session(state,root,baseline,ranking,500,args.session_request_budget):return
        if not begin_stage(root, "ranking", collection_id=ranking):return
        start(ranking, {"scope": "ranking", "catalog_snapshot_id": parent, "top_n": args.top,
                        "ranking_policy": "exclude_ce_v1",
                        **({'data_model':conf['data_model']} if 'data_model' in conf else {})})
        wait(ranking, ("games_metadata",))
        parent = ranking + "-games_metadata"
    if not begin_stage(root):return
    games = selection(state, parent, conf['mode'] != 'top')
    games = ordered_selection(games, conf)
    if conf['mode'] == 'sample':
        games = stratified_selection(games, args.sample)
    if not games or (conf['mode'] == 'top' and len(games) != args.top):
        raise ValueError("Verified selection does not contain the requested number of games")
    reconcile_selection(args.collection, games, parent, state, refresh=args.refresh_selection)
    atomic_write(root / "selection.json", {"snapshot_id": parent,
        "policy": "catalog_run_count_desc_v1" if conf.get('game_order') == 'run-count-desc' else "stratified_run_count_v1" if args.sample else "exclude_ce_v1",
        "game_ids": [g["id"] for g in games],
        **({"game_order":conf['game_order'], "ranking_metric":"catalog.runCount" if conf['game_order']=='run-count-desc' else 'source_id'} if 'game_order' in conf else {}),
        **({"game_limit":conf['game_limit']} if 'game_limit' in conf else {})})
    for position, game in enumerate(games, 1):
        with pg_connect(True) as conn:
            collection, status, details = conn.execute("SELECT collection_id,state,details FROM entity_batch_queue WHERE batch_id=%s AND game_id=%s", [args.collection,game["id"]]).fetchone()
        if status in ("complete", "not_applicable"):
            continue
        if time.monotonic() >= session_end:
            save_status(root, "session_paused")
            print("Session time reached; queue preserved. Run the same command to continue.", flush=True)
            return
        reuse_partial = status == "partial" and args.continue_on_drift
        if not reuse_partial:
            if not reserve_session(state,root,baseline,collection,args.game_request_budget,args.session_request_budget):return
        if not begin_stage(root, "game", collection_id=collection, position=position, total=len(games), game=game["name"]):return
        print(f"Batch: game={position}/{len(games)} name={game['name']} id={game['id']}", flush=True)
        if not reuse_partial:
            with pg_connect(True) as conn:
                conn.execute("""UPDATE entity_batch_queue SET state='running',updated_at=now(),
                    details=details || jsonb_build_object('first_started_at',COALESCE(details->>'first_started_at',%s))
                    WHERE batch_id=%s AND game_id=%s""", [utcnow(),args.collection,game["id"]])
        try:
            if not reuse_partial:
                start(collection, {"scope": "game", "game": game["url"], "game_id": game["id"],
                    "selection_snapshot_id": details["selection_snapshot_id"], "request_budget": args.game_request_budget,
                    **({'data_model':conf['data_model']} if 'data_model' in conf else {}),
                    **({'game_label':game['name']} if version(conf)==V3 else {}),
                    **({"run_partition_strategy":conf["run_partition_strategy"]} if "run_partition_strategy" in conf else {})})
            chain_results = wait(collection, pipeline_kinds(conf))
            package = EntityPackage(state / "snapshots" / (collection + "-runs") / "entity-package")
            result = next(package.records("manifests"))["source_result"]
            package.db.close()
            status = result_state(result)
            retry = drift_retry_partitions(result) if status == "partial" else None
            wr_details = {}
            if isinstance(chain_results,dict) and 'wr_history' in chain_results:
                from collections import Counter
                wr_result = chain_results['wr_history']
                wr_details = {'wr_coverage':wr_result.get('coverage'),
                              'wr_category_coverage':dict(Counter(c['coverage'] for c in wr_result.get('categories',[])))}
                print(f'Batch: runs={status}; WR={wr_details}',flush=True)
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
                         [status,Jsonb({**retry_details,**wr_details}),args.collection,game["id"]])
        if status != "complete":
            if status == "not_applicable":
                print("Game has no full-game categories; scope result retained in three stores, continuing", flush=True)
            elif args.continue_on_drift and retry:
                print(f"Game coverage=partial; {len(retry)} drifting sections retained for retry; "
                      "three stores acknowledged, continuing to the next game", flush=True)
            else:
                save_status(root, "coverage_paused", collection_id=collection, coverage=status,
                    reason=result.get("stop_reason") or result.get("diagnostic") or "Review partition coverage")
                print(f"Game coverage={status}; data retained, queue paused for review", flush=True)
                return
        if not begin_stage(root):return
    with pg_connect(True) as conn:
        totals = dict(conn.execute("SELECT state,count(*) FROM entity_batch_queue WHERE batch_id=%s AND state!='excluded' GROUP BY state",
                                   [args.collection]).fetchall())
    if not reserve_session(state,root,baseline,None,0,args.session_request_budget):return
    phase = "complete" if sum(totals.get(s, 0) for s in ("complete", "not_applicable")) == len(games) else "complete_with_gaps"
    save_status(root, phase, total=len(games), states=totals)
    print(f"Batch traversal finished: {totals}; source gaps remain in coverage_retry and manifests", flush=True)


if __name__ == "__main__":
    main()
