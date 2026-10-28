"""Small read-only progress report; no source requests and no raw profile output."""
import json
import argparse
import os
from collections import Counter
from datetime import datetime
from pathlib import Path
import time

from .jsonutil import read


def progress(collection, http=False):
    state = Path(os.environ["ETL_STATE"])
    result = {"collection": collection, "snapshots": {}}
    budget = state / "collections" / collection / "budget.json"
    result["budget"] = read(budget) if budget.exists() else None
    telemetry = state / "collections" / collection / "telemetry.json"
    if telemetry.exists():
        metrics = read(telemetry)
        result["http_telemetry"] = {k:v for k,v in metrics.items() if k != "recent"}
        recent = metrics.get("recent", [])
        if recent:
            from statistics import median
            result["http_telemetry"]["median_response_ms"] = median(r["elapsed_ms"] for r in recent)
            last_finished = max(r["start_ms"] + r["elapsed_ms"] for r in recent)
            result["http_telemetry"]["last_response_age_seconds"] = round(max(0, time.time() - last_finished / 1000))
    limiter = state / "limiter.json"
    result["limiter"] = read(limiter) if limiter.exists() else None
    responses = []
    for kind in ("games_metadata", "runs", "players"):
        root = state / "snapshots" / (collection + "-" + kind)
        if not root.exists():
            continue
        item = {"extracted": (root / "extracted.json").exists(), "verified": (root / "verified.json").exists()}
        item["phase"] = "ready" if item["verified"] else "loading" if item["extracted"] else "collecting"
        item["records"] = {}
        if (root / "failed.json").exists() and not item["verified"]:
            item["last_error"] = read(root / "failed.json")
        if (root / "package").exists():
            item["records"] = {name: sum(1 for _ in (root / "package" / name).glob("*.json"))
                               for name in ("payloads", "responses", "observations", "manifests")}
        database = root / "package" / "records.sqlite3"
        if database.exists():
            import sqlite3
            with sqlite3.connect(f"file:{database}?mode=ro", uri=True) as db:
                item["records"].update(dict(db.execute("SELECT kind,count(*) FROM records GROUP BY kind")))
            item["staging_format"] = "sqlite"
        database = root / "entity-package" / "entities.sqlite3"
        if database.exists():
            import sqlite3
            try:
                with sqlite3.connect(f"file:{database}?mode=ro", uri=True, timeout=0.2) as db:
                    item["records"] = dict(db.execute("SELECT dataset,count(*) FROM records GROUP BY dataset"))
                    if kind == "runs":
                        item["records"].pop("players", None)
                        item["records"]["checks"] = db.execute("SELECT count(*) FROM records WHERE dataset='checks' AND json_extract(document,'$.dataset')!='players'").fetchone()[0]
            except sqlite3.OperationalError:
                item["counts_pending"] = True  # A writer may be creating/committing the package.
            item["staging_format"] = "entity-v2"
            if not item["extracted"]:
                item["phase"] = "packing" if kind != "players" else "profiles"
        checkpoints = []
        for path in (root / "checkpoints").glob("*.json"):
            checkpoint = read(path)
            coverage = checkpoint.get("coverage") or checkpoint.get("result") or {}
            extra = {}
            visible_path = path
            fallback = root / 'date-fallback' / path.stem / 'checkpoints' / path.name
            attempt = coverage.get('date_subdivision_attempt', coverage)
            if fallback.exists() and attempt.get('diagnostic') == 'date_partition_coverage_mismatch':
                checkpoint = read(fallback)
                coverage = checkpoint.get('coverage') or {}
                visible_path = fallback
                extra = {'unfiltered_fallback':True,'original_date_gap':attempt['api_count']-attempt['unique_runs']}
            checkpoints.append({"name": path.stem, "pages": len(checkpoint.get("pages", {})), **extra,
                "pages_planned": checkpoint.get("pages_planned", coverage.get("pages_received", coverage.get("pages"))),
                "source_pages": checkpoint.get("source_pages", coverage.get("api_pages")),
                "source_count": checkpoint.get("source_count", coverage.get("api_count", coverage.get("unique_games"))),
                "unique_runs": checkpoint.get("unique_runs", coverage.get("unique_runs")),
                "unique_games": checkpoint.get("unique_games", coverage.get("unique_games")),
                "coverage": coverage.get("coverage"), "complete": checkpoint.get("complete"),
                "pass": checkpoint.get("pass"), "updated_at": datetime.fromtimestamp(visible_path.stat().st_mtime).isoformat()})
        item["checkpoints"] = sorted(checkpoints, key=lambda c: c["updated_at"])
        item["completed_sections"] = sum(bool(c["complete"]) for c in checkpoints)
        item["known_sections"] = len(checkpoints)
        item["coverage_counts"] = dict(Counter(c["coverage"] for c in checkpoints if c["coverage"]))
        config_path = root / "config.json"
        config = read(config_path) if config_path.exists() else {}
        if kind == "runs" and config.get("repair_partitions"):
            item["expected_sections"] = len(config["repair_partitions"])
            item["retained_base_snapshot_id"] = config["repair_from_snapshot_id"]
            date_points = []
            for path in (root / "date-parts").glob("*/*/checkpoints/*.json"):
                point = read(path)
                date_points.append({"section": path.parent.parent.parent.name,
                    "range": path.parent.parent.name, "pages": len(point.get("pages", {})),
                    "pages_planned": point.get("pages_planned"), "complete": point.get("complete"),
                    "mtime": path.stat().st_mtime})
            if date_points:
                item["latest_date_partition"] = max(date_points, key=lambda p:p["mtime"])
                item["date_partitions_complete"] = sum(bool(p["complete"]) for p in date_points)
        metadata = state / "snapshots" / (collection + "-games_metadata") / "entity-package" / "entities.sqlite3"
        if kind == "runs" and metadata.exists():
            import sqlite3
            try:
                with sqlite3.connect(f"file:{metadata}?mode=ro", uri=True, timeout=0.2) as db:
                    row = db.execute("SELECT document FROM records WHERE dataset='manifests' LIMIT 1").fetchone()
                    from .jsonutil import loads
                    count = loads(row[0]).get("source_result", {}).get("category_count") if row else None
                    if count is not None and "expected_sections" not in item:
                        item["expected_sections"] = count * 3
            except sqlite3.OperationalError:
                pass
        if http:
            for path in (root / "requests").glob("*/*.json"):
                saved = read(path)
                responses.append({k: saved.get(k) for k in
                                  ("requested_at", "http_status", "elapsed_ms", "pacing_interval_ms", "network_bytes")})
        result["snapshots"][kind] = item
    if not http:
        return result
    result["http_statuses"] = dict(Counter(str(r["http_status"]) for r in responses))
    result["rate_steps"] = dict(Counter(str(r["pacing_interval_ms"]) for r in responses))
    result["network_bytes"] = sum(r["network_bytes"] or 0 for r in responses)
    times = sorted(datetime.fromisoformat(r["requested_at"]).timestamp() for r in responses)
    if len(times) >= 2:
        result["observed_rpm"] = round(60 * (len(times) - 1) / (times[-1] - times[0]), 2)
        recent = times[-60:]
        result["last_60_observed_rpm"] = round(60 * (len(recent) - 1) / (recent[-1] - recent[0]), 2)
    return result


def driver_running(path):
    """Inspect the existing advisory lock, including after container restarts."""
    import fcntl
    if not path.exists():
        return False
    with path.open("rb") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        fcntl.flock(handle, fcntl.LOCK_UN)
    return False


def batch_progress(batch_id):
    from .storage import pg_connect
    state = Path(os.environ["ETL_STATE"])
    root = state / "batches" / batch_id
    with pg_connect(True) as conn:
        exists = conn.execute("SELECT to_regclass('entity_batch_queue')").fetchone()[0]
        rows = conn.execute("SELECT position,game_id,collection_id,state,details FROM entity_batch_queue WHERE batch_id=%s AND state!='excluded' ORDER BY position", [batch_id]).fetchall() if exists else []
    games = [dict(zip(("position", "game_id", "collection_id", "state", "details"), row)) for row in rows]
    config = read(root / "config.json") if (root / "config.json").exists() else {}
    collections = [config['catalog_snapshot_id'][:-len('-games_metadata')] if config.get('catalog_snapshot_id') else batch_id + '-catalog']
    if config.get('mode', 'top') == 'top':
        collections.append(batch_id + '-ranking-v2')
    collections += [g["collection_id"] for g in games if g["state"] in ("running", "failed", "partial")]
    collections += [retry["repair_collection_id"] for g in games
                    if (retry := g["details"].get("coverage_retry", {})).get("state") == "running"
                    and retry.get("repair_collection_id")]
    reports = {c: progress(c) for c in collections}
    return {"batch": batch_id, "driver_running": driver_running(root / "driver.lock"),
        "driver": read(root / "status.json") if (root / "status.json").exists() else {},
        "games_complete": sum(g["state"] == "complete" for g in games),
        "games_partial": sum(g["state"] == "partial" for g in games),
        "games_not_applicable": sum(g["state"] == "not_applicable" for g in games),
        "games_processed": sum(g["state"] in ("complete", "partial", "unavailable", "not_applicable") for g in games),
        "games_total": max(len(games), config.get("top") or config.get('sample_size') or 0) or None,
        "selection_ready": (root / "selection.json").exists(),
        "games": games, "collections": reports}


def render_collection(report):
    lines = [report["collection"]]
    budget = report.get("budget")
    metrics = report.get("http_telemetry", {})
    rate = (report.get("limiter") or {}).get("rate", {})
    if budget:
        lines.append(f"  HTTP {budget['used']}/{budget['limit']} | recent requests/min: {metrics.get('recent_rpm', metrics.get('observed_rpm', '?'))}")
    if metrics.get("configured_page_window"):
        lines.append(f"    configured page window: {metrics['configured_page_window']}")
    if rate:
        mode = "cooldown" if rate.get("throttled") else "probe" if rate.get("recovering") else "adaptive"
        lines.append(f"    shared ceiling NOW: {rate['rpm']} requests/min; mode={mode}")
        if rate.get("recover_not_before") and rate.get("throttled"):
            seconds = max(0, round(float(rate["recover_not_before"]) - time.time()))
            lines.append(f"    recovery needs cooldown {seconds}s + 20 clean responses (have {rate.get('successes', 0)})")
    next_at = float((report.get("limiter") or {}).get("next_at", 0))
    if next_at > time.time() + 5:
        lines.append(f"    shared source backoff: next HTTP in {max(0, round(next_at-time.time()))}s")
    if metrics.get("status_counts"):
        lines.append("    HTTP results: " + ", ".join(f"{k}={v}" for k,v in metrics["status_counts"].items()))
    if "last_response_age_seconds" in metrics:
        lines.append(f"    last response {metrics['last_response_age_seconds']}s ago; median response {metrics['median_response_ms']}ms")
    for kind, item in report["snapshots"].items():
        counts = ", ".join(f"{k}={v}" for k, v in item["records"].items() if k in ("games", "runs", "players", "wr_history"))
        lines.append(f"  {kind}: {item['phase']}" + (f" | versions: {counts}" if counts else ""))
        points = item["checkpoints"]
        if points:
            latest = points[-1]
            planned = latest.get("pages_planned")
            pages = f"{latest['pages']}/{planned}" if planned is not None else str(latest['pages'])
            sections = item.get("expected_sections", item["known_sections"])
            lines.append(f"    sections {item['completed_sections']}/{sections}; latest {latest['name']}: pages {pages}, coverage {latest.get('coverage') or 'in progress'}")
            if latest.get("unique_runs") is not None and latest.get("source_count") is not None:
                lines.append(f"    unique runs in section: {latest['unique_runs']}/{latest['source_count']} (initial API count)")
            if latest.get('unfiltered_fallback'):
                lines.append(f"    automatic unfiltered fallback; original date gap={latest['original_date_gap']}")
            if item.get("coverage_counts"):
                lines.append("    section coverage: " + ", ".join(f"{k}={v}" for k,v in sorted(item["coverage_counts"].items())))
        if item.get("last_error"):
            lines.append(f"    previous finalization error: {item['last_error'].get('exception_type')}; inspect Airflow")
        if item.get("retained_base_snapshot_id"):
            lines.append(f"    retained base: {item['retained_base_snapshot_id']}; sections above count only repairs")
        if item.get("latest_date_partition"):
            leaf = item["latest_date_partition"]
            lines.append(f"    date range {leaf['range']}: pages {leaf['pages']}/{leaf.get('pages_planned') or '?'}; "
                         f"date partitions complete={item['date_partitions_complete']}")
    if not report["snapshots"]:
        lines.append("  not started")
    return "\n".join(lines)


def render_batch(report):
    driver = report["driver"]
    active = "running" if report["driver_running"] else "stopped"
    processed = report.get("games_processed", report["games_complete"])
    lines = [f"Batch {report['batch']}: {processed}/{report['games_total'] or '?'} games processed "
             f"(complete={report['games_complete']}, partial={report.get('games_partial', 0)}, "
             f"not_applicable={report.get('games_not_applicable', 0)}) "
             f"| driver {active} | {driver.get('phase', 'not started')}"]
    if not report["selection_ready"]:
        lines.append("  Game selection pending: collecting directory/ranking")
    if driver.get("error"):
        lines.append("  Last error: " + driver["error"])
    if driver.get("reason"):
        lines.append("  Paused: " + driver["reason"])
    for game in report["games"]:
        lines.append(f"  {game['position']:2}. {game['details'].get('name', game['game_id'])}: {game['state']} [{game['collection_id']}]")
        retry = game["details"].get("coverage_retry")
        if retry:
            if retry.get("state") == "resolved":
                lines.append(f"      repaired; retained base: {retry['retained_base_collection_id']}")
            else:
                sections = ", ".join(f"{p['category_id']}/{p.get('status', p['status_code'])}" for p in retry["partitions"])
                lines.append(f"      retry {retry.get('state', 'pending')}: {sections}")
    for collection in report["collections"].values():
        sid = collection["collection"]
        bootstrap = {report["batch"] + "-catalog": "Directory",
                     report["batch"] + "-ranking-v2": "Ranking"}
        metadata = collection["snapshots"].get("games_metadata", {})
        repairing_base = any(g["collection_id"] == sid and
                             g["details"].get("coverage_retry", {}).get("state") == "running" for g in report["games"])
        if repairing_base:
            lines.append(f"  Retained base: {sid}; ready data reused; incomplete sections are being repaired")
        elif sid in bootstrap and metadata.get("phase") == "ready":
            count = metadata.get("records", {}).get("games")
            suffix = f"; game versions={count}" if count is not None else ""
            lines.append(f"  {bootstrap[sid]}: ready, shared snapshot reused{suffix} [{sid}-games_metadata]")
        else:
            lines.append(render_collection(collection))
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("collections", nargs="*")
    parser.add_argument("--batch")
    parser.add_argument("--watch", type=float, metavar="SECONDS")
    parser.add_argument("--human", action="store_true")
    parser.add_argument("--json", action="store_true", help="JSON output also in batch mode")
    parser.add_argument("--http", action="store_true", help="Scan raw journals (expensive), single collections only")
    args = parser.parse_args()
    import re
    if (not args.collections and not args.batch) or (args.collections and args.batch):
        parser.error("Provide collection IDs or --batch ID")
    if any(not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", c) for c in args.collections + ([args.batch] if args.batch else [])):
        parser.error("Invalid collection ID")
    if args.watch is not None and args.watch < 2:
        parser.error("Watch interval must be at least 2 seconds")
    if args.batch and args.http:
        parser.error("Raw scanning is only available for individual collections")
    try:
        while True:
            if args.watch:
                print(datetime.now().astimezone().isoformat(timespec="seconds"), flush=True)
            if args.batch:
                result = batch_progress(args.batch)
                print(json.dumps(result, default=str) if args.json else render_batch(result), flush=True)
            else:
                for collection in args.collections:
                    result = progress(collection, http=args.http)
                    print(render_collection(result) if args.human else json.dumps(result, default=str), flush=True)
            if args.watch is None:
                break
            time.sleep(args.watch)
    except KeyboardInterrupt:
        pass  # Stop watching; collection continues.


if __name__ == "__main__":
    main()
