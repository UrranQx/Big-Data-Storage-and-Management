"""Independent journal/checkpoint comparison for a new entity-oriented package."""
import argparse
from collections import defaultdict
from pathlib import Path

from .entity_ods import EntityPackage, profile_projection
from .jsonutil import atomic_write, digest, dumps, read, loads
from .model import V3, run_core


def expected_wr_categories(raw, manifest):
    categories = {c["id"] for c in raw.get('categories',[]) if c.get("isPerLevel") is False}
    if manifest.get('input_config',{}).get('smoke'):
        selected=manifest['input_config'].get('smoke_category_id')
        if selected:
            if selected not in categories:raise ValueError('Selected smoke category absent from metadata')
            return {selected}
        return {next(c['id'] for c in raw.get('categories',[]) if c.get('isPerLevel') is False)} if categories else set()
    if manifest.get("input_config", {}).get("scope") == "e2e":
        selected = manifest["source_result"]["category_id"]
        if selected not in categories:
            raise ValueError("Selected e2e category absent from full-game metadata")
        return {selected}
    return categories


def checkpoint_run_ids(checkpoint, responses):
    """Compare the selected pass, keeping superseded passes as source history."""
    coverage = checkpoint.get("coverage") or {}
    pages = checkpoint.get("pages", {})
    if not checkpoint.get("complete") or not coverage:
        raise ValueError("Incomplete input partition")
    if not pages and "unique_runs" not in coverage and coverage.get("coverage") == "partial":
        return None  # Old repeated-drift checkpoint has no completed final pass.
    if not pages:
        raise ValueError("Checkpoint has no saved pages")
    ids = set()
    for page in pages.values():
        rid = page["response_id"]
        if rid not in responses:
            raise ValueError("Checkpoint response missing from source journal")
        ids.update(responses[rid])
    if len(ids) != coverage.get("unique_runs"):
        raise ValueError("Checkpoint run count differs")
    return ids


def audit(snapshot, output, metadata=None):
    snapshot, output = Path(snapshot), Path(output)
    package = EntityPackage(output)
    source_ids, run_ids, player_ids = set(), set(), set()
    partitions = defaultdict(set)
    response_run_ids = {}
    wr_categories, empty_wr = set(), set()
    versions, all_versions, matched = defaultdict(list), set(), set()
    manifest = next(package.records("manifests"))
    unified = manifest.get('format_version') == V3
    marker = read(output/'builder.json') if (output/'builder.json').exists() else {}
    runs_input = EntityPackage(marker['runs_input']) if marker.get('runs_input') else None
    unfiltered = {}
    source_roots = [snapshot]
    base_id = (manifest.get("source_result") or {}).get("repair_from_snapshot_id")
    # Repaired packages retain original response documents/IDs, not fabricated HTTP.
    seen = {snapshot.name}
    while base_id:
        import re
        if not re.fullmatch(r"[a-zA-Z0-9_-]{1,105}-runs", base_id) or base_id in seen:
            raise ValueError("Invalid repair provenance chain")
        seen.add(base_id)
        base_root = snapshot.parent / base_id
        source_roots.append(base_root)
        base_package = EntityPackage(base_root / "entity-package")
        try:
            base_manifest = next(base_package.records("manifests"))
            base_id = (base_manifest.get("source_result") or {}).get("repair_from_snapshot_id")
        finally:
            base_package.db.close()
    for dataset in ("games", "runs", "players", "wr_history"):
        for row in package.records(dataset):
            versions[dataset, row["source_id"], row["projection"]].append((row["key"], package.decode(row["raw"])))
            all_versions.add((dataset, row["key"]))

    def check_entity(dataset, source_id, projection, raw):
        if not source_id:
            return  # Unidentified objects stay in the exact source response.
        for key, value in versions[dataset, str(source_id), projection]:
            if value == raw:
                matched.add((dataset, key))
                return
        raise ValueError(f"Missing or modified {dataset} source object: {source_id}")

    for path in (p for source_root in source_roots for p in (source_root / "requests").glob("*/*.json")):
        saved = read(path)
        source_ids.add(saved["response_id"])
        stored = package.get("responses", saved["response_id"])
        body = saved["body"]
        if package.decode(stored["body"]) != body or stored["metadata"] != {k:v for k,v in saved.items() if k != "body"}:
            raise ValueError("Source response/metadata changed")
        if saved["http_status"] != 200 or saved.get("error") or not isinstance(body, dict):
            continue
        endpoint = saved["endpoint"]
        if endpoint == "GetGameLeaderboard2":
            response_run_ids[saved["response_id"]] = {r["id"] for r in body.get("runList", [])}
            filters = saved.get('params',{}).get('params',{})
            if not any(k in filters for k in ('minRunDate','maxRunDate')):
                unfiltered[saved['response_id']] = {'filters':filters,'time':saved['fetched_at'],
                    'runs':[(r.get('id'),{k:r[k] for k in ('place','obsolete') if k in r}) for r in body.get('runList',[])]}
        if isinstance(body.get("game"), dict):
            detail = endpoint == "GetGameData"
            check_entity("games", body["game"].get("id"), "game_detail" if detail else "game_card", body if detail else body["game"])
        for game in body.get("gameList", []):
            check_entity("games", game.get("id"), "game_card", game)
        for run in body.get("runList", []):
            filters = saved.get("params", {}).get("params", {})
            date_filters = {k: filters[k] for k in ("minRunDate", "maxRunDate") if k in filters}
            projection = "leaderboard_run_date:" + digest(date_filters) if date_filters else "leaderboard_run"
            check_entity("runs", run.get("id"), 'run_core' if unified else projection, run_core(run) if unified else run)
            run_ids.add(run["id"])
            partitions[run.get("categoryId"), run.get("verified")].add(run["id"])
        for profile in body.get("playerList", []):
            check_entity("players", profile.get("id"), profile_projection(tuple(sorted(profile))), profile)
            if profile.get("id"):
                player_ids.add(profile["id"])
        if endpoint == "GetGameRecordHistory":
            scope = saved["params"].get("params", {})
            page = saved["params"].get("page", 1)
            if not unified:check_entity("wr_history", digest([scope, page]), "source_wr_page", body)
            wr_categories.add(scope.get("categoryId"))
            if body == {}:
                empty_wr.add(scope.get("categoryId"))
            events = body.get('recordEventList',[])
            for event_index,event in enumerate(events if isinstance(events,list) else []):
                if not isinstance(event,dict) or not isinstance(event.get('runList'),list):continue
                if unified:
                    check_entity('wr_history',digest([scope,page,event_index]),'source_wr_event',
                                 {'event':{k:v for k,v in event.items() if k!='runList'},
                                  'run_ids':[r.get('id') for r in event['runList']]})
                for run in event.get("runList", []):
                    if not unified:
                        check_entity("runs", run.get("id"), "source_wr_run", run)
                    else:
                        core = run_core(run)
                        own = any(raw==core for key,raw in versions['runs',str(run.get('id')),'run_core'])
                        if own:
                            check_entity('runs',run.get('id'),'run_core',core)
                        else:
                            if runs_input is None:raise ValueError('WR run has no source entity or input link')
                            candidates = runs_input.db.execute("SELECT document FROM records WHERE dataset='runs' AND json_extract(document,'$.source_id')=?",(str(run.get('id')),))
                            if next(candidates,None) is None:
                                raise ValueError('WR run is neither retained nor linked by ID to its input')
    if matched != all_versions:
        raise ValueError("New package contains versions absent from the journal")
    if source_ids != {r["key"] for r in package.records("responses")}:
        raise ValueError("Response identity set differs")
    checkpoint_count = 0
    for path in (p for source_root in source_roots for p in (source_root / "checkpoints").glob("*.json")):
        checkpoint = read(path)
        coverage = checkpoint.get("coverage")
        if unified and manifest.get('load_role')=='wr_history':
            if not checkpoint.get('complete') or any(p['response_id'] not in source_ids for p in checkpoint.get('pages',{}).values()):
                raise ValueError('Invalid WR checkpoint/source link')
        else:
            checkpoint_run_ids(checkpoint, response_run_ids)
        checkpoint_count += 1
    for check in package.records("checks"):
        for observed in check.get('unfiltered_leaderboard',{}).get('states',[]):
            context = unfiltered.get(observed['response_id'])
            if context is None or observed['path'][:1]!=['runList']:
                raise ValueError('Unfiltered context points to a filtered/non-leaderboard response')
            source_id,values = context['runs'][observed['path'][1]]
            if (str(source_id)!=check['source_id'] or values!=observed['values'] or
                    context['filters']!=observed['filters'] or context['time']!=observed['observed_at']):
                raise ValueError('Unfiltered flags or provenance differ from source')
        previous = None
        for state in check["states"]:
            row = package.get(check["dataset"], state["version_key"])
            if row["source_id"] != check["source_id"] or row["projection"] != check["projection"]:
                raise ValueError("History points to another entity/projection")
            if state["version_key"] == previous or state["response_id"] not in source_ids:
                raise ValueError("Invalid state transition or response reference")
            previous = state["version_key"]
        if not check["states"] or check["first_seen"]["observed_at"] > check["last_seen"]["observed_at"]:
            raise ValueError("Invalid observation time interval")
    expected_categories = None
    if metadata:
        meta = EntityPackage(metadata)
        detail = next(r for r in meta.records("games") if r["projection"] == "game_detail")
        raw = meta.decode(detail["raw"])
        meta_manifest = next(meta.records("manifests"))
        expected_categories = expected_wr_categories(raw, meta_manifest)
        meta.db.close()
        if (not unified or manifest.get('load_role')=='wr_history') and wr_categories != expected_categories:
            raise ValueError("WR category coverage differs from metadata")
    result = {"snapshot_id": snapshot.name, "verified_against": "original_journal_and_partition_checkpoints",
        "source_responses": len(source_ids), "leaderboard_unique_run_ids": len(run_ids),
        "source_snapshot_ids": [r.name for r in source_roots],
        "embedded_profile_ids": len(player_ids), "matched_entity_versions": len(matched),
        "checked_partitions": checkpoint_count, "wr_categories": sorted(wr_categories),
        "empty_wr_categories": sorted(empty_wr), "metadata_categories": sorted(expected_categories) if expected_categories else None,
        "limitations": "Local data preservation audit; not storage load performance or complete-site proof."}
    package.db.close()
    if runs_input:runs_input.db.close()
    atomic_write(output / "audit.json", result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", required=True)
    parser.add_argument("--package", required=True)
    parser.add_argument("--metadata")
    args = parser.parse_args()
    print(dumps(audit(args.snapshot, args.package, args.metadata)), flush=True)


if __name__ == "__main__":
    main()
