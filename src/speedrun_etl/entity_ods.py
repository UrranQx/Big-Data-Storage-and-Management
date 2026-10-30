"""Entity-oriented ODS builder from the durable journal; no HTTP or live-store I/O."""
import argparse
import base64
import gzip
import hashlib
from functools import lru_cache
from pathlib import Path
import sqlite3
import time

from .jsonutil import atomic_write, digest, dumps, loads, read
from .model import V2, V3, run_core
from .ods import native_safe

DATASETS = ("games", "runs", "players", "wr_history", "checks", "responses", "blocks", "manifests")
CHUNK_BYTES = 4 * 1024**2
SHARED_PROFILE_MIN_COUNT = 1000


@lru_cache(maxsize=128)
def profile_projection(fields):
    """Only cache the field-set hash; every distinct profile value is still saved."""
    return "player:" + digest(list(fields))


class EntityPackage:
    def __init__(self, root, role="all"):
        if role not in ("all", "runs", "wr_history"):
            raise ValueError("Unknown package role")
        self.role = role
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.root / "entities.sqlite3")
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("""CREATE TABLE IF NOT EXISTS records(
            dataset TEXT NOT NULL,key TEXT NOT NULL,document TEXT NOT NULL,
            PRIMARY KEY(dataset,key)) WITHOUT ROWID""")
        self.known = set()

    def put(self, dataset, row):
        if dataset not in DATASETS:
            raise ValueError(dataset)
        raw = dumps(row)
        previous = self.db.execute("SELECT document FROM records WHERE dataset=? AND key=?", (dataset, row["key"])).fetchone()
        if previous and previous[0] != raw:
            raise ValueError("Immutable entity package conflict")
        if not previous:
            self.db.execute("INSERT INTO records VALUES(?,?,?)", (dataset, row["key"], raw))

    def get(self, dataset, key):
        row = self.db.execute("SELECT document FROM records WHERE dataset=? AND key=?", (dataset, key)).fetchone()
        if row is None:
            raise KeyError((dataset, key))
        return loads(row[0])

    def records(self, dataset):
        if self.role in ("runs", "wr_history") and dataset == "players":
            return
        for row in self.db.execute("SELECT document FROM records WHERE dataset=? ORDER BY key", (dataset,)):
            value = loads(row[0])
            if self.role in ("runs", "wr_history") and dataset == "checks" and value["dataset"] == "players":
                continue
            yield value

    def encoded(self, raw, compressed=False, force_blocks=False):
        data = gzip.compress(raw, mtime=0) if compressed else raw
        if len(data) <= CHUNK_BYTES and not force_blocks:
            return {"encoding": "gzip-json-v1" if compressed else "json-v1",
                    "text": base64.b64encode(data).decode() if compressed else data.decode()}
        blocks = []
        for position in range(0, len(data), CHUNK_BYTES):
            chunk = data[position:position + CHUNK_BYTES]
            key = hashlib.sha256(chunk).hexdigest()
            self.put("blocks", {"key": key, "data": base64.b64encode(chunk).decode()})
            blocks.append(key)
        return {"encoding": "gzip-json-v1-blocks" if compressed else "json-v1-blocks", "blocks": blocks}

    def decode(self, value):
        encoding = value["encoding"]
        if encoding == 'native-json-v1':
            return value['value']
        if encoding == "json-v1-player-list-ref":
            body = self.decode(value["envelope"])
            if not isinstance(body, dict) or "playerList" in body:
                raise ValueError("Invalid shared playerList envelope")
            profiles = self.decode(value["player_list"])
            if not isinstance(profiles, list):
                raise ValueError("Invalid shared playerList fragment")
            body["playerList"] = profiles
            return body
        if "blocks" in value:
            chunks = []
            for key in value["blocks"]:
                chunk = base64.b64decode(self.get("blocks", key)["data"])
                if hashlib.sha256(chunk).hexdigest() != key:
                    raise ValueError("Entity block checksum differs")
                chunks.append(chunk)
            raw = b"".join(chunks)
        else:
            raw = base64.b64decode(value["text"]) if encoding.startswith("gzip") else value["text"].encode()
        if encoding.startswith("gzip"):
            raw = gzip.decompress(raw)
        return loads(raw)

    def summary(self):
        result = {}
        for dataset in DATASETS:
            checksum, count = hashlib.sha256(b"["), 0
            for row in self.records(dataset):
                if count:
                    checksum.update(b",")
                checksum.update(dumps([row["key"], digest(row)]).encode())
                count += 1
            checksum.update(b"]")
            result[dataset] = {"count": count, "digest": checksum.hexdigest()}
        return result


def retain_base(package, base, snapshot_id, checks):
    """Reuse immutable raw/versions, merge checks with their original source times."""
    for dataset in DATASETS:
        if dataset in ("manifests", "checks"):
            continue
        for row in base.records(dataset):
            package.put(dataset, row)
    for old in base.records("checks"):
        key = digest([snapshot_id, old["dataset"], old["source_id"], old["projection"]])
        current = checks.get(key)
        if current is None:
            checks[key] = {**old, "key": key, "snapshot_id": snapshot_id}
            continue
        events = old["states"] + [old["first_seen"], old["last_seen"]] + current["states"] + [current["first_seen"], current["last_seen"]]
        events.sort(key=lambda event: (event["observed_at"], event["response_id"], dumps(event["path"])))
        states = []
        for event in events:
            if not states or states[-1]["version_key"] != event["version_key"]:
                states.append(event)
        current.update(first_seen=events[0], last_seen=events[-1], states=states)
    package.db.commit()


def build(snapshot_root, target_root, resume=False, source_result=None, load_role="all", base_root=None,
          format_version=V2, runs_input=None, input_snapshot_ids=None):
    if format_version not in (V2, V3):
        raise ValueError('Unknown entity package version')
    if format_version == V3 and base_root is not None:
        raise ValueError('Do not merge legacy repair into unified runs')
    snapshot_root, target_root = Path(snapshot_root), Path(target_root)
    if target_root.exists() and not resume:
        raise ValueError("Output must be a new directory; existing data is never overwritten")
    started = time.perf_counter()
    snapshot_id = snapshot_root.name
    checks = {}
    unresolved = []
    response_count = 0
    # The journal filenames are hashes, not chronological sequence numbers.
    paths = list((snapshot_root / "requests").glob("*/*.json"))
    if not paths and not (format_version == V3 and load_role == 'wr_history' and
                         (source_result or {}).get('diagnostic') in ('no_full_game_categories_in_source_metadata','upstream_stopped')):
        raise ValueError("No durable source responses")
    ordered = sorted((read(path)["fetched_at"], str(path)) for path in paths)
    marker = {"snapshot_id": snapshot_id, "journal": [[t, str(Path(p).relative_to(snapshot_root))] for t,p in ordered],
              "source_result": source_result, "load_role": load_role,
              "response_encoding": "shared-player-list-v1"}
    if format_version == V3:
        marker['format_version'] = V3
        if input_snapshot_ids is not None:marker['input_snapshot_ids']=input_snapshot_ids
        if runs_input:
            marker['runs_input'] = str(runs_input.root)
    if base_root is not None:
        print(f"Packing: retaining immutable base {base_root}", flush=True)
        base = EntityPackage(base_root)
        try:
            marker["retained_base"] = {"path": str(base_root), "digest": digest(base.summary())}
        finally:
            base.db.close()
    marker_path = target_root / "builder.json"
    if target_root.exists():
        if not marker_path.exists() or read(marker_path) != marker:
            raise ValueError("Cannot resume another source journal or scope")
        if (target_root / "verified_local.json").exists():
            return read(target_root / "verified_local.json")
    else:
        atomic_write(marker_path, marker)
    package = EntityPackage(target_root)
    if format_version == V3:
        package.db.execute("CREATE INDEX IF NOT EXISTS records_source ON records(dataset,json_extract(document,'$.source_id'))")
    print(f"Packing: snapshot={snapshot_id} source_responses=0/{len(ordered)}", flush=True)

    def entity(dataset, source_id, raw, projection, saved, path, attributes=None):
        if not source_id:
            unresolved.append({"response_id": saved["response_id"], "path": path,
                               "diagnostic": "no_standalone_source_id"})
            return
        raw_bytes = dumps(raw).encode()
        content_hash = hashlib.sha256(raw_bytes).hexdigest()
        key = digest(["speedruncom", source_id, projection, content_hash])
        if (dataset, key) not in package.known:
            encoded = ({'encoding':'native-json-v1','value':raw}
                       if format_version==V3 and native_safe(raw) and len(raw_bytes)<=CHUNK_BYTES
                       else package.encoded(raw_bytes))
            package.put(dataset, {"key": key, "source_system": "speedruncom", "source_id": str(source_id),
                "projection": projection, "content_hash": content_hash, "raw": encoded,
                **(attributes or {})})
            package.known.add((dataset, key))
        check_key = digest([snapshot_id, dataset, source_id, projection])
        event = {"observed_at": saved["fetched_at"], "response_id": saved["response_id"],
                 "path": path, "version_key": key}
        check = checks.setdefault(check_key, {"key": check_key, "snapshot_id": snapshot_id,
            "dataset": dataset, "source_id": str(source_id), "projection": projection,
            "first_seen": event, "last_seen": event, "states": []})
        check["last_seen"] = event
        if not check["states"] or check["states"][-1]["version_key"] != key:
            check["states"].append(event)
        return check

    def run(raw, saved, path, projection):
        if format_version == V3:
            core = run_core(raw)
            if runs_input is not None:
                # WR is an independent source projection, often partial. Link a
                # known run by ID; keep ALL WR-specific fields in the exact WR
                # response rather than replacing the ordinary run with a subset.
                known = runs_input.db.execute("SELECT 1 FROM records WHERE dataset='runs' AND json_extract(document,'$.source_id')=? LIMIT 1", (str(raw.get('id')),)).fetchone()
                if known is not None:return
            check = entity('runs', raw.get('id'), core, 'run_core', saved, path, {
                'game_id':raw.get('gameId'), 'category_id':raw.get('categoryId'),
                'status':raw.get('verified'), 'player_ids':raw.get('playerIds', [])})
            filters = saved.get('params',{}).get('params',{})
            if (check is not None and saved['endpoint']=='GetGameLeaderboard2'
                    and not any(k in filters for k in ('minRunDate','maxRunDate'))):
                context = {'observed_at':saved['fetched_at'],'response_id':saved['response_id'],
                           'path':path,'filters':filters,
                           'values':{k:raw[k] for k in ('place','obsolete') if k in raw}}
                observed = check.setdefault('unfiltered_leaderboard',{'first_seen':context,'last_seen':context,'states':[]})
                observed['last_seen'] = context
                if not observed['states'] or any(observed['states'][-1][k]!=context[k] for k in ('values','filters')):
                    observed['states'].append(context)
            return
        filters = saved.get("params", {}).get("params", {})
        date_filters = {k: filters[k] for k in ("minRunDate", "maxRunDate") if k in filters}
        if projection == "leaderboard_run" and date_filters:
            projection = "leaderboard_run_date:" + digest(date_filters)
        entity("runs", raw.get("id"), raw, projection, saved, path, {
            "game_id": raw.get("gameId"), "category_id": raw.get("categoryId"),
            "status": raw.get("verified"), "player_ids": raw.get("playerIds", []),
            **({"leaderboard_filters": filters, "contextual_fields": ["place", "obsolete"]} if date_filters else {})})

    profile_group = None
    profile_work = {"lists": 0, "groups": 0, "entity_visits": 0}

    def profile_entities(profiles, saved):
        for index, raw in enumerate(profiles):
            projection = profile_projection(tuple(sorted(raw)))
            entity("players", raw.get("id"), raw, projection, saved, ["playerList", index], {"name": raw.get("name")})
            profile_work["entity_visits"] += 1

    def flush_profiles():
        if (profile_group and profile_group["compact"]
                and profile_group["last"]["response_id"] != profile_group["first"]["response_id"]):
            profile_entities(profile_group["profiles"], profile_group["last"])

    for _, name in ordered:
        saved = read(name)
        body = saved["body"]
        raw_body = dumps(body).encode()
        shared = (saved["http_status"] == 200 and not saved.get("error") and isinstance(body, dict)
                  and isinstance(body.get("playerList"), list)
                  and len(body["playerList"]) >= SHARED_PROFILE_MIN_COUNT)
        if shared:
            profile_bytes = dumps(body["playerList"]).encode()
            profile_hash = hashlib.sha256(profile_bytes).hexdigest()
            profile_work["lists"] += 1
            if not profile_group or profile_group["hash"] != profile_hash:
                flush_profiles()
                profile_entities(body["playerList"], saved)
                ids = [p.get("id") for p in body["playerList"]]
                compact = all(isinstance(v, str) and v for v in ids) and len(set(ids)) == len(ids)
                profile_group = {"hash": profile_hash, "profiles": body["playerList"],
                    "first": saved, "last": saved, "compact": compact,
                    "encoded": package.encoded(profile_bytes, compressed=True, force_blocks=True)}
                profile_work["groups"] += 1
            else:
                if not profile_group["compact"]:
                    profile_entities(body["playerList"], saved)
                profile_group["last"] = saved
            envelope = {k:v for k,v in body.items() if k != "playerList"}
            encoded_body = {"encoding": "json-v1-player-list-ref",
                "envelope": package.encoded(dumps(envelope).encode(), compressed=True),
                "player_list": profile_group["encoded"]}
        else:
            # Flush before another profile shape/state so A -> B -> A keeps its order.
            if isinstance(body, dict) and "playerList" in body:
                flush_profiles()
                profile_group = None
            encoded_body = package.encoded(raw_body, compressed=True)
        package.put("responses", {"key": saved["response_id"], "snapshot_id": snapshot_id,
            "metadata": {k: v for k, v in saved.items() if k != "body"},
            "body_hash": hashlib.sha256(raw_body).hexdigest(), "body": encoded_body})
        response_count += 1
        if saved["http_status"] != 200 or saved.get("error") or not isinstance(body, dict):
            continue
        endpoint = saved["endpoint"]
        if isinstance(body.get("game"), dict):
            game = body["game"]
            detail = endpoint == "GetGameData"
            entity("games", game.get("id"), body if detail else game,
                   "game_detail" if detail else "game_card", saved, [] if detail else ["game"], {"name": game.get("name")})
        for index, game in enumerate(body.get("gameList", [])):
            entity("games", game.get("id"), game, "game_card", saved, ["gameList", index], {"name": game.get("name")})
        for index, raw in enumerate(body.get("runList", [])):
            run(raw, saved, ["runList", index], "leaderboard_run")
        if not shared:
            profile_entities(body.get("playerList", []), saved)
        if endpoint == "GetGameRecordHistory":
            params = saved["params"]
            scope = params.get("params", {})
            page = params.get("page", 1)
            history_id = digest([scope, page])
            if format_version == V2:
                entity("wr_history", history_id, body, "source_wr_page", saved, [], {
                    "game_id": scope.get("gameId"), "category_id": scope.get("categoryId"),
                    "filters": scope, "page": page, "coverage": "unavailable" if body == {} else "not_checked"})
            events = body.get('recordEventList', [])
            for event_index, event in enumerate(events if isinstance(events,list) else []):
                if not isinstance(event,dict) or not isinstance(event.get('runList'),list):
                    continue  # Full response retained; collector records malformed coverage.
                if format_version == V3:
                    event_raw = {'event':{k:v for k,v in event.items() if k != 'runList'},
                                 'run_ids':[r.get('id') for r in event['runList']]}
                    entity('wr_history', digest([scope,page,event_index]), event_raw,
                           'source_wr_event', saved, ['recordEventList',event_index], {
                               'game_id':scope.get('gameId'), 'category_id':scope.get('categoryId'),
                               'filters':scope, 'page':page, 'event_index':event_index,
                               'run_ids':event_raw['run_ids'],
                               'player_ids':sorted({p for r in event['runList'] for p in r.get('playerIds',[]) if isinstance(p,str) and p})})
                for run_index, raw in enumerate(event.get("runList", [])):
                    run(raw, saved, ["recordEventList", event_index, "runList", run_index], "source_wr_run")
        package.db.commit()
        if response_count % 50 == 0:
            print(f"Packing: responses={response_count}/{len(ordered)} unique_entity_versions={len(package.known)}", flush=True)
    flush_profiles()
    if base_root is not None:
        base = EntityPackage(base_root)
        try:
            retain_base(package, base, snapshot_id, checks)
        finally:
            base.db.close()
    for check in checks.values():
        package.put("checks", check)
    # Verify exact source JSON and every extracted entity version before sealing.
    print("Packing: verifying reconstructed response JSON", flush=True)
    for index, response in enumerate(package.records("responses"), 1):
        if digest(package.decode(response["body"])) != response["body_hash"]:
            raise ValueError("Source response reconstruction differs")
        if index % 100 == 0:
            print(f"Packing: verified source responses={index}", flush=True)
    for dataset in ("games", "runs", "players", "wr_history"):
        print(f"Packing: verifying {dataset} versions", flush=True)
        for record in package.records(dataset):
            if digest(package.decode(record["raw"])) != record["content_hash"]:
                raise ValueError("Entity reconstruction differs")
    package.put("manifests", {"key": snapshot_id + ":" + format_version, "format_version": format_version,
        "snapshot_id": snapshot_id, "provenance": "retained_base_and_fresh_repair_HTTP" if base_root else "repacked_durable_journal_no_new_HTTP",
        "input_config": read(snapshot_root / "config.json") if (snapshot_root / "config.json").exists() else None,
        "responses": package.db.execute("SELECT count(*) FROM records WHERE dataset='responses'").fetchone()[0], "unresolved": unresolved,
        "source_result": source_result, "load_role": load_role,
        **({'wr_run_policy':'link_known_ids; preserve_WR_specific_fields_in_exact_responses; retain_WR_only_ids'} if runs_input is not None else {}),
        **({'input_snapshot_ids':[runs_input.root.parent.name]} if runs_input is not None else {}),
        **({'input_snapshot_ids':input_snapshot_ids} if input_snapshot_ids is not None else {}),
        **({"input_snapshot_ids": [source_result["repair_from_snapshot_id"]],
            "new_journal_responses": response_count,
            "retained_base": marker["retained_base"]} if base_root else {}),
        "limitations": "Source WR page without pagination does not prove complete site history."})
    package.db.commit()
    print("Packing: computing package checksums", flush=True)
    summary = package.summary()
    package.db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    package.db.close()
    result = {"format_version": format_version, "snapshot_id": snapshot_id, "records": summary,
              "seconds": round(time.perf_counter() - started, 3), "digest": digest(summary),
              "shared_profile_work": profile_work}
    atomic_write(target_root / "verified_local.json", result)
    print(f"Packing: completed snapshot={snapshot_id} records={sum(r['count'] for r in summary.values())} seconds={result['seconds']}", flush=True)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    print(dumps(build(args.snapshot, args.output)), flush=True)


if __name__ == "__main__":
    main()
