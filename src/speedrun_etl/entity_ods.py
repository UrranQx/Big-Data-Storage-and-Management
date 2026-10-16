"""Entity-oriented ODS builder from the durable journal; no HTTP or live-store I/O."""
import argparse
import base64
import gzip
import hashlib
from pathlib import Path
import sqlite3
import time

from .jsonutil import atomic_write, digest, dumps, loads, read

DATASETS = ("games", "runs", "players", "wr_history", "checks", "responses", "blocks", "manifests")
CHUNK_BYTES = 4 * 1024**2


class EntityPackage:
    def __init__(self, root, role="all"):
        if role not in ("all", "runs"):
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
        if self.role == "runs" and dataset == "players":
            return
        for row in self.db.execute("SELECT document FROM records WHERE dataset=? ORDER BY key", (dataset,)):
            value = loads(row[0])
            if self.role == "runs" and dataset == "checks" and value["dataset"] == "players":
                continue
            yield value

    def encoded(self, raw, compressed=False):
        data = gzip.compress(raw, mtime=0) if compressed else raw
        if len(data) <= CHUNK_BYTES:
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


def build(snapshot_root, target_root, resume=False, source_result=None, load_role="all"):
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
    if not paths:
        raise ValueError("No durable source responses")
    ordered = sorted((read(path)["fetched_at"], str(path)) for path in paths)
    marker = {"snapshot_id": snapshot_id, "journal": [[t, str(Path(p).relative_to(snapshot_root))] for t,p in ordered],
              "source_result": source_result, "load_role": load_role}
    marker_path = target_root / "builder.json"
    if target_root.exists():
        if not marker_path.exists() or read(marker_path) != marker:
            raise ValueError("Cannot resume another source journal or scope")
        if (target_root / "verified_local.json").exists():
            return read(target_root / "verified_local.json")
    else:
        atomic_write(marker_path, marker)
    package = EntityPackage(target_root)
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
            package.put(dataset, {"key": key, "source_system": "speedruncom", "source_id": str(source_id),
                "projection": projection, "content_hash": content_hash, "raw": package.encoded(raw_bytes),
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

    def run(raw, saved, path, projection):
        entity("runs", raw.get("id"), raw, projection, saved, path, {
            "game_id": raw.get("gameId"), "category_id": raw.get("categoryId"),
            "status": raw.get("verified"), "player_ids": raw.get("playerIds", [])})

    for _, name in ordered:
        saved = read(name)
        body = saved["body"]
        raw_body = dumps(body).encode()
        package.put("responses", {"key": saved["response_id"], "snapshot_id": snapshot_id,
            "metadata": {k: v for k, v in saved.items() if k != "body"},
            "body_hash": hashlib.sha256(raw_body).hexdigest(), "body": package.encoded(raw_body, compressed=True)})
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
        for index, raw in enumerate(body.get("playerList", [])):
            # Field-set projection distinguishes thin/full profile responses.
            projection = "player:" + digest(sorted(raw))
            entity("players", raw.get("id"), raw, projection, saved, ["playerList", index], {"name": raw.get("name")})
        if endpoint == "GetGameRecordHistory":
            params = saved["params"]
            scope = params.get("params", {})
            page = params.get("page", 1)
            history_id = digest([scope, page])
            entity("wr_history", history_id, body, "source_wr_page", saved, [], {
                "game_id": scope.get("gameId"), "category_id": scope.get("categoryId"),
                "filters": scope, "page": page, "coverage": "unavailable" if body == {} else "not_checked"})
            for event_index, event in enumerate(body.get("recordEventList", [])):
                for run_index, raw in enumerate(event.get("runList", [])):
                    run(raw, saved, ["recordEventList", event_index, "runList", run_index], "source_wr_run")
        package.db.commit()
        if response_count % 50 == 0:
            print(f"Packing: responses={response_count}/{len(ordered)} unique_entity_versions={len(package.known)}", flush=True)
    for check in checks.values():
        package.put("checks", check)
    # Verify exact source JSON and every extracted entity version before sealing.
    for response in package.records("responses"):
        if digest(package.decode(response["body"])) != response["body_hash"]:
            raise ValueError("Source response reconstruction differs")
    for dataset in ("games", "runs", "players", "wr_history"):
        for record in package.records(dataset):
            if digest(package.decode(record["raw"])) != record["content_hash"]:
                raise ValueError("Entity reconstruction differs")
    package.put("manifests", {"key": snapshot_id + ":entity-v2", "format_version": "entity-v2",
        "snapshot_id": snapshot_id, "provenance": "repacked_durable_journal_no_new_HTTP",
        "input_config": read(snapshot_root / "config.json") if (snapshot_root / "config.json").exists() else None,
        "responses": response_count, "unresolved": unresolved,
        "source_result": source_result, "load_role": load_role,
        "limitations": "Source WR page without pagination does not prove complete site history."})
    package.db.commit()
    summary = package.summary()
    package.db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    package.db.close()
    result = {"format_version": "entity-v2", "snapshot_id": snapshot_id, "records": summary,
              "seconds": round(time.perf_counter() - started, 3), "digest": digest(summary)}
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
