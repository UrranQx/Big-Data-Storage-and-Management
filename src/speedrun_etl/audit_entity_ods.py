"""Independent journal/checkpoint comparison for a new entity-oriented package."""
import argparse
from collections import defaultdict
from pathlib import Path

from .entity_ods import EntityPackage
from .jsonutil import atomic_write, digest, dumps, read


def audit(snapshot, output, metadata=None):
    snapshot, output = Path(snapshot), Path(output)
    package = EntityPackage(output)
    source_ids, run_ids, player_ids = set(), set(), set()
    partitions = defaultdict(set)
    wr_categories, empty_wr = set(), set()
    versions, all_versions, matched = defaultdict(list), set(), set()
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

    for path in (snapshot / "requests").glob("*/*.json"):
        saved = read(path)
        source_ids.add(saved["response_id"])
        stored = package.get("responses", saved["response_id"])
        body = saved["body"]
        if package.decode(stored["body"]) != body or stored["metadata"] != {k:v for k,v in saved.items() if k != "body"}:
            raise ValueError("Source response/metadata changed")
        if saved["http_status"] != 200 or saved.get("error") or not isinstance(body, dict):
            continue
        endpoint = saved["endpoint"]
        if isinstance(body.get("game"), dict):
            detail = endpoint == "GetGameData"
            check_entity("games", body["game"].get("id"), "game_detail" if detail else "game_card", body if detail else body["game"])
        for game in body.get("gameList", []):
            check_entity("games", game.get("id"), "game_card", game)
        for run in body.get("runList", []):
            check_entity("runs", run.get("id"), "leaderboard_run", run)
            run_ids.add(run["id"])
            partitions[run.get("categoryId"), run.get("verified")].add(run["id"])
        for profile in body.get("playerList", []):
            check_entity("players", profile.get("id"), "player:" + digest(sorted(profile)), profile)
            if profile.get("id"):
                player_ids.add(profile["id"])
        if endpoint == "GetGameRecordHistory":
            scope = saved["params"].get("params", {})
            page = saved["params"].get("page", 1)
            check_entity("wr_history", digest([scope, page]), "source_wr_page", body)
            wr_categories.add(scope.get("categoryId"))
            if body == {}:
                empty_wr.add(scope.get("categoryId"))
            for event in body.get("recordEventList", []):
                for run in event.get("runList", []):
                    check_entity("runs", run.get("id"), "source_wr_run", run)
    if matched != all_versions:
        raise ValueError("New package contains versions absent from the journal")
    if source_ids != {r["key"] for r in package.records("responses")}:
        raise ValueError("Response identity set differs")
    checkpoint_count = 0
    for path in (snapshot / "checkpoints").glob("*.json"):
        checkpoint = read(path)
        coverage = checkpoint.get("coverage")
        if not checkpoint.get("complete") or not coverage:
            raise ValueError("Incomplete input partition")
        key = coverage["category_id"], coverage["status_code"]
        if len(partitions[key]) != coverage["unique_runs"]:
            raise ValueError("Checkpoint run count differs")
        checkpoint_count += 1
    for check in package.records("checks"):
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
        expected_categories = {c["id"] for c in raw["categories"] if c.get("isPerLevel") is False}
        meta.db.close()
        if wr_categories != expected_categories:
            raise ValueError("WR category coverage differs from metadata")
    result = {"snapshot_id": snapshot.name, "verified_against": "original_journal_and_partition_checkpoints",
        "source_responses": len(source_ids), "leaderboard_unique_run_ids": len(run_ids),
        "embedded_profile_ids": len(player_ids), "matched_entity_versions": len(matched),
        "checked_partitions": checkpoint_count, "wr_categories": sorted(wr_categories),
        "empty_wr_categories": sorted(empty_wr), "metadata_categories": sorted(expected_categories) if expected_categories else None,
        "limitations": "Local data preservation audit; not storage load performance or complete-site proof."}
    package.db.close()
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
