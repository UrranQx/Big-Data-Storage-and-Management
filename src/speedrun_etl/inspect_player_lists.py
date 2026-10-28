"""Read-only comparison of embedded player lists with locally saved runs. No HTTP."""
import argparse
from collections import Counter
from pathlib import Path

from .jsonutil import atomic_write, digest, dumps, read


def inspect(snapshot):
    groups = {}
    scanned = 0
    for path in snapshot.glob("requests/*/*.json"):
        response = read(path)
        body = response.get("body")
        if response.get("endpoint") != "GetGameLeaderboard2" or not isinstance(body, dict):
            continue
        if response.get("http_status") != 200 or "runList" not in body:
            continue
        params = response["params"]["params"]
        key = (params["gameId"], params["categoryId"], params.get("verified"))
        group = groups.setdefault(key, {"responses": 0, "runs": {}, "profile_ids": set(),
            "profile_counts": Counter(), "id_set_digests": Counter(), "examples": {},
            "first": None, "last": None, "counts": set()})
        profiles = body.get("playerList", [])
        ids = {p["id"] for p in profiles}
        signature = digest(sorted(ids))
        group["responses"] += 1
        group["profile_ids"].update(ids)
        group["profile_counts"][len(profiles)] += 1
        group["id_set_digests"][signature] += 1
        group["examples"].setdefault(signature, {"response_id": response["response_id"],
            "page": response["params"].get("page"), "profiles": len(profiles)})
        group["counts"].add(body.get("pagination", {}).get("count"))
        for run in body["runList"]:
            group["runs"].setdefault(run["id"], set()).update(run.get("playerIds", []))
        stamp = response["fetched_at"]
        reference = (stamp, path, ids)
        if group["first"] is None or stamp < group["first"][0]:
            group["first"] = reference
        if group["last"] is None or stamp > group["last"][0]:
            group["last"] = reference
        scanned += 1
    result = []
    for (game, category, status), group in sorted(groups.items()):
        participants = set().union(*group["runs"].values()) if group["runs"] else set()
        first, last = group["first"], group["last"]
        first_response, last_response = read(first[1]), read(last[1])
        def state_hash(response):
            return digest(sorted(response["body"].get("playerList", []), key=lambda p: p["id"]))
        result.append({"game_id": game, "category_id": category, "status": status,
            "responses": group["responses"], "observed_unique_run_ids": len(group["runs"]),
            "source_run_counts": sorted(group["counts"], key=str),
            "observed_unique_participant_ids": len(participants),
            "union_profile_ids": len(group["profile_ids"]),
            "profile_count_histogram": {str(k): v for k,v in sorted(group["profile_counts"].items())},
            "distinct_profile_id_sets": len(group["id_set_digests"]),
            "profile_id_sets": [{"digest": k, "responses": v, **group["examples"][k]}
                                for k,v in group["id_set_digests"].most_common()],
            "first_response_id": first_response["response_id"],
            "last_response_id": last_response["response_id"],
            "first_page": first_response["params"].get("page"),
            "last_page": last_response["params"].get("page"),
            "first_profiles": len(first[2]), "last_profiles": len(last[2]),
            "first_profiles_missing_observed_participants": len(participants-first[2]),
            "last_profiles_missing_observed_participants": len(participants-last[2]),
            "union_profiles_missing_observed_participants": len(participants-group["profile_ids"]),
            "first_profiles_not_in_observed_runs": len(first[2]-participants),
            "first_last_profile_ids_equal": first[2] == last[2],
            "first_last_full_profile_states_equal": state_hash(first_response) == state_hash(last_response),
            "guest_participant_ids": sum(p.startswith("u-") for p in participants)})
    return {"snapshot_id": snapshot.name, "responses_scanned": scanned, "sections": result}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--snapshots", nargs="+", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if any(Path(s).name != s or s in (".", "..") for s in args.snapshots):
        parser.error("Snapshot IDs must be plain directory names")
    report = {"method": "All saved successful GetGameLeaderboard2 responses; no HTTP. "
        "ID unions include all saved passes and boundary rechecks, not only final checkpoints. "
        "Full profile states compared only in first and last responses per section. "
        "Incomplete snapshots cannot prove coverage of all source runs.", "snapshots": []}
    for sid in args.snapshots:
        path = args.state / "snapshots" / sid
        if not path.is_dir():
            raise ValueError(f"Missing snapshot: {sid}")
        result = inspect(path)
        report["snapshots"].append(result)
        atomic_write(args.output, report)
        for row in result["sections"]:
            print(dumps({k: row[k] for k in ("game_id", "category_id", "status", "responses",
                "observed_unique_run_ids", "observed_unique_participant_ids", "first_profiles",
                "first_profiles_missing_observed_participants", "first_profiles_not_in_observed_runs",
                "distinct_profile_id_sets", "first_last_full_profile_states_equal")}), flush=True)


if __name__ == "__main__":
    main()
