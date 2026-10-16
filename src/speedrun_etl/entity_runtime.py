"""Entity-v2 extraction, preserving the established sequential source collector."""
from pathlib import Path

from .collect import validate_conf, location, snapshot_id, metadata, collect_runs
from .entity_ods import EntityPackage, build
from .jsonutil import atomic_write, digest, read
from .source import Client, BudgetExceeded


def profiles(source, target, sid, input_sid):
    participants = set()
    total_runs = source.db.execute("SELECT count(*) FROM records WHERE dataset='runs'").fetchone()[0]
    print(f"Players: scanning run versions 0/{total_runs}", flush=True)
    for index, row in enumerate(source.records("runs"), 1):
        participants.update(p for p in row.get("player_ids", []) if isinstance(p, str) and p)
        if index % 1000 == 0 or index == total_runs:
            print(f"Players: run_versions={index}/{total_runs} participant_ids={len(participants)}", flush=True)
    found = set()
    copied_blocks = set()
    total_profiles = source.db.execute("SELECT count(*) FROM records WHERE dataset='players'").fetchone()[0]
    print(f"Players: scanning embedded profile versions 0/{total_profiles}", flush=True)
    for index, row in enumerate(source.records("players"), 1):
        if index % 1000 == 0 or index == total_profiles:
            print(f"Players: profile_versions={index}/{total_profiles} matched_ids={len(found)}", flush=True)
        if row["source_id"] not in participants:
            continue
        for key in row["raw"].get("blocks", []):
            if key not in copied_blocks:
                target.put("blocks", source.get("blocks", key))
                copied_blocks.add(key)
        target.put("players", row)
        found.add(row["source_id"])
    for index, row in enumerate(source.records("checks"), 1):
        if row["dataset"] == "players" and row["source_id"] in found:
            target.put("checks", row)
        if index % 10000 == 0:
            print(f"Players: history checks scanned={index}", flush=True)
    result = {"participants": len(participants), "available_profiles": len(found),
              "unavailable_or_unresolved": sorted(participants - found),
              "coverage": "partial" if participants - found else "verified_public_partitions",
              "scope": "participants_in_input_snapshot", "profile_time": "original_response_time"}
    target.put("manifests", {"key": sid + ":entity-v2", "snapshot_id": sid, "format_version": "entity-v2",
        "input_snapshot_ids": [input_sid], "result": result,
        "provenance": "derived_embedded_profiles_no_new_HTTP"})
    target.db.commit()
    print(f"Players: completed profiles={len(found)}/{len(participants)} unresolved={len(participants-found)} no profile HTTP requests", flush=True)
    return result


def extract(state, conf, kind):
    conf = validate_conf(conf)
    root = location(state, conf, kind)
    completed = root / "extracted.json"
    if completed.exists():
        print(f"Collect: {kind} already extracted; reusing immutable package", flush=True)
        reference = read(completed)
        if reference.get("format_version") != "entity-v2":
            raise ValueError("Legacy snapshot must not be resumed with entity-v2")
        return reference
    sid = snapshot_id(conf, kind)
    output = root / "entity-package"
    role = "runs" if kind == "runs" else "all"
    if kind == "players":
        source = EntityPackage(location(state, conf, "runs") / "entity-package")
        target = EntityPackage(output)
        profiles(source, target, sid, snapshot_id(conf, "runs"))
        source.db.close()
        target.db.close()
    else:
        client = Client(state, conf["collection_id"], sid, budget=conf["request_budget"],
                        adaptive_rate=conf.get("adaptive_rate", False), deadline=conf.get("deadline"))
        if kind == "games_metadata":
            if conf["scope"] == "catalog":
                from .catalog import collect_catalog
                result = collect_catalog(client, root)
            elif conf["scope"] == "ranking":
                from .ranking import rank_games
                result = rank_games(client, conf["top_n"])
            else:
                result = metadata(client, conf)
        else:
            source = EntityPackage(location(state, conf, "games_metadata") / "entity-package")
            try:
                result = collect_runs(client, root, source)
            except BudgetExceeded as error:
                result = {"coverage": "partial", "stop_reason": str(error),
                          "partitions": [read(p) for p in (root / "checkpoints").glob("*.json")]}
            finally:
                source.db.close()
        build(root, output, resume=True, source_result=result, load_role=role)
    package = EntityPackage(output, role=role)
    summary = package.summary()
    package.db.close()
    reference = {"snapshot_id": sid, "format_version": "entity-v2", "package_uri": str(output),
                 "role": role, "digest": digest(summary)}
    atomic_write(completed, reference)
    return reference
