"""Offline grouped-history prototype. Never modifies source packages or live ODS."""

import argparse
import json
from pathlib import Path
import time

from .jsonutil import atomic_write, digest, dumps, loads
from .ods import Package, restore, LISTS, OBJECTS


def state_key(observation):
    return "state-v2:" + digest(
        {
            k: observation[k]
            for k in (
                "source_system",
                "api_variant",
                "snapshot_id",
                "entity_type",
                "source_id",
                "payload",
                "context",
            )
        }
    )


def expand(group):
    """Recover every original observation, including A -> A and A -> B -> A."""
    common = {
        k: v for k, v in group.items() if k not in ("occurrences", "history_format")
    }
    for response_id, path, observed_at in group["occurrences"]:
        yield {
            **common,
            "key": digest([response_id, group["entity_type"], path]),
            "response_id": response_id,
            "path": path,
            "observed_at": observed_at,
        }


def repack(source_root, target_root, response_ids=None):
    source_root, target_root = Path(source_root), Path(target_root)
    if target_root.exists():
        raise ValueError(
            "Use a new output directory; existing packages are never overwritten"
        )
    started = time.perf_counter()
    source = Package(source_root)
    target = Package(target_root, compact=True)
    db = target.db
    db.execute(
        "CREATE TABLE groups (key TEXT PRIMARY KEY, anchor TEXT NOT NULL) WITHOUT ROWID"
    )
    db.execute("""CREATE TABLE events (group_key TEXT NOT NULL, old_key TEXT NOT NULL,
        event TEXT NOT NULL, old_digest TEXT NOT NULL, PRIMARY KEY(group_key,old_key)) WITHOUT ROWID""")
    selected = set(response_ids) if response_ids else None

    def responses():
        if selected:
            for key in sorted(selected):
                yield source.get("responses", key)
        else:
            yield from source.records("responses")

    def selected_observations():
        for response in responses():
            for ref in response["references"]:
                path = ref["path"]
                kind = ({**LISTS, **OBJECTS}).get(path[0]) if path else None
                if kind:
                    key = digest([response["key"], kind, path])
                    yield source.get("observations", key)

    original_count, known_groups = 0, set()
    # Stream once; repeated appearances become short entries in one entity-state
    # record rather than millions of separately addressed ODS objects.
    for observation in (
        selected_observations() if selected else source.records("observations")
    ):
        key = state_key(observation)
        if key not in known_groups:
            db.execute("INSERT INTO groups VALUES(?,?)", (key, dumps(observation)))
            known_groups.add(key)
        event = [
            observation["response_id"],
            observation["path"],
            observation["observed_at"],
        ]
        db.execute(
            "INSERT INTO events VALUES(?,?,?,?)",
            (
                key,
                observation["key"],
                json.dumps(event, separators=(",", ":")),
                digest(observation),
            ),
        )
        original_count += 1
        if original_count % 10000 == 0:
            target.flush()
        if original_count % 250000 == 0:
            print(
                "Grouped",
                original_count,
                "appearances into",
                len(known_groups),
                "states",
                flush=True,
            )
    target.flush()
    # In full mode retain all source payloads and response records, including
    # unknown JSON fields and failed responses. Nothing is normalized away.
    required = set()
    if selected:
        for response in responses():
            required.add(response["envelope"])
            required.update(r["payload"] for r in response["references"])

    def copy_payload(key):
        if target.contains("payloads", key):
            return
        item = source.get("payloads", key)
        for child in item.get("blocks", []):
            copy_payload(child)
        target.put("payloads", item)

    if selected is None:
        for item in source.records("payloads"):
            target.put("payloads", item)
    else:
        for key in sorted(required):
            copy_payload(key)
    checked = 0
    for key, anchor in db.execute("SELECT key,anchor FROM groups ORDER BY key"):
        events = list(
            db.execute(
                "SELECT event,old_digest FROM events WHERE group_key=? ORDER BY old_key",
                (key,),
            )
        )
        occurrences = [json.loads(row[0]) for row in events]
        first = min(occurrences, key=lambda e: (e[2], e[0], dumps(e[1])))
        group = {
            **loads(anchor),
            "key": key,
            "response_id": first[0],
            "path": first[1],
            "observed_at": first[2],
            "history_format": "grouped-v2",
            "occurrences": occurrences,
        }
        for recovered, (_, expected_digest) in zip(expand(group), events):
            if digest(recovered) != expected_digest:
                raise ValueError("Grouped history changed an original observation")
            checked += 1
        if len(dumps(group).encode()) > 8 * 1024**2:
            raise ValueError(
                "Grouped record exceeds budget; split its occurrences before production use"
            )
        target.put("observations", group)
    response_count = 0
    for response in responses():
        target.put("responses", response)
        restore(target.get, response, resolver=target.resolve)
        response_count += 1
    if checked != original_count:
        raise ValueError("History membership differs")
    target.put(
        "manifests",
        {
            "key": "repack-v2:1",
            "history_format": "grouped-v2",
            "source_package": str(source_root),
            "original_observations": original_count,
            "grouped_observations": len(known_groups),
            "checked_observations": checked,
            "restored_responses": response_count,
            "scope": "selected_responses" if selected else "full_package",
            "source_manifests": list(source.records("manifests")),
            "limitation": "Offline prototype; legacy production loaders do not support grouped-v2",
        },
    )
    target.flush()
    summary = target.summary()
    # Temporary indexes contain only derived verification state; drop these new
    # tables after successful verification, never source or existing ODS data.
    db.execute("DROP TABLE events")
    db.execute("DROP TABLE groups")
    target.flush()
    db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    db.close()
    if source.db is not None:
        source.db.close()
    result = {
        "original_observations": original_count,
        "grouped_observations": len(known_groups),
        "checked_observations": checked,
        "restored_responses": response_count,
        "records": summary,
        "seconds": round(time.perf_counter() - started, 3),
        "target": str(target_root),
    }
    atomic_write(target_root.parent / (target_root.name + "-result.json"), result)
    print(json.dumps(result), flush=True)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True)
    parser.add_argument("--target", required=True)
    parser.add_argument("--response", action="append")
    args = parser.parse_args()
    repack(args.source, args.target, args.response)


if __name__ == "__main__":
    main()
