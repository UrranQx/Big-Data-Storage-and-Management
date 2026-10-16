"""Small read-only progress report; no source requests and no raw profile output."""
import json
import os
from collections import Counter
from datetime import datetime
from pathlib import Path
import sys

from .jsonutil import read


def progress(collection, http=False):
    state = Path(os.environ["ETL_STATE"])
    result = {"collection": collection, "snapshots": {}}
    budget = state / "collections" / collection / "budget.json"
    result["budget"] = read(budget) if budget.exists() else None
    telemetry = state / "collections" / collection / "telemetry.json"
    if telemetry.exists():
        result["http_telemetry"] = {k:v for k,v in read(telemetry).items() if k != "recent"}
    limiter = state / "limiter.json"
    result["limiter"] = read(limiter) if limiter.exists() else None
    responses = []
    for kind in ("games_metadata", "runs", "players"):
        root = state / "snapshots" / (collection + "-" + kind)
        if not root.exists():
            continue
        item = {"extracted": (root / "extracted.json").exists(), "verified": (root / "verified.json").exists()}
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
            with sqlite3.connect(f"file:{database}?mode=ro", uri=True) as db:
                item["records"] = dict(db.execute("SELECT dataset,count(*) FROM records GROUP BY dataset"))
                reference = read(root / "extracted.json") if (root / "extracted.json").exists() else {}
                if reference.get("role") == "runs":
                    item["records"]["players"] = 0
                    item["records"]["checks"] = db.execute("SELECT count(*) FROM records WHERE dataset='checks' AND json_extract(document,'$.dataset')!='players'").fetchone()[0]
            item["staging_format"] = "entity-v2"
        checkpoints = []
        for path in (root / "checkpoints").glob("*.json"):
            checkpoint = read(path)
            checkpoints.append({"name": path.stem, "pages": len(checkpoint.get("pages", {})),
                                "complete": checkpoint.get("complete"), "pass": checkpoint.get("pass")})
        item["checkpoints"] = checkpoints
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


if __name__ == "__main__":
    for collection in (arg for arg in sys.argv[1:] if arg != "--http"):
        print(json.dumps(progress(collection, http="--http" in sys.argv), default=str))
