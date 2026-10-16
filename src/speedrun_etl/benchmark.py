"""Small reproducible comparison in retained isolated namespaces; no source I/O."""
import argparse
import csv
from datetime import datetime, timezone
import os
from pathlib import Path
import time

from .cli import api
from .jsonutil import atomic_write, digest, dumps, file_lock, read
from .ods import Package, SETS, resolve_payload
from .storage import STORES, init_stores, load_package, require_ready


def collections_busy(state):
    # The queue can submit another DAG between two chains; wait for its lock
    # as well as Airflow runs, not just a momentarily empty task list.
    import fcntl
    for path in (state / "queues").glob("*/driver.lock"):
        with path.open("a") as handle:
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return True
            fcntl.flock(handle, fcntl.LOCK_UN)
    auth = api("POST", "/auth/token", {"username": "admin", "password": os.environ["AIRFLOW_ADMIN_PASSWORD"]})["access_token"]
    for kind in ("games_metadata", "runs", "players"):
        runs = api("GET", f"/api/v2/dags/{kind}/dagRuns?limit=100&order_by=-start_date", token=auth)["dag_runs"]
        if any(r["state"] in ("running", "queued") for r in runs):
            return True
    return False


def sizes(store, name):
    if name == "postgres":
        rows = [store.conn.execute("SELECT pg_table_size(%s::regclass),pg_indexes_size(%s::regclass)",
                                   [kind, kind]).fetchone() for kind in SETS]
        return {"data_bytes": sum(r[0] for r in rows), "index_bytes": sum(r[1] for r in rows),
                "reported_bytes": sum(sum(r) for r in rows), "size_method": "pg_table_size + pg_indexes_size"}
    if name == "mongo":
        stats = store.db.command("dbStats")
        return {"data_bytes": stats["dataSize"], "index_bytes": stats["indexSize"],
                "reported_bytes": stats["storageSize"] + stats["indexSize"],
                "size_method": "Mongo storageSize + indexSize"}
    objects = [item for page in store.client.get_paginator("list_objects_v2").paginate(
        Bucket=store.bucket, Prefix=store.prefix) for item in page.get("Contents", [])]
    return {"data_bytes": sum(item["Size"] for item in objects), "index_bytes": None,
            "reported_bytes": sum(item["Size"] for item in objects),
            "size_method": "S3 sum object sizes, excludes Garage volume metadata", "object_count": len(objects)}


def filter_observations(store, name, selected):
    game, category, status = selected
    if name == "postgres":
        return [r[0] for r in store.conn.execute(
            "SELECT document FROM observations WHERE entity_type='run' AND game_id=%s AND category_id=%s AND status=%s",
            [game, category, status])]
    if name == "mongo":
        rows = store.db.observations.find({"entity_type": "run", "context.game_id": game,
                                          "context.category_id": category, "context.status": status})
        return [{k:v for k,v in r.items() if k != "_id"} for r in rows]
    rows = []
    from .jsonutil import loads
    for page in store.client.get_paginator("list_objects_v2").paginate(
        Bucket=store.bucket, Prefix=store.prefix + "observations/speedruncom/run/"):
        for obj in page.get("Contents", []):
            response = store.client.get_object(Bucket=store.bucket, Key=obj["Key"])
            with response["Body"] as stream:
                row = loads(stream.read())
            if all(row["context"].get(k) == v for k,v in zip(("game_id", "category_id", "status"), selected)):
                rows.append(row)
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--collection", default="half-life-full-001")
    parser.add_argument("--wait", action="store_true", help="Wait for the collection queue and DAGs to finish")
    parser.add_argument("--log", action="store_true", help="Write unattended output into the state volume")
    args = parser.parse_args()
    import re
    if not re.fullmatch(r"[a-zA-Z0-9_-]{1,80}", args.collection):
        parser.error("Invalid collection ID")
    state = Path(os.environ["ETL_STATE"])
    if args.log:
        import sys
        (state / "benchmarks").mkdir(exist_ok=True)
        sys.stdout = sys.stderr = (state / "benchmarks" / "driver.log").open("a", buffering=1)
    print("Checking collection queue before benchmark", flush=True)
    while collections_busy(state):
        if not args.wait:
            raise RuntimeError("Wait until collection DAGs finish before benchmarking")
        time.sleep(30)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    root = state / "benchmarks" / stamp
    package = Package(root / "package")
    inputs = {}
    for kind in ("games_metadata", "runs", "players"):
        snapshot = args.collection + "-" + kind
        require_ready(snapshot)
        original = state / "snapshots" / snapshot
        inputs[snapshot] = read(original / "verified.json")["digest"]
        for dataset in SETS:
            for row in Package(original / "package").records(dataset):
                package.put(dataset, row)
    run_observations = [r for r in package.records("observations") if r["entity_type"] == "run"]
    ids = sorted({r["source_id"] for r in run_observations}, key=digest)[:100]
    sample = next(r for r in run_observations if r["context"].get("status") == "verified")
    selection = tuple(sample["context"][k] for k in ("game_id", "category_id", "status"))
    input_summary = package.summary()
    input_digest = digest(input_summary)
    records = sum(s["count"] for s in input_summary.values())
    logical_bytes = sum(len(dumps(r).encode()) for kind in SETS for r in package.records(kind))
    atomic_write(root / "input.json", {"source_snapshots": inputs, "digest": input_digest,
        "counts": input_summary, "logical_bytes": logical_bytes, "read_ids": ids, "filter": list(selection),
        "notes": "Verified load includes readback and reconstruction. First read pass is not guaranteed cold. "
                 "S3 filtering is client-side; reported S3 bytes exclude physical Garage overhead."})
    csv_path = root / "measurements.csv"
    fields = ["repeat", "store", "namespace", "operation", "pass", "seconds", "count", "digest",
              "data_bytes", "index_bytes", "reported_bytes", "size_method", "object_count", "logical_bytes"]
    read_checks = {}
    with file_lock(state / "source.lock"), csv_path.open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=fields)
        writer.writeheader()
        for repeat in range(3):
            namespace = f"test_bench_{stamp}_{repeat}"
            os.environ["ETL_NAMESPACE"] = namespace
            init_stores()
            order = list(STORES)
            order = order[repeat:] + order[:repeat]
            for name in order:
                base = {"repeat": repeat + 1, "store": name, "namespace": namespace, "logical_bytes": logical_bytes}
                for operation in ("load_verified", "replay_verified"):
                    started = time.perf_counter()
                    result = load_package(name, package, namespace)
                    elapsed = time.perf_counter() - started
                    if result["digest"] != input_digest:
                        raise ValueError("Benchmark load changed content")
                    writer.writerow({**base, "operation": operation, "seconds": elapsed,
                                     "count": records, "digest": result["digest"]})
                    output.flush()
                    print(stamp, repeat + 1, name, operation, round(elapsed, 3), flush=True)
                with STORES[name]() as store:
                    measured = sizes(store, name)
                    writer.writerow({**base, "operation": "size", **measured})
                    if name == "garage" and measured["object_count"] != records:
                        raise ValueError("Replay created unexpected S3 objects")
                    if name in ("postgres", "mongo"):
                        for kind in SETS:
                            count = (store.conn.execute(f"SELECT count(*) FROM {kind}").fetchone()[0]
                                     if name == "postgres" else store.db[kind].count_documents({}))
                            if count != input_summary[kind]["count"]:
                                raise ValueError("Replay changed physical record count")
                    for pass_no in range(3):
                        for operation in ("history_100", "latest_100", "filter"):
                            started = time.perf_counter()
                            if operation == "filter":
                                values = sorted(filter_observations(store, name, selection), key=lambda r:r["key"])
                            else:
                                values = []
                                for source_id in ids:
                                    history = store.history("run", source_id)
                                    values.append(history if operation == "history_100" else
                                                  resolve_payload(store.get, history[-1]["payload"]))
                            elapsed = time.perf_counter() - started
                            check = digest(values)
                            previous = read_checks.setdefault(operation, check)
                            if check != previous:
                                raise ValueError("Read benchmark returned different data")
                            writer.writerow({**base, "operation": operation, "pass": pass_no + 1,
                                             "seconds": elapsed, "count": len(values), "digest": check})
                            output.flush()
        atomic_write(root / "completed.json", {"input_digest": input_digest, "csv": str(csv_path), "repeats": 3})
    print("Benchmark completed:", root, flush=True)


if __name__ == "__main__":
    main()
