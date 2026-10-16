"""Thin Airflow tasks. No Airflow/source I/O at DAG import time."""
from datetime import timedelta
import os


def make_dag(kind, child=None):
    from airflow.sdk import dag, task, get_current_context
    from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

    @dag(dag_id=kind, schedule=None, catchup=False, max_active_runs=1,
         default_args={"retries": 2, "retry_delay": timedelta(seconds=20)},
         tags=["lab1", "ods", "small-e2e"], render_template_as_native_obj=True)
    def pipeline():
        @task
        def initialize():
            from .collect import validate_conf, snapshot_id, location
            from .jsonutil import atomic_write, read
            from .storage import require_ready, set_status
            conf = validate_conf(dict(get_current_context()["dag_run"].conf or {}))
            if kind == "games_metadata" and conf["scope"] in ("ranking", "ranked_game"):
                parent = conf.get("catalog_snapshot_id") or conf["ranking_snapshot_id"]
                require_ready(parent)
                if conf["scope"] == "ranked_game":
                    from .ods import Package
                    from pathlib import Path
                    package = Package(Path(os.environ["ETL_STATE"]) / "snapshots" / parent / "package")
                    manifest = next(package.records("manifests"))
                    if not any(row["id"] == conf["game_id"] and row["url"] == conf["game"]
                               for row in manifest["result"]["top5"]):
                        raise ValueError("Game is not in the verified top-five selection")
            if kind != "games_metadata":
                parent = "games_metadata" if kind == "runs" else "runs"
                if get_current_context()["dag_run"].conf.get("input_snapshot_id") != snapshot_id(conf, parent):
                    raise ValueError("Explicit input snapshot ID must match this collection")
                require_ready(snapshot_id(conf, parent))
            path = location(os.environ["ETL_STATE"], conf, kind) / "config.json"
            if path.exists() and read(path) != conf:
                raise ValueError("Snapshot scope cannot change during retry")
            atomic_write(path, conf)
            set_status(snapshot_id(conf, kind), "running", conf)
            return conf

        @task(pool="source_api", execution_timeout=timedelta(hours=4))
        def collect(conf):
            from .collect import extract
            return extract(os.environ["ETL_STATE"], conf, kind)

        @task(pool="ods_load", execution_timeout=timedelta(hours=4))
        def load(reference, name):
            from .ods import Package
            from .storage import load_package
            return load_package(name, Package(reference["package_uri"]), reference["snapshot_id"])

        @task(trigger_rule="all_done", pool="ods_load")
        def finalize(conf):
            from .collect import snapshot_id, location
            from .jsonutil import atomic_write, digest, read, utcnow
            from .ods import Package
            from .storage import pg_connect, set_status, STORES, verify_store
            sid = snapshot_id(conf, kind)
            root = location(os.environ["ETL_STATE"], conf, kind)
            try:
                reference = read(root / "extracted.json")
                with pg_connect(True) as conn:
                    rows = conn.execute("SELECT store,package_digest FROM receipts WHERE snapshot_id=%s", [sid]).fetchall()
                if {r[0] for r in rows} != set(STORES) or {r[1] for r in rows} != {reference["digest"]}:
                    raise RuntimeError("Not all three stores acknowledged this package")
                summaries = {}
                for name, cls in STORES.items():
                    with cls() as store:
                        summaries[name] = verify_store(store, Package(reference["package_uri"]))
                evidence = {"snapshot_id": sid, "state": "ready", "verified_at": utcnow(), "stores": summaries,
                            "digest": reference["digest"]}
                atomic_write(root / "verified.json", evidence)
                set_status(sid, "ready", evidence)
            except Exception as error:
                details = {"exception_type": type(error).__name__, "snapshot_id": sid, "state": "failed"}
                atomic_write(root / "failed.json", details)
                set_status(sid, "failed", details)
                raise
            return {**conf, "input_snapshot_id": sid}

        conf = initialize()
        reference = collect(conf)
        branches = [load.override(task_id="load_" + name)(reference, name) for name in ("postgres", "mongo", "garage")]
        done = finalize(conf)
        for branch in branches:
            branch >> done
        if child:
            @task.short_circuit
            def needs_child(conf):
                return conf.get("scope") not in ("catalog", "ranking")
            gate = needs_child(done)
            trigger = TriggerDagRunOperator(task_id="trigger_" + child, trigger_dag_id=child,
                trigger_run_id="{{ dag_run.conf['collection_id'] }}-" + child,
                conf=done, wait_for_completion=False, reset_dag_run=False, skip_when_already_exists=True)
            gate >> trigger
    return pipeline()
