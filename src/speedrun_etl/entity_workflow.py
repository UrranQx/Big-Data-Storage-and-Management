"""Three entity-v2 DAGs; source pacing and store concurrency are independent."""
from datetime import timedelta
import os


def make_dag(kind, child=None):
    from airflow.sdk import dag, task, get_current_context
    from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

    @dag(dag_id=kind, schedule=None, catchup=False, max_active_runs=1,
         default_args={"retries": 2, "retry_delay": timedelta(seconds=30)},
         tags=["lab1", "ods", "entity-v2"], render_template_as_native_obj=True)
    def pipeline():
        @task
        def initialize():
            from .collect import validate_conf, location, snapshot_id
            from .jsonutil import atomic_write, read
            from .entity_storage import ready
            conf = validate_conf(dict(get_current_context()["dag_run"].conf or {}))
            if conf["scope"] not in ("e2e", "full_game", "catalog", "ranking", "game"):
                raise ValueError("Unsupported source scope")
            root = location(os.environ["ETL_STATE"], conf, kind)
            if kind == "games_metadata" and conf["scope"] in ("ranking", "game"):
                from .batch import validate_selection
                validate_selection(os.environ["ETL_STATE"], conf)
            if kind != "games_metadata":
                parent = "games_metadata" if kind == "runs" else "runs"
                expected = snapshot_id(conf, parent)
                if get_current_context()["dag_run"].conf.get("input_snapshot_id") != expected:
                    raise ValueError("Explicit input snapshot ID is required")
                ready(expected, role="runs" if parent == "runs" else "all")
            path = root / "config.json"
            if path.exists() and read(path) != conf:
                raise ValueError("Cannot change snapshot scope during retry")
            if (root / "package").exists():
                raise ValueError("Legacy package detected; use a new collection ID")
            atomic_write(path, conf)
            return conf

        @task(pool="source_api", execution_timeout=timedelta(hours=4))
        def collect(conf):
            from .entity_runtime import extract
            return extract(os.environ["ETL_STATE"], conf, kind)

        @task(pool="ods_load", execution_timeout=timedelta(hours=4))
        def load(reference, name):
            from .entity_ods import EntityPackage
            from .entity_storage import load as write
            package = EntityPackage(reference["package_uri"], role=reference["role"])
            try:
                return write(name, package, reference["snapshot_id"], role=reference["role"])
            finally:
                package.db.close()

        @task(trigger_rule="all_done")
        def finalize(conf):
            from .collect import snapshot_id, location
            from .jsonutil import atomic_write, read, utcnow
            from .entity_storage import ready
            root = location(os.environ["ETL_STATE"], conf, kind)
            try:
                reference = read(root / "extracted.json")
                evidence = ready(reference["snapshot_id"], role=reference["role"], expected=reference["digest"])
                atomic_write(root / "verified.json", {**evidence, "snapshot_id": reference["snapshot_id"],
                    "format_version": "entity-v2", "state": "ready", "verified_at": utcnow()})
            except Exception as error:
                atomic_write(root / "failed.json", {"snapshot_id": snapshot_id(conf, kind),
                    "exception_type": type(error).__name__, "state": "failed"})
                raise
            return {**conf, "input_snapshot_id": reference["snapshot_id"]}

        conf = initialize()
        reference = collect(conf)
        done = finalize(conf)
        for name in ("postgres", "mongo", "garage"):
            load.override(task_id="load_" + name)(reference, name) >> done
        if child:
            @task.short_circuit
            def needs_child(conf):
                return conf["scope"] not in ("catalog", "ranking")
            gate = needs_child(done)
            trigger = TriggerDagRunOperator(task_id="trigger_" + child, trigger_dag_id=child,
                trigger_run_id="{{ dag_run.conf['collection_id'] }}-" + child,
                conf=done, wait_for_completion=False, reset_dag_run=False, skip_when_already_exists=True)
            gate >> trigger
    return pipeline()
