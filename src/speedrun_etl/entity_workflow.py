"""Создаёт цепочку DAG сущностей и подтверждает три загрузки каждого входа.

Источник использует общий pacing; trigger_next направляет v3 в WR, v2 в players."""

from datetime import timedelta
import os
from .model import V3, version, namespace, role, dag_run_id


def needs_child(conf):
    return conf["scope"] not in ("catalog", "ranking") and not conf.get(
        "metadata_only", False
    )


def next_kind(kind, conf):
    if kind == "games_metadata":
        return "runs"
    if kind == "runs":
        return "wr_history" if version(conf) == V3 else "players"
    if kind == "wr_history":
        return "players"
    return None


def make_dag(kind, child=None):
    """Создаёт DAG с тремя загрузками, общей проверкой и условным запуском ребёнка."""
    from airflow.sdk import dag, task, get_current_context
    from airflow.providers.standard.operators.trigger_dagrun import (
        TriggerDagRunOperator,
    )

    @dag(
        dag_id=kind,
        schedule=None,
        catchup=False,
        max_active_runs=1,
        default_args={"retries": 2, "retry_delay": timedelta(seconds=30)},
        tags=["lab1", "ods", "entities"],
        render_template_as_native_obj=True,
    )
    def pipeline():
        @task
        def initialize():
            from .collect import validate_conf, location, snapshot_id
            from .jsonutil import atomic_write, read
            from .entity_storage import ready

            conf = validate_conf(dict(get_current_context()["dag_run"].conf or {}))
            if kind == "wr_history" and version(conf) != V3:
                raise ValueError("Separate WR DAG requires entity-v3")
            if conf["scope"] not in (
                "e2e",
                "full_game",
                "catalog",
                "ranking",
                "game",
                "single_game",
            ):
                raise ValueError("Unsupported source scope")
            root = location(os.environ["ETL_STATE"], conf, kind)
            if kind == "games_metadata" and version(conf) == V3:
                from .entity_storage import init_entities

                init_entities(namespace(conf))
            if kind == "games_metadata" and conf["scope"] in ("ranking", "game"):
                from .batch import validate_selection

                validate_selection(os.environ["ETL_STATE"], conf)
                if conf.get("repair_from_snapshot_id"):
                    from .repair import validate_base

                    validate_base(os.environ["ETL_STATE"], conf)
            if kind != "games_metadata":
                if conf.get("metadata_only"):
                    raise ValueError(
                        "Metadata-only collections cannot start runs or players"
                    )
                parent = (
                    "games_metadata"
                    if kind == "runs"
                    else "wr_history"
                    if kind == "players" and version(conf) == V3
                    else "runs"
                )
                expected = snapshot_id(conf, parent)
                if (
                    get_current_context()["dag_run"].conf.get("input_snapshot_id")
                    != expected
                ):
                    raise ValueError("Explicit input snapshot ID is required")
                ready(expected, namespace=namespace(conf), role=role(parent))
                if kind in ("wr_history", "players") and version(conf) == V3:
                    ready(
                        snapshot_id(conf, "runs"),
                        namespace=namespace(conf),
                        role="runs",
                    )
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
                return write(
                    name,
                    package,
                    reference["snapshot_id"],
                    namespace=reference.get("namespace", "entity_v2"),
                    role=reference["role"],
                )
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
                evidence = ready(
                    reference["snapshot_id"],
                    namespace=namespace(conf),
                    role=reference["role"],
                    expected=reference["digest"],
                )
                atomic_write(
                    root / "verified.json",
                    {
                        **evidence,
                        "snapshot_id": reference["snapshot_id"],
                        "format_version": version(conf),
                        "state": "ready",
                        "verified_at": utcnow(),
                    },
                )
            except Exception as error:
                atomic_write(
                    root / "failed.json",
                    {
                        "snapshot_id": snapshot_id(conf, kind),
                        "exception_type": type(error).__name__,
                        "state": "failed",
                    },
                )
                raise
            forwarded = dict(conf)
            if kind == "games_metadata" and version(conf) == V3 and needs_child(conf):
                from .entity_ods import EntityPackage

                package = EntityPackage(reference["package_uri"])
                try:
                    result = (
                        next(package.records("manifests")).get("source_result") or {}
                    )
                    forwarded.setdefault(
                        "game_label",
                        result.get("game_name") or conf.get("game") or "game",
                    )
                    forwarded["display_game_id"] = result["game_id"]
                finally:
                    package.db.close()
            destination = next_kind(kind, forwarded)
            return {
                **forwarded,
                "input_snapshot_id": reference["snapshot_id"],
                "next_dag_id": destination,
                "next_run_id": dag_run_id(forwarded, destination)
                if destination
                else None,
                "child_run_ids": {
                    k: dag_run_id(forwarded, k)
                    for k in ("runs", "wr_history", "players")
                },
            }

        conf = initialize()
        reference = collect(conf)
        done = finalize(conf)
        for name in ("postgres", "mongo", "garage"):
            load.override(task_id="load_" + name)(reference, name) >> done
        if child:

            @task.short_circuit
            def should_trigger_child(conf):
                return needs_child(conf)

            gate = should_trigger_child.override(task_id="needs_child")(done)
            trigger = TriggerDagRunOperator(
                task_id="trigger_next" if kind == "runs" else "trigger_" + child,
                trigger_dag_id="{{ ti.xcom_pull(task_ids='finalize')['next_dag_id'] }}",
                trigger_run_id="{{ ti.xcom_pull(task_ids='finalize')['next_run_id'] }}",
                conf=done,
                wait_for_completion=False,
                reset_dag_run=False,
                skip_when_already_exists=True,
            )
            gate >> trigger

    return pipeline()
