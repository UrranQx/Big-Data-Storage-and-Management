"""Publish one validated analytical snapshot without collecting source data."""

from datetime import timedelta

from airflow.sdk import Asset, Metadata, dag, get_current_context, task

from speedrun_etl.analytics_quality import DDS_VALIDATED, MARTS_READY


@dag(
    schedule=[Asset(DDS_VALIDATED)],
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["lab3", "marts"],
)
def refresh_marts():
    @task(outlets=[Asset(MARTS_READY)], execution_timeout=timedelta(hours=4))
    def refresh():
        from speedrun_etl.marts import DEFAULT_COLLECTION, build_marts

        conf = get_current_context()["dag_run"].conf or {}
        result = build_marts(
            conf.get("collection", DEFAULT_COLLECTION), conf.get("as_of")
        )
        yield Metadata(Asset(MARTS_READY), result)
        return result

    refresh()


refresh_marts()
