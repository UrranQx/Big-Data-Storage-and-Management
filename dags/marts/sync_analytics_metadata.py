"""Metadata publication can retry independently of the analytical build."""

from datetime import timedelta

from airflow.sdk import Asset, dag, task

from speedrun_etl.analytics_quality import MARTS_READY


@dag(
    schedule=[Asset(MARTS_READY)],
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=1)},
    tags=["lab3", "metadata"],
)
def sync_analytics_metadata():
    @task(execution_timeout=timedelta(minutes=30))
    def synchronize():
        from speedrun_etl.analytics_metadata import synchronize

        return synchronize()

    synchronize()


sync_analytics_metadata()
