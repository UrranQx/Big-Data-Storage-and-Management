"""Проверка DDS по событию успешной нормализации."""

import json
import logging

from airflow.sdk import Asset, Metadata, dag, get_current_context, task

from speedrun_etl.analytics_quality import DDS_VALIDATED
from speedrun_etl.data_quality import DDS_READY


@dag(
    schedule=[Asset(DDS_READY)],
    catchup=False,
    max_active_runs=1,
    tags=["dds", "data-quality"],
)
def check_data_quality():
    @task(outlets=[Asset(DDS_VALIDATED)])
    def validate():
        from airflow.sdk.exceptions import AirflowFailException

        from speedrun_etl.analytics_quality import validate_dds

        events = get_current_context()["triggering_asset_events"]
        batches = [
            batch
            for items in events.values()
            for event in items
            for batch in event.extra["batches"]
        ]
        conf = get_current_context()["dag_run"].conf or {}
        if not batches and conf.get("collection"):
            from speedrun_etl.marts import collection_batches

            batches = collection_batches(conf["collection"])
        if not batches:
            raise AirflowFailException(
                "Data Quality requires a DDS event or an explicit existing collection"
            )

        report = validate_dds(batches)
        logging.getLogger(__name__).info("Data Quality: %s", json.dumps(report))
        yield Metadata(Asset(DDS_VALIDATED), {**report, "batches": batches})
        return report

    validate()


check_data_quality()
