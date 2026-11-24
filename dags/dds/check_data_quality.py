"""Проверка DDS по событию успешной нормализации."""

import json
import logging

from airflow.sdk import Asset, dag, get_current_context, task

from speedrun_etl.data_quality import DDS_READY


@dag(
    schedule=[Asset(DDS_READY)],
    catchup=False,
    max_active_runs=1,
    tags=["dds", "data-quality"],
)
def check_data_quality():
    @task
    def validate():
        from airflow.sdk.exceptions import AirflowFailException
        from speedrun_etl.data_quality import check_quality

        events = get_current_context()["triggering_asset_events"]
        batches = [
            batch
            for items in events.values()
            for event in items
            for batch in event.extra["batches"]
        ]
        if not batches:
            raise AirflowFailException("Data Quality requires a successful DDS event")

        report = check_quality(batches)
        logging.getLogger(__name__).info("Data Quality: %s", json.dumps(report))
        if any(report["anomalies"].values()) or any(
            volume["ods"] != volume["dds_matched"]
            for batch in report["batches"]
            for volume in batch["volumes"].values()
        ):
            raise AirflowFailException(
                "Data Quality failed. See the report in task logs."
            )
        return report

    validate()


check_data_quality()
