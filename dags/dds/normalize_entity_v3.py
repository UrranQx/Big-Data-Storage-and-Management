"""DDS запускается по событию подтверждённой загрузки ODS."""

import os
from datetime import timedelta
from pathlib import Path

from airflow.sdk import Asset, Metadata, dag, get_current_context, task

from speedrun_etl.data_quality import DDS_READY
from speedrun_etl.dds import ODS_READY


@dag(
    schedule=[Asset(ODS_READY)],
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(seconds=30)},
    tags=["dds", "entity-v3"],
)
def normalize_entity_v3():
    @task.sensor(poke_interval=15, timeout=600, mode="reschedule")
    def verified_snapshots():
        from airflow.sdk import PokeReturnValue

        from speedrun_etl.dds import verify_snapshot_batches

        events = get_current_context()["triggering_asset_events"]
        batches = [
            event.extra["snapshots"] for items in events.values() for event in items
        ]
        conf = get_current_context()["dag_run"].conf or {}
        if not batches and conf.get("collection"):
            from speedrun_etl.marts import collection_batches

            # Explicit replay of committed ODS receipts; no source tasks are run.
            batches = collection_batches(conf["collection"])
        if not batches:
            raise ValueError(
                "DDS requires an ODS event or an explicit existing collection"
            )
        return PokeReturnValue(verify_snapshot_batches(batches), batches)

    @task(execution_timeout=timedelta(hours=4), outlets=[Asset(DDS_READY)])
    def normalize(batches):
        from speedrun_etl.analytics_dds import load_analytics_batch
        from speedrun_etl.dds import load_batch

        ddl = Path(
            os.environ.get(
                "DDS_SCHEMA_SQL", "/opt/project/docs/design/entity_v3_3nf.sql"
            )
        )
        results = [load_batch(batch, ddl) for batch in batches]
        for batch in batches:
            load_analytics_batch(batch)
        yield Metadata(Asset(DDS_READY), {"batches": batches})
        return results

    normalize(verified_snapshots())


normalize_entity_v3()
