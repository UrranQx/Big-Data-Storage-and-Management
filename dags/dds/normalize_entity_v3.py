"""DDS запускается по событию подтверждённой загрузки ODS."""

from datetime import timedelta
from pathlib import Path
import os

from airflow.sdk import Asset, dag, get_current_context, task

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
        if not batches:
            raise ValueError("DDS requires an ODS asset event with snapshot IDs")
        return PokeReturnValue(verify_snapshot_batches(batches), batches)

    @task(execution_timeout=timedelta(hours=4))
    def normalize(batches):
        from speedrun_etl.dds import load_batch

        ddl = Path(
            os.environ.get(
                "DDS_SCHEMA_SQL", "/opt/project/docs/design/entity_v3_3nf.sql"
            )
        )
        return [load_batch(batch, ddl) for batch in batches]

    normalize(verified_snapshots())


normalize_entity_v3()
