"""Audited quality gate for DDS publication, with an explicit date quarantine."""

from pathlib import Path

from psycopg.types.json import Jsonb

from .connections import pg_connect
from .data_quality import check_quality
from .jsonutil import digest

DDS_VALIDATED = "x-speedrun://dds/entity-v3/validated"
MARTS_READY = "x-speedrun://marts/analytics/ready"


def quality_key(conn, batches):
    rows = conn.execute(
        """
        SELECT batch_key,projection_version,loaded_at::text FROM speedrun_3nf.analytics_batch
        WHERE batch_key=ANY(%s) ORDER BY batch_key
    """,
        ([digest(batch) for batch in batches],),
    ).fetchall()
    if len(rows) != len(batches):
        raise ValueError("Analytics DDS projection is incomplete")
    return digest(
        [[list(row) for row in rows], Path(__file__).read_text(encoding="utf-8")]
    )


def validate_dds(batches):
    with pg_connect() as conn:
        conn.execute("SELECT pg_advisory_xact_lock(hashtext('speedrun_3nf_load'))")
        conn.execute("""CREATE TABLE IF NOT EXISTS speedrun_3nf.analytics_quality (
            quality_key text PRIMARY KEY, checked_at timestamptz NOT NULL DEFAULT now(),
            passed boolean NOT NULL, report jsonb NOT NULL
        )""")
        key = quality_key(conn, batches)
        previous = conn.execute(
            "SELECT passed,report FROM speedrun_3nf.analytics_quality WHERE quality_key=%s",
            (key,),
        ).fetchone()
        if previous:
            if not previous[0]:
                raise ValueError("The DDS quality gate previously failed")
            return {"quality_key": key, "cached": True}
        report = check_quality(batches)
        failures = {
            name: count
            for name, count in report["anomalies"].items()
            if name != "verification_before_submission" and count
        }
        mismatches = sum(
            volume["ods"] != volume["dds_matched"]
            for batch in report["batches"]
            for volume in batch["volumes"].values()
        )
        report["policy"] = {
            "verification_before_submission": "Preserve DDS, quarantine from all analytical runs and medals",
            "all_other_anomalies": "Block publication",
        }
        passed = not failures and not mismatches
        conn.execute(
            "INSERT INTO speedrun_3nf.analytics_quality (quality_key,passed,report) VALUES (%s,%s,%s)",
            (key, passed, Jsonb(report)),
        )
    if not passed:
        raise ValueError(
            f"DDS quality failed: {failures}, volume mismatches={mismatches}"
        )
    return {
        "quality_key": key,
        "anomalies": report["anomalies"],
        "batches": len(batches),
    }
