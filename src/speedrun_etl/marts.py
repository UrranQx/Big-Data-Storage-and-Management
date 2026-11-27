"""Build the Lab 3 warehouse from committed PostgreSQL snapshots."""

import argparse
import json
import logging
import time
from datetime import datetime
from pathlib import Path

from psycopg import sql
from psycopg.types.json import Jsonb

from .connections import pg_connect
from .dds import STAGES, load_batch, verify_snapshot_batches
from .jsonutil import digest

LOG = logging.getLogger(__name__)
PROJECT = Path(__file__).resolve().parents[2]
DEFAULT_COLLECTION = "lab1-popular500-20261030"


def collection_batches(collection):
    """Select completed game chains and require all four PostgreSQL receipts."""
    with pg_connect(True) as conn:
        entries = conn.execute(
            """SELECT q.collection_id, COALESCE((r.summary->'runs'->>'count')::bigint, 0)
               FROM entity_batch_queue q
               JOIN entity_receipts r ON r.snapshot_id=q.collection_id || '-runs'
                    AND r.store='postgres' AND r.namespace='entity_v3'
               WHERE q.batch_id=%s AND q.state='complete'
               ORDER BY 2, q.collection_id""",
            (collection,),
        ).fetchall()
    if not entries:
        raise ValueError(f"No completed PostgreSQL chains for {collection}")
    batches = [{stage: key + "-" + stage for stage in STAGES} for key, _ in entries]
    if not verify_snapshot_batches(batches):
        raise ValueError("The collection has missing PostgreSQL receipts")
    return batches


def prepare_dds(collection):
    batches = collection_batches(collection)
    ddl = PROJECT / "docs/design/entity_v3_3nf.sql"
    with pg_connect() as conn:
        conn.execute("""CREATE INDEX IF NOT EXISTS checks_snapshot_projection
                        ON entity_v3.checks ((document->>'snapshot_id'), projection)""")
        if conn.execute("SELECT to_regnamespace('speedrun_3nf')").fetchone()[0]:
            for name, table, key in (
                ("game", "dim_game_version", "game_id"),
                ("player", "dim_player_version", "player_id"),
                ("run", "fact_run_version", "run_id"),
            ):
                conn.execute(
                    f"CREATE INDEX IF NOT EXISTS {name}_version_timeline "
                    f"ON speedrun_3nf.{table} ({key}, valid_from)"
                )
    for number, batch in enumerate(batches, 1):
        started = time.monotonic()
        result = load_batch(batch, ddl)
        LOG.info(
            "DDS %s/%s %s %.1fs %s",
            number,
            len(batches),
            batch["runs"],
            time.monotonic() - started,
            result,
        )
    return {"collection": collection, "batches": len(batches)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command", choices=["prepare-dds", "prepare-analytics", "build"]
    )
    parser.add_argument("--collection", default=DEFAULT_COLLECTION)
    parser.add_argument("--as-of")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    if args.command == "prepare-dds":
        result = prepare_dds(args.collection)
    elif args.command == "prepare-analytics":
        from .analytics_dds import load_analytics_batch

        batches = collection_batches(args.collection)
        for number, batch in enumerate(batches, 1):
            LOG.info(
                "Analytics DDS %s/%s %s",
                number,
                len(batches),
                load_analytics_batch(batch),
            )
        result = {"batches": len(batches)}
    else:
        result = build_marts(args.collection, args.as_of)
    print(json.dumps(result, default=str), flush=True)


def build_marts(collection=DEFAULT_COLLECTION, as_of=None):
    from .analytics_quality import quality_key, validate_dds
    from .cohort_survival import build_survival
    from .record_history import build_history

    batches = collection_batches(collection)
    quality = validate_dds(batches)
    snapshots = sorted({value for batch in batches for value in batch.values()})
    scripts = sorted((PROJECT / "sql/marts").glob("*.sql"))
    method = digest(
        [p.read_text(encoding="utf-8") for p in scripts]
        + [
            (Path(__file__).parent / name).read_text(encoding="utf-8")
            for name in (
                "record_history.py",
                "cohort_survival.py",
                "analytics_dds.py",
                "marts.py",
            )
        ]
    )
    with pg_connect() as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        conn.execute("SELECT pg_advisory_xact_lock(hashtext('speedrun_3nf_load'))")
        if quality_key(conn, batches) != quality["quality_key"]:
            raise ValueError("DDS changed after validation. Retry the build.")
        missing = conn.execute(
            """
            SELECT count(*) FROM unnest(%s::text[]) b(batch_key)
            LEFT JOIN speedrun_3nf.analytics_batch a USING (batch_key) WHERE a.batch_key IS NULL
        """,
            ([digest(batch) for batch in batches],),
        ).fetchone()[0]
        if missing:
            raise ValueError(f"Analytics DDS is missing {missing} confirmed batches")
        cutoff = (
            datetime.fromisoformat(as_of)
            if as_of
            else conn.execute(
                "SELECT max(observed_until) FROM speedrun_3nf.snapshot_coverage WHERE snapshot_id=ANY(%s)",
                (snapshots,),
            ).fetchone()[0]
        )
        if cutoff is None or cutoff.tzinfo is None:
            raise ValueError("The cut-off must be an observed timestamp with timezone")
        build_id = digest([snapshots, cutoff.isoformat(), method])
        schema = "speedrun_build_" + build_id[:16]
        conn.execute("CREATE SCHEMA IF NOT EXISTS speedrun_marts")
        conn.execute("""CREATE TABLE IF NOT EXISTS speedrun_marts.build_registry (
            build_id text PRIMARY KEY,physical_schema text NOT NULL,as_of timestamptz NOT NULL,
            collection_id text NOT NULL,method_version text NOT NULL,snapshots jsonb NOT NULL,
            published_at timestamptz NOT NULL DEFAULT now()
        )""")
        if conn.execute(
            "SELECT 1 FROM speedrun_marts.build_registry WHERE build_id=%s", (build_id,)
        ).fetchone():
            active = conn.execute(
                "SELECT build_id FROM speedrun_marts.context"
            ).fetchone()[0]
            if active != build_id:
                publish_build(
                    conn, schema, build_id, cutoff, collection, method, snapshots
                )
            return {"build_id": build_id, "skipped": True}
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        conn.execute(
            sql.SQL("SET LOCAL search_path TO {},pg_catalog").format(
                sql.Identifier(schema)
            )
        )
        conn.execute("SET LOCAL work_mem='32MB'")
        conn.execute("SET LOCAL max_parallel_workers_per_gather=0")
        conn.execute("SET LOCAL cursor_tuple_fraction=1.0")
        conn.execute(
            "CREATE TABLE context (build_id text,as_of timestamptz,collection_id text,method_version text)"
        )
        conn.execute(
            "INSERT INTO context VALUES (%s,%s,%s,%s)",
            (build_id, cutoff, collection, method),
        )
        conn.execute(
            "CREATE TEMP TABLE _snapshots (snapshot_id text PRIMARY KEY) ON COMMIT DROP"
        )
        with conn.cursor().copy("COPY _snapshots FROM STDIN") as copy:
            for snapshot in snapshots:
                copy.write_row((snapshot,))
        for script in scripts:
            LOG.info("Building %s", script.name)
            conn.execute(script.read_text(encoding="utf-8"))
            if script.name == "01_base.sql":
                LOG.info("Reconstructing record events")
                build_history(conn)
        build_survival(conn)
        # Planner statistics must be available before readers see the new build.
        # In particular, context is one row, not the default estimate of 610.
        for (table,) in conn.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname=%s", (schema,)
        ).fetchall():
            conn.execute(
                sql.SQL("ANALYZE {}.{}").format(
                    sql.Identifier(schema), sql.Identifier(table)
                )
            )
        validate_build(conn)
        publish_build(conn, schema, build_id, cutoff, collection, method, snapshots)
        return {"build_id": build_id, "as_of": cutoff.isoformat(), "schema": schema}


def validate_build(conn):
    """Fail before atomic publication if a ranking or temporal invariant breaks."""
    checks = {
        "empty_pb": "SELECT (count(*)=0)::int FROM pb",
        "invalid_pb": "SELECT count(*) FROM pb WHERE place<1 OR place>field_size OR strength NOT BETWEEN 0 AND 100",
        "invalid_cohort": "SELECT count(*) FROM cohort_member WHERE days_to_target<0 OR followup_days<0 OR days_to_target>followup_days",
        "invalid_survival": "SELECT count(*) FROM cohort_survival WHERE reached_pct NOT BETWEEN 0 AND 100 OR at_risk<reached_people+censored_people",
        "medal_order": "SELECT count(*) FROM (SELECT overall_rank,rank() OVER (ORDER BY medal_score DESC) r FROM country_medals) q WHERE r<>overall_rank",
        "missing_manifest": "SELECT count(*) FROM _snapshots s LEFT JOIN speedrun_3nf.snapshot_coverage c USING(snapshot_id) WHERE c.snapshot_id IS NULL OR c.source_coverage='[]'::jsonb",
    }
    results = {
        name: int(conn.execute(query).fetchone()[0]) for name, query in checks.items()
    }
    conn.execute(
        "CREATE TABLE validation (check_name text PRIMARY KEY,failures bigint NOT NULL)"
    )
    for name, failures in results.items():
        conn.execute("INSERT INTO validation VALUES (%s,%s)", (name, failures))
    if any(results.values()):
        raise ValueError(f"Mart validation failed: {results}")


def publish_build(conn, schema, build_id, cutoff, collection, method, snapshots):
    tables = [
        row[0]
        for row in conn.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname=%s ORDER BY tablename",
            (schema,),
        )
    ]
    conn.execute(
        "DROP VIEW IF EXISTS speedrun_marts.calendar_activity, speedrun_marts.game_similarity RESTRICT"
    )
    for table in tables:
        if table in ("calendar_activity", "game_similarity"):
            continue
        if table == "context":
            query = sql.SQL(
                "CREATE OR REPLACE VIEW speedrun_marts.context AS SELECT * FROM {}.context"
            ).format(sql.Identifier(schema))
        else:
            query = sql.SQL("""CREATE OR REPLACE VIEW speedrun_marts.{} AS
                SELECT c.build_id,c.as_of,t.* FROM {}.{} t CROSS JOIN {}.context c""").format(
                sql.Identifier(table),
                sql.Identifier(schema),
                sql.Identifier(table),
                sql.Identifier(schema),
            )
        conn.execute(query)
    conn.execute(
        """INSERT INTO speedrun_marts.build_registry
        (build_id,physical_schema,as_of,collection_id,method_version,snapshots)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT (build_id) DO UPDATE SET published_at=now()""",
        (build_id, schema, cutoff, collection, method, Jsonb(snapshots)),
    )
    if conn.execute(
        "SELECT 1 FROM pg_roles WHERE rolname='analytics_reader'"
    ).fetchone():
        conn.execute("GRANT USAGE ON SCHEMA speedrun_marts TO analytics_reader")
        conn.execute(
            "GRANT SELECT ON ALL TABLES IN SCHEMA speedrun_marts TO analytics_reader"
        )


if __name__ == "__main__":
    main()
