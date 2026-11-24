"""Проверки покрытия снимков ODS и аномалий в DDS."""

from collections import Counter

from psycopg import sql

from .connections import pg_connect
from .dds import Projection, observations
from .jsonutil import digest


DDS_READY = "x-speedrun://dds/entity-v3/ready"

VERSION_TARGETS = (
    ("games", ["game_card", "game_detail"], "dim_game_version", "game_id"),
    ("runs", ["run_core"], "fact_run_version", "run_id"),
    ("players", ["player:%"], "dim_player_version", "player_id"),
)


def version_coverage(conn, batch, dataset, projections, table, column):
    snapshots = list(batch.values())
    if dataset == "players":
        snapshots = [batch[stage] for stage in ("runs", "wr_history") if stage in batch]
    reader = Projection(conn)
    expected = Counter()
    for row, observed_at in observations(conn, dataset, snapshots, projections):
        if dataset == "players" and reader.decode(row["raw"]).get("isGuest") is True:
            continue
        expected[row["source_id"], observed_at] += 1

    matched = 0
    query = sql.SQL("""
        SELECT coalesce(sum(least(e.expected, matching.n)), 0)
        FROM unnest(%s::text[], %s::timestamptz[], %s::bigint[])
             AS e(source_id, observed_at, expected)
        CROSS JOIN LATERAL (
            SELECT count(*) AS n FROM speedrun_3nf.{} d
            WHERE d.{}=e.source_id AND d.valid_from=e.observed_at
        ) matching
    """).format(sql.Identifier(table), sql.Identifier(column))
    # Bound parameter size even for large collections.
    chunk = []
    for (source_id, observed_at), count in expected.items():
        chunk.append((source_id, observed_at, count))
        if len(chunk) == 5000:
            matched += conn.execute(
                query, [list(col) for col in zip(*chunk)]
            ).fetchone()[0]
            chunk.clear()
    if chunk:
        matched += conn.execute(query, [list(col) for col in zip(*chunk)]).fetchone()[0]
    return {"ods": expected.total(), "dds_matched": int(matched)}


def check_quality(batches):
    with pg_connect() as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        volumes = []
        for batch in batches:
            if not conn.execute(
                "SELECT 1 FROM public.dds_loaded_batches WHERE batch_key=%s",
                (digest(batch),),
            ).fetchone():
                raise ValueError("DDS batch has no committed receipt")
            volumes.append(
                {
                    "snapshots": batch,
                    "volumes": {
                        dataset: version_coverage(
                            conn, batch, dataset, projections, table, column
                        )
                        for dataset, projections, table, column in VERSION_TARGETS
                    },
                }
            )
        anomalies = conn.execute("""
            SELECT
                (SELECT count(*) FROM speedrun_3nf.dim_game_version
                 WHERE run_count < 0 OR total_player_count < 0 OR active_player_count < 0),
                (SELECT count(*) FROM speedrun_3nf.fact_run_version
                 WHERE status_code NOT IN (0, 1, 2)),
                (SELECT count(*) FROM speedrun_3nf.dim_run_time
                 WHERE btrim(source_duration) ~ '^-[[:space:]]*[0-9]'),
                (SELECT count(*) FROM speedrun_3nf.fact_run_version
                 WHERE verified_at < submitted_at),
                (SELECT count(*) FROM speedrun_3nf.fact_run_version
                 WHERE valid_to IS NOT NULL AND valid_from IS NULL)
        """).fetchone()
    return {
        "batches": volumes,
        "anomalies": dict(
            zip(
                (
                    "negative_game_counters",
                    "unknown_run_status",
                    "negative_run_duration",
                    "verification_before_submission",
                    "missing_run_interval_start",
                ),
                anomalies,
                strict=True,
            )
        ),
    }
