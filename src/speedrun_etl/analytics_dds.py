"""Add the observed leaderboard context needed by the analytical layer."""

from pathlib import Path

from psycopg import sql
from psycopg.types.json import Jsonb

from .connections import pg_connect
from .dds import Projection, verify_snapshot_batches
from .jsonutil import digest
from .timing import duration_ns

VERSION = "analytics-dds-v2"
SQL_ROOT = Path(__file__).resolve().parents[2] / "sql"


def load_analytics_batch(batch):
    if not verify_snapshot_batches([batch]):
        raise ValueError("Missing source receipts")
    with pg_connect() as conn:
        conn.execute("SET LOCAL cursor_tuple_fraction=1.0")
        conn.execute("SELECT pg_advisory_xact_lock(hashtext('speedrun_3nf_load'))")
        conn.execute((SQL_ROOT / "lab3_dds.sql").read_text(encoding="utf-8"))
        key = digest(batch)
        if not conn.execute(
            "SELECT 1 FROM public.dds_loaded_batches WHERE batch_key=%s", (key,)
        ).fetchone():
            raise ValueError("The DDS batch has not been committed")
        if conn.execute(
            "SELECT 1 FROM speedrun_3nf.analytics_batch WHERE batch_key=%s AND projection_version=%s",
            (key, VERSION),
        ).fetchone():
            return {"skipped": True}
        reader = Projection(conn)
        counts = {}
        for dataset, projections in (
            ("games", ["game_card", "game_detail"]),
            ("runs", ["run_core"]),
            ("players", ["player:%"]),
            ("wr_history", ["source_wr_event"]),
        ):
            query = sql.SQL("""
                SELECT c.key, c.document, e.document, s.value, s.position
                FROM entity_v3.checks c
                CROSS JOIN LATERAL jsonb_array_elements(c.document->'states')
                    WITH ORDINALITY s(value, position)
                JOIN entity_v3.{} e ON e.key=s.value->>'version_key'
                WHERE c.document->>'snapshot_id'=ANY(%s) AND c.projection LIKE ANY(%s)
            """).format(sql.Identifier(dataset))
            count = 0
            with conn.cursor(name="analytics_source") as cursor:
                cursor.itersize = 2000
                cursor.execute(query, (list(batch.values()), projections))
                for check_key, check, entity, state, position in cursor:
                    raw = reader.decode(entity["raw"])
                    observation_key = digest([check_key, position])
                    snapshot = check["snapshot_id"]
                    at = state["observed_at"]
                    if dataset == "runs":
                        version = conn.execute(
                            """
                            SELECT version_id FROM speedrun_3nf.fact_run_version
                            WHERE run_id=%s AND valid_from=%s
                            ORDER BY version_id DESC LIMIT 1
                        """,
                            (entity["source_id"], at),
                        ).fetchone()
                        if version is None:
                            raise ValueError("Missing normalized run observation")
                        conn.execute(
                            """
                            INSERT INTO speedrun_3nf.run_observation
                                (observation_key,snapshot_id,run_id,version_id,source_version_key,
                                 source_check_key,observed_at,is_orphaned,participant_ids)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING
                        """,
                            (
                                observation_key,
                                snapshot,
                                entity["source_id"],
                                version[0],
                                state["version_key"],
                                check_key,
                                at,
                                raw.get("orphaned"),
                                sorted(set(filter(None, raw.get("playerIds") or []))),
                            ),
                        )
                        for field in ("time", "timeWithLoads", "igt"):
                            value = raw.get(field)
                            parsed = duration_ns(value)
                            if value is not None:
                                conn.execute(
                                    """
                                    UPDATE speedrun_3nf.dim_run_time SET duration_ns=%s,parse_status=%s
                                    WHERE version_id=%s AND source_field=%s
                                """,
                                    (
                                        parsed,
                                        "valid" if parsed is not None else "invalid",
                                        version[0],
                                        field,
                                    ),
                                )
                        if position == 1:
                            write_leaderboard(conn, check_key, check)
                    elif dataset == "games":
                        version = conn.execute(
                            """
                            SELECT game_version_id FROM speedrun_3nf.dim_game_version
                            WHERE game_id=%s AND valid_from=%s ORDER BY game_version_id DESC LIMIT 1
                        """,
                            (entity["source_id"], at),
                        ).fetchone()
                        if version is None:
                            raise ValueError("Missing normalized game observation")
                        conn.execute(
                            """
                            INSERT INTO speedrun_3nf.game_observation VALUES (%s,%s,%s,%s,%s,%s)
                            ON CONFLICT DO NOTHING
                        """,
                            (
                                observation_key,
                                snapshot,
                                version[0],
                                state["version_key"],
                                at,
                                entity["projection"],
                            ),
                        )
                    elif dataset == "players":
                        conn.execute(
                            """
                            INSERT INTO speedrun_3nf.player_area_observation VALUES
                                (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING
                        """,
                            (
                                observation_key,
                                snapshot,
                                entity["source_id"],
                                at,
                                state["version_key"],
                                raw.get("name"),
                                raw.get("areaId"),
                                "areaId" in raw,
                                raw.get("isGuest"),
                            ),
                        )
                    else:
                        conn.execute(
                            """
                            INSERT INTO speedrun_3nf.wr_source_observation VALUES
                                (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING
                        """,
                            (
                                observation_key,
                                snapshot,
                                check_key,
                                state["version_key"],
                                digest(entity["filters"]),
                                at,
                                raw["event"].get("performedAt"),
                                raw["run_ids"],
                            ),
                        )
                    count += 1
            counts[dataset] = count
        for stage, snapshot in batch.items():
            manifests = conn.execute(
                "SELECT document FROM entity_v3.manifests WHERE key=%s",
                (snapshot + ":entity-v3",),
            ).fetchall()
            if not manifests:
                raise ValueError(f"Missing manifest for {snapshot}")
            coverage = [
                {
                    name: row[0].get(name)
                    for name in (
                        "source_result",
                        "limitations",
                        "unresolved",
                        "input_snapshot_ids",
                    )
                }
                for row in manifests
            ]
            conn.execute(
                """
                INSERT INTO speedrun_3nf.snapshot_coverage
                SELECT %s,%s,max((document->'last_seen'->>'observed_at')::timestamptz),%s
                FROM entity_v3.checks WHERE document->>'snapshot_id'=%s
                ON CONFLICT (snapshot_id) DO UPDATE SET
                    observed_until=excluded.observed_until,source_coverage=excluded.source_coverage
            """,
                (snapshot, stage, Jsonb(coverage), snapshot),
            )
        conn.execute(
            """
            INSERT INTO speedrun_3nf.analytics_batch (batch_key,snapshots,projection_version)
            VALUES (%s,%s,%s) ON CONFLICT (batch_key) DO UPDATE SET
                projection_version=excluded.projection_version, loaded_at=now()
        """,
            (key, Jsonb(batch), VERSION),
        )
        return counts


def write_leaderboard(conn, check_key, check):
    context = check.get("unfiltered_leaderboard", {})
    states = list(context.get("states", []))
    if context.get("last_seen") and context["last_seen"] not in states:
        states.append(context["last_seen"])
    for number, state in enumerate(states):
        raw_place = state.get("values", {}).get("place")
        place = int(raw_place) if str(raw_place).isdigit() else None
        conn.execute(
            """
            INSERT INTO speedrun_3nf.leaderboard_observation VALUES
                (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING
        """,
            (
                digest([check_key, "leaderboard", number]),
                check["snapshot_id"],
                check["source_id"],
                check_key,
                state["observed_at"],
                state.get("response_id"),
                str(state.get("path", "")),
                Jsonb(state.get("filters", {})),
                place,
                state.get("values", {}).get("obsolete"),
            ),
        )
