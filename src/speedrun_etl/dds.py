"""Транзакционная проекция подтверждённых снимков entity-v3 в существующую 3НФ."""

from datetime import datetime

from psycopg import sql

from .connections import pg_connect
from .entity_ods import EntityPackage
from .jsonutil import digest
from .model import role

ODS_READY = "x-speedrun://ods/entity-v3/ready"
STAGES = ("games_metadata", "runs", "wr_history", "players")


def verify_snapshot_batches(batches):
    with pg_connect(True) as conn:
        for batch in batches:
            if set(batch) not in ({"games_metadata"}, set(STAGES)):
                raise ValueError("Invalid DDS snapshot stages")
            for stage, snapshot in batch.items():
                if not isinstance(snapshot, str) or not snapshot.endswith("-" + stage):
                    raise ValueError("Invalid DDS snapshot ID")
                if not conn.execute(
                    """SELECT 1 FROM entity_receipts WHERE namespace='entity_v3'
                       AND store='postgres' AND snapshot_id=%s AND role=%s""",
                    (snapshot, role(stage)),
                ).fetchone():
                    return False
    return True


class Projection:
    def __init__(self, conn):
        self.conn = conn

    def get(self, dataset, key):
        return self.conn.execute(
            sql.SQL("SELECT document FROM entity_v3.{} WHERE key=%s").format(
                sql.Identifier(dataset)
            ),
            (key,),
        ).fetchone()[0]

    decode = EntityPackage.decode

    def insert(self, table, fields, *, ignore=False, returning=None):
        query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier(table),
            sql.SQL(", ").join(map(sql.Identifier, fields)),
            sql.SQL(", ").join(sql.Placeholder() for _ in fields),
        )
        if ignore:
            query += sql.SQL(" ON CONFLICT DO NOTHING")
        if returning:
            query += sql.SQL(" RETURNING {}").format(sql.Identifier(returning))
        cursor = self.conn.execute(query, list(fields.values()))
        if returning:
            return cursor.fetchone()[0]

    def identity(self, table, key, value, **attributes):
        if not value:
            return None
        fields = {key: value, **attributes}
        query = sql.SQL("INSERT INTO {} AS current ({}) VALUES ({})").format(
            sql.Identifier(table),
            sql.SQL(", ").join(map(sql.Identifier, fields)),
            sql.SQL(", ").join(sql.Placeholder() for _ in fields),
        )
        if attributes:
            query += sql.SQL(" ON CONFLICT ({}) DO UPDATE SET ").format(
                sql.Identifier(key)
            )
            query += sql.SQL(", ").join(
                sql.SQL("{}=COALESCE(current.{}, excluded.{})").format(
                    *[sql.Identifier(col)] * 3
                )
                for col in attributes
            )
        else:
            query += sql.SQL(" ON CONFLICT DO NOTHING")
        self.conn.execute(query, list(fields.values()))
        return value

    def version(self, table, identity, source_id, key, at, **fields):
        # Insert into the observation timeline, including late-arriving snapshots.
        table_id, entity_id = map(sql.Identifier, (table, identity))
        next_start = self.conn.execute(
            sql.SQL(
                "SELECT min(valid_from) FROM {} WHERE {}=%s AND valid_from>%s"
            ).format(table_id, entity_id),
            (source_id, at),
        ).fetchone()[0]
        self.conn.execute(
            sql.SQL("""UPDATE {} SET valid_to=%s WHERE {}=%s AND valid_from<=%s
                       AND (valid_to IS NULL OR valid_to>%s)""").format(
                table_id, entity_id
            ),
            (at, source_id, at, at),
        )
        return self.insert(
            table,
            {identity: source_id, "valid_from": at, "valid_to": next_start, **fields},
            returning=key,
        )

    def owner(self, table, key, identifier, owner_key, owner):
        self.identity(table, key, identifier, **{owner_key: owner})
        actual = self.conn.execute(
            sql.SQL("SELECT {} FROM {} WHERE {}=%s").format(
                *map(sql.Identifier, (owner_key, table, key))
            ),
            (identifier,),
        ).fetchone()[0]
        if actual != owner:
            raise ValueError(f"Conflicting owner for {table} {identifier}")

    def player(self, identifier, raw, at):
        if raw.get("isGuest") is True:
            return
        self.identity(
            "dim_player", "player_id", identifier, signed_up_at=raw.get("signedUpAt")
        )
        self.version(
            "dim_player_version",
            "player_id",
            identifier,
            "version_id",
            at,
            **pick(raw, PLAYER),
        )

    def game(self, identifier, raw, at):
        game = raw.get("game", raw)
        date = game.get("releaseDate") or {}
        self.identity(
            "dim_game",
            "game_id",
            identifier,
            **{
                "release_" + field: date.get(field) or None
                for field in ("year", "month", "day")
            },
        )
        vid = self.version(
            "dim_game_version",
            "game_id",
            identifier,
            "game_version_id",
            at,
            **pick(game, GAME),
        )
        version = {"game_version_id": vid}
        for field, table, column, ref in (
            ("platformIds", "game_platform", "platform_id", "dim_platform"),
            ("regionIds", "dim_game_region", "region_id", None),
            ("validTimers", "dim_game_timer", "timer_code", None),
        ):
            for position, value in enumerate(game.get(field) or []):
                if value is None or value == "":
                    continue
                if ref:
                    self.identity(ref, column, value)
                self.insert(table, {**version, "position": position, column: value})

        for item in raw.get("platforms", []):
            if not item.get("id"):
                continue  # The exact unkeyed entry remains in ODS.
            platform = self.identity("dim_platform", "platform_id", item["id"])
            self.insert(
                "dim_platform_definition",
                {
                    **version,
                    "platform_id": platform,
                    **pick(item, "name source_url:url release_year:year"),
                },
                ignore=True,
            )

        for field, table, column, definition, mapping in GAME_DEFINITIONS:
            for item in raw.get(field, []):
                if not item.get("id"):
                    continue
                if item.get("gameId") not in (None, identifier):
                    raise ValueError(f"Foreign {field} item in game {identifier}")
                self.owner(table, column, item["id"], "game_id", identifier)
                self.insert(
                    definition,
                    {
                        **version,
                        column: item["id"],
                        "valid_from": at,
                        **pick(item, mapping),
                    },
                    ignore=True,
                )

        for item in raw.get("values", []):
            if not item.get("id"):
                continue  # The exact unkeyed entry remains in ODS.
            parent = self.identity(
                "dim_variable", "variable_id", item.get("variableId")
            )
            if parent:
                self.owner(
                    "dim_variable_value", "value_id", item["id"], "variable_id", parent
                )
            else:
                self.identity("dim_variable_value", "value_id", item["id"])
            self.insert(
                "dim_value_definition",
                {
                    **version,
                    "value_id": item["id"],
                    **pick(
                        item,
                        "name source_url:url rules_text:rules archived source_position:position",
                    ),
                },
                ignore=True,
            )

        for item in raw.get("variables", []):
            if not item.get("id"):
                continue  # The exact unkeyed entry remains in ODS.
            variable = item["id"]
            if item.get("gameId") not in (None, identifier):
                raise ValueError(f"Foreign variable in game {identifier}")
            self.owner("dim_variable", "variable_id", variable, "game_id", identifier)
            fields = {**version, "variable_id": variable}
            default = item.get("defaultValueId")
            if default:
                self.owner(
                    "dim_variable_value", "value_id", default, "variable_id", variable
                )
            self.insert(
                "dim_variable_definition",
                {
                    **fields,
                    "default_value_id": default,
                    "valid_from": at,
                    **pick(item, VARIABLE),
                },
                ignore=True,
            )
            for name, column, table in (
                ("category", "category_id", "dim_category"),
                ("level", "level_id", "dim_game_level"),
            ):
                for target in item.get(name + "Ids") or [item.get(name + "Id")]:
                    if target:
                        self.owner(table, column, target, "game_id", identifier)
                        self.insert(
                            "dim_variable_" + name,
                            {**fields, column: target},
                            ignore=True,
                        )

        for position, item in enumerate(raw.get("moderators", [])):
            player = self.identity("dim_player", "player_id", item.get("userId"))
            if player:
                self.insert(
                    "dim_game_moderator",
                    {
                        **version,
                        "position": position,
                        "player_id": player,
                        **pick(item, "forum_id:forumId moderator_level:level"),
                    },
                )

    def game_scope(self, raw):
        gid = self.identity("dim_game", "game_id", raw.get("gameId"))
        category = self.identity("dim_category", "category_id", raw.get("categoryId"))
        return {
            "category_id": category,
            "game_without_category_id": gid if not category else None,
        }

    def run(self, identifier, raw, at):
        self.identity("fact_run", "run_id", identifier)
        fields = {**self.game_scope(raw), **pick(raw, RUN)}
        for column, table, key in (
            ("platform_id", "dim_platform", "platform_id"),
            ("level_id", "dim_game_level", "level_id"),
            ("submitted_by_id", "dim_player", "player_id"),
            ("verified_by_id", "dim_player", "player_id"),
        ):
            self.identity(table, key, fields[column])
        version = {
            "version_id": self.version(
                "fact_run_version", "run_id", identifier, "version_id", at, **fields
            )
        }
        for position, player in enumerate(raw.get("playerIds") or []):
            if player:
                self.identity("dim_player", "player_id", player)
                self.insert(
                    "dim_run_participant",
                    {**version, "position": position, "player_id": player},
                )
        for field in ("time", "timeWithLoads", "igt"):
            if raw.get(field) is not None:
                self.insert(
                    "dim_run_time",
                    {**version, "source_field": field, "source_duration": raw[field]},
                )
        for position, value in enumerate(raw.get("valueIds") or []):
            if not value:
                continue
            self.identity("dim_variable_value", "value_id", value)
            self.insert(
                "dim_run_value", {**version, "position": position, "value_id": value}
            )
        videos = raw.get("videos") or raw.get("videoUrls") or []
        for position, video in enumerate(
            videos if isinstance(videos, list) else [videos]
        ):
            if isinstance(video, dict):
                video = video.get("url")
            if isinstance(video, str):
                self.insert(
                    "dim_run_video",
                    {**version, "position": position, "source_video": video},
                )

    def wr(self, row, raw):
        # The ODS row contains the effective request parameters and exact event slots.
        params = row["filters"]
        scope = digest(params)
        self.insert(
            "dim_wr_scope",
            {
                "scope_key": scope,
                **self.game_scope(params),
                **pick(
                    params,
                    "verified_filter:verified obsolete_filter:obsolete emulator_filter:emulator video_filter:video",
                ),
            },
            ignore=True,
        )
        for position, value in enumerate(params.get("values") or []):
            self.identity("dim_variable_value", "value_id", value)
            self.insert(
                "dim_wr_scope_value",
                {"scope_key": scope, "position": position, "value_id": value},
                ignore=True,
            )
        event = self.insert(
            "fact_wr_event",
            {
                "scope_key": scope,
                "performed_at": raw["event"].get("performedAt"),
            },
            returning="event_id",
        )
        for position, run in enumerate(raw["run_ids"]):
            self.identity("fact_run", "run_id", run)
            self.insert(
                "fact_wr_event_run",
                {"event_id": event, "position": position, "run_id": run},
            )


def pick(raw, mapping):
    fields = {}
    for pair in mapping.split():
        column, _, source = pair.partition(":")
        fields[column] = raw.get(source or column)
    return fields


GAME = "name slug:url website_url:websiteUrl game_type:type run_count:runCount total_player_count:totalPlayerCount active_player_count:activePlayerCount default_timer_code:defaultTimer"
PLAYER = "display_name:name profile_url:url area_id:areaId power_level:powerLevel color1_id:color1Id color2_id:color2Id color_animate:colorAnimate"
RUN = "platform_id:platformId level_id:levelId region_id:regionId status_code:verified is_emulated:emulator enforce_ms:enforceMs performed_at:performedAt submitted_at:submittedAt verified_at:verifiedAt submitted_by_id:submittedById verified_by_id:verifiedById comment_text:comment video_state:videoState"
VARIABLE = "name source_url:url archived is_mandatory:isMandatory is_subcategory:isSubcategory is_obsoleting:isObsoleting is_user_defined:isUserDefined display_mode:displayMode source_position:position"
GAME_DEFINITIONS = (
    (
        "categories",
        "dim_category",
        "category_id",
        "dim_category_definition",
        "name source_url:url rules_text:rules archived is_per_level:isPerLevel is_misc:isMisc enforce_ms:enforceMs exact_players:exactPlayers num_players:numPlayers player_match_mode:playerMatchMode time_direction:timeDirection source_position:position",
    ),
    (
        "levels",
        "dim_game_level",
        "level_id",
        "dim_level_definition",
        "name source_url:url archived source_position:position",
    ),
)


def observations(conn, dataset, snapshots, projections):
    # Server cursor bounds memory and the join reads only referenced entity keys.
    query = sql.SQL("""
        SELECT snapshot.document, state.value->>'observed_at'
        FROM entity_v3.checks observation
        CROSS JOIN LATERAL jsonb_array_elements(observation.document->'states')
            WITH ORDINALITY AS state(value, position)
        JOIN entity_v3.{} snapshot ON snapshot.key=state.value->>'version_key'
        WHERE observation.document->>'snapshot_id'=ANY(%s)
          AND observation.document->>'dataset'=%s AND snapshot.projection LIKE ANY(%s)
        ORDER BY (state.value->>'observed_at')::timestamptz, snapshot.projection, observation.key, state.position
    """).format(sql.Identifier(dataset))
    with conn.cursor(name="dds_observations") as cursor:
        cursor.execute(query, (snapshots, dataset, projections))
        for row, at in cursor:
            yield row, datetime.fromisoformat(at)


def load_batch(batch, ddl):
    snapshots = list(batch.values())
    with pg_connect() as conn:
        conn.execute("SELECT pg_advisory_xact_lock(hashtext('speedrun_3nf_load'))")
        if not conn.execute("SELECT to_regnamespace('speedrun_3nf')").fetchone()[0]:
            # Execute the existing declaration verbatim in its own transaction.
            with pg_connect() as schema:
                schema.execute(ddl.read_text())
        conn.execute("""CREATE TABLE IF NOT EXISTS public.dds_loaded_batches (
            batch_key text PRIMARY KEY, loaded_at timestamptz NOT NULL DEFAULT now()
        )""")
        if not conn.execute(
            "INSERT INTO public.dds_loaded_batches(batch_key) VALUES (%s) ON CONFLICT DO NOTHING RETURNING batch_key",
            (digest(batch),),
        ).fetchone():
            return {"skipped": True}
        conn.execute("SET LOCAL search_path TO speedrun_3nf, pg_catalog")
        writer = Projection(conn)
        counts = {}
        for dataset, projections, handler in (
            ("games", ["game_card", "game_detail"], writer.game),
            ("runs", ["run_core"], writer.run),
        ):
            counts[dataset] = 0
            for row, at in observations(conn, dataset, snapshots, projections):
                handler(row["source_id"], writer.decode(row["raw"]), at)
                counts[dataset] += 1
        counts["players"] = 0
        if "players" in batch:
            # The players package retains original run/WR observation snapshot IDs.
            for row, at in observations(
                conn, "players", [batch["runs"], batch["wr_history"]], ["player:%"]
            ):
                writer.player(row["source_id"], writer.decode(row["raw"]), at)
                counts["players"] += 1
        counts["wr_history"] = 0
        if "wr_history" in batch:
            for row, _ in observations(
                conn, "wr_history", [batch["wr_history"]], ["source_wr_event"]
            ):
                writer.wr(row, writer.decode(row["raw"]))
                counts["wr_history"] += 1
        return counts
