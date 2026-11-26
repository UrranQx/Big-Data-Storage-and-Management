"""Publish PostgreSQL, Airflow and Superset lineage through OpenMetadata REST."""

import base64
import importlib.util
import json
import os
import re
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from .connections import pg_connect

ROOT = Path(__file__).resolve().parents[2]
SERVICE = "speedrun_postgres"
DATABASE = SERVICE + ".speedrun_ods"


def refresh_superset_notes(assets, build):
    """Refresh the visible archive timestamp after the published view changes."""
    from http.cookiejar import CookieJar
    from urllib.request import HTTPCookieProcessor, build_opener

    base = os.environ.get("SUPERSET_URL", "http://superset:8088").rstrip("/")
    client = build_opener(HTTPCookieProcessor(CookieJar()))
    headers = {"Content-Type": "application/json"}

    def request(method, path, body=None):
        req = Request(
            base + "/api/v1/" + path,
            headers=headers,
            method=method,
            data=None if body is None else json.dumps(body).encode(),
        )
        with client.open(req, timeout=60) as response:
            return json.load(response)

    token = request(
        "POST",
        "security/login",
        {
            "username": "admin",
            "password": os.environ["SUPERSET_ADMIN_PASSWORD"],
            "provider": "db",
            "refresh": False,
        },
    )
    headers["Authorization"] = "Bearer " + token["access_token"]
    headers["X-CSRFToken"] = request("GET", "security/csrf_token/")["result"]
    dashboards = request("GET", "dashboard/?q=(page_size:100)")["result"]
    updated = 0
    for dashboard in dashboards:
        slug = dashboard.get("slug", "")
        if slug not in {"lab3-" + key for key in assets.DASHBOARDS}:
            continue
        entity = request("GET", f"dashboard/{dashboard['id']}")["result"]
        position = json.loads(entity["position_json"])
        position["NOTE"]["meta"]["code"] = (
            f"**Срез: {build[1]} · {build[2]}**\n\n"
            + assets.NOTES[slug.removeprefix("lab3-")]
        )
        request(
            "PUT",
            f"dashboard/{dashboard['id']}",
            {"position_json": json.dumps(position, ensure_ascii=False)},
        )
        updated += 1
    if updated != len(assets.DASHBOARDS):
        raise ValueError(
            "Provision all three Superset dashboards before metadata synchronization"
        )
    return updated


class Catalog:
    def __init__(self):
        self.base = os.environ.get(
            "OPENMETADATA_URL", "http://openmetadata:8585/api"
        ).rstrip("/")
        self.token = None

    def request(self, method, path, body=None):
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = "Bearer " + self.token
        request = Request(
            self.base + "/v1/" + path,
            data=None if body is None else json.dumps(body).encode(),
            headers=headers,
            method=method,
        )
        try:
            with urlopen(request, timeout=90) as response:
                raw = response.read().decode()
                try:
                    return json.loads(raw) if raw else {}
                except json.JSONDecodeError:
                    return {"status": response.status}
        except HTTPError as error:
            message = error.read().decode()[:1500]
            for name, value in os.environ.items():
                if value and any(
                    word in name for word in ("PASSWORD", "SECRET", "TOKEN", "KEY")
                ):
                    message = message.replace(value, "[redacted]").replace(
                        base64.b64encode(value.encode()).decode(), "[redacted]"
                    )
            raise RuntimeError(
                f"OpenMetadata {method} {path}: HTTP {error.code}: {message}"
            ) from None

    def login(self):
        password = os.environ["OPENMETADATA_ADMIN_PASSWORD"]

        def authenticate(value):
            return self.request(
                "POST",
                "users/login",
                {
                    "email": "admin@open-metadata.org",
                    "password": base64.b64encode(value.encode()).decode(),
                },
            )["accessToken"]

        try:
            self.token = authenticate(password)
        except RuntimeError as error:
            if "HTTP 401" not in str(error):
                raise
            self.token = authenticate("admin")
            self.request(
                "PUT",
                "users/changePassword",
                {
                    "username": "admin",
                    "requestType": "SELF",
                    "oldPassword": "admin",
                    "newPassword": password,
                    "confirmPassword": password,
                },
            )
            self.token = authenticate(password)

    def put(self, path, value):
        return self.request("PUT", path, value)


def reference(entity, kind):
    return {"id": entity["id"], "type": kind}


def column_type(data_type, udt):
    if data_type == "ARRAY":
        element = {
            "_int8": "BIGINT",
            "_int4": "INT",
            "_numeric": "NUMERIC",
            "_float8": "DOUBLE",
            "_text": "TEXT",
            "_timestamptz": "TIMESTAMPZ",
        }.get(udt, "STRING")
        return {"dataType": "ARRAY", "arrayDataType": element}
    mapping = {
        "bigint": "BIGINT",
        "integer": "INT",
        "smallint": "SMALLINT",
        "numeric": "NUMERIC",
        "double precision": "DOUBLE",
        "real": "FLOAT",
        "boolean": "BOOLEAN",
        "jsonb": "JSON",
        "json": "JSON",
        "timestamp with time zone": "TIMESTAMPZ",
        "timestamp without time zone": "TIMESTAMP",
        "date": "DATE",
        "text": "TEXT",
        "character varying": "VARCHAR",
    }
    result = {"dataType": mapping.get(data_type, "STRING")}
    if result["dataType"] == "VARCHAR":
        result["dataLength"] = 65535
    return result


def synchronize():
    catalog = Catalog()
    catalog.login()
    catalog.put(
        "services/databaseServices",
        {
            "name": SERVICE,
            "serviceType": "Postgres",
            "description": "ODS → DDS → витрины. Только уже собранные снимки Speedrun.com.",
        },
    )
    catalog.put("databases", {"name": "speedrun_ods", "service": SERVICE})
    catalog.put(
        "services/pipelineServices",
        {"name": "speedrun_airflow", "serviceType": "Airflow"},
    )
    pipelines = {}
    for name in (
        "normalize_entity_v3",
        "check_data_quality",
        "refresh_marts",
        "sync_analytics_metadata",
    ):
        pipelines[name] = catalog.put(
            "pipelines",
            {
                "name": name,
                "service": "speedrun_airflow",
                "sourceUrl": "http://localhost:8080/dags/" + name,
                "description": "Код: dags/; без запуска сбора из источника.",
            },
        )
    tables = {}
    with pg_connect() as conn:
        build = conn.execute(
            "SELECT build_id,as_of,collection_id FROM speedrun_marts.context"
        ).fetchone()
        rows = conn.execute("""
            SELECT table_schema,table_name,column_name,data_type,udt_name,ordinal_position
            FROM information_schema.columns WHERE table_schema IN ('entity_v3','speedrun_3nf','speedrun_marts')
            ORDER BY table_schema,table_name,ordinal_position
        """).fetchall()
    from itertools import groupby

    for schema in sorted({row[0] for row in rows}):
        catalog.put(
            "databaseSchemas",
            {
                "name": schema,
                "database": DATABASE,
                "description": {
                    "entity_v3": "Lossless ODS: исходные JSON, проверки и манифесты.",
                    "speedrun_3nf": "Нормализованная история и наблюдаемый контекст.",
                    "speedrun_marts": f"Атомарно опубликованный срез {build[0]}, as_of={build[1]}, collection={build[2]}.",
                }[schema],
            },
        )
    for (schema, name), fields in groupby(rows, key=lambda row: row[:2]):
        columns = [
            {
                "name": row[2],
                **column_type(row[3], row[4]),
                "description": "Происхождение и формулы: SQL в sql/marts и sql/lab3_dds.sql.",
            }
            for row in fields
        ]
        key = schema + "." + name
        tables[key] = catalog.put(
            "tables",
            {
                "name": name,
                "databaseSchema": DATABASE + "." + schema,
                "tableType": "View"
                if schema == "speedrun_marts" and name != "build_registry"
                else "Regular",
                "columns": columns,
                "description": f"ЛР3 · {key}. Код: docs/design/entity_v3_3nf.sql, sql/marts/, src/speedrun_etl/.",
            },
        )

    edges = 0

    def lineage(
        source, destination, pipeline="refresh_marts", columns=None, expression=""
    ):
        nonlocal edges
        a, b = tables[source], tables[destination]
        details = {
            "pipeline": reference(pipelines[pipeline], "pipeline"),
            "description": expression
            or "Воспроизводимое преобразование из репозитория ЛР3.",
        }
        if columns:
            details["columnsLineage"] = [
                {
                    "fromColumns": [a["fullyQualifiedName"] + "." + x for x in origins],
                    "toColumn": b["fullyQualifiedName"] + "." + target,
                    "function": formula,
                }
                for origins, target, formula in columns
            ]
        catalog.put(
            "lineage",
            {
                "edge": {
                    "fromEntity": reference(a, "table"),
                    "toEntity": reference(b, "table"),
                    "lineageDetails": details,
                }
            },
        )
        edges += 1

    # Table lineage for DDS is explicit rather than pretending SQL parses JSON loaders.
    for key in sorted(tables):
        if not key.startswith("speedrun_3nf."):
            continue
        name = key.split(".")[1]
        if "game" in name or name.startswith(
            ("dim_category", "dim_variable", "dim_value", "dim_platform")
        ):
            source = "games"
        elif "player" in name:
            source = "players"
        elif "wr_" in name:
            source = "wr_history"
        elif "leaderboard" in name:
            source = "checks"
        elif "run" in name:
            source = "runs"
        elif name in ("snapshot_coverage", "analytics_batch"):
            source = "manifests"
        else:
            source = "checks"
        if "entity_v3." + source in tables:
            lineage(
                "entity_v3." + source,
                key,
                "normalize_entity_v3",
                expression="Decode entity-v3 raw dictionary, preserve source ID and observed_at; src/speedrun_etl/dds.py and analytics_dds.py.",
            )

    dependencies = {
        "player": ["speedrun_3nf.player_area_observation"],
        "scope": [
            "speedrun_3nf.dim_game_version",
            "speedrun_3nf.dim_category_definition",
            "speedrun_3nf.dim_variable_definition",
            "speedrun_3nf.dim_variable_category",
            "speedrun_3nf.dim_variable_level",
            "speedrun_3nf.dim_run_value",
            "speedrun_3nf.dim_value_definition",
            "speedrun_3nf.dim_platform_definition",
        ],
        "run": [
            "speedrun_3nf.fact_run_version",
            "speedrun_3nf.dim_run_time",
            "speedrun_3nf.run_observation",
            "speedrun_3nf.leaderboard_observation",
            "speedrun_3nf.dim_run_video",
            "scope",
            "player",
        ],
        "pb": ["run"],
        "field": ["scope", "pb"],
        "target": ["field", "pb"],
        "goal": ["pb", "target"],
        "country_contribution": [
            "speedrun_3nf.leaderboard_observation",
            "speedrun_3nf.fact_run_version",
            "speedrun_3nf.run_observation",
            "player",
            "country",
        ],
        "country_medals": ["country_contribution", "country"],
        "record_event": ["run", "scope"],
        "activity": ["field", "record_event", "pb", "run"],
        "wr_opportunity": ["pb", "activity"],
        "revival": ["record_event"],
        "calendar_activity": ["record_event"],
        "runner_form": ["run", "scope"],
        "runner_window": ["run"],
        "scope_tag": ["scope"],
        "runner_profile": ["pb", "scope_tag", "scope"],
        "first_publication": ["run"],
        "cohort_member": ["first_publication", "target", "run"],
        "cohort_summary": ["cohort_member"],
        "cohort_survival": ["cohort_member"],
        "game_portfolio": ["first_publication", "scope", "player", "pb"],
        "game_similarity": ["game_portfolio"],
        "game_transition": ["game_portfolio"],
        "destination_category": ["game_portfolio", "scope", "pb"],
        "demo_selection": ["run", "scope", "field", "runner_profile", "runner_window"],
        "coverage": ["speedrun_3nf.snapshot_coverage"],
        "runner_selector": ["pb"],
        "source_wr": [
            "speedrun_3nf.wr_source_observation",
            "speedrun_3nf.dim_wr_scope",
            "speedrun_3nf.dim_game_version",
        ],
        "recommendation_evaluation": ["game_portfolio"],
        "quality_summary": [
            "speedrun_3nf.fact_run_version",
            "speedrun_3nf.dim_run_time",
        ],
        "validation": [
            "pb",
            "country_medals",
            "cohort_survival",
            "cohort_member",
            "coverage",
        ],
    }
    for destination, sources in dependencies.items():
        for source in sources:
            lineage(
                source if "." in source else "speedrun_marts." + source,
                "speedrun_marts." + destination,
            )
    for source, destination, mappings, formula in (
        (
            "entity_v3.checks",
            "speedrun_3nf.leaderboard_observation",
            [
                (
                    ["document"],
                    "source_place",
                    "document.unfiltered_leaderboard.states[].values.place",
                ),
                (
                    ["document"],
                    "source_obsolete",
                    "document.unfiltered_leaderboard.states[].values.obsolete",
                ),
            ],
            "Source placement comes from the separately preserved unfiltered leaderboard context.",
        ),
        (
            "entity_v3.runs",
            "speedrun_3nf.dim_run_time",
            [
                (
                    ["document"],
                    "duration_ns",
                    "decode(raw).time / timeWithLoads / igt → exact Decimal nanoseconds",
                )
            ],
            "Exact timer parser; invalid durations retained and quarantined.",
        ),
        (
            "speedrun_3nf.dim_run_time",
            "speedrun_marts.run",
            [(["duration_ns"], "seconds", "duration_ns / 1e9")],
            "Select the game's primary timer; keep timer in the comparable scope.",
        ),
        (
            "speedrun_marts.run",
            "speedrun_marts.pb",
            [
                (
                    ["score"],
                    "place",
                    "RANK() OVER(PARTITION BY scope_id ORDER BY score)",
                ),
                (["seconds"], "seconds", "current non-obsolete best per holder"),
            ],
            "Ties use competition rank; holder filters follow ranking.",
        ),
        (
            "speedrun_marts.pb",
            "speedrun_marts.goal",
            [
                (["place"], "place", "copy global place before personal filters"),
                (
                    ["score"],
                    "improvement_seconds",
                    "greatest(0,score-threshold_score)/1e9",
                ),
            ],
            "Fixed targets in the selected archive.",
        ),
        (
            "speedrun_marts.country_contribution",
            "speedrun_marts.country_medals",
            [
                (["source_place"], "gold", "count(*) FILTER(WHERE source_place=1)"),
                (["source_place"], "medal_score", "G*B^2+S*B+Br; B=1+max(S,Br)"),
            ],
            "One source podium run per represented country.",
        ),
    ):
        lineage(
            source,
            destination,
            "normalize_entity_v3"
            if source.startswith("entity_v3")
            else "refresh_marts",
            mappings,
            formula,
        )

    # SQL virtual datasets are published with links to the corresponding physical views.
    spec = importlib.util.spec_from_file_location(
        "superset_assets", ROOT / "infra/superset_assets.py"
    )
    assets = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(assets)
    refresh_superset_notes(assets, build)
    catalog.put(
        "services/dashboardServices",
        {"name": "speedrun_superset", "serviceType": "Superset"},
    )
    dashboards = {}
    for key, (title, chart_keys) in assets.DASHBOARDS.items():
        dashboard = catalog.put(
            "dashboards",
            {
                "name": "lab3-" + key,
                "displayName": title,
                "service": "speedrun_superset",
                "sourceUrl": "http://localhost:8088/superset/dashboard/lab3-"
                + key
                + "/",
                "description": assets.NOTES[key],
            },
        )
        dashboards[key] = dashboard
        chart_sources = {item[0]: item[2] for item in assets.charts()}
        sources = set()
        for chart_key in chart_keys:
            query = assets.datasets()[chart_sources[chart_key]]
            sources.update(re.findall(r"speedrun_marts\.([a-z_]+)", query))
        for source in sorted(sources):
            catalog.put(
                "lineage",
                {
                    "edge": {
                        "fromEntity": reference(
                            tables["speedrun_marts." + source], "table"
                        ),
                        "toEntity": reference(dashboard, "dashboard"),
                        "lineageDetails": {
                            "pipeline": reference(
                                pipelines["sync_analytics_metadata"], "pipeline"
                            ),
                            "description": "Superset virtual SQL: infra/superset_assets.py; stable dataset UUIDs from infra/provision_superset.py.",
                        },
                    }
                },
            )
            edges += 1
    proof = catalog.request(
        "GET",
        "lineage/table/"
        + tables["speedrun_marts.country_medals"]["id"]
        + "?upstreamDepth=3&downstreamDepth=2",
    )
    column_edges = sum(
        len(edge.get("lineageDetails", {}).get("columnsLineage", []))
        for edge in proof.get("upstreamEdges", [])
    )
    if column_edges < 2:
        raise ValueError("Readback did not confirm medal column lineage")
    return {
        "build_id": build[0],
        "tables": len(tables),
        "dashboards": len(dashboards),
        "pipelines": len(pipelines),
        "edges": edges,
        "medal_upstream_edges": len(proof.get("upstreamEdges", [])),
        "medal_column_mappings": column_edges,
    }


if __name__ == "__main__":
    print(json.dumps(synchronize(), ensure_ascii=False))
