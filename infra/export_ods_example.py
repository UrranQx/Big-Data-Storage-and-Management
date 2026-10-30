"""Читает один сохранённый ран Portal и сверяет пример во всех трёх ODS.

Выводит небольшой JSON для отчёта. Не пишет в хранилища, не опрашивает источник;
исходный объект восстанавливается из response и общих blocks по сохранённому пути.
"""

import hashlib

from speedrun_etl.entity_ods import EntityPackage
from speedrun_etl.entity_storage import mongo_document, read_entity, ready
from speedrun_etl.jsonutil import digest, dumps
from speedrun_etl.model import run_core
from speedrun_etl.storage import Mongo, pg_connect


COLLECTION = "lab1-popular500-20261030-g-4pd0n31e"
RUN_ID = "yv2r2oom"


class StoredDecoder:
    """Use the production codec with read-only PostgreSQL block resolution."""

    decode = EntityPackage.decode

    def __init__(self, connection):
        self.connection = connection

    def get(self, dataset, key):
        if dataset != "blocks":
            raise ValueError("Only immutable blocks are decoded")
        row = self.connection.execute(
            "SELECT document FROM entity_v3.blocks WHERE key=%s", [key]
        ).fetchone()
        if row is None:
            raise KeyError(key)
        return row[0]


def example():
    sid = COLLECTION + "-runs"
    proof = ready(sid, namespace="entity_v3", role="runs")
    with pg_connect() as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        core = conn.execute(
            "SELECT document FROM entity_v3.runs WHERE source_id=%s AND projection='run_core' ORDER BY key LIMIT 1",
            [RUN_ID],
        ).fetchone()[0]
        check = conn.execute(
            "SELECT document FROM entity_v3.checks WHERE source_id=%s AND projection='run_core' "
            "AND document->>'snapshot_id'=%s",
            [RUN_ID, sid],
        ).fetchone()[0]
        observed = check["last_seen"]
        response = conn.execute(
            "SELECT document FROM entity_v3.responses WHERE key=%s",
            [observed["response_id"]],
        ).fetchone()[0]
        body = StoredDecoder(conn).decode(response["body"])
        if hashlib.sha256(dumps(body).encode()).hexdigest() != response["body_hash"]:
            raise ValueError("Restored response digest differs")
        source_run = body
        for part in observed["path"]:
            source_run = source_run[part]
        if source_run["id"] != RUN_ID or run_core(source_run) != core["raw"]["value"]:
            raise ValueError("Source run and stored core differ")
        game = conn.execute(
            "SELECT document FROM entity_v3.games WHERE source_id=%s AND projection='game_detail' ORDER BY key LIMIT 1",
            [core["game_id"]],
        ).fetchone()[0]
        profile = conn.execute(
            "SELECT document FROM entity_v3.players WHERE source_id=%s ORDER BY key LIMIT 1",
            [core["player_ids"][0]],
        ).fetchone()[0]
    versions = {
        name: read_entity(
            name,
            "runs",
            RUN_ID,
            sid,
            proof["stores"][name],
            namespace="entity_v3",
            role="runs",
        )
        for name in ("postgres", "mongo", "garage")
    }
    hashes = {name: digest(rows) for name, rows in versions.items()}
    if len(set(hashes.values())) != 1:
        raise ValueError("Three-store example differs")
    with Mongo() as store:
        mongo = store.client.entity_v3.runs.find_one({"_id": core["key"]})
        if mongo_document(mongo) != core:
            raise ValueError("Mongo wrapper differs")
    return {
        "source_page": "https://www.speedrun.com/portal/runs/" + RUN_ID,
        "collection_id": COLLECTION,
        "snapshot_id": sid,
        "source_run": source_run,
        "core": core,
        "check": check,
        "game": game,
        "profile": profile,
        "mongo": mongo,
        "response_metadata": response["metadata"],
        "verification": {
            "store_digests": hashes,
            "response_hash_verified": True,
            "source_core_equal": True,
            "source_HTTP": 0,
            "package_digest": proof["digest"],
        },
    }


if __name__ == "__main__":
    print(dumps(example()))
