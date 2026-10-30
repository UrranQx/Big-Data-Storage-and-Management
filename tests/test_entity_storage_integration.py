"""Opt-in проверки пакетов в трёх реальных ODS без HTTP источника.

Частичная запись, replay и repair должны сохранять историю и общие фрагменты JSON."""

import os
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from uuid import uuid4
from unittest.mock import patch

from speedrun_etl.entity_ods import build, EntityPackage, DATASETS
from speedrun_etl.entity_storage import init_entities, load, ready, read_entity
from speedrun_etl.jsonutil import atomic_write, digest
from speedrun_etl.storage import pg_connect, Mongo, S3
from test_ods import response


@unittest.skipUnless(os.environ.get("ETL_INTEGRATION") == "1", "Opt in inside Compose")
class EntityStorageTests(unittest.TestCase):
    def test_shared_profile_fragments_roundtrip_and_replay_in_all_three_stores(self):
        namespace = "test_shared_" + uuid4().hex[:10]
        init_entities(namespace)
        with (
            TemporaryDirectory() as directory,
            patch("speedrun_etl.entity_ods.SHARED_PROFILE_MIN_COUNT", 1),
        ):
            root = Path(directory)
            source = root / "snapshot"
            bodies = []
            for i, name in enumerate(["A", "A", "B", "A"]):
                body = {
                    "runList": [{"id": "r", "playerIds": ["p"]}],
                    "playerList": [
                        {
                            "id": "p",
                            "name": name,
                            "x": Decimal("0.123456789012345678901"),
                        }
                    ],
                    "unknown": [None, [2, 2]],
                }
                bodies.append(body)
                atomic_write(
                    source / "requests" / str(i) / "response.json",
                    response(body, str(i), f"2026-10-28T00:00:{i:02}+00:00"),
                )
            build(source, root / "output")
            p = EntityPackage(root / "output")
            summary = p.summary()
            for name in ("postgres", "mongo", "garage"):
                for _ in range(2):
                    load(name, p, "snapshot", namespace)
            ready("snapshot", namespace, expected=digest(summary))
            for name in ("postgres", "mongo", "garage"):
                restored = EntityPackage(root / name)
                for dataset in ("responses", "blocks"):
                    if name == "postgres":
                        from psycopg import sql

                        with pg_connect() as conn:
                            rows = [
                                r[0]
                                for r in conn.execute(
                                    sql.SQL("SELECT document FROM {}").format(
                                        sql.Identifier(namespace, dataset)
                                    )
                                )
                            ]
                    elif name == "mongo":
                        from speedrun_etl.entity_storage import mongo_document

                        with Mongo() as m:
                            rows = [
                                mongo_document(r)
                                for r in m.client[namespace][dataset].find({})
                            ]
                    else:
                        import gzip
                        from speedrun_etl.jsonutil import loads

                        with S3() as s:
                            key = (
                                namespace
                                + "/entity-v2/snapshots/snapshot/all/"
                                + digest(summary)
                                + ".json"
                            )
                            index = loads(
                                s.client.get_object(Bucket=s.bucket, Key=key)[
                                    "Body"
                                ].read()
                            )
                            rows = []
                            for chunk in index["chunks"]:
                                if chunk["dataset"] == dataset:
                                    raw = s.client.get_object(
                                        Bucket=s.bucket, Key=chunk["key"]
                                    )["Body"].read()
                                    rows.extend(
                                        map(loads, gzip.decompress(raw).splitlines())
                                    )
                    for row in rows:
                        restored.put(dataset, row)
                self.assertEqual(
                    bodies,
                    [
                        restored.decode(row["body"])
                        for row in restored.records("responses")
                    ],
                )
                restored.db.close()
            p.db.close()

    def test_repair_reuses_response_ids_and_full_history_in_three_stores(self):
        from speedrun_etl.entity_runtime import profiles

        namespace = "test_repair_" + uuid4().hex[:10]
        init_entities(namespace)
        with TemporaryDirectory() as directory:
            root = Path(directory)
            base = root / "base-runs"
            fresh = root / "repair-runs"
            for index, name in enumerate(("A", "B")):
                saved = response(
                    {
                        "runList": [{"id": "r", "playerIds": ["p"], "comment": name}],
                        "playerList": [{"id": "p", "name": name}],
                    },
                    f"old{index}",
                    f"2026-10-16T0{index}:00:00+00:00",
                )
                atomic_write(base / "requests" / str(index) / "response.json", saved)
            build(base, base / "entity-package", load_role="runs")
            saved = response(
                {
                    "runList": [{"id": "r", "playerIds": ["p"], "comment": "A"}],
                    "playerList": [{"id": "p", "name": "A"}],
                },
                "fresh",
                "2026-10-28T00:00:00+00:00",
            )
            atomic_write(fresh / "requests/one/response.json", saved)
            build(
                fresh,
                fresh / "entity-package",
                load_role="runs",
                base_root=base / "entity-package",
                source_result={"repair_from_snapshot_id": "base-runs"},
            )
            for source in (base, fresh):
                package = EntityPackage(source / "entity-package", role="runs")
                for store in ("postgres", "mongo", "garage"):
                    load(store, package, source.name, namespace, role="runs")
                ready(
                    source.name,
                    namespace,
                    role="runs",
                    expected=digest(package.summary()),
                )
                if source == fresh:
                    reads = [
                        read_entity(
                            s,
                            "checks",
                            "r",
                            source.name,
                            package.summary(),
                            namespace,
                            role="runs",
                        )
                        for s in ("postgres", "mongo", "garage")
                    ]
                    self.assertEqual(1, len({digest(r) for r in reads}))
                    self.assertEqual(
                        ["old0", "old1", "fresh"],
                        [e["response_id"] for e in reads[0][0]["states"]],
                    )
                    for store in ("postgres", "mongo", "garage"):
                        self.assertEqual(
                            2,
                            len(
                                read_entity(
                                    store,
                                    "runs",
                                    "r",
                                    source.name,
                                    package.summary(),
                                    namespace,
                                    role="runs",
                                )
                            ),
                        )
                package.db.close()
            source = EntityPackage(fresh / "entity-package")
            target = EntityPackage(root / "profiles")
            profiles(source, target, "repair-players", "repair-runs")
            source.db.close()
            for store in ("postgres", "mongo", "garage"):
                load(store, target, "repair-players", namespace)
                history = read_entity(
                    store, "checks", "p", "repair-players", target.summary(), namespace
                )
                self.assertEqual(
                    ["old0", "old1", "fresh"],
                    [e["response_id"] for e in history[0]["states"]],
                )
            ready("repair-players", namespace, expected=digest(target.summary()))
            target.db.close()

    def test_partial_load_resume_replay_and_entity_history(self):
        namespace = "test_entity_" + uuid4().hex[:10]
        with TemporaryDirectory() as directory:
            root = Path(directory)
            for i, name in enumerate(("A", "B", "A")):
                body = {
                    "runList": [
                        {
                            "id": "r",
                            "gameId": "g",
                            "categoryId": "c",
                            "verified": 2,
                            "orphaned": True,
                            "playerIds": ["p"],
                        }
                    ],
                    "playerList": [{"id": "p", "name": name}],
                }
                saved = response(body, f"resp{i}", f"2026-10-16T0{i}:00:00+00:00")
                saved["backoff_seconds"] = Decimal("1.1628848562066436")
                atomic_write(
                    root / "snapshot/requests" / str(i) / "response.json", saved
                )
            build(
                root / "snapshot",
                root / "package",
                source_result={
                    "precise": Decimal("0.123456789012345678901234567890123456789")
                },
            )
            package = EntityPackage(root / "package")
            summary = package.summary()
            init_entities(namespace)
            for name in ("postgres", "mongo", "garage"):
                with self.assertRaisesRegex(RuntimeError, "Injected"):
                    load(name, package, "snapshot", namespace, fail_after=1)
                first = load(name, package, "snapshot", namespace)
                second = load(name, package, "snapshot", namespace)
                self.assertEqual(first["digest"], second["digest"])
                self.assertEqual(digest(summary), first["digest"])
            self.assertEqual(digest(summary), ready("snapshot", namespace)["digest"])
            for dataset, entity in (("runs", "r"), ("players", "p"), ("checks", "p")):
                results = [
                    read_entity(name, dataset, entity, "snapshot", summary, namespace)
                    for name in ("postgres", "mongo", "garage")
                ]
                self.assertEqual(1, len({digest(r) for r in results}))
                if dataset == "checks":
                    states = results[0][0]["states"]
                    self.assertEqual(3, len(states))
                    self.assertEqual(
                        states[0]["version_key"], states[-1]["version_key"]
                    )
            from psycopg import sql

            with pg_connect() as conn, Mongo() as mongo:
                self.assertEqual(
                    3,
                    mongo.client[namespace].responses.count_documents(
                        {"document_encoding": "canonical-json-utf8"}
                    ),
                )
                self.assertEqual(
                    1,
                    mongo.client[namespace].manifests.count_documents(
                        {"document_encoding": "canonical-json-utf8"}
                    ),
                )
                for dataset in DATASETS:
                    count = summary[dataset]["count"]
                    self.assertEqual(
                        count,
                        conn.execute(
                            sql.SQL("SELECT count(*) FROM {}").format(
                                sql.Identifier(namespace, dataset)
                            )
                        ).fetchone()[0],
                    )
                    self.assertEqual(
                        count, mongo.client[namespace][dataset].count_documents({})
                    )
            with S3() as store:
                keys = [
                    obj["Key"]
                    for page in store.client.get_paginator("list_objects_v2").paginate(
                        Bucket=store.bucket, Prefix=namespace + "/"
                    )
                    for obj in page.get("Contents", [])
                ]
                load("garage", package, "snapshot", namespace)
                after = [
                    obj["Key"]
                    for page in store.client.get_paginator("list_objects_v2").paginate(
                        Bucket=store.bucket, Prefix=namespace + "/"
                    )
                    for obj in page.get("Contents", [])
                ]
                self.assertEqual(keys, after)
            package.db.close()
