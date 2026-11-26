"""Opt-in validation of the published archive; no source collection or ODS writes."""

import os
import unittest
from uuid import uuid4

from psycopg import sql

from speedrun_etl.connections import pg_connect
from speedrun_etl.marts import build_marts, publish_build


@unittest.skipUnless(
    os.environ.get("ETL_LAB3_INTEGRATION") == "1", "Requires published Lab 3 marts"
)
class Lab3IntegrationTests(unittest.TestCase):
    def test_publication_is_idempotent(self):
        result = build_marts()
        self.assertTrue(result["skipped"])
        with pg_connect() as conn:
            self.assertEqual(
                result["build_id"],
                conn.execute("SELECT build_id FROM speedrun_marts.context").fetchone()[
                    0
                ],
            )

    def test_global_medal_places_survive_country_selection(self):
        with pg_connect() as conn:
            countries = conn.execute(
                "SELECT country_code,overall_rank FROM speedrun_marts.country_medals ORDER BY overall_rank"
            ).fetchall()
            self.assertGreater(len(countries), 10)
            country, expected = countries[-1]
            self.assertGreater(expected, 1)
            self.assertEqual(
                expected,
                conn.execute(
                    "SELECT overall_rank FROM speedrun_marts.country_medals WHERE country_code=%s",
                    (country,),
                ).fetchone()[0],
            )
            self.assertEqual(
                0,
                conn.execute(
                    "SELECT count(*) FROM speedrun_marts.validation WHERE failures<>0"
                ).fetchone()[0],
            )

    def test_pb_unique_and_ties_preserved(self):
        with pg_connect() as conn:
            duplicate = conn.execute(
                "SELECT count(*) FROM (SELECT scope_id,holder_id FROM speedrun_marts.pb GROUP BY 1,2 HAVING count(*)>1) q"
            ).fetchone()[0]
            self.assertEqual(0, duplicate)
            self.assertGreater(
                conn.execute(
                    "SELECT count(*) FROM speedrun_marts.pb WHERE tie_size>1"
                ).fetchone()[0],
                0,
            )

    def test_failed_publication_rolls_back_all_view_changes(self):
        class PublicationFailure(Exception):
            pass

        class FailAtRegistry:
            def __init__(self, conn):
                self.conn = conn

            def execute(self, query, params=None):
                if (
                    isinstance(query, str)
                    and "INSERT INTO speedrun_marts.build_registry" in query
                ):
                    raise PublicationFailure()
                return self.conn.execute(query, params)

        with pg_connect() as conn:
            before = conn.execute("SELECT * FROM speedrun_marts.context").fetchone()
            source = conn.execute(
                "SELECT physical_schema FROM speedrun_marts.build_registry WHERE build_id=%s",
                (before[0],),
            ).fetchone()[0]
        probe = "speedrun_build_probe_" + uuid4().hex[:12]
        with self.assertRaises(PublicationFailure), pg_connect() as conn:
            conn.execute("SELECT pg_advisory_xact_lock(hashtext('speedrun_3nf_load'))")
            conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(probe)))
            for table in ("context", "country_medals"):
                conn.execute(
                    sql.SQL(
                        "CREATE TABLE {}.{} AS SELECT * FROM {}.{} WHERE false"
                    ).format(
                        sql.Identifier(probe),
                        sql.Identifier(table),
                        sql.Identifier(source),
                        sql.Identifier(table),
                    )
                )
            publish_build(
                FailAtRegistry(conn),
                probe,
                "failure-probe",
                before[1],
                before[2],
                before[3],
                [],
            )
        with pg_connect() as conn:
            self.assertEqual(
                before, conn.execute("SELECT * FROM speedrun_marts.context").fetchone()
            )
            self.assertIsNone(
                conn.execute("SELECT to_regnamespace(%s)", (probe,)).fetchone()[0]
            )


if __name__ == "__main__":
    unittest.main()
