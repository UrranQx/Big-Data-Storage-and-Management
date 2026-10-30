"""Исследовательский v1 route на подставном transport без credentials.

Пагинация и фильтры должны проверяться без переключения production DAG."""

import base64
from urllib.parse import parse_qs, urlsplit
from urllib.request import Request
import unittest

from speedrun_etl.jsonutil import dumps
from speedrun_etl.v1_research import V1RunsTransport, validate_runs


class V1ResearchTests(unittest.TestCase):
    def test_public_route_query_and_headers_are_preserved_without_credentials(self):
        params = {"game": "g", "max": 200, "offset": 10000, "status": "verified"}
        encoded = base64.urlsafe_b64encode(dumps(params).encode()).decode().rstrip("=")
        request = Request(
            "https://www.speedrun.com/api/v2/v1/runs?_r=" + encoded,
            headers={
                "Accept-Encoding": "gzip",
                "X-API-Key": "test-placeholder",
                "Authorization": "test-placeholder",
            },
        )
        captured = []
        V1RunsTransport(lambda req, timeout: captured.append(req))(request)
        actual = captured[0]
        parts = urlsplit(actual.full_url)
        self.assertEqual(parts.path, "/api/v1/runs")
        self.assertEqual(parse_qs(parts.query)["offset"], ["10000"])
        self.assertEqual(
            {"accept-encoding": "gzip"},
            {k.lower(): v for k, v in actual.header_items()},
        )
        with self.assertRaisesRegex(ValueError, "only accepts"):
            V1RunsTransport()(
                Request("https://example.com/api/v2/v1/runs?_r=" + encoded)
            )

    def test_validation_rejects_pagination_and_filter_mismatches(self):
        from copy import deepcopy

        params = {
            "game": "g",
            "category": "c",
            "status": "verified",
            "max": 200,
            "offset": 0,
        }
        saved = {
            "body": {
                "pagination": {"size": 1, "offset": 0, "max": 200},
                "data": [
                    {
                        "id": "r",
                        "game": "g",
                        "category": "c",
                        "status": {"status": "verified"},
                    }
                ],
            }
        }
        self.assertEqual(["r"], validate_runs(saved, params))
        for field, value in (
            ("game", "wrong"),
            ("category", "wrong"),
            ("status", {"status": "new"}),
        ):
            wrong = deepcopy(saved)
            wrong["body"]["data"][0][field] = value
            with self.assertRaises(ValueError):
                validate_runs(wrong, params)
        wrong = deepcopy(saved)
        wrong["body"]["pagination"]["offset"] = 200
        with self.assertRaisesRegex(ValueError, "offset"):
            validate_runs(wrong, params)
        with self.assertRaisesRegex(ValueError, "filter violation"):
            validate_runs(saved, {**params, "examiner": "moderator"})


if __name__ == "__main__":
    unittest.main()
