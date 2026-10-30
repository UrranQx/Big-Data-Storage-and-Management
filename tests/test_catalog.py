"""Каталог: продолжение checkpoints и контроль конечных границ.

Дубли ID и дрейф не должны превращаться в подтверждённое полное покрытие."""

from tempfile import TemporaryDirectory
import unittest

from speedrun_etl.catalog import collect_catalog, validate_catalog_page
from speedrun_etl.source import SourceError


def body(n):
    return {
        "pagination": {"count": 3, "pages": 2, "per": 2, "page": n},
        "gameList": [{"id": str(i)} for i in range(2 * (n - 1), min(2 * n, 3))],
    }


class CatalogClient:
    def __init__(self):
        self.cache, self.calls = {}, []
        self.fail = True

    def request(self, endpoint, params, key, context):
        if key in self.cache:
            return self.cache[key]
        self.calls.append(key)
        if params["page"] == 2 and self.fail:
            self.fail = False
            raise SourceError("HTTP attempts exhausted")
        self.cache[key] = {"response_id": key, "body": body(params["page"])}
        return self.cache[key]


class CatalogTests(unittest.TestCase):
    def test_resume_and_final_boundaries(self):
        with TemporaryDirectory() as root:
            client = CatalogClient()
            with self.assertRaises(SourceError):
                collect_catalog(client, root)
            result = collect_catalog(client, root)
            self.assertEqual(3, result["unique_games"])
            self.assertEqual(1, client.calls.count("catalog/pass0/page/1"))
            self.assertIn("catalog/pass0/resume1/1", client.calls)
            before = list(client.calls)
            self.assertEqual(result, collect_catalog(client, root))
            self.assertEqual(before, client.calls)

    def test_catalog_duplicates_and_drift_rejected(self):
        page = body(1)
        page["gameList"][1]["id"] = "0"
        with self.assertRaises(SourceError):
            validate_catalog_page(page, 1)
        with self.assertRaises(SourceError):
            validate_catalog_page(body(1), 1, {"count": 4, "pages": 2, "per": 2})
