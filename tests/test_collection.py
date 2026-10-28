from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from speedrun_etl.collect import partition, validate_conf, validate_page
from speedrun_etl.source import SourceError


def page(number=1, total=3, per=2):
    return {"pagination": {"page": number, "count": total, "per": per, "pages": (total + per - 1) // per},
            "runList": [{"id": f"r{i}", "gameId": "g", "categoryId": "c", "verified": 1}
                        for i in range((number - 1) * per, min(number * per, total))]}


class FakeClient:
    def __init__(self, failure=None, drift=False):
        self.cache = {}
        self.calls = []
        self.failure = failure
        self.drift = drift

    def request(self, endpoint, params, key, context):
        if key in self.cache:
            return self.cache[key]
        self.calls.append(key)
        if self.failure and self.failure in key:
            self.failure = None
            raise SourceError("HTTP attempts exhausted")
        result = {"response_id": key, "body": page(params["page"])}
        if self.drift and ("end" in key or "resume" in key) and "pass0" in key:
            result["body"]["runList"][0]["id"] = "new_id_same_count"
        self.cache[key] = result
        return result


class CollectionTests(unittest.TestCase):
    def test_count_growth_finishes_observed_pages_as_partial_without_restarting(self):
        class Growing(FakeClient):
            def request(self, endpoint, params, key, context):
                if key not in self.cache:
                    self.calls.append(key)
                    total = 6 if key.endswith("pass0/page1") else 7
                    self.cache[key] = {"response_id": key, "body": page(params["page"], total=total)}
                return self.cache[key]
        with TemporaryDirectory() as temp:
            client = Growing()
            result = partition(client, temp, "g", "c", 1, cap=None)
            self.assertEqual("partial", result["coverage"])
            self.assertEqual("pagination_changed_during_traversal", result["diagnostic"])
            self.assertEqual(7, result["unique_runs"])
            self.assertEqual(4, result["pages_received"])
            self.assertIn("c-verified/pass0/page4", client.calls)
            self.assertFalse(any("pass1" in key for key in client.calls))
            self.assertEqual([6, 7], result["drift"]["observed_counts"])
            before = list(client.calls)
            self.assertEqual(result, partition(client, temp, "g", "c", 1, cap=None))
            self.assertEqual(before, client.calls)

    def test_live_shrink_keeps_received_runs_and_diagnoses_vanished_tail(self):
        class Shrinking(FakeClient):
            page_concurrency = 2
            def request_pages(self, endpoint, specs):
                return [self.request(endpoint, *s) for s in specs]
            def request(self, endpoint, params, key, context):
                self.calls.append(key)
                total = 5 if key.endswith("pass0/page1") else 3
                return {"response_id": key, "body": page(params["page"], total=total)}
        with TemporaryDirectory() as temp:
            result = partition(Shrinking(), temp, "g", "c", 1, cap=None)
            self.assertEqual("partial", result["coverage"])
            self.assertTrue(result["drift"]["tail_shrank"])
            self.assertEqual(3, result["unique_runs"])
            self.assertEqual(2, result["pages_received"])

    def test_cross_page_duplicate_does_not_abort_remaining_pages(self):
        class Repeated(FakeClient):
            def request(self, endpoint, params, key, context):
                self.calls.append(key)
                body = page(params["page"], total=6)
                if params["page"] == 2:
                    body["runList"][0]["id"] = "r1"
                return {"response_id": key, "body": body}
        with TemporaryDirectory() as temp:
            client = Repeated()
            result = partition(client, temp, "g", "c", 1, cap=None)
            self.assertEqual("partial", result["coverage"])
            self.assertTrue(result["drift"]["repeated_ids_across_pages"])
            self.assertEqual(5, result["unique_runs"])
            self.assertIn("c-verified/pass0/page3", client.calls)

    def test_count_drift_does_not_hide_filter_violation(self):
        class WrongCategory(FakeClient):
            def request(self, endpoint, params, key, context):
                body = page(params["page"], total=6 if params["page"] == 1 else 7)
                if params["page"] == 2:
                    body["runList"][0]["categoryId"] = "wrong"
                return {"response_id": key, "body": body}
        with TemporaryDirectory() as temp:
            with self.assertRaisesRegex(SourceError, "filter violation"):
                partition(WrongCategory(), temp, "g", "c", 1, cap=None)

    def test_prefetched_pages_keep_order_validation_and_completed_checkpoint(self):
        class PrefetchClient(FakeClient):
            page_concurrency = 2
            groups = []
            def request(self, endpoint, params, key, context):
                self.calls.append(key)
                return {"response_id": key, "body": page(params["page"], total=5)}
            def request_pages(self, endpoint, specs):
                self.groups.append([s[0]["page"] for s in specs])
                return [self.request(endpoint, *s) for s in specs]
        with TemporaryDirectory() as temp:
            client = PrefetchClient()
            result = partition(client, temp, "g", "c", 1, cap=None)
            self.assertEqual([[2,3]], client.groups)
            self.assertEqual(5, result["unique_runs"])
            self.assertEqual("verified_public_partitions", result["coverage"])
            calls = list(client.calls)
            self.assertEqual(result, partition(client, temp, "g", "c", 1, cap=None))
            self.assertEqual(calls, client.calls)

    def test_game_metadata_rejects_extension_even_from_a_legacy_selection(self):
        from unittest.mock import Mock
        from speedrun_etl.collect import metadata
        client = Mock()
        client.request.return_value = {"body": {"game": {
            "id": "g", "name": "Unrelated title", "type": "game", "gameTypeIds": [8]}, "categories": []}}
        with self.assertRaisesRegex(SourceError, "Category Extensions"):
            metadata(client, {"game": "url", "scope": "game", "game_id": "g"})

    def test_repeated_drift_finishes_as_explicit_partial_and_replays(self):
        class Unstable(FakeClient):
            def request(self, endpoint, params, key, context):
                saved = super().request(endpoint, params, key, context)
                if "/end/" in key:
                    saved["body"]["runList"][0]["id"] = "changed"
                return saved
        with TemporaryDirectory() as temp:
            client = Unstable()
            result = partition(client, temp, "g", "c", 1)
            self.assertEqual("partial", result["coverage"])
            self.assertFalse(result["intentional_limit"])
            self.assertEqual(result, partition(client, temp, "g", "c", 1))

    def test_empty_wr_response_preserved_and_other_categories_continue(self):
        from unittest.mock import Mock, patch
        from speedrun_etl.collect import collect_runs
        from speedrun_etl.jsonutil import atomic_write
        body = {"game": {"id": "g"}, "categories": [
            {"id": "empty", "isPerLevel": False}, {"id": "normal", "isPerLevel": False}]}
        client = Mock()
        def request(endpoint, params, key, context):
            value = ({ } if params.get("params", {}).get("categoryId") == "empty" else
                     {"recordEventList": [], "pagination": {"pages": 1}})
            return {"response_id": key, "body": value}
        client.request.side_effect = request
        package = Mock()
        package.records.return_value = iter([{"endpoint": "GetGameData", "http_status": 200}])
        with TemporaryDirectory() as temp:
            atomic_write(Path(temp) / "config.json", {"scope": "full_game"})
            with patch("speedrun_etl.collect.restore", return_value=body), patch(
                "speedrun_etl.collect.partition", return_value={"coverage": "verified_public_partitions"}):
                result = collect_runs(client, temp, package)
        self.assertEqual("partial", result["coverage"])
        self.assertEqual("unavailable", result["wr"][0]["coverage"])
        self.assertEqual("verified_public_partitions", result["wr"][1]["coverage"])
        self.assertEqual("stats-after", client.request.call_args.args[2])

    def test_empty_last_page_and_filters(self):
        with self.assertRaisesRegex(SourceError, "outside"):
            validate_page({"pagination":{"page":3,"count":3,"per":2,"pages":2}},3)
        self.assertEqual([], validate_page(page(total=0), 1))
        empty = {"pagination": {"page": 1, "count": 0, "per": 100, "pages": 1}}
        self.assertEqual([], validate_page(empty, 1))
        from speedrun_etl.collect import boundary_signature
        self.assertTrue(boundary_signature(empty))
        self.assertEqual(["r2"], validate_page(page(2), 2, game_id="g", category_id="c", status=1))
        for field, bad in (("gameId", "wrong"), ("categoryId", "wrong"), ("verified", 2), ("levelId", "level")):
            body = page()
            body["runList"][0][field] = bad
            with self.assertRaises(SourceError):
                validate_page(body, 1, game_id="g", category_id="c", status=1)
        body = page()
        body["runList"][1]["id"] = "r0"
        with self.assertRaises(SourceError):
            validate_page(body, 1)

    def test_resume_and_completed_section_no_refetch(self):
        with TemporaryDirectory() as temp:
            client = FakeClient(failure="page2")
            with self.assertRaises(SourceError):
                partition(client, temp, "g", "c", 1)
            result = partition(client, temp, "g", "c", 1)
            self.assertEqual(3, result["unique_runs"])
            self.assertTrue(any("resume" in key for key in client.calls))
            before = list(client.calls)
            partition(client, temp, "g", "c", 1)
            self.assertEqual(before, client.calls)
            self.assertEqual(1, client.calls.count("c-verified/pass0/page1"))

    def test_same_count_boundary_drift_restarts_only_section(self):
        with TemporaryDirectory() as temp:
            client = FakeClient(drift=True)
            result = partition(client, temp, "g", "c", 1)
            self.assertEqual(1, result["pass"])
            self.assertTrue(any("pass0" in key for key in client.cache))
            self.assertTrue(any("pass1" in key for key in client.cache))

    def test_stop_scope_and_path_traversal(self):
        for conf in ({"collection_id": "../oops"}, {"collection_id": "valid", "scope": "global"},
                     {"collection_id": "valid", "game": "other"}):
            with self.assertRaises(ValueError):
                validate_conf(conf)


if __name__ == "__main__":
    unittest.main()
