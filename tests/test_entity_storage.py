from decimal import Decimal
import unittest

from bson import BSON

from speedrun_etl.entity_storage import mongo_record, mongo_document
from speedrun_etl.jsonutil import dumps


class MongoEntityEncodingTests(unittest.TestCase):
    def test_exact_fallback_and_history_lookup_preserve_json(self):
        row = {"key": "k", "source_id": "p", "snapshot_id": "s",
               "nested": [Decimal("1.1628848562066436"),
                          Decimal("0.123456789012345678901234567890123456789"),
                          2**100, {"$ref": "source field", "a.b": None}],
               "document_encoding": "source field, not a storage marker"}
        stored = BSON(BSON.encode({"_id": "k", **mongo_record(row, dumps(row))})).decode()
        self.assertEqual(stored["document_encoding"], "canonical-json-utf8")
        self.assertEqual(stored["document"]["snapshot_id"], "s")
        self.assertEqual(dumps(mongo_document(stored)), dumps(row))

    def test_native_and_legacy_documents_remain_readable(self):
        row = {"key": "k", "source_id": "p", "snapshot_id": "s", "states": [None, 1]}
        stored = BSON(BSON.encode(mongo_record(row, dumps(row)))).decode()
        self.assertNotIn("document_encoding", stored)
        self.assertEqual(mongo_document(stored), row)
        self.assertEqual(mongo_document({"document": row}), row)
        with self.assertRaisesRegex(ValueError, "Unknown"):
            mongo_document({"document_encoding": "unknown", "document": row})


if __name__ == "__main__":
    unittest.main()
