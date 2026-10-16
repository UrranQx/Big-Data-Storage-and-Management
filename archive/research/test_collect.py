import tempfile
import unittest
from pathlib import Path
from collect import Client, any_percent, export


class ETLTests(unittest.TestCase):
    def test_any_percent_variants(self):
        for name in ('Any%', 'Any% Glitchless', 'Any Percent', 'any %'):
            self.assertTrue(any_percent(name))
        for name in ('100%', 'Many%', 'Any Ending'):
            self.assertFalse(any_percent(name))

    def test_pagination_including_short_final_page(self):
        client = Client(Path('.'), 60)
        offsets = []
        def get(route, **params):
            offsets.append(params['offset'])
            if params['offset'] == 0:
                return {'data': [1, 2], 'pagination': {'links': [{'rel': 'next'}]}}
            return {'data': [3], 'pagination': {'links': []}}
        client.get = get
        self.assertEqual(list(client.pages('runs')), [1, 2, 3])
        self.assertEqual(offsets, [0, 2])

    def test_loading_rejects_missing_category(self):
        with tempfile.TemporaryDirectory() as folder:
            with self.assertRaises(AssertionError):
                export(Path(folder), {
                    'categories': ('id TEXT PRIMARY KEY', []),
                    'runs': ('id TEXT PRIMARY KEY,category_id TEXT,game_id TEXT,primary_seconds REAL', [('r', 'missing', 'g', 10)]),
                })


if __name__ == '__main__':
    unittest.main()
