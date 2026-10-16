import tempfile
from contextlib import closing
import unittest
from pathlib import Path
import sqlite3

from collect_games import collect_catalog, export_catalog


class FakeClient:
    def __init__(self, count=20201, duplicate=False, changed=False):
        self.count, self.duplicate, self.changed = count, duplicate, changed
        self.requested = []

    def v2(self, endpoint, params):
        assert endpoint == 'GetGameList'
        page = params['page']
        self.requested.append(page)
        start = (page - 1) * 200
        games = [{'id': str(i), 'name': f'Game {i}', 'type': 'game', 'runCount': '1'}
                 for i in range(start, min(start + 200, self.count))]
        if self.duplicate and page == 2:
            games[0]['id'] = '0'
        return {'gameList': games, 'platformList': [],
                'pagination': {'count': str(self.count + (1 if self.changed and page == 2 else 0)),
                               'page': str(page), 'pages': str((self.count + 199) // 200), 'per': '200'}}


class CatalogTests(unittest.TestCase):
    def test_pages_beyond_ten_thousand_and_exports(self):
        client = FakeClient()
        games, platforms, pagination = collect_catalog(client, progress=lambda *a, **k: None)
        self.assertEqual(len(games), 20201)
        self.assertEqual(set(client.requested), set(range(1, 103)))
        with tempfile.TemporaryDirectory() as folder:
            manifest = export_catalog(Path(folder), games, platforms, pagination, 'test', 102)
            with closing(sqlite3.connect(Path(folder) / 'processed/games.sqlite')) as db:
                self.assertEqual(db.execute('SELECT count(*) FROM games').fetchone()[0], 20201)
                self.assertEqual(db.execute('SELECT site_run_count FROM games LIMIT 1').fetchone()[0], 1)
            self.assertFalse(manifest['next_stages_executed'])

    def test_duplicate_is_rejected(self):
        with self.assertRaisesRegex(RuntimeError, 'duplicate'):
            collect_catalog(FakeClient(401, duplicate=True), progress=lambda *a, **k: None)

    def test_changing_total_is_rejected(self):
        with self.assertRaisesRegex(RuntimeError, 'changed'):
            collect_catalog(FakeClient(401, changed=True), progress=lambda *a, **k: None)


if __name__ == '__main__':
    unittest.main()
