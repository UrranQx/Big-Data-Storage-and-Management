import unittest
from unittest.mock import patch
import tempfile
from pathlib import Path
from collect import Client
from collect_all import fetch_partition, seconds, build_tables, supplement_v1, is_orphaned, statistics_eligible, primary_time


class FakeClient:
    def __init__(self, count=10100, duplicate=False, changed=False):
        self.count, self.duplicate, self.changed = count, duplicate, changed
        self.pages = []

    def v2(self, endpoint, payload):
        page = payload['page']
        self.pages.append(page)
        params = payload['params']
        assert params['obsolete'] == 1 and params['video'] == 0 and params['emulator'] == 1
        start = (page-1)*100
        rows = [{'id': str(i), 'gameId': 'g', 'categoryId': 'c', 'verified': 1}
                for i in range(start, min(start+100, self.count))]
        if self.duplicate and page == 2:
            rows[0]['id'] = '0'
        return {'pagination': {'count': self.count+(1 if self.changed and page == 2 else 0),
                               'pages': (self.count+99)//100, 'per': 100, 'page': page}, 'runList': rows}


class AllRunsTests(unittest.TestCase):
    def test_collects_past_ten_thousand(self):
        client, evidence = FakeClient(), []
        rows = fetch_partition(client, 'g', 'c', 1, {}, evidence)
        self.assertEqual(len(rows), 10100)
        self.assertEqual(client.pages[-1], 101)
        self.assertEqual(evidence[0]['max_offset_equivalent'], 10000)

    def test_rejects_duplicate_page(self):
        with self.assertRaisesRegex(RuntimeError, 'Duplicate'):
            fetch_partition(FakeClient(200, duplicate=True), 'g', 'c', 1, {}, [])

    def test_rejects_source_count_drift(self):
        with self.assertRaisesRegex(RuntimeError, 'count changed'):
            fetch_partition(FakeClient(200, changed=True), 'g', 'c', 1, {}, [])

    def test_preserves_missing_rejected_time(self):
        metadata = {'game': {'id': 'g', 'name': 'Game', 'url': 'game', 'defaultTimer': 2}}
        tables = build_tables(metadata, [{'id': 'c', 'name': 'Category'}],
                              {'r': {'id': 'r', 'gameId': 'g', 'categoryId': 'c', 'verified': 2}}, {})
        self.assertIsNone(tables['runs'][1][0][5])
        self.assertEqual(tables['runs'][1][0][15], 'rejected')

    def test_decimal_seconds_and_null(self):
        self.assertEqual(seconds('1488.843s'), 1488.843)
        self.assertIsNone(seconds(None))
        with self.assertRaises(ValueError):
            seconds('PT1M')

    def test_v2_does_not_receive_v1_api_key(self):
        class Response:
            def __enter__(self):
                return self
            def __exit__(self, *args):
                pass
            def read(self):
                return b'{"pagination": {"count": "0"}}'
        with tempfile.TemporaryDirectory() as folder:
            client = Client(Path(folder), 60)
            with patch.dict('os.environ', {'SPEEDRUNCOM_API_KEY': 'fake-test-key'}):
                with patch('urllib.request.urlopen', return_value=Response()) as call:
                    client.v2('GetGameData', {'gameId': 'g'})
                    headers = dict(call.call_args.args[0].header_items())
                    self.assertNotIn('X-api-key', headers)
                    client.v2('GetGameData', {'gameId': 'g'})
                    self.assertEqual(call.call_count, 1)
                    self.assertNotIn('fake-test-key', next((Path(folder)/'raw').glob('*.json')).read_text())

    def test_preserves_v1_record_without_players(self):
        raw = {'id': 'old', 'game': 'g', 'category': 'c', 'level': None,
               'players': [], 'status': {'status': 'verified'}, 'system': {},
               'times': {'primary_t': 12.345, 'ingame': 'PT12.345S', 'ingame_t': 12.345},
               'videos': {'links': [{'uri': 'https://example.org/video'}]}}
        run = supplement_v1(raw, {})
        metadata = {'game': {'id': 'g', 'name': 'Game', 'url': 'game', 'defaultTimer': 2}}
        tables = build_tables(metadata, [{'id': 'c', 'name': 'Category'}], {'old': run}, {})
        row = tables['runs'][1][0]
        self.assertEqual(row[5], 12.345)
        self.assertIsNone(row[16])
        self.assertEqual(row[-1], 'v1')
        self.assertIn('https://example.org/video', row[14])
        self.assertEqual(tables['run_players'][1], [])

    def test_submitter_is_not_used_as_a_missing_player(self):
        metadata = {'game': {'id': 'g', 'name': 'Game', 'url': 'game', 'defaultTimer': 0}}
        run = {'id': 'r', 'gameId': 'g', 'categoryId': 'c', 'verified': 1,
               'time': '12s', 'playerIds': [], 'submittedById': 'submitter'}
        tables = build_tables(metadata, [{'id': 'c', 'name': 'Category'}], {'r': run}, {})
        fields = [column.strip().split()[0] for column in tables['runs'][0].split(',')]
        row = dict(zip(fields, tables['runs'][1][0]))
        self.assertEqual(row['submitted_by_id'], 'submitter')
        self.assertEqual(row['orphaned'], 1)
        self.assertEqual(row['statistics_eligible'], 0)
        self.assertEqual(tables['run_players'][1], [])

    def test_explicit_orphan_flag_overrides_present_players(self):
        run = {'playerIds': ['p'], 'orphaned': True}
        self.assertTrue(is_orphaned(run))
        self.assertFalse(statistics_eligible(run))

    def test_v1_supplements_are_outside_v2_statistics(self):
        self.assertFalse(statistics_eligible({'playerIds': ['p'], '_source_api': 'v1'}))
        self.assertTrue(statistics_eligible({'playerIds': ['p']}))

    def test_portal_with_loads_does_not_fabricate_loadless_time(self):
        game = {'id': 'g', 'name': 'Game', 'url': 'game', 'defaultTimer': 0,
                'loadTimes': True, 'validTimers': [0, 1]}
        run = {'id': 'r', 'gameId': 'g', 'categoryId': 'c', 'verified': 1,
               'timeWithLoads': '587.010s', 'playerIds': ['p']}
        tables = build_tables({'game': game}, [{'id': 'c', 'name': 'Category'}], {'r': run}, {})
        fields = [column.strip().split()[0] for column in tables['runs'][0].split(',')]
        row = dict(zip(fields, tables['runs'][1][0]))
        self.assertEqual(row['primary_seconds'], 587.010)
        self.assertEqual(row['realtime_seconds'], 587.010)
        self.assertIsNone(row['realtime_noloads_seconds'])
        self.assertEqual(row['primary_time_source'], 'timeWithLoads')
        self.assertEqual(primary_time(dict(run, time='500s'), game), (500, 'time'))


if __name__ == '__main__':
    unittest.main()
