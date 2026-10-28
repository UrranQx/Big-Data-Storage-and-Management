from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from speedrun_etl.collect import validate_conf, validate_page
from speedrun_etl.date_partitions import annual_ranges, date_params, date_partition
from speedrun_etl.jsonutil import dumps, read
from speedrun_etl.source import SourceError


class DatedClient:
    page_concurrency = 2

    def __init__(self, missing_date=False, drift=False, fail=False):
        dates = ['2019-12-31', '2020-01-01', '2020-01-02', '2020-03-01',
                 '2020-06-30', '2020-12-31', '2021-01-01']
        self.rows = [{'id':str(i),'gameId':'g','categoryId':'c','verified':1,
                      'performedAt':d+'T12:00:00Z'} for i,d in enumerate(dates)]
        if missing_date:
            self.rows.append({'id':'missing','gameId':'g','categoryId':'c','verified':1})
        self.cache = {}; self.calls = []; self.drift = drift; self.fail = fail

    def request_pages(self, endpoint, specs):
        return [self.request(endpoint, *s) for s in specs]

    def request(self, endpoint, params, key, context):
        identity = dumps([endpoint,params,key])
        if identity in self.cache:
            return self.cache[identity]
        self.calls.append(identity)
        if self.fail and key.endswith('/page2'):
            self.fail = False
            raise SourceError('HTTP attempts exhausted')
        rows = list(self.rows)
        if self.drift and key.endswith('date-baseline-after'):
            rows += [{'id':'late','gameId':'g','categoryId':'c','verified':1,'performedAt':'2020-01-01T01:00:00Z'}]
        filters = params['params']
        if 'minRunDate' in filters:
            def day(v): return f"{v['year']:04}-{v['month']:02}-{v['day']:02}"
            start,end=day(filters['minRunDate']),day(filters['maxRunDate'])
            rows=[r for r in rows if start<=r.get('performedAt','')[:10]<=end]
        total = len(rows); page = params['page']; per = 2
        result = {'response_id':identity, 'body':{'pagination':{'count':total,'page':page,'per':per,
                  'pages':max(1,(total+per-1)//per)},'runList':deepcopy(rows[(page-1)*per:page*per])}}
        self.cache[identity] = result
        return result


RANGES=[('1900-01-01','2019-12-31'),('2020-01-01','2020-12-31'),('2021-01-01','2100-12-31')]


class DatePartitionTests(unittest.TestCase):
    def test_inclusive_calendar_partition_and_replay(self):
        with TemporaryDirectory() as root, patch('speedrun_etl.date_partitions.annual_ranges', return_value=RANGES):
            client=DatedClient()
            result=date_partition(client,root,'g','c',1,2012,max_runs=3)
            self.assertEqual('verified_public_partitions',result['coverage'])
            self.assertEqual(7,result['unique_runs'])
            self.assertEqual(7,result['leaf_count_sum'])
            self.assertGreater(result['leaf_partitions'],3)
            before=list(client.calls)
            self.assertEqual(result,date_partition(client,root,'g','c',1,2012,max_runs=3))
            self.assertEqual(before,client.calls)

    def test_source_growth_stays_partial_without_silently_accepting_the_count(self):
        for options in ({'drift':True},):
            with self.subTest(options=options), TemporaryDirectory() as root, patch(
                    'speedrun_etl.date_partitions.annual_ranges',return_value=RANGES):
                result=date_partition(DatedClient(**options),root,'g','c',1,2012,max_runs=3)
                self.assertEqual('partial',result['coverage'])
                self.assertEqual('date_partition_coverage_mismatch',result['diagnostic'])

    def test_missing_date_falls_back_to_checked_unfiltered_pages_and_replays(self):
        from speedrun_etl.audit_entity_ods import checkpoint_run_ids
        with TemporaryDirectory() as root, patch('speedrun_etl.date_partitions.annual_ranges',return_value=RANGES):
            client = DatedClient(missing_date=True)
            result = date_partition(client,root,'g','c',1,2012,max_runs=10)
            self.assertEqual('verified_public_partitions',result['coverage'])
            self.assertEqual('unfiltered_fallback_v1',result['strategy'])
            self.assertEqual(8,result['unique_runs'])
            self.assertEqual(7,result['date_subdivision_attempt']['unique_runs'])
            self.assertEqual('partial',result['date_subdivision_attempt']['coverage'])
            point = read(Path(root)/'checkpoints/c-verified.json')
            responses = {r['response_id']:{v['id'] for v in r['body']['runList']} for r in client.cache.values()}
            self.assertEqual({str(i) for i in range(7)}|{'missing'},checkpoint_run_ids(point,responses))
            self.assertTrue((Path(root)/'date-parts/c-verified/attempt.json').exists())
            before = list(client.calls)
            self.assertEqual(result,date_partition(client,root,'g','c',1,2012,max_runs=10))
            self.assertEqual(before,client.calls)

    def test_fallback_http_failure_resumes_without_repeating_date_ranges(self):
        with TemporaryDirectory() as root, patch('speedrun_etl.date_partitions.annual_ranges',return_value=RANGES):
            client = DatedClient(missing_date=True)
            request = client.request
            failed = False
            def fail_once(endpoint,params,key,context):
                nonlocal failed
                if not failed and key.startswith('date-fallback/') and key.endswith('/page2'):
                    failed = True
                    raise SourceError('HTTP attempts exhausted')
                return request(endpoint,params,key,context)
            client.request = fail_once
            with self.assertRaisesRegex(SourceError,'attempts'):
                date_partition(client,root,'g','c',1,2012,max_runs=10)
            self.assertEqual('partial',read(Path(root)/'checkpoints/c-verified.json')['coverage']['coverage'])
            before = len(client.calls)
            result = date_partition(client,root,'g','c',1,2012,max_runs=10)
            self.assertEqual('verified_public_partitions',result['coverage'])
            self.assertTrue(all('date-fallback/' in call for call in client.calls[before:]))

    def test_fallback_does_not_hide_wrong_category_filters(self):
        with TemporaryDirectory() as root, patch('speedrun_etl.date_partitions.annual_ranges',return_value=RANGES):
            client = DatedClient(missing_date=True)
            request = client.request
            def wrong_category(endpoint,params,key,context):
                response = deepcopy(request(endpoint,params,key,context))
                if key.startswith('date-fallback/'):
                    response['body']['runList'][0]['categoryId'] = 'other'
                return response
            client.request = wrong_category
            with self.assertRaisesRegex(SourceError,'filter violation'):
                date_partition(client,root,'g','c',1,2012,max_runs=10)

    def test_run_moving_between_ranges_is_not_counted_twice(self):
        with TemporaryDirectory() as root, patch('speedrun_etl.date_partitions.annual_ranges',return_value=RANGES):
            client=DatedClient()
            client.rows.append({**client.rows[0],'performedAt':'2021-02-01T12:00:00Z'})
            result=date_partition(client,root,'g','c',1,2012,max_runs=10)
            self.assertEqual('partial',result['coverage'])
            self.assertEqual(['0'],result['overlapping_ids'])
            self.assertEqual(7,result['unique_runs'])
            self.assertEqual(8,result['leaf_count_sum'])

    def test_unstable_leaf_without_final_page_count_stays_partial(self):
        from speedrun_etl.jsonutil import atomic_write
        def failed_leaf(client,root,gid,cid,status,**kwargs):
            result={'coverage':'partial','diagnostic':'pagination_unstable_after_one_retry'}
            atomic_write(Path(root)/'checkpoints/c-verified.json',{'pass':2,'pages':{},'coverage':result})
            return result
        with TemporaryDirectory() as root, patch('speedrun_etl.date_partitions.annual_ranges',return_value=RANGES),patch(
                'speedrun_etl.date_partitions.partition',side_effect=failed_leaf):
            result=date_partition(DatedClient(),root,'g','c',1,2012,max_runs=10)
            self.assertEqual('partial',result['coverage'])
            self.assertEqual(0,result['unique_runs'])

    def test_failure_resumes_without_refetching_completed_leaf(self):
        with TemporaryDirectory() as root, patch('speedrun_etl.date_partitions.annual_ranges',return_value=RANGES):
            client=DatedClient(fail=True)
            with self.assertRaisesRegex(SourceError,'attempts'):
                date_partition(client,root,'g','c',1,2012,max_runs=10)
            checkpoint=read(Path(root)/'date-parts/c-verified/1900-01-01_2019-12-31/checkpoints/c-verified.json')
            self.assertTrue(checkpoint['complete'])
            before=list(client.calls)
            result=date_partition(client,root,'g','c',1,2012,max_runs=10)
            self.assertEqual('verified_public_partitions',result['coverage'])
            new=client.calls[len(before):]
            self.assertFalse(any('2019-12-31' in call for call in new))

    def test_wrong_or_ambiguous_date_is_rejected(self):
        body=DatedClient().request('x',{'params':{},'page':1},'x',{})['body']
        for value in ('2021-01-01T00:00:00Z',None,'2020-01-01','bad'):
            candidate=deepcopy(body);candidate['runList'][0]['performedAt']=value
            with self.assertRaisesRegex(SourceError,'date filter violation'):
                validate_page(candidate,1,date_range=('2020-01-01','2020-12-31'))
        # A timezone offset must be compared as a UTC date.
        body['runList']=[{**r,'performedAt':'2020-01-01T01:00:00+03:00'} for r in body['runList']]
        validate_page(body,1,date_range=('2019-12-31','2019-12-31'))

    def test_calendar_ranges_have_no_gaps_or_overlaps(self):
        from datetime import date,timedelta
        ranges=annual_ranges(2012,2026)
        self.assertEqual(('1900-01-01','2011-12-31'),ranges[0])
        self.assertEqual(('2027-01-01','2100-12-31'),ranges[-1])
        for a,b in zip(ranges,ranges[1:]):
            self.assertEqual(date.fromisoformat(a[1])+timedelta(days=1),date.fromisoformat(b[0]))
        self.assertEqual(29,date_params('2020-02-29','2020-02-29')['maxRunDate']['day'])
        with self.assertRaises(ValueError): date_params('2020-03-01','2020-02-29')

    def test_explicit_repair_scope_is_immutable_and_unambiguous(self):
        conf={'collection_id':'repair','scope':'game','game':'subsurf','game_id':'y65797de',
              'selection_snapshot_id':'selection-games_metadata','repair_from_snapshot_id':'base-runs',
              'repair_partitions':[{'category_id':'n2y350ed','status_code':1}],
              'run_partition_strategy':'date_subdivision_v1'}
        self.assertEqual('base-runs',validate_conf(conf)['repair_from_snapshot_id'])
        self.assertEqual('standard',validate_conf({**conf,'run_partition_strategy':'standard'})['run_partition_strategy'])
        for extra in ({'repair_from_snapshot_id':'repair-runs'}, {'repair_from_snapshot_id':'../base-runs'},
                      {'repair_partitions':conf['repair_partitions']*2}, {'run_partition_strategy':'unknown'},
                      {'repair_partitions':[{'category_id':'c','status_code':True}]}):
            with self.assertRaises(ValueError): validate_conf({**conf,**extra})
