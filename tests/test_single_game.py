import io
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch
from urllib.parse import urlsplit,parse_qs
import base64

from speedrun_etl.collect import validate_conf,metadata,collect_run_partition
from speedrun_etl import date_partitions
from speedrun_etl.entity_workflow import needs_child
from speedrun_etl.jsonutil import dumps,loads,read,atomic_write,digest
from speedrun_etl.source import Client,SourceError


class Response(io.BytesIO):
    status=200
    headers={}


class FixtureSource:
    """Fresh source without any preloaded catalog, staging, profiles or repair base."""
    def __init__(self): self.calls=[]

    def __call__(self,request,timeout):
        endpoint=urlsplit(request.full_url).path.rsplit('/',1)[-1]
        encoded=parse_qs(urlsplit(request.full_url).query)['_r'][0]
        p=loads(base64.urlsafe_b64decode(encoded+'='*((-len(encoded))%4)))
        self.calls.append((endpoint,p))
        if endpoint=='GetGameData':
            body={'game':{'id':'g','url':'tiny','name':'Tiny','type':'game','gameTypeIds':[1],
                          'releaseDate':{'year':2020,'month':1,'day':1}},
                  'categories':[{'id':'c','name':'Any%','isPerLevel':False}]}
        elif endpoint=='GetGameSummary': body={'game':{'id':'g','name':'Tiny'}}
        elif endpoint=='GetGameRecordHistory':body={'recordEventList':[],'pagination':{'page':1,'pages':1,'per':100,'count':0}}
        elif endpoint=='GetGameLeaderboard2':
            params=p['params'];status=params['verified']
            all_rows=[{'id':f'r{status}-{i}','gameId':'g','categoryId':'c','verified':status,
                       'playerIds':['p'],'performedAt':f'2020-0{i+1}-01T12:00:00Z','time':'10s',
                       'unknown':[None,2,2]} for i in range(3)]
            if 'minRunDate' in params:
                def day(d):return f"{d['year']:04}-{d['month']:02}-{d['day']:02}"
                a,b=day(params['minRunDate']),day(params['maxRunDate'])
                all_rows=[r for r in all_rows if a<=r['performedAt'][:10]<=b]
            total=len(all_rows);per=2;page=p['page']
            body={'pagination':{'count':total,'per':per,'page':page,'pages':max(1,(total+per-1)//per)},
                  'runList':all_rows[(page-1)*per:page*per],
                  'playerList':[{'id':'p','name':'Player','country':None}] if total else []}
        else:raise AssertionError('Unexpected endpoint: '+endpoint)
        return Response(dumps(body).encode())


class SingleGameTests(unittest.TestCase):
    def test_direct_identity_and_metadata_only_gate_need_no_catalog(self):
        conf=validate_conf({'collection_id':'one','scope':'single_game','game_id':'g','metadata_only':True})
        self.assertNotIn('selection_snapshot_id',conf)
        self.assertFalse(needs_child(conf))
        self.assertTrue(needs_child({**conf,'metadata_only':False}))
        for extra in ({'selection_snapshot_id':'old-games_metadata'},{'repair_from_snapshot_id':'old-runs'},
                      {'metadata_only':'false'},{'game_id':'../bad'},{'game':'tiny'}):
            with self.assertRaises(ValueError):validate_conf({**conf,**extra})
        with TemporaryDirectory() as root:
            source=FixtureSource();client=Client(root,'one','one-games_metadata',budget=100,
                initial_pause=0,interval=0,transport=source)
            result=metadata(client,conf)
            self.assertEqual('g',result['game_id'])
            self.assertEqual([('GetGameData',{'gameId':'g'})],source.calls)

    def test_wrong_direct_identity_is_rejected(self):
        with TemporaryDirectory() as root:
            source=FixtureSource();client=Client(root,'one','one-games_metadata',budget=100,
                initial_pause=0,interval=0,transport=source)
            with self.assertRaisesRegex(SourceError,'identity mismatch'):
                metadata(client,validate_conf({'collection_id':'one','scope':'single_game','game_id':'wrong'}))

    def test_level_only_game_seals_unavailable_result_without_leaderboard_requests(self):
        from speedrun_etl.entity_runtime import extract
        from speedrun_etl.entity_ods import EntityPackage
        source=FixtureSource()
        def transport(request,timeout):
            response=source(request,timeout);body=loads(response.getvalue())
            if 'categories' in body:body['categories'][0]['isPerLevel']=True
            return Response(dumps(body).encode())
        def factory(state,collection,snapshot,**kwargs):
            return Client(state,collection,snapshot,**{**kwargs,'adaptive_rate':False},
                          initial_pause=0,interval=0,transport=transport)
        with TemporaryDirectory() as temp,patch('speedrun_etl.entity_runtime.Client',side_effect=factory):
            conf=validate_conf({'collection_id':'levels','scope':'single_game','game_id':'g'})
            for kind in ('games_metadata','runs','players'):
                root=Path(temp)/'snapshots'/('levels-'+kind);atomic_write(root/'config.json',conf)
                ref=extract(temp,conf,kind)
                package=EntityPackage(ref['package_uri'],role=ref['role'])
                if kind=='runs':
                    self.assertEqual('unavailable',next(package.records('manifests'))['source_result']['coverage'])
                    self.assertEqual([],list(package.records('runs')))
                package.db.close()
            self.assertEqual(['GetGameData','GetGameSummary'],[e for e,p in source.calls])

    def test_omitted_empty_categories_retains_raw_and_finishes_profiles_without_run_queries(self):
        from speedrun_etl.entity_runtime import extract
        from speedrun_etl.entity_ods import EntityPackage
        from speedrun_etl.batch import result_state
        source = FixtureSource()
        def transport(request, timeout):
            response = source(request, timeout); body = loads(response.getvalue())
            if 'categories' in body:
                body.pop('categories');body['game']['runCount'] = 0
            return Response(dumps(body).encode())
        def factory(state, collection, snapshot, **kwargs):
            return Client(state, collection, snapshot, **{**kwargs, 'adaptive_rate':False},
                          initial_pause=0, interval=0, transport=transport)
        with TemporaryDirectory() as temp, patch('speedrun_etl.entity_runtime.Client', side_effect=factory):
            conf = validate_conf({'collection_id':'omitted', 'scope':'single_game', 'game_id':'g'})
            for kind in ('games_metadata', 'runs', 'players'):
                root = Path(temp)/'snapshots'/('omitted-'+kind);atomic_write(root/'config.json', conf)
                ref = extract(temp, conf, kind)
                package = EntityPackage(ref['package_uri'], role=ref['role'])
                if kind == 'games_metadata':
                    response = next(r for r in package.records('responses') if r['metadata']['endpoint']=='GetGameData')
                    self.assertNotIn('categories', package.decode(response['body']))
                if kind == 'runs':
                    result = next(package.records('manifests'))['source_result']
                    self.assertEqual('not_applicable', result_state(result))
                    self.assertFalse(result['category_list_present'])
                    self.assertEqual(0, result['source_category_count'])
                    self.assertEqual([], list(package.records('runs')))
                package.db.close()
        self.assertEqual(['GetGameData', 'GetGameSummary'], [e for e,p in source.calls])

    def test_malformed_category_lists_are_not_reported_as_empty_scope(self):
        from unittest.mock import Mock
        from speedrun_etl.collect import collect_runs
        with TemporaryDirectory() as temp:
            atomic_write(Path(temp)/'config.json', {'scope':'game'})
            for value in (None, {}, 'categories', 3, [None], [{'id':'unknown'}]):
                package = Mock()
                package.records.return_value = iter([{'endpoint':'GetGameData', 'http_status':200}])
                client = Mock()
                with self.subTest(categories=value), patch('speedrun_etl.collect.restore',
                        return_value={'game':{'id':'g'}, 'categories':value}):
                    with self.assertRaises(SourceError):collect_runs(client, temp, package)
                client.request.assert_not_called()

    def test_date_large_does_not_split_small_or_low_profile_partitions(self):
        from unittest.mock import Mock
        for count,profiles,year in ((0,0,2020),(12001,1,2020),(12001,5000,None)):
            rows=[{'id':str(i),'gameId':'g','categoryId':'c','verified':1} for i in range(min(count,100))]
            body={'pagination':{'count':count,'pages':max(1,(count+99)//100),'page':1,'per':100},
                  'runList':rows,'playerList':[{'id':str(i)} for i in range(profiles)]}
            client=Mock();client.request.return_value={'body':body,'response_id':'first'}
            conf={'run_partition_strategy':'date_large_v1'}
            with self.subTest(count=count,profiles=profiles,year=year),patch(
                    'speedrun_etl.collect.partition',return_value={'coverage':'normal'}) as normal,patch(
                    'speedrun_etl.date_partitions.date_partition') as split:
                self.assertEqual({'coverage':'normal'},collect_run_partition(client,'root','g','c',1,conf,{'releaseDate':{'year':year}}))
                normal.assert_called_once();split.assert_not_called()

    def test_large_partition_reuses_baseline_and_routes_without_repair_base(self):
        from unittest.mock import Mock
        body={'pagination':{'count':12001,'pages':121,'page':1,'per':100},
              'runList':[{'id':str(i),'gameId':'g','categoryId':'c','verified':1} for i in range(100)],
              'playerList':[{'id':str(i)} for i in range(5000)]}
        saved={'body':body,'response_id':'first'}
        client=Mock();client.request.return_value=saved
        with patch('speedrun_etl.date_partitions.date_partition',return_value={'coverage':'split'}) as split:
            self.assertEqual({'coverage':'split'},collect_run_partition(client,'root','g','c',1,
                {'run_partition_strategy':'date_large_v1'},{'releaseDate':{'year':2020}}))
            split.assert_called_once_with(client,'root','g','c',1,2020,baseline_response=saved)
            self.assertEqual(1,client.request.call_count)

    def test_fresh_strategy_is_preserved_without_repair(self):
        conf=validate_conf({'collection_id':'b-g','scope':'game','game':'tiny','game_id':'g',
            'selection_snapshot_id':'b-catalog-games_metadata','run_partition_strategy':'date_large_v1'})
        self.assertEqual('date_large_v1',conf['run_partition_strategy'])
        self.assertNotIn('repair_from_snapshot_id',conf)

    @unittest.skipUnless(os.environ.get('ETL_INTEGRATION')=='1','Real three-store opt-in')
    def test_fresh_empty_staging_collects_all_statuses_profiles_and_wr_in_three_stores(self):
        from uuid import uuid4
        from speedrun_etl.entity_runtime import extract
        from speedrun_etl.entity_ods import EntityPackage
        from speedrun_etl.entity_storage import init_entities,load,ready,read_entity
        namespace='test_clean_'+uuid4().hex[:10];init_entities(namespace)
        source=FixtureSource()
        with TemporaryDirectory() as temp:
            conf=validate_conf({'collection_id':'clean','scope':'single_game','game_id':'g',
                'request_budget':200,'run_partition_strategy':'date_large_v1'})
            def client_factory(state,collection,snapshot,**kwargs):
                return Client(state,collection,snapshot,**{**kwargs,'adaptive_rate':False},
                              initial_pause=0,interval=0,transport=source)
            self.assertEqual([],list(Path(temp).iterdir()))
            with patch('speedrun_etl.entity_runtime.Client',side_effect=client_factory),patch(
                    'speedrun_etl.collect.DATE_SPLIT_MIN_RUNS',2),patch(
                    'speedrun_etl.collect.DATE_SPLIT_MIN_PROFILES',1):
                for kind in ('games_metadata','runs','players'):
                    root=Path(temp)/'snapshots'/('clean-'+kind);atomic_write(root/'config.json',conf)
                    ref=extract(temp,conf,kind)
                    package=EntityPackage(ref['package_uri'],role=ref['role'])
                    for store in ('postgres','mongo','garage'):
                        load(store,package,ref['snapshot_id'],namespace,role=ref['role'])
                    ready(ref['snapshot_id'],namespace,role=ref['role'],expected=ref['digest'])
                    if kind=='runs':
                        result=next(package.records('manifests'))['source_result']
                        self.assertEqual(3,len(result['partitions']))
                        self.assertTrue(all(p['unique_runs']==3 and p['coverage']=='verified_public_partitions' for p in result['partitions']))
                        self.assertTrue(all(p['strategy']=='date_subdivision_v1' for p in result['partitions']))
                        self.assertEqual({'r0-0','r0-1','r0-2','r1-0','r1-1','r1-2','r2-0','r2-1','r2-2'},
                                         {r['source_id'] for r in package.records('runs')})
                    if kind=='players':
                        for store in ('postgres','mongo','garage'):
                            self.assertEqual(1,len(read_entity(store,'players','p',ref['snapshot_id'],package.summary(),namespace)))
                    package.db.close()
                    calls=len(source.calls)
                    self.assertEqual(ref,extract(temp,conf,kind))
                    self.assertEqual(calls,len(source.calls))
            endpoints=[e for e,p in source.calls]
            self.assertNotIn('GetGameList',endpoints)
            self.assertNotIn('GetGameList2',endpoints)
            self.assertEqual(1,endpoints.count('GetGameData'))
            self.assertEqual(1,endpoints.count('GetGameRecordHistory'))
