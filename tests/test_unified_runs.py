import io
import os
import sys
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from uuid import uuid4

from speedrun_etl.audit_entity_ods import audit
from speedrun_etl.collect import validate_conf
from speedrun_etl.entity_ods import EntityPackage, build
from speedrun_etl.entity_runtime import extract, profiles
from speedrun_etl.jsonutil import atomic_write, digest, dumps, loads
from speedrun_etl.model import V3, dag_run_id, kinds
from speedrun_etl.source import Client, SourceError
from speedrun_etl.wr import collect_wr
from test_ods import response
from test_single_game import FixtureSource, Response


def journal(root, body, index, params=None, endpoint='GetGameLeaderboard2'):
    saved = response(body,str(index),f'2026-10-30T00:00:{index:02}+00:00')
    saved.update(params={'params':params or {},'page':1},endpoint=endpoint)
    atomic_write(root/'requests'/str(index)/'response.json',saved)
    return saved


class UnifiedRunsTests(unittest.TestCase):
    def test_standard_pagination_is_default_and_dates_require_explicit_opt_in(self):
        from speedrun_etl.batch import main
        with TemporaryDirectory() as temp,patch.dict(os.environ,{'ETL_STATE':temp}),patch('speedrun_etl.batch.pg_connect'):
            for index,flags in enumerate(([],['--accelerate-dates'])):
                with patch.object(sys,'argv',['batch','run','--collection',f'default{index}',
                                             '--all','--limit-games','500',*flags]),patch('speedrun_etl.batch.run_queue') as queue:
                    main()
                    conf=queue.call_args.args[3]
                    self.assertEqual('standard' if not flags else 'date_large_v1',conf['run_partition_strategy'])
                    self.assertEqual('run-count-desc',conf['game_order'])
                    self.assertEqual(500,conf['game_limit']);self.assertEqual(V3,conf['data_model'])

    def test_date_context_changes_do_not_create_versions_and_unfiltered_flags_remain(self):
        with TemporaryDirectory() as temp:
            root=Path(temp); source=root/'runs'; bodies=[]
            base={'id':'r','gameId':'g','categoryId':'c','time':'10s','playerIds':['p'],
                  'unknown':[None,[2,2]],'orphaned':False}
            for index in range(3):
                body={'runList':[{**base,'place':str(71-index*20),'obsolete':index==0}]}
                bodies.append(body)
                journal(source,body,index,{} if index==0 else {'minRunDate':{'year':2020+index,'month':1,'day':1}})
            build(source,root/'package',format_version=V3,load_role='runs')
            p=EntityPackage(root/'package'); rows=list(p.records('runs'))
            self.assertEqual(1,len(rows)); self.assertEqual('run_core',rows[0]['projection'])
            self.assertEqual(base,p.decode(rows[0]['raw']))
            self.assertEqual('native-json-v1',rows[0]['raw']['encoding'])
            check=next(p.records('checks'))
            self.assertEqual(1,len(check['states']))
            self.assertEqual({'place':'71','obsolete':True},check['unfiltered_leaderboard']['last_seen']['values'])
            self.assertEqual('0',check['unfiltered_leaderboard']['last_seen']['response_id'])
            self.assertEqual(bodies,[p.decode(r['body']) for r in p.records('responses')])
            p.db.close(); audit(source,root/'package')

    def test_real_changes_and_obsolete_history_survive_a_b_a(self):
        with TemporaryDirectory() as temp:
            root=Path(temp); source=root/'runs'
            for index,comment in enumerate(('A','B','A')):
                journal(source,{'runList':[{'id':'r','comment':comment,'obsolete':index!=1,'place':str(index+1)}]},index)
            build(source,root/'package',format_version=V3)
            p=EntityPackage(root/'package'); check=next(p.records('checks'))
            self.assertEqual(2,len(list(p.records('runs'))))
            self.assertEqual(['0','1','2'],[s['response_id'] for s in check['states']])
            self.assertEqual([True,False,True],[s['values']['obsolete'] for s in check['unfiltered_leaderboard']['states']])
            self.assertEqual(check['states'][0]['version_key'],check['states'][2]['version_key'])
            p.db.close();audit(source,root/'package')

    def test_dated_only_run_does_not_acquire_a_global_obsolete(self):
        with TemporaryDirectory() as temp:
            root=Path(temp); source=root/'runs'
            journal(source,{'runList':[{'id':'r','obsolete':False}]},0,{'maxRunDate':{'year':2020,'month':12,'day':31}})
            build(source,root/'package',format_version=V3)
            p=EntityPackage(root/'package')
            self.assertNotIn('unfiltered_leaderboard',next(p.records('checks')))
            self.assertNotIn('obsolete',p.decode(next(p.records('runs'))['raw']))
            p.db.close();audit(source,root/'package')

    def test_exact_numbers_and_unknown_values_use_lossless_fallback(self):
        with TemporaryDirectory() as temp:
            root=Path(temp); source=root/'runs'
            raw={'id':'r','number':Decimal('0.12345678901234567890123456789'),'big':2**100,'null':None}
            journal(source,{'runList':[raw]},0)
            build(source,root/'package',format_version=V3)
            p=EntityPackage(root/'package');row=next(p.records('runs'))
            self.assertEqual('json-v1',row['raw']['encoding']);self.assertEqual(raw,p.decode(row['raw']))
            p.db.close();audit(source,root/'package')

    def test_wr_subset_links_existing_run_and_wr_only_profiles_are_included(self):
        with TemporaryDirectory() as temp:
            root=Path(temp); src=root/'sample-runs'; wr=root/'sample-wr_history'
            raw={'id':'r','gameId':'g','categoryId':'c','time':'10s','comment':'full','playerIds':['p'],'obsolete':True,'place':'2'}
            journal(src,{'runList':[raw],'playerList':[{'id':'p','name':'P'}]},0)
            build(src,src/'entity-package',format_version=V3,load_role='runs')
            runs=EntityPackage(src/'entity-package')
            old={'id':'r','gameId':'g','categoryId':'c','time':'10s','playerIds':['p'],
                 'enforceMs':True,'comment':'WR source differs from the ordinary run'}
            only={'id':'wr-only','gameId':'g','categoryId':'c','time':'8s','playerIds':['q']}
            body={'recordEventList':[{'performedAt':'2020','run_ids':['unknown source field'],
                                     'runList':[old,only,only]}],
                  'playerList':[{'id':'q','name':'Q'},{'name':'guest without ID'}]}
            journal(wr,body,1,{'gameId':'g','categoryId':'c','values':[]},'GetGameRecordHistory')
            build(wr,wr/'entity-package',format_version=V3,load_role='wr_history',runs_input=runs)
            history=EntityPackage(wr/'entity-package')
            self.assertEqual(['wr-only'],[r['source_id'] for r in history.records('runs')])
            event=next(history.records('wr_history'));value=history.decode(event['raw'])
            self.assertEqual(['r','wr-only','wr-only'],value['run_ids'])
            self.assertEqual(['unknown source field'],value['event']['run_ids'])
            self.assertEqual(body,history.decode(next(history.records('responses'))['body']))
            target=EntityPackage(root/'players')
            result=profiles(runs,target,'sample-players','sample-runs',[(history,'sample-wr_history')],V3)
            self.assertEqual(2,result['participants']);self.assertEqual(2,result['available_profiles'])
            self.assertEqual({'p','q'},{r['source_id'] for r in target.records('players')})
            target.db.close();runs.db.close();history.db.close()
            audit(wr,wr/'entity-package')

    def test_wr_pagination_and_unavailability_are_independent_and_resumable(self):
        class Fake:
            def __init__(self):self.calls=[]
            def request(self,endpoint,params,key,context,**kwargs):
                self.calls.append(params)
                cid=params['params']['categoryId'];page=params['page']
                if cid=='unavailable':return {'response_id':cid,'http_status':404,'body':None}
                return {'response_id':str(page),'http_status':200,'body':{
                    'recordEventList':[{'performedAt':str(page),'runList':[]}],
                    'pagination':{'page':page,'pages':2,'per':1,'count':2}}}
        with TemporaryDirectory() as temp:
            root=Path(temp); meta=root/'meta'
            journal(meta,{'game':{'id':'g'},'categories':[{'id':c,'isPerLevel':False} for c in ('available','unavailable')]},0,endpoint='GetGameData')
            build(meta,meta/'entity-package',format_version=V3);p=EntityPackage(meta/'entity-package');fake=Fake()
            result=collect_wr(fake,root/'wr',p,{'scope':'single_game'})
            self.assertEqual(['verified_public_partitions','unavailable'],[r['coverage'] for r in result['categories']])
            self.assertEqual(3,len(fake.calls))
            self.assertEqual(result,collect_wr(fake,root/'wr',p,{'scope':'single_game'}));self.assertEqual(3,len(fake.calls))
            p.db.close()

    def test_only_deliberate_wr_404_is_a_cached_unavailability(self):
        with TemporaryDirectory() as root:
            calls=[]
            def missing(request,timeout):
                calls.append(request.full_url)
                raise HTTPError(request.full_url,404,'Not found',{},io.BytesIO(b'{}'))
            client=Client(root,'one','one-wr_history',budget=10,initial_pause=0,interval=0,transport=missing)
            for _ in range(2):
                self.assertEqual(404,client.request('GetGameRecordHistory',{},'one',allow_statuses=(404,))['http_status'])
            self.assertEqual(1,len(calls))
            with self.assertRaises(ValueError):client.request('GetGameData',{},'two',allow_statuses=(404,))
            with self.assertRaises(SourceError):client.request('GetGameRecordHistory',{},'other')

    def test_names_and_dag_dependencies_are_stable_without_source_calls(self):
        from speedrun_etl.entity_workflow import next_kind,make_dag
        conf=validate_conf({'collection_id':'fresh','scope':'single_game','game_id':'g',
                            'game_label':'Subway Surfers','data_model':V3})
        self.assertEqual('fresh--g--subway-surfers--runs',dag_run_id(conf,'runs'))
        self.assertEqual('wr_history',next_kind('runs',conf))
        self.assertEqual(('games_metadata','runs','wr_history','players'),kinds(conf))
        dag=make_dag('runs','players')
        self.assertIn('trigger_next',dag.task_ids)
        self.assertNotIn('trigger_players',dag.task_ids)
        self.assertNotIn('trigger_wr_history',dag.task_ids)
        self.assertEqual(1,sum(t.task_type=='TriggerDagRunOperator' for t in dag.tasks))
        self.assertTrue(dag.get_task('needs_child').python_callable(conf))
        self.assertEqual('players',next_kind('runs',{'scope':'game'}))
        for wrong in ('unknown',None):
            with self.assertRaises(ValueError):validate_conf({**conf,'data_model':wrong})

    def test_single_trigger_renders_the_correct_child_for_both_contracts(self):
        from unittest.mock import Mock
        from speedrun_etl.entity_workflow import make_dag,next_kind
        task=make_dag('runs','players').get_task('trigger_next')
        for model,destination in ((V3,'wr_history'),('entity-v2','players')):
            conf={'collection_id':'one','scope':'single_game','game_id':'g','game_label':'Game','data_model':model}
            value={'next_dag_id':next_kind('runs',conf),'next_run_id':dag_run_id(conf,destination)}
            ti=Mock();ti.xcom_pull.return_value=value
            self.assertEqual(destination,task.render_template(task.trigger_dag_id,{'ti':ti}))
            self.assertEqual(dag_run_id(conf,destination),task.render_template(task.trigger_run_id,{'ti':ti}))

    def test_runs_file_is_discovered_by_airflow_safe_mode(self):
        from airflow.utils.file import might_contain_dag
        path=Path(__file__).resolve().parents[1]/'dags/ods/runs.py'
        if not path.exists():path=Path('/opt/airflow/dags/ods/runs.py')
        self.assertTrue(might_contain_dag(str(path),safe_mode=True))


class FreshFourStageTests(unittest.TestCase):
    def run_fixture(self,root):
        source=FixtureSource()
        def transport(request,timeout):
            r=source(request,timeout);body=loads(r.getvalue())
            if 'recordEventList' in body:
                body={'recordEventList':[{'performedAt':'2020','runList':[
                    {'id':'r1-0','gameId':'g','categoryId':'c','verified':1,'time':'10s','playerIds':['p']},
                    {'id':'historic','gameId':'g','categoryId':'c','verified':1,'time':'8s','playerIds':['q']}]}],
                      'playerList':[{'id':'q','name':'WR only'}],
                      'pagination':{'page':1,'pages':1,'per':100,'count':1}}
            return Response(dumps(body).encode())
        def factory(state,collection,snapshot,**kwargs):
            return Client(state,collection,snapshot,**{**kwargs,'adaptive_rate':False},
                          initial_pause=0,interval=0,transport=transport)
        conf=validate_conf({'collection_id':'fresh','scope':'single_game','game_id':'g','data_model':V3})
        refs={}
        with patch('speedrun_etl.entity_runtime.Client',side_effect=factory):
            for kind in kinds(conf):
                atomic_write(root/'snapshots'/('fresh-'+kind)/'config.json',conf)
                before=len(source.calls);refs[kind]=extract(root,conf,kind)
                calls=[e for e,p in source.calls[before:]]
                if kind=='runs':self.assertNotIn('GetGameRecordHistory',calls)
                if kind=='wr_history':self.assertEqual(['GetGameRecordHistory'],calls)
                if kind=='players':self.assertEqual([],calls)
                self.assertEqual(refs[kind],extract(root,conf,kind));self.assertEqual(before+len(calls),len(source.calls))
        return source,refs

    def test_four_fresh_stages_are_separate_and_idempotent(self):
        with TemporaryDirectory() as temp:
            root=Path(temp);source,refs=self.run_fixture(root)
            self.assertNotIn('GetGameList',[e for e,p in source.calls])
            for kind in ('runs','wr_history'):
                audit(root/'snapshots'/('fresh-'+kind),Path(refs[kind]['package_uri']))
            p=EntityPackage(refs['players']['package_uri'])
            self.assertEqual({'p','q'},{r['source_id'] for r in p.records('players')});p.db.close()

    @unittest.skipUnless(os.environ.get('ETL_INTEGRATION')=='1','Three real ODS opt-in')
    def test_new_contract_replay_partial_load_and_wr_only_player_in_three_stores(self):
        from speedrun_etl.entity_storage import init_entities,load,ready,read_entity
        from speedrun_etl.storage import pg_connect
        from psycopg import sql
        ns='test_core_'+uuid4().hex[:10];init_entities(ns)
        with TemporaryDirectory() as temp:
            source,refs=self.run_fixture(Path(temp));attempts=len(source.calls)
            for kind,ref in refs.items():
                p=EntityPackage(ref['package_uri'],role=ref['role']);summary=p.summary()
                for store in ('postgres','mongo','garage'):
                    if kind=='runs' and store=='mongo':
                        with self.assertRaises(RuntimeError):load(store,p,ref['snapshot_id'],ns,role=ref['role'],fail_after=1)
                    for _ in range(2):load(store,p,ref['snapshot_id'],ns,role=ref['role'])
                ready(ref['snapshot_id'],ns,ref['role'],expected=digest(summary))
                if kind=='players':
                    for dataset in ('players','checks'):
                        values=[read_entity(s,dataset,'q',ref['snapshot_id'],summary,ns,format_version=V3) for s in ('postgres','mongo','garage')]
                        self.assertEqual(1,len({digest(v) for v in values}));self.assertEqual(1,len(values[0]))
                p.db.close()
            with pg_connect() as c:
                count=c.execute(sql.SQL('SELECT count(*),count(DISTINCT source_id),count(DISTINCT projection) FROM {}').format(sql.Identifier(ns,'runs'))).fetchone()
                self.assertEqual((10,10,1),count)
            self.assertEqual(attempts,len(source.calls))
