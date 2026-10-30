from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
import os
import sys
from uuid import uuid4

from speedrun_etl.batch import start,validate_selection,result_state,drift_retry_partitions
from speedrun_etl.collect import validate_conf
from speedrun_etl.entity_ods import EntityPackage


class BatchPauseTests(unittest.TestCase):
    def invoke(self, command, batch_id, *options):
        from speedrun_etl.batch import main
        with patch.object(sys, 'argv', ['batch', command, '--collection', batch_id, *options]):
            main()

    def test_pause_is_received_by_another_process_without_waiting_for_driver(self):
        import subprocess
        from speedrun_etl.batch import begin_stage
        from speedrun_etl.jsonutil import atomic_write, file_lock, read
        with TemporaryDirectory() as temp, patch.dict(os.environ, {'ETL_STATE': temp}):
            root = Path(temp)/'batches/example'
            atomic_write(root/'config.json', {'mode': 'top', 'top': 2, 'game_request_budget': 100})
            with file_lock(root/'driver.lock'):
                self.assertTrue(begin_stage(root, 'game', collection_id='example-g-one'))
                result = subprocess.run([sys.executable, '-m', 'speedrun_etl.batch', 'pause',
                                         '--collection', 'example'], capture_output=True, text=True, timeout=15)
                self.assertEqual(0, result.returncode, result.stderr)
                self.assertTrue(read(root/'control.json')['pause_requested'])
                self.assertEqual('game', read(root/'status.json')['phase'])
                self.assertFalse(begin_stage(root, 'game', collection_id='example-g-two'))
                status = read(root/'status.json')
                self.assertEqual(('paused', 'example-g-one'), (status['phase'], status['collection_id']))

    def test_resume_restores_all_scope_strategy_and_session_settings(self):
        from speedrun_etl.jsonutil import atomic_write, read
        with TemporaryDirectory() as temp, patch.dict(os.environ, {'ETL_STATE': temp}), patch(
                'speedrun_etl.batch.pg_connect'), patch('speedrun_etl.batch.run_queue') as run:
            for mode in ('all', 'sample', 'top'):
                with self.subTest(mode=mode):
                    root = Path(temp)/'batches'/mode
                    conf = {'mode': mode, 'top': 3 if mode == 'top' else None, 'game_request_budget': 321,
                            'catalog_snapshot_id': 'shared-games_metadata', 'run_partition_strategy': 'date_large_v1'}
                    if mode == 'sample':conf['sample_size'] = 4
                    atomic_write(root/'config.json', conf)
                    atomic_write(root/'runtime-policy.json', {'continue_on_drift': True, 'hours': 2.5,
                                                              'session_request_budget': 777})
                    atomic_write(root/'control.json', {'pause_requested': True})
                    self.invoke('resume', mode)
                    args, state, actual_root, actual_conf = run.call_args.args
                    self.assertEqual(conf, actual_conf)
                    self.assertEqual(root, actual_root)
                    self.assertEqual((321, 777, 2.5, True), (args.game_request_budget, args.session_request_budget,
                                                                          args.hours, args.continue_on_drift))
                    self.assertEqual(mode == 'all', args.all)
                    self.assertEqual(4 if mode == 'sample' else None, args.sample)
                    self.assertFalse(read(root/'control.json')['pause_requested'])

    def test_resume_does_not_queue_a_second_driver_or_cancel_an_active_pause(self):
        from speedrun_etl.jsonutil import atomic_write, file_lock, read
        with TemporaryDirectory() as temp, patch.dict(os.environ, {'ETL_STATE': temp}), patch(
                'speedrun_etl.batch.pg_connect'), patch('speedrun_etl.batch.run_queue') as run:
            root = Path(temp)/'batches/example'
            atomic_write(root/'config.json', {'mode': 'all', 'top': None, 'game_request_budget': 100})
            atomic_write(root/'control.json', {'pause_requested': True})
            with file_lock(root/'driver.lock'):
                self.invoke('resume', 'example')
            run.assert_not_called()
            self.assertTrue(read(root/'control.json')['pause_requested'])

    def test_unknown_batch_and_scope_changes_are_rejected_without_side_effects(self):
        from contextlib import redirect_stderr
        from io import StringIO
        from speedrun_etl.jsonutil import atomic_write, read
        with TemporaryDirectory() as temp, patch.dict(os.environ, {'ETL_STATE': temp}), patch(
                'speedrun_etl.batch.pg_connect') as database, redirect_stderr(StringIO()):
            for command in ('pause', 'resume'):
                with self.assertRaises(SystemExit):self.invoke(command, 'unknown')
            self.assertFalse((Path(temp)/'batches/unknown').exists())
            root = Path(temp)/'batches/example'
            atomic_write(root/'config.json', {'mode': 'all', 'top': None, 'game_request_budget': 100})
            atomic_write(root/'control.json', {'pause_requested': True})
            with self.assertRaises(SystemExit):self.invoke('resume', 'example', '--top', '10')
            self.assertTrue(read(root/'control.json')['pause_requested'])
            database.assert_not_called()

    @unittest.skipUnless(os.environ.get('ETL_INTEGRATION') == '1', 'Real control DB opt-in')
    def test_game_finishes_before_pause_and_resume_skips_it_with_the_same_config(self):
        from speedrun_etl.jsonutil import atomic_write, read
        from speedrun_etl.storage import pg_connect
        from speedrun_etl.progress import batch_progress
        for stop_at in ('g1', 'g2'):
            with self.subTest(stop_at=stop_at), TemporaryDirectory() as temp, patch.dict(os.environ, {'ETL_STATE': temp}):
                batch_id = 'testpause'+uuid4().hex[:10]
                root = Path(temp)/'batches'/batch_id
                starts, waits = [], []
                games = [{'id':g,'url':g,'name':g,'runCount':2-index} for index,g in enumerate(('g1','g2'))]
                def fake_start(collection, conf):
                    starts.append((collection, conf))
                    atomic_write(Path(temp)/'collections'/collection/'budget.json', {'used': 1, 'limit': 100})
                    package = EntityPackage(Path(temp)/'snapshots'/(collection+'-runs')/'entity-package')
                    package.put('manifests', {'key': 'm', 'source_result': {
                        'partitions': [{'coverage': 'verified_public_partitions'}]}})
                    package.db.commit();package.db.close()
                def fake_wait(collection, kinds):
                    waits.append((collection, kinds))
                    if collection.endswith('-g-'+stop_at):
                        self.invoke('pause', batch_id)
                        self.assertEqual('game', read(root/'status.json')['phase'])
                        self.assertEqual(('games_metadata', 'runs', 'wr_history', 'players'), kinds)
                        self.assertTrue(batch_progress(batch_id)['control']['pause_requested'])
                with patch('speedrun_etl.batch.check_capacity'), patch('speedrun_etl.batch.start', side_effect=fake_start), patch(
                        'speedrun_etl.batch.wait', side_effect=fake_wait), patch('speedrun_etl.batch.selection', return_value=games), patch(
                        'speedrun_etl.batch.validate_catalog_input'):
                    options = ('--all', '--catalog-snapshot', 'shared-games_metadata', '--runs-strategy', 'date-large',
                               '--game-request-budget', '100', '--session-request-budget', '150', '--hours', '1.5',
                               '--continue-on-drift')
                    self.invoke('run', batch_id, *options)
                    self.assertEqual('paused', read(root/'status.json')['phase'])
                    with pg_connect(True) as conn:
                        states = dict(conn.execute('SELECT game_id,state FROM entity_batch_queue WHERE batch_id=%s', [batch_id]).fetchall())
                    self.assertEqual('complete', states[stop_at])
                    if stop_at == 'g1':self.assertEqual('pending', states['g2'])
                    first_count = len(starts)
                    # A repeated run cannot silently cancel a durable pause or replace its session policy.
                    self.invoke('run', batch_id, *options[:-1])
                    self.assertEqual(first_count, len(starts))
                    self.assertTrue(read(root/'runtime-policy.json')['continue_on_drift'])
                    self.invoke('resume', batch_id)
                self.assertEqual('complete', read(root/'status.json')['phase'])
                self.assertEqual([batch_id+'-g-g1', batch_id+'-g-g2'], [c for c, _ in starts])
                self.assertEqual(2, len(waits))
                self.assertTrue(all(c['run_partition_strategy'] == 'date_large_v1' for _, c in starts))
                self.assertEqual(1, read(Path(temp)/'collections'/(batch_id+'-g-g1')/'budget.json')['used'])

    @unittest.skipUnless(os.environ.get('ETL_INTEGRATION') == '1', 'Real control DB opt-in')
    def test_pause_during_catalog_or_ranking_stops_before_the_next_stage(self):
        from speedrun_etl.jsonutil import read
        for suffix in ('-catalog', '-ranking-v2'):
            with self.subTest(stage=suffix), TemporaryDirectory() as temp, patch.dict(os.environ, {'ETL_STATE': temp}):
                batch_id = 'testbootpause'+uuid4().hex[:8]
                starts = []
                def fake_wait(collection, kinds):
                    if collection.endswith(suffix):self.invoke('pause', batch_id)
                with patch('speedrun_etl.batch.start', side_effect=lambda c, conf: starts.append(c)), patch(
                        'speedrun_etl.batch.wait', side_effect=fake_wait), patch('speedrun_etl.batch.selection') as select:
                    self.invoke('run', batch_id, '--top', '1')
                self.assertEqual('paused', read(Path(temp)/'batches'/batch_id/'status.json')['phase'])
                self.assertEqual(batch_id+suffix, starts[-1])
                self.assertFalse(any('-g-' in c for c in starts))
                select.assert_not_called()

    @unittest.skipUnless(os.environ.get('ETL_INTEGRATION') == '1', 'Real control DB opt-in')
    def test_pause_request_does_not_mask_an_active_store_failure(self):
        from speedrun_etl.jsonutil import read
        from speedrun_etl.storage import pg_connect
        with TemporaryDirectory() as temp, patch.dict(os.environ, {'ETL_STATE': temp}):
            batch_id = 'testpausefail'+uuid4().hex[:8]
            starts = []
            def fake_wait(collection, kinds):
                if collection.endswith('-g-g1'):
                    self.invoke('pause', batch_id)
                    raise RuntimeError('Store acknowledgement missing')
            with patch('speedrun_etl.batch.start', side_effect=lambda c, conf: starts.append(c)), patch(
                    'speedrun_etl.batch.wait', side_effect=fake_wait), patch('speedrun_etl.batch.selection', return_value=[
                    {'id': g, 'url': g, 'name': g} for g in ('g1', 'g2')]):
                with self.assertRaisesRegex(RuntimeError, 'acknowledgement'):
                    self.invoke('run', batch_id, '--top', '2')
            root = Path(temp)/'batches'/batch_id
            self.assertEqual('failed', read(root/'status.json')['phase'])
            self.assertTrue(read(root/'control.json')['pause_requested'])
            with pg_connect(True) as conn:
                states = dict(conn.execute('SELECT game_id,state FROM entity_batch_queue WHERE batch_id=%s', [batch_id]).fetchall())
            self.assertEqual({'g1': 'failed', 'g2': 'pending'}, states)
            self.assertNotIn(batch_id+'-g-g2', starts)


class BatchTests(unittest.TestCase):
    def test_popularity_order_is_numeric_stable_and_limited_after_sorting(self):
        from speedrun_etl.batch import ordered_selection
        games = [{'id':'z','runCount':'10'}, {'id':'b','runCount':'100'},
                 {'id':'a','runCount':100}, {'id':'zero','runCount':'0'}]
        conf = {'game_order':'run-count-desc', 'game_limit':2}
        self.assertEqual(['a','b'], [g['id'] for g in ordered_selection(games, conf)])
        self.assertEqual(ordered_selection(games, conf), ordered_selection(list(reversed(games)), conf))
        self.assertEqual(games, ordered_selection(games, {}))
        with self.assertRaisesRegex(ValueError, 'fewer'):ordered_selection(games, {**conf,'game_limit':5})
        for count in (None, -1, 'unknown', 1.5, True):
            with self.subTest(count=count), self.assertRaises(ValueError):
                ordered_selection([{'id':'bad','runCount':count}], conf)

    @unittest.skipUnless(os.environ.get('ETL_INTEGRATION') == '1', 'Real control DB opt-in')
    def test_priority_queue_resume_preserves_order_limit_and_completed_games(self):
        from speedrun_etl.batch import main
        from speedrun_etl.jsonutil import atomic_write, read
        from speedrun_etl.storage import pg_connect
        batch_id = 'testpriority'+uuid4().hex[:10]
        games = [{'id':g,'url':g,'name':g,'runCount':n} for g,n in [('g0',1),('g1',100),('g2',2000),('g3',100)]]
        starts = []
        with TemporaryDirectory() as temp:
            def fake_start(collection, conf):
                starts.append(collection)
                atomic_write(Path(temp)/'collections'/collection/'budget.json', {'used':60,'limit':100})
                package = EntityPackage(Path(temp)/'snapshots'/(collection+'-runs')/'entity-package')
                package.put('manifests', {'key':'m','source_result':{'partitions':[{'coverage':'verified_public_partitions'}]}})
                package.db.commit();package.db.close()
            with patch.dict(os.environ, {'ETL_STATE':temp}), patch('speedrun_etl.batch.check_capacity'), patch(
                    'speedrun_etl.batch.validate_catalog_input'), patch('speedrun_etl.batch.start', side_effect=fake_start), patch(
                    'speedrun_etl.batch.wait'), patch('speedrun_etl.batch.selection', return_value=games):
                with patch.object(sys,'argv',['batch','run','--collection',batch_id,'--all','--order','run-count-desc',
                        '--limit-games','2','--catalog-snapshot','shared-games_metadata','--game-request-budget','100',
                        '--session-request-budget','150']):main()
                root = Path(temp)/'batches'/batch_id
                self.assertEqual('session_paused', read(root/'status.json')['phase'])
                self.assertEqual(['g2','g1'], read(root/'selection.json')['game_ids'])
                with patch.object(sys,'argv',['batch','resume','--collection',batch_id]):main()
                self.assertEqual('complete', read(root/'status.json')['phase'])
                self.assertEqual('run-count-desc', read(root/'config.json')['game_order'])
                with pg_connect(True) as conn:
                    rows=conn.execute('SELECT game_id,state FROM entity_batch_queue WHERE batch_id=%s ORDER BY position',[batch_id]).fetchall()
                self.assertEqual([('g2','complete'),('g1','complete')],rows)
        self.assertEqual([batch_id+'-g-g2',batch_id+'-g-g1'], starts)

    def test_hash_sample_is_order_independent_and_covers_four_counter_bands(self):
        from collections import Counter
        from speedrun_etl.batch import stratified_selection
        games=[{'id':f'{band}-{i}','runCount':count} for band,count in enumerate((0,101,1001,10001)) for i in range(8)]
        selected=stratified_selection(games,20)
        self.assertEqual(selected,stratified_selection(list(reversed(games)),20))
        self.assertEqual({'0-100':5,'101-1000':5,'1001-10000':5,'>10000':5},dict(Counter(g['sampling_band'] for g in selected)))

    def test_session_reservation_counts_previous_chains_and_keeps_existing_attempts(self):
        from speedrun_etl.batch import reserve_session
        from speedrun_etl.jsonutil import atomic_write,read
        with TemporaryDirectory() as temp:
            state=Path(temp);root=state/'batches/b';baseline={}
            self.assertTrue(reserve_session(state,root,baseline,'b-catalog',100,150))
            atomic_write(state/'collections/b-catalog/budget.json',{'used':60,'limit':100})
            self.assertFalse(reserve_session(state,root,baseline,'b-g-next',100,150))
            self.assertEqual(60,read(root/'session-budget.json')['used'])
            self.assertEqual(60,read(state/'collections/b-catalog/budget.json')['used'])
            self.assertEqual('session_paused',read(root/'status.json')['phase'])
            self.assertTrue(reserve_session(state,root,{'b-catalog':60},'b-g-next',100,150))

    def test_catalog_reuse_checks_source_freshness_and_rejects_ranking(self):
        from speedrun_etl.batch import validate_catalog_input
        from speedrun_etl.jsonutil import utcnow
        with TemporaryDirectory() as temp:
            root=Path(temp);p=EntityPackage(root/'catalog')
            manifest={'key':'m','input_config':{'scope':'catalog'},'source_result':{'coverage':'verified_public_partitions'}}
            p.put('manifests',manifest);p.put('responses',{'key':'r','metadata':{
                'endpoint':'GetGameList','http_status':200,'fetched_at':utcnow()}});p.db.commit();p.db.close()
            with patch('speedrun_etl.batch.parent_package',side_effect=lambda *a:EntityPackage(root/'catalog')):
                validate_catalog_input(root,'catalog-games_metadata')
                p=EntityPackage(root/'catalog')
                from speedrun_etl.jsonutil import dumps
                p.db.execute("UPDATE records SET document=? WHERE dataset='responses'",[dumps({'key':'r','metadata':{
                    'endpoint':'GetGameList','http_status':200,'fetched_at':'2026-01-01T00:00:00+00:00'}})])
                p.db.commit();p.db.close()
                with self.assertRaisesRegex(ValueError,'seven days'):validate_catalog_input(root,'catalog-games_metadata')
                validate_catalog_input(root,'catalog-games_metadata',require_fresh=False)
                p=EntityPackage(root/'catalog');manifest['input_config']['scope']='ranking'
                p.db.execute("UPDATE records SET document=? WHERE dataset='manifests'",[dumps(manifest)])
                p.db.commit();p.db.close()
                with self.assertRaisesRegex(ValueError,'directory snapshot'):
                    validate_catalog_input(root,'catalog-games_metadata',require_fresh=False)

    @unittest.skipUnless(os.environ.get('ETL_INTEGRATION')=='1','Real control DB opt-in')
    def test_sample_reuses_catalog_and_resumes_pending_games_after_session_budget(self):
        from speedrun_etl.batch import main
        from speedrun_etl.storage import pg_connect
        from speedrun_etl.jsonutil import atomic_write,read
        batch_id='testpilot'+uuid4().hex[:10]
        games=[{'id':f'g{i}','url':f'game{i}','name':str(i),'runCount':count}
               for i,count in enumerate((1,200,2000,20000))]
        starts=[]
        with TemporaryDirectory() as temp:
            def fake_start(collection,conf):
                starts.append((collection,conf))
                atomic_write(Path(temp)/'collections'/collection/'budget.json',{'used':60,'limit':100})
                p=EntityPackage(Path(temp)/'snapshots'/(collection+'-runs')/'entity-package')
                p.put('manifests',{'key':'m','source_result':{'partitions':[{'coverage':'verified_public_partitions'}]}})
                p.db.commit();p.db.close()
            with patch.dict(os.environ,{'ETL_STATE':temp}),patch.object(sys,'argv',[
                    'batch','run','--collection',batch_id,'--sample','4','--catalog-snapshot','shared-games_metadata',
                    '--game-request-budget','100','--session-request-budget','150']),patch(
                    'speedrun_etl.batch.start',side_effect=fake_start),patch('speedrun_etl.batch.wait'),patch(
                    'speedrun_etl.batch.selection',return_value=games),patch('speedrun_etl.batch.validate_catalog_input'):
                main()
                with pg_connect(True) as conn:
                    states=dict(conn.execute('SELECT game_id,state FROM entity_batch_queue WHERE batch_id=%s',[batch_id]).fetchall())
                self.assertEqual({'g0':'complete','g1':'pending','g2':'pending','g3':'pending'},states)
                self.assertEqual('session_paused',read(Path(temp)/'batches'/batch_id/'status.json')['phase'])
                for _ in range(3):main()
            self.assertEqual(4,len(starts))
            self.assertTrue(all(conf['selection_snapshot_id']=='shared-games_metadata' for _,conf in starts))
            self.assertEqual('complete',read(Path(temp)/'batches'/batch_id/'status.json')['phase'])

    @unittest.skipUnless(os.environ.get("ETL_INTEGRATION")=="1","Real control DB opt-in")
    def test_fresh_date_strategy_uses_one_catalog_and_survives_resume(self):
        from speedrun_etl.batch import main
        from speedrun_etl.jsonutil import atomic_write,read
        batch_id='teststrategy'+uuid4().hex[:10]
        games=[{'id':'g1','url':'one','name':'One'},{'id':'g2','url':'two','name':'Two'}]
        starts=[]
        with TemporaryDirectory() as temp:
            def fake_start(collection,conf):
                starts.append((collection,conf))
                p=EntityPackage(Path(temp)/'snapshots'/(collection+'-runs')/'entity-package')
                p.put('manifests',{'key':'m','source_result':{'partitions':[{'coverage':'verified_public_partitions'}]}})
                p.db.commit();p.db.close()
            argv=['batch','run','--collection',batch_id,'--top','2','--runs-strategy','date-large']
            with patch.dict(os.environ,{'ETL_STATE':temp}),patch.object(sys,'argv',argv),patch(
                    'speedrun_etl.batch.start',side_effect=fake_start),patch('speedrun_etl.batch.wait'),patch(
                    'speedrun_etl.batch.selection',return_value=games):
                main()
                self.assertEqual(1,sum(c.endswith('-catalog') for c,conf in starts))
                requested=[conf for c,conf in starts if '-g-' in c]
                self.assertEqual(2,len(requested))
                self.assertTrue(all(c['run_partition_strategy']=='date_large_v1' for c in requested))
                del argv[-2:]
                main()
                self.assertEqual(2,sum('-g-' in c for c,conf in starts))
                self.assertEqual('date_large_v1',read(Path(temp)/'batches'/batch_id/'config.json')['run_partition_strategy'])

    def test_only_documented_source_drift_can_be_deferred(self):
        from copy import deepcopy
        result = {"partitions": [{"coverage": "partial", "category_id": "c", "status_code": 1,
            "diagnostic": "pagination_unstable_after_one_retry",
            "errors": [{"error": "Pagination changed within a section"}]}]}
        self.assertEqual(result["partitions"], drift_retry_partitions(result))
        for patch_value in ({"stop_reason": "HTTP budget exhausted"}, {"intentional_limit": True}):
            self.assertIsNone(drift_retry_partitions({**result, **patch_value}))
        wrong = deepcopy(result)
        wrong["partitions"][0]["errors"] = [{"error": "Wrong category filter"}]
        self.assertIsNone(drift_retry_partitions(wrong))
        wrong["partitions"][0].update(diagnostic="unknown", errors=[])
        self.assertIsNone(drift_retry_partitions(wrong))
        wrong["partitions"][0].update(diagnostic="pagination_changed_during_traversal",
                                      drift={"pagination_changed": True})
        self.assertEqual(wrong["partitions"], drift_retry_partitions(wrong))
        wrong["partitions"].append({"coverage": "unavailable"})
        self.assertIsNone(drift_retry_partitions(wrong))

    @unittest.skipUnless(os.environ.get("ETL_INTEGRATION") == "1", "Real control DB opt-in")
    def test_reviewed_drift_resume_retains_retry_and_moves_to_next_game_once(self):
        from speedrun_etl.batch import main
        from speedrun_etl.storage import pg_connect
        from speedrun_etl.jsonutil import read, atomic_write
        from speedrun_etl.progress import batch_progress
        batch_id = "testdrift" + uuid4().hex[:10]
        games = [{"id": "g1", "url": "one", "name": "One"}, {"id": "g2", "url": "two", "name": "Two"}]
        started = []
        with TemporaryDirectory() as temp:
            def fake_start(collection, conf):
                started.append(collection)
                if "game_id" in conf:
                    atomic_write(Path(temp) / "snapshots" / (collection + "-games_metadata") / "config.json", conf)
                result = {"partitions": [{"coverage": "verified_public_partitions"}]}
                if collection.endswith("-g-g1"):
                    result = {"partitions": [{"coverage": "partial", "category_id": "c", "status_code": 1,
                        "diagnostic": "pagination_unstable_after_one_retry",
                        "errors": [{"error": "Pagination changed within a section"}]}]}
                package = EntityPackage(Path(temp) / "snapshots" / (collection + "-runs") / "entity-package")
                package.put("manifests", {"key": "m", "source_result": result})
                package.db.commit()
                package.db.close()
            argv = ["batch", "run", "--collection", batch_id, "--top", "2"]
            with patch.dict(os.environ, {"ETL_STATE": temp}), patch.object(sys, "argv", argv), patch(
                    "speedrun_etl.batch.start", side_effect=fake_start), patch("speedrun_etl.batch.wait"), patch(
                    "speedrun_etl.batch.selection", return_value=games):
                main()
                self.assertEqual("coverage_paused", read(Path(temp) / "batches" / batch_id / "status.json")["phase"])
                argv.append("--continue-on-drift")
                main()
                main()
                report = batch_progress(batch_id)
                self.assertEqual("complete_with_gaps", report["driver"]["phase"])
                self.assertEqual((2, 1, 1), (report["games_processed"], report["games_complete"], report["games_partial"]))
            self.assertEqual(1, started.count(batch_id + "-g-g1"))
            self.assertEqual(1, started.count(batch_id + "-g-g2"))
        with pg_connect(True) as conn:
            rows = conn.execute("SELECT game_id,state,details FROM entity_batch_queue WHERE batch_id=%s", [batch_id]).fetchall()
        states = {r[0]: (r[1], r[2]) for r in rows}
        self.assertEqual("partial", states["g1"][0])
        self.assertEqual("pending_review", states["g1"][1]["coverage_retry"]["state"])
        self.assertEqual("c", states["g1"][1]["coverage_retry"]["partitions"][0]["category_id"])
        self.assertEqual("complete", states["g2"][0])

    @unittest.skipUnless(os.environ.get("ETL_INTEGRATION") == "1", "Real control DB opt-in")
    def test_continue_on_drift_does_not_ignore_failed_dag_or_store(self):
        from speedrun_etl.batch import main
        from speedrun_etl.storage import pg_connect
        batch_id = "teststorefail" + uuid4().hex[:10]
        games = [{"id": "g1", "url": "one"}, {"id": "g2", "url": "two"}]
        for game in games:
            game["name"] = game["url"]
        started = []
        def fail_game(collection, kinds):
            if collection.endswith("-g-g1"):
                raise RuntimeError("Store acknowledgement missing")
        with TemporaryDirectory() as temp, patch.dict(os.environ, {"ETL_STATE": temp}), patch.object(
                sys, "argv", ["batch", "run", "--collection", batch_id, "--top", "2", "--continue-on-drift"]), patch(
                "speedrun_etl.batch.start", side_effect=lambda collection, conf: started.append(collection)), patch(
                "speedrun_etl.batch.wait", side_effect=fail_game), patch("speedrun_etl.batch.selection", return_value=games):
            with self.assertRaisesRegex(RuntimeError, "acknowledgement"):
                main()
        self.assertNotIn(batch_id + "-g-g2", started)
        with pg_connect(True) as conn:
            states = dict(conn.execute("SELECT game_id,state FROM entity_batch_queue WHERE batch_id=%s", [batch_id]).fetchall())
        self.assertEqual({"g1": "failed", "g2": "pending"}, states)

    @unittest.skipUnless(os.environ.get("ETL_INTEGRATION") == "1", "Real control DB opt-in")
    def test_queue_persists_and_resume_skips_completed_games(self):
        from speedrun_etl.batch import main
        from speedrun_etl.storage import pg_connect
        batch_id="testbatch"+uuid4().hex[:10]
        games=[{"id":"g1","url":"one","name":"One"},{"id":"g2","url":"two","name":"Two"}]
        started=[]
        with TemporaryDirectory() as temp:
            def fake_start(collection,conf):
                started.append(collection)
                package=EntityPackage(Path(temp)/"snapshots"/(collection+"-runs")/"entity-package")
                package.put("manifests",{"key":"m","source_result":{"coverage":"partial",
                    "partitions":[{"coverage":"verified_public_partitions"}], "wr":[{"coverage":"partial"}]}})
                package.db.commit();package.db.close()
            argv=["batch","run","--collection",batch_id,"--top","2"]
            with patch.dict(os.environ,{"ETL_STATE":temp}),patch.object(sys,"argv",argv),patch(
                "speedrun_etl.batch.start",side_effect=fake_start),patch("speedrun_etl.batch.wait"),patch(
                "speedrun_etl.batch.selection",return_value=games):
                main()
                first=sum("-g-" in x for x in started)
                main()
            self.assertEqual(2,first)
            self.assertEqual(2,sum("-g-" in x for x in started))
        with pg_connect(True) as conn:
            rows=conn.execute("SELECT state FROM entity_batch_queue WHERE batch_id=%s",[batch_id]).fetchall()
        self.assertEqual(["complete","complete"],sorted(r[0] for r in rows))

    def test_missing_partitions_and_unavailable_are_not_complete(self):
        self.assertEqual("partial", result_state({"partitions": []}))
        self.assertEqual("unavailable", result_state({"coverage": "unavailable", "partitions": []}))
        self.assertEqual("partial", result_state({"partitions": [{"coverage": "partial"}]}))
        self.assertEqual("complete", result_state({"coverage": "partial",
            "partitions": [{"coverage": "verified_public_partitions"}], "wr": [{"coverage": "partial"}]}))

    def test_only_explicit_empty_full_game_scope_is_not_applicable(self):
        result = {'coverage':'unavailable','diagnostic':'no_full_game_categories_in_source_metadata',
                  'partitions':[],'wr':[]}
        self.assertEqual('not_applicable',result_state(result))
        for extra in ({'stop_reason':'budget'},{'partitions':[{'coverage':'partial'}]},
                      {'diagnostic':'HTTP 404'},{'wr':[{'coverage':'unavailable'}]}):
            self.assertEqual('unavailable',result_state({**result,**extra}))

    @unittest.skipUnless(os.environ.get('ETL_INTEGRATION')=='1','Real control DB opt-in')
    def test_level_only_game_does_not_block_next_game_or_repeat_on_resume(self):
        from speedrun_etl.batch import main
        from speedrun_etl.storage import pg_connect
        from speedrun_etl.jsonutil import read
        batch_id = 'testscope'+uuid4().hex[:10]
        games = [{'id':'g1','url':'levels','name':'Levels'}, {'id':'g2','url':'full','name':'Full'}]
        started = []
        with TemporaryDirectory() as temp:
            def fake_start(collection,conf):
                started.append(collection)
                result = {'partitions':[{'coverage':'verified_public_partitions'}]}
                if conf.get('game_id') == 'g1':
                    result = {'coverage':'unavailable','diagnostic':'no_full_game_categories_in_source_metadata',
                              'partitions':[],'wr':[]}
                p = EntityPackage(Path(temp)/'snapshots'/(collection+'-runs')/'entity-package')
                p.put('manifests',{'key':'m','source_result':result});p.db.commit();p.db.close()
            with patch.dict(os.environ,{'ETL_STATE':temp}),patch.object(sys,'argv',[
                    'batch','run','--collection',batch_id,'--top','2']),patch(
                    'speedrun_etl.batch.start',side_effect=fake_start),patch(
                    'speedrun_etl.batch.wait'),patch('speedrun_etl.batch.selection',return_value=games):
                main();main()
            self.assertEqual(2,sum('-g-' in c for c in started))
            self.assertEqual('complete',read(Path(temp)/'batches'/batch_id/'status.json')['phase'])
        with pg_connect(True) as conn:
            states = dict(conn.execute('SELECT game_id,state FROM entity_batch_queue WHERE batch_id=%s',[batch_id]).fetchall())
        self.assertEqual({'g1':'not_applicable','g2':'complete'},states)

    def test_low_disk_pauses_before_any_airflow_request(self):
        from types import SimpleNamespace
        with patch.dict(os.environ, {"ETL_STATE": "/tmp"}), patch(
            "speedrun_etl.batch.shutil.disk_usage", return_value=SimpleNamespace(total=100 * 1024**3, free=2 * 1024**3)), patch(
            "speedrun_etl.batch.token") as auth:
            with self.assertRaisesRegex(RuntimeError, "space"):
                start("example", {"scope": "catalog", "request_budget": 650})
            auth.assert_not_called()

    @unittest.skipUnless(os.environ.get("ETL_INTEGRATION") == "1", "Real control DB opt-in")
    def test_catalog_failure_is_visible_and_driver_lock_released(self):
        from speedrun_etl.batch import main
        from speedrun_etl.progress import batch_progress
        with TemporaryDirectory() as temp:
            batch_id = "testfailed" + uuid4().hex[:10]
            with patch.dict(os.environ, {"ETL_STATE": temp}), patch.object(sys, "argv", ["batch", "run", "--collection", batch_id]), patch(
                "speedrun_etl.batch.start", side_effect=RuntimeError("Test source outage")):
                with self.assertRaisesRegex(RuntimeError, "Test source outage"):
                    main()
                report = batch_progress(batch_id)
            self.assertEqual("failed", report["driver"]["phase"])
            self.assertIn("Test source outage", report["driver"]["error"])
            self.assertFalse(report["driver_running"])

    def test_general_game_scope_has_no_old_machine_deadline(self):
        conf=validate_conf({"collection_id":"team-g-123","scope":"game","game":"example",
            "game_id":"game123","selection_snapshot_id":"team-ranking-games_metadata"})
        self.assertNotIn("deadline",conf)
        self.assertEqual(5000,conf["request_budget"])
        with self.assertRaises(ValueError):
            validate_conf({**conf,"game":"../bad"})

    def test_source_url_punctuation_does_not_relax_ids_or_allow_paths(self):
        conf = {'collection_id':'b-g', 'scope':'game', 'game':'ubermosh_vol.3',
                'game_id':'2684jydp', 'selection_snapshot_id':'b-catalog-games_metadata'}
        for slug in ('ubermosh_vol.3', '.hackmutation', 'e.y.e', 'ttt.', 'a|b'):
            with self.subTest(slug=slug):
                self.assertEqual(slug, validate_conf({**conf, 'game':slug})['game'])
                self.assertEqual(slug, validate_conf({'collection_id':'one', 'scope':'single_game', 'game':slug})['game'])
        for slug in ('', '.', '..', '../bad', 'a/b', 'a\\b', 'https://example.com', 'x?y', 'x#y', 'x\ny', None, 123):
            with self.subTest(unsafe_slug=slug), self.assertRaises(ValueError):
                validate_conf({**conf, 'game':slug})
        for key in ('game_id', 'selection_snapshot_id'):
            with self.subTest(id_field=key), self.assertRaises(ValueError):
                validate_conf({**conf, key:'id.with.punctuation'})
        with self.assertRaises(ValueError):
            validate_conf({'collection_id':'one', 'scope':'single_game', 'game_id':'id|bad'})

    @unittest.skipUnless(os.environ.get("ETL_INTEGRATION") == "1", "Real control DB opt-in")
    def test_reviewed_selection_repair_preserves_started_chain_and_retires_pending(self):
        from speedrun_etl.batch import reconcile_selection
        from speedrun_etl.storage import pg_connect
        from speedrun_etl.jsonutil import atomic_write
        batch_id = "testrepair" + uuid4().hex[:10]
        one = {"id": "g1", "url": "one", "name": "One"}
        extension = {"id": "g2", "url": "ext", "name": "Extension"}
        replacement = {"id": "g3", "url": "three", "name": "Three"}
        with TemporaryDirectory() as temp:
            with pg_connect(True) as conn:
                conn.execute("""CREATE TABLE IF NOT EXISTS entity_batch_queue (
                    batch_id text NOT NULL,position integer NOT NULL,game_id text NOT NULL,collection_id text NOT NULL,
                    state text NOT NULL DEFAULT 'pending',details jsonb NOT NULL,updated_at timestamptz NOT NULL DEFAULT now(),
                    PRIMARY KEY(batch_id,game_id))""")
            reconcile_selection(batch_id, [one, extension], "old-ranking", temp)
            with pg_connect(True) as conn:
                conn.execute("UPDATE entity_batch_queue SET state='running' WHERE batch_id=%s AND game_id='g1'", [batch_id])
            atomic_write(Path(temp) / "snapshots" / (batch_id + "-g-g1-games_metadata") / "config.json", {
                "game_id": "g1", "game": "one", "selection_snapshot_id": "old-ranking"})
            with self.assertRaisesRegex(ValueError, "refresh-selection"):
                reconcile_selection(batch_id, [one, replacement], "new-ranking", temp)
            with self.assertRaisesRegex(ValueError, "started"):
                reconcile_selection(batch_id, [replacement], "new-ranking", temp, refresh=True)
            reconcile_selection(batch_id, [one, replacement], "new-ranking", temp, refresh=True)
            with pg_connect(True) as conn:
                rows = conn.execute("SELECT game_id,state,details FROM entity_batch_queue WHERE batch_id=%s", [batch_id]).fetchall()
            states = {row[0]: (row[1], row[2]) for row in rows}
            self.assertEqual("running", states["g1"][0])
            self.assertEqual("old-ranking", states["g1"][1]["selection_snapshot_id"])
            self.assertEqual("excluded", states["g2"][0])
            self.assertEqual("new-ranking", states["g3"][1]["selection_snapshot_id"])

    def test_game_must_match_selected_id_and_url(self):
        with TemporaryDirectory() as temp:
            package=EntityPackage(Path(temp)/"parent")
            package.put("manifests",{"key":"manifest","source_result":{"selected":[{"id":"g","url":"game"}]}})
            package.db.commit()
            def parent(*args):return EntityPackage(package.root)
            with patch("speedrun_etl.batch.parent_package",side_effect=parent):
                validate_selection(temp,{"scope":"game","game_id":"g","game":"game","selection_snapshot_id":"parent"})
                with self.assertRaises(ValueError):
                    validate_selection(temp,{"scope":"game","game_id":"g","game":"wrong","selection_snapshot_id":"parent"})
            package.db.close()

    def test_existing_dag_run_is_reused_only_for_same_scope(self):
        conf={"collection_id":"team","scope":"catalog","request_budget":650}
        def call(method,path,body=None,token=None):
            if method=="POST":raise HTTPError(path,409,"exists",{},None)
            return {"conf":conf}
        with patch("speedrun_etl.batch.token",return_value="fake-test-token"),patch("speedrun_etl.batch.api",side_effect=call):
            start("team",{"scope":"catalog","request_budget":650})
            with self.assertRaises(ValueError):start("team",{"scope":"catalog","request_budget":700})
