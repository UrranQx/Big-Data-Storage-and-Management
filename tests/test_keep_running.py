from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock, patch

from speedrun_etl.jsonutil import atomic_write, read
from speedrun_etl.keep_running import monitor, session_command


class KeepRunningTests(unittest.TestCase):
    def test_saved_scope_strategy_and_manual_pause_semantics_are_preserved(self):
        for mode in ('all', 'top', 'sample'):
            conf = {'mode':mode, 'top':10, 'sample_size':4, 'game_request_budget':5000,
                    'catalog_snapshot_id':'shared-games_metadata', 'run_partition_strategy':'date_large_v1'}
            command = session_command('b', conf, {'continue_on_drift':True}, 3)
            self.assertIn('run', command)
            self.assertNotIn('resume', command)
            self.assertIn('--'+mode, command)
            self.assertIn('--accelerate-dates', command)
            self.assertIn('--continue-on-drift', command)
            self.assertIn('shared-games_metadata', command)
        command = session_command('b', {**conf, 'mode':'all', 'game_order':'run-count-desc','game_limit':10000}, {}, 8)
        self.assertEqual('run-count-desc', command[command.index('--order')+1])
        self.assertEqual('10000', command[command.index('--limit-games')+1])
        standard=session_command('b',{**conf,'run_partition_strategy':'standard'}, {}, 1)
        self.assertNotIn('--accelerate-dates',standard)
        self.assertIn('standard',standard)

    def test_normal_limit_starts_one_bounded_session_and_deadline_requests_pause(self):
        with TemporaryDirectory() as temp:
            root = Path(temp)/'batches/b'
            atomic_write(root/'config.json', {'mode':'all', 'game_request_budget':5000})
            atomic_write(root/'runtime-policy.json', {'continue_on_drift':True})
            atomic_write(root/'status.json', {'phase':'session_paused'})
            child = Mock();child.poll.return_value = None
            with patch('speedrun_etl.keep_running.time.time', side_effect=[100, 300]), patch(
                    'speedrun_etl.keep_running.time.sleep'), patch('speedrun_etl.keep_running.driver_running', return_value=False), patch(
                    'speedrun_etl.keep_running.check_capacity') as capacity, patch(
                    'speedrun_etl.keep_running.subprocess.Popen', return_value=child) as start:
                monitor(Path(temp), root, 'b', 300)
            command = start.call_args.args[0]
            self.assertAlmostEqual(200/3600, float(command[command.index('--hours')+1]))
            self.assertEqual('9000', command[command.index('--session-request-budget')+1])
            start.assert_called_once();capacity.assert_called_once_with(Path(temp), require_host=True)
            self.assertEqual('deadline_pause_requested', read(root/'keep-running.json')['phase'])
            self.assertTrue(read(root/'control.json')['pause_requested'])

    def test_failed_or_manually_paused_batch_never_restarts(self):
        for phase, manual in (('failed',False), ('coverage_paused',False), ('complete',False), ('session_paused',True)):
            with self.subTest(phase=phase, manual=manual), TemporaryDirectory() as temp:
                root = Path(temp)
                atomic_write(root/'status.json', {'phase':phase})
                if manual:atomic_write(root/'control.json', {'pause_requested':True})
                with patch('speedrun_etl.keep_running.time.time', return_value=100), patch(
                        'speedrun_etl.keep_running.driver_running', return_value=False), patch(
                        'speedrun_etl.keep_running.subprocess.Popen') as start:
                    monitor(root, root, 'b', 300)
                start.assert_not_called()
                self.assertEqual('manual_pause' if manual else 'stopped', read(root/'keep-running.json')['phase'])

    def test_host_disk_failure_never_restarts_a_session(self):
        with TemporaryDirectory() as temp:
            root = Path(temp)
            atomic_write(root/'config.json', {'mode':'all', 'game_request_budget':5000})
            atomic_write(root/'runtime-policy.json', {})
            atomic_write(root/'status.json', {'phase':'session_paused'})
            with patch('speedrun_etl.keep_running.time.time', return_value=100), patch(
                    'speedrun_etl.keep_running.driver_running', return_value=False), patch(
                    'speedrun_etl.keep_running.check_capacity', side_effect=RuntimeError('Host disk low')), patch(
                    'speedrun_etl.keep_running.subprocess.Popen') as start:
                monitor(root, root, 'b', 300)
            start.assert_not_called()
            self.assertIn('Host disk low', read(root/'keep-running.json')['reason'])

    def test_active_driver_is_left_running_and_deadline_only_sets_soft_pause(self):
        with TemporaryDirectory() as temp:
            root = Path(temp)
            atomic_write(root/'status.json', {'phase':'game'})
            with patch('speedrun_etl.keep_running.time.time', side_effect=[100, 300]), patch(
                    'speedrun_etl.keep_running.time.sleep'), patch('speedrun_etl.keep_running.driver_running', return_value=True), patch(
                    'speedrun_etl.keep_running.subprocess.Popen') as start:
                monitor(root, root, 'b', 300)
            start.assert_not_called()
            self.assertTrue(read(root/'control.json')['pause_requested'])
