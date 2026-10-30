"""Continue one existing batch across normal session limits until an absolute deadline."""
import argparse
from contextlib import ExitStack, redirect_stderr, redirect_stdout
from datetime import datetime, timezone
import os
from pathlib import Path
import re
import subprocess
import sys
import time

from .batch import set_pause
from .capacity import check_capacity
from .jsonutil import atomic_write, file_lock, read, utcnow
from .progress import driver_running


def session_command(batch_id, conf, policy, hours):
    # run deliberately respects a pause requested between our check and its start.
    # resume would cancel that request, which an automatic controller must not do.
    command = [sys.executable, '-u', '-m', 'speedrun_etl.batch', 'run', '--collection', batch_id,
               '--game-request-budget', str(conf['game_request_budget']), '--hours', str(hours),
               '--session-request-budget', '9000', '--log']
    if conf['mode'] == 'all':command += ['--all']
    elif conf['mode'] == 'sample':command += ['--sample', str(conf['sample_size'])]
    elif conf['mode'] == 'top':command += ['--top', str(conf['top'])]
    else:raise ValueError('Unknown saved batch mode')
    if conf.get('catalog_snapshot_id'):command += ['--catalog-snapshot', conf['catalog_snapshot_id']]
    if 'data_model' in conf:command += ['--data-model',conf['data_model']]
    if 'game_order' in conf:command += ['--order', conf['game_order']]
    if 'game_limit' in conf:command += ['--limit-games', str(conf['game_limit'])]
    if 'run_partition_strategy' in conf:
        if conf['run_partition_strategy']=='date_large_v1':command += ['--accelerate-dates']
        elif conf['run_partition_strategy']=='standard':command += ['--runs-strategy','standard']
        else:raise ValueError('Unknown saved strategy')
    if policy.get('continue_on_drift'):command += ['--continue-on-drift']
    return command


def monitor(state, root, batch_id, end_unix):
    path = root/'keep-running.json'
    record = {'batch_id':batch_id, 'until':datetime.fromtimestamp(end_unix, timezone.utc).isoformat(),
              'started_sessions':0, 'session_http_budget':9000, 'session_hours_max':8}
    child = None
    def save(phase, **details):
        record.update(phase=phase, updated_at=utcnow(), **details)
        atomic_write(path, record)
    while True:
        remaining = end_unix-time.time()
        if remaining <= 0:
            set_pause(root, True)
            save('deadline_pause_requested')
            return
        control = read(root/'control.json') if (root/'control.json').exists() else {}
        if control.get('pause_requested'):
            save('manual_pause')
            return
        if child is not None and child.poll() is not None:
            code = child.returncode
            child = None
            if code:
                save('stopped', reason=f'Batch driver exited with code {code}')
                return
        status = read(root/'status.json') if (root/'status.json').exists() else {}
        if driver_running(root/'driver.lock') or child is not None:
            save('watching', driver_phase=status.get('phase'))
        elif status.get('phase') == 'session_paused':
            conf = read(root/'config.json')
            policy = read(root/'runtime-policy.json')
            try:
                check_capacity(state, require_host=conf['mode']=='all')
            except RuntimeError as error:
                save('stopped', reason=str(error))
                return
            command = session_command(batch_id, conf, policy, min(8, remaining/3600))
            child = subprocess.Popen(command, start_new_session=True)
            record['started_sessions'] += 1
            save('starting_session', driver_phase='session_paused')
        else:
            save('stopped', driver_phase=status.get('phase'),
                 reason='Only normal session_paused may continue automatically')
            return
        time.sleep(15)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--collection', required=True)
    parser.add_argument('--until', required=True, help='Absolute ISO timestamp with timezone; never extended on restart')
    parser.add_argument('--log', action='store_true')
    args = parser.parse_args()
    if not re.fullmatch(r'[A-Za-z0-9_-]{1,40}', args.collection):parser.error('Invalid batch ID')
    try:
        end = datetime.fromisoformat(args.until)
        if end.tzinfo is None:raise ValueError('Timezone required')
    except ValueError:parser.error('Use an ISO deadline with timezone')
    state = Path(os.environ['ETL_STATE']); root = state/'batches'/args.collection
    if not (root/'config.json').exists():parser.error('Existing batch required')
    with ExitStack() as stack:
        try:stack.enter_context(file_lock(root/'keep-running.lock', blocking=False))
        except BlockingIOError:parser.error('Continuation controller already running')
        if args.log:
            log = stack.enter_context((root/'keep-running.log').open('a', buffering=1))
            stack.enter_context(redirect_stdout(log));stack.enter_context(redirect_stderr(log))
        monitor(state, root, args.collection, end.timestamp())


if __name__ == '__main__':
    main()
