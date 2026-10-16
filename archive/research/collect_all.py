"""Collect every exposed full-game run using the website's public v2 API."""
import argparse
from collections import Counter
from contextlib import closing
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from decimal import Decimal
import json
import hashlib
from pathlib import Path
import sqlite3
import csv

from collect import Client, export, save

STATUSES = {0: 'new', 1: 'verified', 2: 'rejected'}


def is_orphaned(run):
    return bool(run.get('orphaned')) or not run.get('playerIds')


def statistics_eligible(run):
    return not is_orphaned(run) and run.get('_source_api', 'v2') == 'v2'


def seconds(value):
    if value is None:
        return None
    if not isinstance(value, str) or not value.endswith('s'):
        raise ValueError(f'Unexpected duration format: {value!r}')
    return float(Decimal(value[:-1]))


def primary_time(run, game):
    fields = {0: 'time', 1: 'timeWithLoads', 2: 'igt'}
    default = game['defaultTimer']
    # Some valid runs have only a non-default timer (e.g. Portal with loads).
    # Preserve that source, never invent a missing loadless/ingame duration.
    timers = [default] + [timer for timer in game.get('validTimers', []) if timer != default]
    for timer in timers:
        field = fields[timer]
        if run.get(field) is not None:
            return seconds(run[field]), field
    return None, None


def fetch_partition(client, gid, cid, status, players, evidence, workers=1):
    params = {'gameId': gid, 'categoryId': cid, 'obsolete': 1,
              'video': 0, 'emulator': 1, 'verified': status, 'values': []}
    result = {}
    expected = None
    first = client.v2('GetGameLeaderboard2', {'params': params, 'page': 1})
    pages = int(first['pagination']['pages'])
    def get_page(page):
        return client.v2('GetGameLeaderboard2', {'params': params, 'page': page})
    def process(page, response):
        nonlocal expected
        pagination = response['pagination']
        count = int(pagination['count'])
        if expected is None:
            expected = count
        if count != expected or int(pagination['pages']) != pages:
            raise RuntimeError('Source count changed during collection; use a new output snapshot')
        if int(pagination['page']) != page:
            raise RuntimeError('API returned a different page')
        batch = response.get('runList', [])
        if count > 0 and not batch:
            raise RuntimeError('Unexpected empty page')
        for run in batch:
            if run['gameId'] != gid or run['categoryId'] != cid or run.get('levelId') or run['verified'] != status:
                raise RuntimeError('API ignored a game/category/level/status filter')
            if run['id'] in result:
                raise RuntimeError('Duplicate run across pages; source may have changed')
            result[run['id']] = run
        players.update((player['id'], player) for player in response.get('playerList', []))
        if page == 1 or page % 25 == 0 or page == pages:
            print(f'    {STATUSES[status]}: page {page}/{pages}, {len(result)}/{expected}', flush=True)
    process(1, first)
    with ThreadPoolExecutor(max_workers=workers) as executor:
        for page, response in enumerate(executor.map(get_page, range(2, pages+1)), start=2):
            process(page, response)
    if len(result) != expected:
        raise RuntimeError(f'Incomplete partition: {len(result)} != {expected}')
    evidence.append({'category_id': cid, 'status': STATUSES[status], 'api_count': expected,
                     'unique_runs': len(result), 'pages': pages, 'max_offset_equivalent': (pages-1)*int(first['pagination']['per'])})
    return result


def supplement_v1(raw, players):
    """Retain historical v1 records omitted by v2 leaderboard joins."""
    participants = []
    for player in raw.get('players', []):
        pid = player.get('id') or 'guest:' + hashlib.sha256(player.get('name', '').encode()).hexdigest()
        participants.append(pid)
        players.setdefault(pid, {'id': pid, 'name': player.get('name'), 'kind': player['rel']})
    timing = raw['times']
    run = {'id': raw['id'], 'gameId': raw['game'], 'categoryId': raw['category'],
           'levelId': raw.get('level'), 'verified': next(k for k, v in STATUSES.items() if v == raw['status']['status']),
           'platformId': raw['system'].get('platform'), 'regionId': raw['system'].get('region'),
           'emulator': raw['system'].get('emulated', False), 'playerIds': participants,
           'valueIds': list(raw.get('values', {}).values()), 'performedAt': raw.get('date-time') or raw.get('date'),
           'submittedAt': raw.get('submitted'), 'verifiedAt': raw['status'].get('verify-date'),
           'verifiedById': raw['status'].get('examiner'), 'reason': raw['status'].get('reason'),
           'comment': raw.get('comment'), '_source_api': 'v1', '_v1_raw': raw,
           '_primary_seconds': timing['primary_t']}
    for v1_name, v2_name in (('realtime', 'time'), ('realtime_noloads', 'timeWithLoads'), ('ingame', 'igt')):
        if timing.get(v1_name) is not None:
            run[v2_name] = str(timing[v1_name+'_t'])+'s'
    return run


def audit_v1(client, gid, cid, status, collected, players, workers=1):
    """Compare the independent v1 representation, fully where offset permits."""
    params = {'game': gid, 'category': cid, 'status': STATUSES[status], 'orderby': 'submitted'}
    sample = client.get('runs', max=200, offset=9800, direction='asc', **params)
    capped = any(link['rel'] == 'next' for link in sample.get('pagination', {}).get('links', []))
    actual = {rid for rid, run in collected.items() if run['categoryId'] == cid and run['verified'] == status}
    if not capped:
        source = {run['id']: run for run in client.pages('runs', direction='asc', **params) if run.get('level') is None}
        method = 'full_id_set'
        examiner_count = 0
        unavailable_examiners = []
    else:
        samples = []
        for direction in ('asc', 'desc'):
            for offset in (0, 9800):
                samples.extend(client.get('runs', max=200, offset=offset, direction=direction, **params)['data'])
        source = {run['id']: run for run in samples if run.get('level') is None}
        # Read both accessible halves, including orphan records with deleted examiners.
        def get_half(direction):
            half = []
            for offset in range(0, 10000, 200):
                response = client.get('runs', max=200, offset=offset, direction=direction, **params)
                half.extend(response['data'])
                if offset % 2000 == 0:
                    print(f'    v1 {direction} accessible half: {len(half)} rows', flush=True)
                if not any(link['rel']=='next' for link in response.get('pagination', {}).get('links', [])):
                    break
            return half
        with ThreadPoolExecutor(max_workers=min(workers, 2)) as executor:
            for half in executor.map(get_half, ('asc', 'desc')):
                source.update((run['id'], run) for run in half if run.get('level') is None)
        examiners = {collected[rid].get('verifiedById') for rid in actual}
        examiners.update(run['status'].get('examiner') for run in samples)
        examiners.discard(None)
        def get_examiner(examiner):
            try:
                return examiner, list(client.pages('runs', examiner=examiner, direction='asc', **params)), False
            except RuntimeError as error:
                if 'HTTP 404' not in str(error):
                    raise
                return examiner, [], True
        unavailable_examiners = []
        with ThreadPoolExecutor(max_workers=workers) as executor:
            for index, (examiner, batch, unavailable) in enumerate(executor.map(get_examiner, sorted(examiners)), start=1):
                if unavailable:
                    unavailable_examiners.append(examiner)
                source.update((run['id'], run) for run in batch if run.get('level') is None)
                if index % 10 == 0 or index == len(examiners):
                    print(f'    v1 examiners {index}/{len(examiners)}, unique runs {len(source)}', flush=True)
        method = 'discovered_examiner_partitions_and_both_accessible_10000_halves'
        examiner_count = len(examiners)
    if not capped and actual-source.keys():
        raise RuntimeError(f'v1 audit misses {len(actual-source.keys())} v2 IDs')
    added = 0
    for rid, raw in source.items():
        if rid not in collected:
            collected[rid] = supplement_v1(raw, players)
            added += 1
    return {'category_id': cid, 'status': STATUSES[status], 'method': method, 'compared_ids': len(source),
            'v1_supplemental_runs': added, 'examiner_partitions': examiner_count,
            'unavailable_examiners': unavailable_examiners, 'v2_ids_not_independently_seen_in_v1': len(actual-source.keys()),
            'note': 'Examiner discovery cannot prove absence of an unseen examiner whose runs appear only in the inaccessible v1 middle' if capped else 'All v1 IDs compared'}


def build_tables(metadata, categories, collected, players):
    game = metadata['game']
    gid = game['id']
    counts = Counter(run['categoryId'] for run in collected.values())
    verified = Counter(run['categoryId'] for run in collected.values() if run['verified'] == 1)
    eligible = Counter(run['categoryId'] for run in collected.values() if statistics_eligible(run))
    eligible_verified = Counter(run['categoryId'] for run in collected.values() if statistics_eligible(run) and run['verified'] == 1)
    category_rows = [(cat['id'], gid, cat['name'], 'per-game', int(cat.get('isMisc', False)),
                      verified[cat['id']], counts[cat['id']], 1, 'all_full_game', int(cat.get('archived', False)),
                      eligible[cat['id']], eligible_verified[cat['id']]) for cat in categories]
    rows, participants = [], []
    value_to_var = {value['id']: value['variableId'] for value in metadata.get('values', [])}
    for run in collected.values():
        # v2 time is loadless if enabled by game; timeWithLoads is real time.
        realtime = seconds(run.get('timeWithLoads') if game.get('loadTimes') else run.get('time'))
        loadless = seconds(run.get('time')) if game.get('loadTimes') else None
        igt = seconds(run.get('igt'))
        primary, primary_source = primary_time(run, game)
        if '_v1_raw' in run:
            timing = run['_v1_raw']['times']
            primary = run['_primary_seconds']
            primary_source = 'v1_primary'
            realtime = timing.get('realtime_t') if timing.get('realtime') else None
            loadless = timing.get('realtime_noloads_t') if timing.get('realtime_noloads') else None
        values = {value_to_var[value]: value for value in run.get('valueIds', []) if value in value_to_var}
        video_links = [run['video']] if run.get('video') else []
        if '_v1_raw' in run:
            video_links = [item['uri'] for item in (run['_v1_raw'].get('videos') or {}).get('links', [])]
        obsolete = int(run['obsolete']) if 'obsolete' in run else None
        rows.append((run['id'], gid, run['categoryId'], run.get('performedAt'), run.get('submittedAt'),
                     primary, realtime, loadless, igt, run.get('platformId'), int(run.get('emulator', False)),
                     run.get('regionId'), f'https://www.speedrun.com/run/{run["id"]}', json.dumps(values),
                     json.dumps(video_links), STATUSES[run['verified']],
                     obsolete, run.get('verifiedById'), run.get('verifiedAt'),
                     run.get('reason'), run.get('comment'), run.get('levelId'), json.dumps(run, ensure_ascii=False),
                     run.get('submittedById'), int(is_orphaned(run)),
                     json.dumps(run.get('issues', ['no players'] if not run.get('playerIds') else []), ensure_ascii=False),
                     int(statistics_eligible(run)), primary_source, run.get('_source_api', 'v2')))
        for position, pid in enumerate(run.get('playerIds', [])):
            participants.append((run['id'], position, pid))
    return {
        'games': ('id TEXT PRIMARY KEY,name TEXT,abbreviation TEXT,released INTEGER,weblink TEXT',
                  [(gid, game['name'], game['url'], game.get('releaseDate', {}).get('year'), f'https://www.speedrun.com/{game["url"]}')]),
        'categories': ('id TEXT PRIMARY KEY,game_id TEXT,name TEXT,type TEXT,miscellaneous INTEGER,verified_full_game_runs INTEGER,total_full_game_runs INTEGER,selected INTEGER,selection_reason TEXT,archived INTEGER,statistics_runs INTEGER,statistics_verified_runs INTEGER', category_rows),
        'runs': ('id TEXT PRIMARY KEY,game_id TEXT,category_id TEXT,date TEXT,submitted TEXT,primary_seconds REAL,realtime_seconds REAL,realtime_noloads_seconds REAL,ingame_seconds REAL,platform_id TEXT,emulated INTEGER,region_id TEXT,weblink TEXT,values_json TEXT,videos_json TEXT,status TEXT,obsolete INTEGER,examiner_id TEXT,verified_at TEXT,rejection_reason TEXT,comment TEXT,level_id TEXT,raw_json TEXT,submitted_by_id TEXT,orphaned INTEGER,issues_json TEXT,statistics_eligible INTEGER,primary_time_source TEXT,source_api TEXT', rows),
        'run_players': ('run_id TEXT,position INTEGER,player_id TEXT', participants),
        'players': ('id TEXT PRIMARY KEY,name TEXT,url TEXT,raw_json TEXT,area_id TEXT,country_code TEXT',
                    [(p['id'], p.get('name'), p.get('url'), json.dumps(p, ensure_ascii=False), p.get('areaId'),
                      p['areaId'].split('/')[0] if isinstance(p.get('areaId'), str) and len(p['areaId'].split('/')[0]) == 2 else None)
                     for p in players.values()]),
        'variables': ('id TEXT PRIMARY KEY,name TEXT,raw_json TEXT', [(v['id'], v['name'], json.dumps(v, ensure_ascii=False)) for v in metadata.get('variables', [])]),
        'variable_values': ('id TEXT PRIMARY KEY,variable_id TEXT,name TEXT,raw_json TEXT', [(v['id'], v['variableId'], v['name'], json.dumps(v, ensure_ascii=False)) for v in metadata.get('values', [])]),
        'platforms': ('id TEXT PRIMARY KEY,name TEXT,raw_json TEXT', [(p['id'], p.get('name'), json.dumps(p, ensure_ascii=False)) for p in metadata.get('platforms', [])]),
    }


def export_statistics(root):
    processed = root / 'processed'
    with closing(sqlite3.connect(processed / 'speedruns.sqlite')) as db, db:
        for name, condition in [('runs_for_statistics', 'statistics_eligible=1'),
                                ('verified_runs_for_statistics', "statistics_eligible=1 AND status='verified'")]:
            db.execute(f'DROP VIEW IF EXISTS {name}')
            db.execute(f'CREATE VIEW {name} AS SELECT * FROM runs WHERE {condition}')
            cursor = db.execute(f'SELECT * FROM {name} ORDER BY category_id,id')
            with (processed / f'{name}.csv').open('w', encoding='utf-8-sig', newline='') as handle:
                writer = csv.writer(handle)
                writer.writerow([column[0] for column in cursor.description])
                writer.writerows(cursor)


def snapshot_metadata(root):
    times = []
    for folder in (root / 'raw', root / 'stats_after' / 'raw'):
        for path in folder.glob('*.json'):
            cached = json.loads(path.read_text(encoding='utf-8'))
            times.append(cached['fetched_at'])
    with closing(sqlite3.connect(root / 'processed' / 'speedruns.sqlite')) as db:
        timers = dict(db.execute("SELECT primary_time_source,count(*) FROM verified_runs_for_statistics GROUP BY primary_time_source"))
    return {'snapshot_first_response_at': min(times) if times else None,
            'snapshot_last_response_at': max(times) if times else None,
            'raw_response_count': len(times), 'verified_primary_time_sources': timers,
            'timer_note': 'primary_seconds uses the default timer when available, otherwise another valid timer; group by primary_time_source for time comparisons. Missing loadless time stays null.'}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--game', default='celeste')
    parser.add_argument('--output', type=Path, default=Path('data/celeste'))
    parser.add_argument('--rpm', type=int, default=20)
    parser.add_argument('--workers', type=int, default=2)
    parser.add_argument('--audit-v1', action='store_true', help='Optional v1 audit; supplemental records excluded from v2 statistics')
    args = parser.parse_args()
    if not 1 <= args.rpm <= 60 or not 1 <= args.workers <= 8:
        parser.error('rpm must be 1..60; workers must be 1..8')
    client = Client(args.output, args.rpm)
    manifest = {'status': 'collecting', 'game': args.game, 'started_at': datetime.now(timezone.utc).isoformat(),
                'scope': 'all publicly exposed full-game categories including archived; all statuses; obsolete included; no video requirement',
                'api': 'public website API v2 (not a stable documented API)', 'rpm': args.rpm,
                'orphaned_policy': 'Retain fetched records with a flag; exclude orphaned and v1 supplements from statistics.',
                'v1_audit_enabled': args.audit_v1}
    save(args.output / 'manifest.json', manifest)
    try:
        metadata = client.v2('GetGameData', {'gameUrl': args.game})
        gid = metadata['game']['id']
        categories = [cat for cat in metadata['categories'] if not cat['isPerLevel']]
        before = client.v2('GetGameSummary', {'gameId': gid})['stats']
        save(args.output / 'site_stats_before.json', before)
        players, collected, evidence, audits = {}, {}, [], []
        for cat in categories:
            print(f'Category {cat["name"]} ({cat["id"]}), archived={cat.get("archived", False)}', flush=True)
            for status in (1, 2, 0):
                partition = fetch_partition(client, gid, cat['id'], status, players, evidence, args.workers)
                if collected.keys() & partition.keys():
                    raise RuntimeError('Same run appears in different category/status partitions')
                collected.update(partition)
        if args.audit_v1:
            print('Auditing v1/v2 IDs', flush=True)
            v1_categories = {cat['id'] for cat in client.pages(f'games/{gid}/categories') if cat['type'] == 'per-game'}
            if not v1_categories.issubset({cat['id'] for cat in categories}):
                raise RuntimeError('v2 metadata omitted a v1 full-game category')
            for cat in categories:
                if cat['id'] in v1_categories:
                    for status in (1, 2, 0):
                        audit = audit_v1(client, gid, cat['id'], status, collected, players, args.workers)
                        audits.append(audit)
                        print(f'  Audit {cat["name"]}/{STATUSES[status]}: {audit["method"]}, {audit["compared_ids"]} IDs', flush=True)
        export(args.output, build_tables(metadata, categories, collected, players))
        export_statistics(args.output)
        after_client = Client(args.output / 'stats_after', args.rpm)
        after_client.last = client.last
        after = after_client.v2('GetGameSummary', {'gameId': gid})['stats']
        save(args.output / 'site_stats_after.json', after)
        eligible_runs = [run for run in collected.values() if statistics_eligible(run)]
        manifest.update(status='complete', completed_at=datetime.now(timezone.utc).isoformat(),
                        game_id=gid, categories=len(categories), total_selected_runs=len(collected),
                        status_counts=dict(Counter(STATUSES[r['verified']] for r in collected.values())),
                        obsolete_runs=sum(bool(r.get('obsolete')) for r in collected.values()),
                        partitions=evidence, v1_audits=audits, requests_this_execution=client.requests + after_client.requests,
                        v1_supplemental_runs=sum(a['v1_supplemental_runs'] for a in audits),
                        orphaned_runs=sum(is_orphaned(run) for run in collected.values()),
                        statistics_runs=len(eligible_runs),
                        statistics_status_counts=dict(Counter(STATUSES[run['verified']] for run in eligible_runs)),
                        site_stats_before=before, site_stats_after=after,
                        coverage_status='all_v2_partitions_checked',
                        completeness='Every selected v2 partition checked against pagination count and unique IDs. V1-only records outside the main v2 statistical scope.',
                        limitations='Public API snapshot across time, not a database transaction. v2 leaderboards omit orphaned records; private/removed/inaccessible records are outside scope. Statistics exclude orphaned and v1 supplements.')
        manifest.update(snapshot_metadata(args.output))
        save(args.output / 'manifest.json', manifest)
        (args.output / 'README.md').write_text(
            f'# {metadata["game"]["name"]}: полные забеги v2\n\n'
            f'Получено **{len(collected)}** записей; для общей статистики '
            f'допущено **{len(eligible_runs)}**. Статусы: '
            f'`{manifest["status_counts"]}`.\n\n'
            'Включены все возвращённые категории полной игры, в том числе '
            'архивные, все три статуса и прежние результаты (`obsolete=1`). '
            'Level runs и отдельные Category Extensions не включены.\n\n'
            '- `processed/speedruns.sqlite`: игры, категории, раны, участники, '
            'профили, переменные и платформы.\n'
            '- `processed/runs_for_statistics.csv`: все статусы, без orphaned '
            'и записей только из v1; одноимённое представление SQLite.\n'
            '- `processed/verified_runs_for_statistics.csv`: подтверждённые '
            'раны для статистики; одноимённое представление SQLite.\n'
            '- `processed/runs.csv`: все полученные записи с признаками '
            '`orphaned`, `statistics_eligible`, `issues_json` и автором подачи.\n'
            '- `processed/players.csv`, `run_players.csv`: профили и связь '
            'участников с ранами; `submitted_by_id` не подменяет игрока.\n'
            '- `raw/`: исходные API-ответы с URL и временем получения.\n'
            '- `site_stats_before.json`, `site_stats_after.json`: снимки '
            'статистики сайта для сопоставления областей подсчёта.\n'
            '- `manifest.json`: счётчики, страницы, область полноты и ограничения.\n\n'
            'Все страницы каждой категории/статуса проверены по числу ID '
            'и `pagination.count`. Полнота относится к публичному лидерборду v2; '
            'исключённые самим v2 orphaned и закрытые/удалённые раны за пределами '
            'этого охвата. Снимок получен во времени и не является транзакцией '
            'базы сайта. Статистику сайта нельзя автоматически приравнивать к '
            'числу ранов в аналитическом представлении.\n', encoding='utf-8')
        print(json.dumps({k: v for k, v in manifest.items() if k not in ('partitions', 'v1_audits')}, indent=2), flush=True)
    except Exception as error:
        manifest.update(status='incomplete', error=str(error), requests_this_execution=client.requests)
        save(args.output / 'manifest.json', manifest)
        raise


if __name__ == '__main__':
    main()
