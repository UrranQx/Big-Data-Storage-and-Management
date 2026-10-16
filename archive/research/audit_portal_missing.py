"""Check the two Portal stats/v2 omissions using saved IDs and fresh evidence."""
import json
from datetime import datetime, timezone
from pathlib import Path
import sqlite3
from contextlib import closing
from urllib.parse import urlsplit, parse_qs

from collect import Client, save

ROOT = Path('data/portal_stats_audit_20261010')
DATASET = Path('data/portal_20261010')
GID = '4pd0n31e'


def main():
    old = {}
    for path in Path('data/raw').glob('*.json'):
        source = path.read_text(encoding='utf-8')
        if '/api/v1/runs?' not in source[:500]:
            continue
        entry = json.loads(source)
        params = parse_qs(urlsplit(entry['url']).query)
        if params.get('game') != [GID]:
            continue
        old.update((run['id'], run) for run in entry['response']['data']
                   if run['status']['status'] == 'verified' and run.get('level') is None)
    with closing(sqlite3.connect(DATASET / 'processed/speedruns.sqlite')) as db:
        current_ids = {row[0] for row in db.execute("SELECT id FROM runs WHERE status='verified'")}
        categories = [dict(id=row[0], name=row[1], v2_verified_count=row[2])
                      for row in db.execute('SELECT id,name,verified_full_game_runs FROM categories')]
        historic_differences = [dict(id=rid, current_dataset_status=db.execute('SELECT status FROM runs WHERE id=?', (rid,)).fetchone())
                               for rid in old.keys() - current_ids if old[rid].get('players')]
    suspects = [run for rid, run in old.items() if rid not in current_ids and not run.get('players')]
    client = Client(ROOT, 20)
    stats = client.v2('GetGameSummary', {'gameId': GID})['stats']
    for row in categories:
        offset = max(0, row['v2_verified_count'] - 1)
        response = client.get('runs', game=GID, category=row['id'], status='verified',
                              orderby='submitted', direction='asc', max=200, offset=offset)
        if int(response['pagination']['offset']) != offset:
            raise RuntimeError('V1 offset not honored')
        if any(link['rel'] == 'next' for link in response['pagination'].get('links', [])):
            raise RuntimeError('Probe did not reach the end; cannot infer exact count')
        if not response['data']:
            raise RuntimeError('Empty count probe; source changed')
        if any(run['game'] != GID or run['category'] != row['id'] or run.get('level') or run['status']['status'] != 'verified'
               for run in response['data']):
            raise RuntimeError('V1 ignored scope filters')
        row['v1_verified_count'] = offset + len(response['data'])
        row['v1_minus_v2'] = row['v1_verified_count'] - row['v2_verified_count']
        print(json.dumps(row), flush=True)
    confirmed = []
    for suspect in suspects:
        v1 = client.get('runs/' + suspect['id'])['data']
        v2 = client.v2('GetRun', {'runId': suspect['id']})['run']
        if v1['game'] != GID or v1.get('level') or v1['status']['status'] != 'verified' or v1.get('players'):
            raise RuntimeError('Suspect not a currently verified orphan full-game run')
        if v2['id'] != v1['id'] or v2['verified'] != 1 or not v2.get('orphaned') or 'no players' not in v2.get('issues', []):
            raise RuntimeError('V2 card did not confirm orphaned/no players')
        params = dict(gameId=GID, categoryId=v1['category'], obsolete=1, video=0,
                      emulator=1, verified=1, values=[])
        board = client.v2('GetGameLeaderboard2', {'params': params, 'page': 1})
        category = next(row for row in categories if row['id'] == v1['category'])
        if int(board['pagination']['count']) != category['v2_verified_count']:
            raise RuntimeError('V2 category count changed')
        confirmed.append(dict(id=v1['id'], category=category['name'], date=v1.get('date'),
                              time=v1['times']['primary'], weblink=v1['weblink'], v1=v1, v2=v2))
        print(json.dumps({key: value for key, value in confirmed[-1].items() if key not in ('v1', 'v2')}), flush=True)
    v1_total = sum(row['v1_verified_count'] for row in categories)
    v2_total = sum(row['v2_verified_count'] for row in categories)
    if v1_total != int(stats['totalRunsFG']) or v1_total - v2_total != len(confirmed):
        raise RuntimeError('Orphan count does not fully account for the stats gap')
    for row in categories:
        if row['v1_minus_v2'] != sum(run['v1']['category'] == row['id'] for run in confirmed):
            raise RuntimeError('Category mismatch not explained by identified orphans')
    for historic in historic_differences:
        try:
            current = client.get('runs/' + historic['id'])['data']
            historic['current_public_v1'] = dict(game=current['game'], category=current['category'],
                                                level=current.get('level'), status=current['status']['status'])
        except RuntimeError as error:
            if 'HTTP 404' not in str(error):
                raise
            historic['current_public_v1'] = {'http_status': 404}
        print(json.dumps(historic), flush=True)
    report = dict(completed_at=datetime.now(timezone.utc).isoformat(), stats=stats,
                  categories=categories, v1_verified_total=v1_total, v2_verified_total=v2_total,
                  confirmed_orphans=confirmed, historic_non_orphan_differences=historic_differences,
                  conclusion='The two missing verified Portal runs are orphaned with no players; excluded under the selected v2 policy.',
                  main_dataset_modified=False)
    save(ROOT / 'report.json', report)
    table = '\n'.join(f'| [{run["id"]}]({run["weblink"]}) | {run["category"]} | {run["date"]} | {run["time"]} |' for run in confirmed)
    (ROOT / 'REPORT.md').write_text(
        '# Portal: два пропуска подтверждены как orphaned\n\n'
        '| Ран | Категория | Дата | Время |\n|---|---|---|---|\n' + table + '\n\n'
        'Обе записи сейчас имеют `status=verified` в v1 и пустой `players=[]`. '
        'Карточки v2 подтверждают `orphaned=true`, `issues=["no players"]`. '
        'Обе отсутствуют в уже полностью выгруженных verified-ранах v2.\n\n'
        f'Свежие конечные страницы v1 дали **{v1_total}** verified full-game, '
        f'что совпадает со статистикой сайта. Полная выгрузка v2 содержит '
        f'**{v2_total}**; свежие счётчики категорий с пропусками не изменились. '
        'Разница составляет ровно один ран Inbounds и один Inbounds Legacy.\n\n'
        'Исторический кэш v1 использован только для поиска кандидатов. '
        'Каждая подозрительная карточка и количества категорий проверены свежими '
        'запросами. Исторические отличия кэша от текущих данных отдельно '
        'записаны в `historic_non_orphan_differences` отчёта; старый кэш '
        'не считается свежей выборкой.\n\n'
        'Основной датасет не изменён: orphaned остаются исключёнными по решению '
        'пользователя. Полные доказательства — `report.json`, исходные ответы '
        'с URL и временем — `raw/`.\n', encoding='utf-8')
    print(json.dumps({'v1_verified_total': v1_total, 'v2_verified_total': v2_total,
                      'orphan_ids': [run['id'] for run in confirmed], 'main_dataset_modified': False}), flush=True)


if __name__ == '__main__':
    main()
