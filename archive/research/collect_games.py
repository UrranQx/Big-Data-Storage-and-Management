"""Stage 1: snapshot the public Speedrun.com game directory, without ranking."""
import argparse
from collections import Counter
from contextlib import closing
from concurrent.futures import ThreadPoolExecutor
import csv
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3

from collect import Client, save


def collect_catalog(client, progress=print):
    params = {'orderType': 1, 'limit': 200}
    first = client.v2('GetGameList', dict(params, page=1))
    meta = first['pagination']
    expected, pages, per = (int(meta[k]) for k in ('count', 'pages', 'per'))
    if pages != max(1, (expected + per - 1) // per) or per != 200:
        raise RuntimeError(f'Unexpected pagination: {meta}')
    games, platforms, seen = [], {}, set()

    def accept(page_number, response):
        pagination = response['pagination']
        for field, value in [('count', expected), ('pages', pages), ('per', per), ('page', page_number)]:
            if int(pagination[field]) != value:
                raise RuntimeError(f'Catalog changed or page mismatch at page {page_number}: {pagination}')
        rows = response['gameList']
        if len(rows) != min(per, expected - (page_number - 1) * per):
            raise RuntimeError(f'Unexpected page length at page {page_number}')
        for game in rows:
            if not game.get('id') or game['id'] in seen:
                raise RuntimeError(f'Missing or duplicate game ID at page {page_number}: {game.get("id")}')
            seen.add(game['id'])
            games.append(game)
        platforms.update((item['id'], item) for item in response.get('platformList', []))
        if page_number == 1 or page_number % 10 == 0 or page_number == pages:
            progress(f'Pages {page_number}/{pages}; games {len(games)}/{expected}', flush=True)

    accept(1, first)
    with ThreadPoolExecutor(max_workers=2) as pool:
        responses = pool.map(lambda page: client.v2('GetGameList', dict(params, page=page)), range(2, pages + 1))
        for number, response in enumerate(responses, 2):
            accept(number, response)
    if len(games) != expected:
        raise RuntimeError('Incomplete catalog')
    return games, platforms, dict(meta)


def export_catalog(root, games, platforms, pagination, started_at, requests):
    processed = root / 'processed'
    processed.mkdir(parents=True, exist_ok=True)
    fields = ['id', 'name', 'abbreviation', 'weblink', 'type', 'site_run_count',
              'site_total_player_count', 'site_active_player_count', 'release_date',
              'added_at', 'updated_at', 'platform_ids']
    rows = []
    for game in sorted(games, key=lambda item: (item['name'].casefold(), item['id'])):
        rows.append([game['id'], game['name'], game.get('url'),
                     'https://www.speedrun.com/' + game['url'] if game.get('url') else None,
                     game.get('type'), *[int(game[key]) if game.get(key) is not None else None
                                       for key in ('runCount', 'totalPlayerCount', 'activePlayerCount')],
                     json.dumps(game.get('releaseDate'), ensure_ascii=False), game.get('addedAt'),
                     game.get('updatedAt'), json.dumps(game.get('platformIds', []))])
    with (processed / 'games.csv').open('w', encoding='utf-8-sig', newline='') as handle:
        writer = csv.writer(handle)
        writer.writerow(fields)
        writer.writerows(rows)
    with (processed / 'games.jsonl').open('w', encoding='utf-8') as handle:
        for game in sorted(games, key=lambda item: (item['name'].casefold(), item['id'])):
            handle.write(json.dumps(game, ensure_ascii=False) + '\n')
    save(processed / 'platforms.json', list(platforms.values()))
    with closing(sqlite3.connect(processed / 'games.sqlite')) as db, db:
        db.execute('DROP TABLE IF EXISTS games')
        column_types = ['TEXT PRIMARY KEY'] + ['TEXT'] * 4 + ['INTEGER'] * 3 + ['TEXT'] * 4
        db.execute('CREATE TABLE games (' + ','.join(f'{field} {kind}' for field, kind in zip(fields, column_types)) + ',raw_json TEXT NOT NULL)')
        raw_by_id = {game['id']: json.dumps(game, ensure_ascii=False) for game in games}
        db.executemany('INSERT INTO games VALUES (' + ','.join('?' for _ in range(13)) + ')',
                       [row + [raw_by_id[row[0]]] for row in rows])
        if db.execute('SELECT count(*) FROM games').fetchone()[0] != len(games):
            raise RuntimeError('Export count mismatch')
    response_times = []
    for path in (root / 'raw').glob('*.json'):
        cached = json.loads(path.read_text(encoding='utf-8'))
        if cached.get('url', '').startswith('https://www.speedrun.com/api/v2/GetGameList?'):
            response_times.append(cached['fetched_at'])
    manifest = {
        'stage': 1, 'source': 'https://www.speedrun.com/api/v2/GetGameList',
        'started_at': started_at, 'completed_at': datetime.now(timezone.utc).isoformat(),
        'game_count': len(games), 'unique_game_ids': len({g['id'] for g in games}),
        'game_types': dict(Counter(g.get('type') for g in games)), 'pagination': pagination,
        'network_requests_this_execution': requests,
        'raw_response_count': len(response_times),
        'snapshot_first_response_at': min(response_times) if response_times else None,
        'snapshot_last_response_at': max(response_times) if response_times else None,
        'coverage': 'All pages of the public GetGameList directory; no game type filters.',
        'validation': 'Stable pagination totals, correct page numbers and lengths, unique IDs, export count.',
        'snapshot_limit': 'Pages fetched over time; this is not an atomic site snapshot. Deleted/private entries are outside this public directory.',
        'counter_scope': 'site_run_count is the API runCount, not an independently counted total of full-game runs in all verification statuses.',
        'raw_cache': 'raw/*.json retains original responses, URLs and per-response fetch timestamps.',
        'next_stages_executed': False,
    }
    save(root / 'manifest.json', manifest)
    (root / 'README.md').write_text(
        '# Каталог игр: этап 1\n\n'
        f'Получено {len(games):,} записей из публичного каталога Speedrun.com. '
        'Все типы записей сохранены без фильтрации.\n\n'
        '- `processed/games.csv`: таблица по алфавиту, UTF-8 с BOM для просмотра в Excel.\n'
        '- `processed/games.sqlite`: таблица `games`, включая исходный JSON каждой игры.\n'
        '- `processed/games.jsonl`: полные объекты игр, одна запись на строку.\n'
        '- `processed/platforms.json`: платформы из ответов каталога.\n'
        '- `raw/`: исходные ответы с URL и временем получения.\n'
        '- `manifest.json`: объём, типы записей, проверки и ограничения снимка.\n\n'
        '`site_run_count` — счётчик `runCount` сайта. Его нельзя считать числом всех '
        'полных забегов во всех статусах без проверки определения. Топ-10 и забеги '
        'на этом этапе не собирались.\n\n'
        'Полнота проверена относительно публичного каталога API: все страницы, '
        'стабильный заявленный объём, отсутствие повторов ID. Снимок не атомарный: '
        'изменение сортировки между запросами теоретически может изменить состав '
        'при неизменном количестве. Удалённые и закрытые игры не входят в эту гарантию.\n',
        encoding='utf-8')
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=Path('data/games_catalog_20261010'))
    parser.add_argument('--rpm', type=int, default=20)
    args = parser.parse_args()
    if not 1 <= args.rpm <= 60:
        parser.error('rpm must be 1..60')
    started_at = datetime.now(timezone.utc).isoformat()
    client = Client(args.output, args.rpm)
    games, platforms, pagination = collect_catalog(client)
    manifest = export_catalog(args.output, games, platforms, pagination, started_at, client.requests)
    print(json.dumps(manifest, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
