"""Resumable speedrun.com ETL using only the Python standard library."""
import argparse
import base64
import collections
import csv
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import time
import threading
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

BASE = 'https://www.speedrun.com/api/v1/'

def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix('.tmp')
    temp.write_text(json.dumps(value, ensure_ascii=False), encoding='utf-8')
    temp.replace(path)

class Client:
    def __init__(self, root, rpm):
        self.root, self.interval, self.last = root, 60 / rpm, 0
        self.requests = 0
        self.rate_lock = threading.Lock()

    def get(self, route, **params):
        url = (route if route.startswith('https://www.speedrun.com/api/') else BASE + route) + ('?' + urllib.parse.urlencode(params) if params else '')
        path = self.root / 'raw' / (hashlib.sha256(url.encode()).hexdigest() + '.json')
        if path.exists():
            return json.loads(path.read_text(encoding='utf-8'))['response']
        for attempt in range(7):
            with self.rate_lock:
                time.sleep(max(0, self.last + self.interval - time.monotonic()))
                self.last = time.monotonic()
                self.requests += 1
            headers = {'User-Agent': 'ETL-practice/1.0', 'Accept': 'application/json'}
            key = os.environ.get('SPEEDRUNCOM_API_KEY')
            if key and url.startswith(BASE):
                headers['X-API-Key'] = key
            try:
                with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=60) as response:
                    data = json.load(response)
                save(path, {'url': url, 'fetched_at': datetime.now(timezone.utc).isoformat(), 'response': data})
                return data
            except urllib.error.HTTPError as error:
                if error.code not in (420, 429, 500, 502, 503, 504) or attempt == 6:
                    raise RuntimeError(f'API HTTP {error.code}: {url}') from None
                delay = max(65 if error.code in (420, 429) else 2 ** attempt,
                            float(error.headers.get('Retry-After', '0')) if error.headers.get('Retry-After', '0').isdigit() else 0)
                print(f'Retry HTTP {error.code}, waiting {delay}s', flush=True)
                time.sleep(delay)
            except (urllib.error.URLError, TimeoutError):
                if attempt == 6:
                    raise RuntimeError(f'Network failure: {url}') from None
                time.sleep(2 ** attempt)

    def v2(self, endpoint, params):
        encoded = base64.urlsafe_b64encode(json.dumps(params, separators=(',', ':')).encode()).decode().rstrip('=')
        return self.get('https://www.speedrun.com/api/v2/' + endpoint, _r=encoded)

    def pages(self, route, **params):
        offset = 0
        while True:
            page = self.get(route, max=200, offset=offset, **params)
            yield from page['data']
            links = page.get('pagination', {}).get('links', [])
            if not any(link['rel'] == 'next' for link in links):
                break
            offset += len(page['data'])
            if not page['data']:
                raise RuntimeError('Empty page with next link')
            if offset % 2000 == 0:
                print(f'{route} {params.get("game", "")}: {offset} rows', flush=True)

def any_percent(name):
    return bool(re.search(r'\bany\s*(?:%|percent\b)', name, re.I))

def export(root, tables):
    processed = root / 'processed'
    processed.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(processed / 'speedruns.sqlite')
    try:
        with db:
            for name, (columns, rows) in tables.items():
                db.execute(f'DROP TABLE IF EXISTS {name}')
                db.execute(f'CREATE TABLE {name} ({columns})')
                fields = [item.strip().split()[0] for item in columns.split(',')]
                db.executemany(f'INSERT INTO {name} VALUES ({",".join("?" for _ in fields)})', rows)
                with (processed / f'{name}.csv').open('w', encoding='utf-8', newline='') as handle:
                    writer = csv.writer(handle)
                    writer.writerow(fields)
                    writer.writerows(rows)
            db.execute('CREATE INDEX runs_category ON runs(category_id)')
            db.execute('CREATE INDEX runs_game ON runs(game_id)')
            assert db.execute('SELECT count(*) FROM runs r LEFT JOIN categories c ON r.category_id=c.id WHERE c.id IS NULL').fetchone()[0] == 0
            fields = {row[1] for row in db.execute('PRAGMA table_info(runs)')}
            valid_time_scope = "status='verified' AND " if 'status' in fields else ''
            assert db.execute('SELECT count(*) FROM runs WHERE ' + valid_time_scope + '(primary_seconds IS NULL OR primary_seconds < 0)').fetchone()[0] == 0
    finally:
        db.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--games', nargs='+', default=['portal', 'portal_2', 'supermetroid', 'undertale', 'hollowknight'])
    parser.add_argument('--rpm', type=int, default=60)
    parser.add_argument('--top-categories', type=int, default=2)
    parser.add_argument('--output', type=Path, default=Path('data'))
    args = parser.parse_args()
    if not 1 <= args.rpm <= 60 or args.top_categories < 1:
        parser.error('rpm must be 1..60 and top-categories >= 1')
    client = Client(args.output, args.rpm)
    games, categories, runs, players = [], [], [], []
    summaries = []
    for abbreviation in dict.fromkeys(args.games):
        matches = list(client.pages('games', abbreviation=abbreviation))
        if len(matches) != 1:
            raise RuntimeError(f'Expected one game for {abbreviation}, found {len(matches)}')
        game = matches[0]
        gid = game['id']
        print(f'Collecting {game["names"]["international"]} ({gid})', flush=True)
        cats = list(client.pages(f'games/{gid}/categories'))
        eligible = [cat for cat in cats if cat['type'] == 'per-game']
        raw_runs = {}
        for cat in eligible:
            print(f'  Category: {cat["name"]}', flush=True)
            try:
                category_runs = list(client.pages('runs', game=gid, category=cat['id'], status='verified', orderby='submitted', direction='asc'))
            except RuntimeError as error:
                if 'HTTP 400' not in str(error) or 'offset=10000' not in str(error):
                    raise
                print('  Splitting oversized category by platform', flush=True)
                category_runs = []
                for platform in game['platforms']:
                    category_runs.extend(client.pages('runs', game=gid, category=cat['id'], platform=platform, status='verified', orderby='submitted', direction='asc'))
            raw_runs.update((run['id'], run) for run in category_runs)
        counts = collections.Counter(run['category'] for run in raw_runs.values() if run.get('level') is None)
        ranked = sorted(eligible, key=lambda cat: (-counts[cat['id']], cat['id']))
        top = {cat['id'] for cat in ranked[:args.top_categories] if counts[cat['id']] > 0}
        selected = top | {cat['id'] for cat in eligible if any_percent(cat['name'])}
        games.append((gid, game['names']['international'], game['abbreviation'], game.get('released'), game['weblink']))
        for cat in cats:
            categories.append((cat['id'], gid, cat['name'], cat['type'], int(cat.get('miscellaneous', False)), counts[cat['id']], int(cat['id'] in selected), 'any_percent' if any_percent(cat['name']) and cat['id'] in selected else 'top_count' if cat['id'] in top else ''))
        selected_count = 0
        for run in raw_runs.values():
            if run['category'] not in selected or run.get('level') is not None:
                continue
            assert run['game'] == gid and run['status']['status'] == 'verified'
            timing, system = run['times'], run['system']
            runs.append((run['id'], gid, run['category'], run.get('date'), run.get('submitted'), timing['primary_t'], timing.get('realtime_t'), timing.get('realtime_noloads_t'), timing.get('ingame_t'), system.get('platform'), int(system.get('emulated', False)), system.get('region'), run['weblink'], json.dumps(run.get('values', {})), json.dumps(run.get('videos'), ensure_ascii=False)))
            for position, player in enumerate(run['players']):
                players.append((run['id'], position, player['rel'], player.get('id'), player.get('name')))
            selected_count += 1
        summaries.append({'game': game['names']['international'], 'verified_runs_scanned': len(raw_runs), 'selected_runs': selected_count, 'categories': [{'name': cat['name'], 'runs': counts[cat['id']]} for cat in ranked if cat['id'] in selected]})
        print(json.dumps(summaries[-1], ensure_ascii=False), flush=True)
    export(args.output, {
        'games': ('id TEXT PRIMARY KEY,name TEXT,abbreviation TEXT,released INTEGER,weblink TEXT', games),
        'categories': ('id TEXT PRIMARY KEY,game_id TEXT,name TEXT,type TEXT,miscellaneous INTEGER,verified_full_game_runs INTEGER,selected INTEGER,selection_reason TEXT', categories),
        'runs': ('id TEXT PRIMARY KEY,game_id TEXT,category_id TEXT,date TEXT,submitted TEXT,primary_seconds REAL,realtime_seconds REAL,realtime_noloads_seconds REAL,ingame_seconds REAL,platform_id TEXT,emulated INTEGER,region_id TEXT,weblink TEXT,values_json TEXT,videos_json TEXT', runs),
        'run_players': ('run_id TEXT,position INTEGER,kind TEXT,user_id TEXT,guest_name TEXT', players),
    })
    report = {'completed_at': datetime.now(timezone.utc).isoformat(), 'rpm': args.rpm, 'requests_this_execution': client.requests, 'games': summaries, 'total_selected_runs': len(runs), 'status': 'complete'}
    save(args.output / 'manifest.json', report)
    print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)

if __name__ == '__main__':
    main()
