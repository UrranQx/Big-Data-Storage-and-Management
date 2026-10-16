"""Read-only research of site statistics and record history for Celeste."""
import json
import sqlite3
from pathlib import Path
from collect import Client, save

root = Path('data/celeste/research')
client = Client(root, 30)
summary = client.v2('GetGameSummary', {'gameId': 'o1y9j9v6'})
save(root / 'site_stats.json', {key: summary.get(key) for key in ('gameStats', 'stats', 'game')})
print(json.dumps({key: summary.get(key) for key in ('gameStats', 'stats')}, ensure_ascii=False, indent=2), flush=True)
params = {'gameId': 'o1y9j9v6', 'categoryId': '7kjpl1gk', 'obsolete': 1,
          'video': 0, 'emulator': 1, 'verified': 1, 'values': []}
history = client.v2('GetGameRecordHistory', {'params': params, 'page': 1})
save(root / 'site_wr_history_any.json', history)
print(json.dumps({'history_keys': list(history), 'pagination': history.get('pagination'),
                  'first_run': history.get('runList', [])[:1]}, ensure_ascii=False, indent=2), flush=True)
with sqlite3.connect('data/celeste/processed/speedruns.sqlite') as db:
    print('Local status counts:', db.execute('SELECT status,count(*) FROM runs GROUP BY status').fetchall(), flush=True)
    print('Verified date gaps:', db.execute('SELECT count(*),sum(date IS NULL),sum(verified_at IS NULL) FROM runs WHERE status=?', ('verified',)).fetchone(), flush=True)
