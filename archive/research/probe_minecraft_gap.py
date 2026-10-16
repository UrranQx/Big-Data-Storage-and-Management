"""Small, cached probes for the Minecraft statistics discrepancy."""
from collections import Counter
from pathlib import Path
import json
import sqlite3
from collect import Client, save

root = Path('data/minecraft_java_stats_audit_20261010')
client = Client(root, 20)
db = sqlite3.connect('data/minecraft_java_20261010/processed/speedruns.sqlite')
known = {r[0] for r in db.execute('SELECT id FROM runs')}
for order, direction in [('category', 'asc'), ('category', 'desc'), ('submitted', 'asc')]:
    response = client.get('runs', game='j1npme6p', status='verified',
                          orderby=order, direction=direction, max=200)
    missing = [r for r in response['data'] if r['id'] not in known]
    print(json.dumps({'order': order, 'direction': direction,
                      'categories': dict(Counter(r['category'] for r in response['data'])),
                      'missing': len(missing),
                      'missing_categories': dict(Counter(r['category'] for r in missing)),
                      'missing_no_players': sum(not r.get('players') for r in missing)}, ensure_ascii=True), flush=True)
    save(root / f'probe_{order}_{direction}.json', missing)

print('Relevant variables:')
for vid, name, raw in db.execute('SELECT id,name,raw_json FROM variables'):
    if name in ['Seed Type (Any% Glitchless)', 'Version Range (Any% Glitchless)']:
        print(vid, name, flush=True)
        print(json.dumps(db.execute('SELECT id,name FROM variable_values WHERE variable_id=?', (vid,)).fetchall()), flush=True)
