"""Resolve unstable v1 category-sort ties by collecting overlapping windows."""
from pathlib import Path
import json
from collect import Client, save

root = Path('data/minecraft_java_stats_audit_20261010')
client = Client(root, 20)
found = {r['id']: r for r in json.loads((root/'global_missing_verified.json').read_text(encoding='utf-8'))
         if r['category'] == 'xd14pv8d'}
# Probed contiguous category block: offsets [84, 848), i.e. 764 rows.
# Category sorting has unstable ties; overlapping windows supply extra IDs.
for size, step in [(100, 100), (200, 100), (50, 50)]:
    for offset in range(84, 848, step):
        response = client.get('runs', game='j1npme6p', status='verified',
                              orderby='category', direction='desc', max=size, offset=offset)
        if int(response['pagination']['offset']) != offset:
            raise RuntimeError('Offset ignored')
        for run in response['data']:
            if run['game'] != 'j1npme6p' or run['status']['status'] != 'verified':
                raise RuntimeError('Scope ignored')
            if run['category'] == 'xd14pv8d':
                found[run['id']] = run
        save(root / 'combined_verified_v1.json', list(found.values()))
        print(f'max={size}, offset={offset}: {len(found)} unique Combined IDs', flush=True)
        if len(found) > 764:
            raise RuntimeError('Category count changed or inferred block size invalid')
        if len(found) == 764:
            break
    if len(found) == 764:
        break
if len(found) != 764:
    raise RuntimeError('Could not recover every unique category ID')
print(json.dumps({'unique_ids':len(found),'no_players':sum(not r.get('players') for r in found.values()),
                  'with_level':sum(r.get('level') is not None for r in found.values())}), flush=True)
