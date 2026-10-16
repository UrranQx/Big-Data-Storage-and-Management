"""Recover category-sort tie omissions through equivalent scoped queries."""
from pathlib import Path
import json
from collect import Client, save

root = Path('data/minecraft_java_stats_audit_20261010')
client = Client(root, 20)
found = {r['id']:r for r in json.loads((root/'combined_verified_v1.json').read_text(encoding='utf-8'))}
evidence = []
for extra in [{'platform':'8gej2n93'}, {'emulated':'false'}, {'platform':'8gej2n93','emulated':'false'}, {}]:
    offsets = range(84, 848, 200) if extra else range(91, 848, 37)
    for offset in offsets:
        response = client.get('runs', game='j1npme6p', status='verified',
                              orderby='category', direction='desc', max=200, offset=offset, **extra)
        if int(response['pagination']['offset']) != offset:
            raise RuntimeError('Offset ignored')
        for run in response['data']:
            if run['game'] != 'j1npme6p' or run['status']['status'] != 'verified':
                raise RuntimeError('Scope ignored')
            if run['category'] == 'xd14pv8d':
                found[run['id']] = run
        evidence.append({'offset':offset,'extra':extra,'unique_combined':len(found)})
        save(root/'combined_verified_v1.json',list(found.values()))
        save(root/'recovery_windows.json',evidence)
        print(json.dumps(evidence[-1]),flush=True)
        if len(found)>=764:
            break
    if len(found)>=764:
        break
print('Unique Combined IDs:',len(found),flush=True)
