"""Recover archived IDs with stable submitted ordering and examiner partitions."""
from pathlib import Path
from collections import Counter
import json
from collect import Client, save

root = Path('data/minecraft_java_stats_audit_20261010')
client = Client(root, 20)
found = {r['id']: r for r in json.loads((root/'combined_verified_v1.json').read_text(encoding='utf-8'))}
examiners = Counter(r['status'].get('examiner') for r in found.values())
evidence = []
for examiner, _ in examiners.most_common():
    if not examiner:
        continue
    ids = set()
    combined_count = 0
    try:
        for run in client.pages('runs', game='j1npme6p', status='verified', examiner=examiner,
                                orderby='submitted', direction='asc'):
            if run['id'] in ids or run['status'].get('examiner') != examiner:
                raise RuntimeError('Examiner scope or uniqueness failure')
            ids.add(run['id'])
            if run['category'] == 'xd14pv8d':
                found[run['id']] = run
                combined_count += 1
    except RuntimeError as error:
        if 'HTTP 404' not in str(error):
            raise
        evidence.append({'examiner':examiner,'http_status':404})
        continue
    evidence.append({'examiner':examiner,'all_verified':len(ids),'combined_verified':combined_count})
    save(root/'combined_verified_v1.json',list(found.values()))
    save(root/'examiner_partition_evidence.json',evidence)
    print(json.dumps({'examiner':examiner,'all_verified':len(ids),'combined_verified':combined_count,
                      'unique_combined_so_far':len(found)}),flush=True)
    if len(found) >= 764:
        break
print(json.dumps({'unique_combined':len(found),
                  'no_players':sum(not r.get('players') for r in found.values())}),flush=True)
