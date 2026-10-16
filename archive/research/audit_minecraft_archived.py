"""Find archived Minecraft runs through the unfiltered v1 game run list."""
from collections import Counter
from pathlib import Path
import json
import sqlite3
from collect import Client, save

root = Path('data/minecraft_java_stats_audit_20261010')
client = Client(root, 20)
db = sqlite3.connect('data/minecraft_java_20261010/processed/speedruns.sqlite')
known = {r[0] for r in db.execute('SELECT id FROM runs')}
archived = {}
seen = set()
page_evidence = []
category_counts = Counter()
for offset in range(0, 10000, 200):
    response = client.get('runs', game='j1npme6p', status='verified',
                          orderby='category', direction='desc', max=200, offset=offset)
    if int(response['pagination']['offset']) != offset:
        raise RuntimeError('Offset ignored')
    batch = response['data']
    duplicates = seen.intersection(r['id'] for r in batch)
    seen.update(r['id'] for r in batch)
    category_counts.update(r['category'] for r in batch)
    page_evidence.append({'offset': offset, 'category_counts': dict(Counter(r['category'] for r in batch)),
                          'duplicate_ids': sorted(duplicates)})
    save(root / 'category_order_probes.json', {'pages': page_evidence,
                                              'category_row_counts': dict(category_counts),
                                              'warning': 'Category-only sorting has unstable ties; row counts are not proof of unique-ID completeness.'})
    for run in batch:
        if run['game'] != 'j1npme6p' or run['status']['status'] != 'verified':
            raise RuntimeError('Scope ignored')
        if run['id'] not in known:
            archived[run['id']] = run
    save(root / 'global_missing_verified.json', list(archived.values()))
    print(json.dumps({'offset': offset, 'categories': dict(Counter(r['category'] for r in batch)),
                      'missing_so_far': len(archived),
                      'duplicates_this_page': len(duplicates),
                      'missing_categories': dict(Counter(r['category'] for r in archived.values()))}), flush=True)
    if any(r['category'] == 'mkeyl926' for r in batch):
        break
else:
    raise RuntimeError('Did not reach the large active category before the offset cap')

cards = []
for cid in sorted({r['category'] for r in archived.values()}):
    run = next(r for r in archived.values() if r['category'] == cid)
    card = client.v2('GetRun', {'runId': run['id']})
    cards.append({'category_id': cid, 'v1': run, 'v2': card})
    print(json.dumps({'card': run['id'], 'category': cid,
                      'v2_keys': list(card), 'v2_run': card.get('run')}, ensure_ascii=True), flush=True)
save(root / 'archived_sample_cards.json', cards)
