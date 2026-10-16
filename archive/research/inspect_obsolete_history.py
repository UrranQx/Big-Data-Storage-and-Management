"""Offline evidence that full v2 exports contain past results, not only current PBs."""
import json
from collections import defaultdict
from pathlib import Path
import sqlite3

report = {}
for name, folder in [('Portal', 'portal_20261010'), ('Minecraft Java', 'minecraft_java_20261010')]:
    root = Path('data') / folder
    with sqlite3.connect(root / 'processed/speedruns.sqlite') as db:
        db.row_factory = sqlite3.Row
        counts = [dict(row) for row in db.execute(
            'SELECT status,obsolete,count(*) AS runs FROM runs GROUP BY status,obsolete ORDER BY status,obsolete')]
        groups = defaultdict(list)
        query = '''WITH solo AS (SELECT run_id FROM run_players GROUP BY run_id HAVING count(*)=1)
                   SELECT r.*,rp.player_id,c.name AS category_name,p.name AS player_name
                   FROM runs r JOIN run_players rp ON rp.run_id=r.id
                   JOIN solo ON solo.run_id=r.id
                   JOIN categories c ON c.id=r.category_id JOIN players p ON p.id=rp.player_id
                   WHERE r.status='verified' AND r.statistics_eligible=1'''
        for row in db.execute(query):
            # Deliberately strict grouping for an illustrative pair, not a general PB algorithm.
            key = tuple(row[k] for k in ['player_id','category_id','values_json','platform_id',
                                         'region_id','emulated','primary_time_source'])
            groups[key].append(row)
        example = None
        for rows in groups.values():
            current = [r for r in rows if not r['obsolete'] and r['verified_at']]
            older = [r for r in rows if r['obsolete'] and r['verified_at']]
            for newer in current:
                matches = [r for r in older if r['verified_at'] < newer['verified_at']
                           and r['primary_seconds'] > newer['primary_seconds']]
                if matches:
                    previous = min(matches,key=lambda r:r['primary_seconds'])
                    example = {'player_id':newer['player_id'],'player_name':newer['player_name'],
                               'category':newer['category_name'],'timer':newer['primary_time_source'],
                               'runs':[{k:r[k] for k in ['id','primary_seconds','date','submitted','verified_at','obsolete','weblink']}
                                       for r in [previous,newer]]}
                    break
            if example:
                break
        assert example, 'No historical/current example found'
        report[name] = {'counts':counts,'example':example,
                        'note':'This proves older records are collected; it does not prove historical PB intervals.'}
print(json.dumps(report,ensure_ascii=True,indent=2))
target=Path('data/research_20261010/obsolete_evidence.json')
target.parent.mkdir(parents=True,exist_ok=True)
target.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
