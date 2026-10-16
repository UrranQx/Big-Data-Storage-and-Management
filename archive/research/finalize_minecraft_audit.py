"""Write the Minecraft archive/statistics reconciliation from saved evidence."""
from pathlib import Path
from datetime import datetime, timezone
import json
import sqlite3
from collect import Client, save

root=Path('data/minecraft_java_stats_audit_20261010')
dataset=Path('data/minecraft_java_20261010')
client=Client(root/'final_check',20)
params={'gameId':'j1npme6p','categoryId':'xd14pv8d','obsolete':1,'video':0,
        'emulator':1,'verified':1,'values':[]}
board=client.v2('GetGameLeaderboard2',{'params':params,'page':1})
assert int(board['pagination']['count'])==0
with sqlite3.connect(dataset/'processed/speedruns.sqlite') as db:
    rid=db.execute('SELECT id FROM verified_runs_for_statistics LIMIT 1').fetchone()[0]
    dataset_ids={r[0] for r in db.execute('SELECT id FROM runs')}
control=client.v2('GetRun',{'runId':rid})
assert not control['run'].get('orphaned') and not control['category']['archived']
save(root/'archive_board_control.json',{'archived_board_pagination':board['pagination'],'active_control':control})

manifest=json.loads((dataset/'manifest.json').read_text(encoding='utf-8'))
boundaries=json.loads((root/'exact_archive_boundaries.json').read_text(encoding='utf-8'))
archived=json.loads((root/'combined_verified_v1.json').read_text(encoding='utf-8'))
assert len({r['id'] for r in archived})==len(archived)
assert not dataset_ids.intersection(r['id'] for r in archived)
assert all(r['game']=='j1npme6p' and r['category']=='xd14pv8d'
           and r['status']['status']=='verified' and r.get('level') is None for r in archived)
samples=json.loads((root/'archive_verification.json').read_text(encoding='utf-8'))['samples']
samples += [{'id':r['v1']['id'],'v2':r['v2']} for r in json.loads((root/'archived_sample_cards.json').read_text(encoding='utf-8'))]
assert all(r['v2']['run'].get('orphaned') and r['v2']['run'].get('playerIds')
           and r['v2']['category']['archived'] for r in samples)
site=int(manifest['site_stats_after']['totalRunsFG'])
verified=manifest['status_counts']['verified']
archive_count=boundaries[2]['offset']-boundaries[1]['offset']+1
assert archive_count==764 and site-verified==archive_count
report={'status':'count_reconciled_to_archived_category','checked_at':datetime.now(timezone.utc).isoformat(),
        'site_verified_count':site,'v2_verified_count':verified,'gap':site-verified,
        'archive_category_id':'xd14pv8d','archive_category_name':'Combined Any% Glitchless',
        'archived_verified_row_count':archive_count,'archive_boundaries':boundaries,
        'recovered_unique_archived_ids':len(archived),'archived_missing_from_main_dataset':len(archived),
        'archived_no_players_in_recovered_ids':sum(not r.get('players') for r in archived),
        'checked_archived_cards':len(samples),'checked_archived_orphaned_with_players':len(samples),
        'archived_leaderboard_count':0,'active_control_id':rid,
        'separate_any_percent_orphan_id':'znk937lz',
        'correction':'The Any% orphan was prematurely subtracted from the stats gap. Its v1 presence does not prove membership in the site stats counter.',
        'main_dataset_modified':False,
        'limitations':'Numerical reconciliation, not access to the stats SQL. Only the saved sample cards were checked for orphaned. Archived unique-ID enumeration is partial; the exact category row count is independently established by boundaries.'}
save(root/'report.json',report)
manifest['stats_reconciliation']={k:report[k] for k in ['status','site_verified_count','v2_verified_count','gap','archive_category_id','archived_verified_row_count','recovered_unique_archived_ids']}
manifest['stats_reconciliation']['report']='../minecraft_java_stats_audit_20261010/report.json'
save(dataset/'manifest.json',manifest)
print(json.dumps(report,ensure_ascii=True,indent=2))
