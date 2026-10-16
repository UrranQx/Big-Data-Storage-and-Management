"""Check exact category boundaries and recover remaining IDs with varied windows."""
from pathlib import Path
from datetime import datetime, timezone
import json
from collect import Client, save

root=Path('data/minecraft_java_stats_audit_20261010')
client=Client(root,20)
boundaries=[]
for offset, expected in [(83,'xd114pzd'),(84,'xd14pv8d'),(847,'xd14pv8d'),(848,'7kj6qvn2')]:
    r=client.get('runs',game='j1npme6p',status='verified',orderby='category',direction='desc',max=1,offset=offset)
    assert r['pagination']['offset']==offset and len(r['data'])==1
    assert r['data'][0]['category']==expected
    boundaries.append({'offset':offset,'category_id':expected,'run_id':r['data'][0]['id']})
save(root/'exact_archive_boundaries.json',boundaries)
found={r['id']:r for r in json.loads((root/'combined_verified_v1.json').read_text(encoding='utf-8'))}
for size in [199,173,137,89]:
    for offset in range(84,848,size):
        response=client.get('runs',game='j1npme6p',status='verified',orderby='category',direction='desc',max=size,offset=offset)
        assert response['pagination']['offset']==offset
        for r in response['data']:
            assert r['game']=='j1npme6p' and r['status']['status']=='verified'
            if r['category']=='xd14pv8d':
                found[r['id']]=r
        save(root/'combined_verified_v1.json',list(found.values()))
        print(f'Window {size}/{offset}: {len(found)} unique archived IDs',flush=True)
        assert len(found)<=764
        if len(found)==764: break
    if len(found)==764: break
samples=[]
for rid in [min(found),max(found),min(found,key=lambda x: found[x]['submitted']),max(found,key=lambda x: found[x]['submitted'])]:
    card=client.v2('GetRun',{'runId':rid})
    samples.append({'id':rid,'v2':card})
    print(json.dumps({'id':rid,'orphaned':card['run'].get('orphaned'),'players':card['run'].get('playerIds'),
                      'category_archived':card['category'].get('archived')}),flush=True)
save(root/'archive_verification.json',{'checked_at':datetime.now(timezone.utc).isoformat(),
      'boundaries':boundaries,'category_row_count':764,'unique_archived_ids':len(found),
      'no_players':sum(not r.get('players') for r in found.values()),'samples':samples})
