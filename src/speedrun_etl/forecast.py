"""Expand a completed four-band resource pilot; no Speedrun.com requests."""
import argparse
from collections import Counter
from datetime import datetime
import os
from pathlib import Path
import statistics

from .batch import counter_band, selection, stratified_selection
from .entity_ods import EntityPackage
from .entity_storage import ready
from .jsonutil import atomic_write, dumps, read, utcnow
from .storage import pg_connect


def scenarios(populations, rows, metric):
    result={'observed_min_scenario':0.0,'mean_projection':0.0,'observed_max_scenario':0.0}
    for band,population in populations.items():
        values=[float(r[metric]) for r in rows if r['band']==band]
        if not values:
            raise ValueError('No completed observations in resource stratum '+band)
        result['observed_min_scenario']+=population*min(values)
        result['mean_projection']+=population*statistics.mean(values)
        result['observed_max_scenario']+=population*max(values)
    return result


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--pilot',required=True)
    args=parser.parse_args()
    import re
    if not re.fullmatch(r'[A-Za-z0-9_-]{1,40}',args.pilot):parser.error('Invalid pilot ID')
    state=Path(os.environ['ETL_STATE']);root=state/'batches'/args.pilot;conf=read(root/'config.json')
    if conf.get('mode')!='sample':raise ValueError('A hash-selected four-band sample is required')
    parent=read(root/'selection.json')['snapshot_id'];games=selection(state,parent,True)
    populations=Counter(counter_band(g['runCount']) for g in games)
    expected={g['id'] for g in stratified_selection(games,conf['sample_size'])}
    with pg_connect(True) as conn:
        queue=conn.execute('SELECT game_id,collection_id,state,details,updated_at FROM entity_batch_queue WHERE batch_id=%s ORDER BY position',[args.pilot]).fetchall()
    if {r[0] for r in queue}!=expected or any(r[2] not in ('complete','not_applicable') for r in queue):
        raise RuntimeError('Finish/review every selected pilot game before expanding the sample')
    rows=[]
    for gid,collection,status,details,finished in queue:
        budget=read(state/'collections'/collection/'budget.json')
        telemetry=read(state/'collections'/collection/'telemetry.json')
        logical=records=staging=0;counts=Counter()
        for kind in ('games_metadata','runs','players'):
            path=state/'snapshots'/(collection+'-'+kind);ref=read(path/'extracted.json')
            ack=ready(ref['snapshot_id'],role=ref['role'],expected=ref['digest'])
            summary=next(iter(ack['stores'].values()))
            counts.update({k:v['count'] for k,v in summary.items()});records+=sum(v['count'] for v in summary.values())
            package=EntityPackage(ref['package_uri'],role=ref['role'])
            where=" WHERE dataset!='players' AND NOT (dataset='checks' AND json_extract(document,'$.dataset')='players')" if ref['role']=='runs' else ''
            logical+=package.db.execute('SELECT COALESCE(sum(length(CAST(document AS BLOB))),0) FROM records'+where).fetchone()[0]
            package.db.close()
            staging+=sum(p.stat().st_size for p in path.rglob('*') if p.is_file())
        staging+=sum(p.stat().st_size for p in (state/'collections'/collection).rglob('*') if p.is_file())
        started=datetime.fromisoformat(details['first_started_at'])
        rows.append({'game_id':gid,'name':details['name'],'collection_id':collection,'state':status,
            'band':details['sampling_band'],'card_counter':int(details['runCount']),
            'http_attempts':budget['used'],'network_bytes':telemetry['network_bytes'],
            'http_status_counts':telemetry['status_counts'],'logical_record_bytes':logical,
            'logical_records':records,'staging_bytes':staging,'counts':dict(counts),
            'game_wall_seconds':(finished-started).total_seconds()})
    expanded={metric:scenarios(populations,rows,metric) for metric in (
        'http_attempts','network_bytes','logical_record_bytes','logical_records','staging_bytes','game_wall_seconds')}
    report={'at':utcnow(),'pilot':args.pilot,'catalog_snapshot_id':parent,'source_HTTP_in_report':0,
        'population_games':sum(populations.values()),'population_by_band':dict(populations),
        'sample_games':len(rows),'rows':rows,'expanded':expanded,
        'mean_game_wall_days':expanded['game_wall_seconds']['mean_projection']/86400,
        'mean_pacing_floor_hours_at_40_RPM':expanded['http_attempts']['mean_projection']/40/60,
        'limitations':[
            'Deterministic hash sample, at most five games per band; counter is not an independently verified archive size.',
            'Observed-min/max scenarios are not hard bounds or statistical confidence intervals; unseen tails may exceed them.',
            'Times include three DAGs, loads and driver polling, not only source HTTP. Hardware/source load may change.',
            'Expansion excludes the one-time input catalog and does not promise completion within the lab deadline.',
            'Logical/staging bytes are expanded additively; cross-game payload sharing, compression and indexes change service sizes.',
            'No fixed daily freshness or full-site completeness claim. Existing coverage/WR limitations remain.'
        ]}
    atomic_write(root/'resource-forecast.json',report)
    print(dumps({'report':str(root/'resource-forecast.json'),'games':len(rows),
                 'mean_game_wall_days':report['mean_game_wall_days'],
                 'mean_pacing_floor_hours_at_40_RPM':report['mean_pacing_floor_hours_at_40_RPM']}))


if __name__=='__main__':main()
