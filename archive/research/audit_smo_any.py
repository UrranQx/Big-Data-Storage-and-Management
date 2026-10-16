"""Enumerate SMO verified Any% via overlapping first/last v1 windows."""
from datetime import datetime, timezone
import json

from audit_smo_stats import ROOT, GID
from collect import Client, save

CID = 'w20w1lzd'


def main():
    client = Client(ROOT, 20)
    params = dict(game=GID, category=CID, status='verified', orderby='submitted')
    runs, asc_ids, directional_ids = {}, set(), {'asc': set(), 'desc': set()}
    overlap = set()
    for direction in ('asc', 'desc'):
        for offset in range(0, 10000, 200):
            response = client.get('runs', **params, direction=direction, max=200, offset=offset)
            batch = response['data']
            ids = {run['id'] for run in batch}
            if len(ids) != len(batch) or ids.intersection(directional_ids[direction]):
                raise RuntimeError('Repeated IDs within a direction')
            directional_ids[direction].update(ids)
            for run in batch:
                if run['game'] != GID or run['category'] != CID or run.get('level') or run['status']['status'] != 'verified':
                    raise RuntimeError('Unexpected run scope')
                runs[run['id']] = run
            if direction == 'asc':
                asc_ids.update(ids)
            else:
                overlap.update(ids.intersection(asc_ids))
            if offset % 2000 == 0 or overlap:
                orphan_ids = [rid for rid, run in runs.items() if not run.get('players')]
                print(json.dumps({'direction': direction, 'offset': offset,
                                  'unique_runs': len(runs), 'overlap_ids': len(overlap),
                                  'empty_player_run_ids': orphan_ids}, ensure_ascii=False), flush=True)
                save(ROOT / 'any_scan_progress.json', {'unique_runs': len(runs), 'direction': direction,
                                                      'offset': offset, 'empty_player_runs': [runs[rid] for rid in orphan_ids]})
            if overlap or not any(link['rel'] == 'next' for link in response.get('pagination', {}).get('links', [])):
                break
    if not overlap:
        raise RuntimeError('The first/last windows do not overlap; completeness not proven')
    orphan_runs = [run for run in runs.values() if not run.get('players')]
    report = {'completed_at': datetime.now(timezone.utc).isoformat(), 'v1_any_verified_count': len(runs),
              'asc_ids': len(asc_ids), 'desc_ids': len(directional_ids['desc']),
              'overlap_ids': len(overlap), 'empty_player_runs': orphan_runs,
              'scope': 'Verified full-game Any%; both sorted windows meet, no middle offset gap.'}
    save(ROOT / 'any_v1_runs.json', list(runs.values()))
    save(ROOT / 'any_v1_audit.json', report)
    print(json.dumps(report, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
