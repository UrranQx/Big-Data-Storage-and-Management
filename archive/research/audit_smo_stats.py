"""Investigate the SMO stats/leaderboard discrepancy without trusting stats as rows."""
from datetime import datetime, timezone
import json
from pathlib import Path

from collect import Client, save


ROOT = Path('data/smo_stats_audit_20261010')
GID = '76r55vd8'


def main():
    client = Client(ROOT, 20)
    summary = client.v2('GetGameSummary', {'gameId': GID})
    metadata = client.v2('GetGameData', {'gameId': GID})
    v1_categories = list(client.pages(f'games/{GID}/categories'))
    save(ROOT / 'metadata_comparison.json', {'v2_categories': metadata['categories'], 'v1_categories': v1_categories})
    rows, anomalies = [], []
    for category in metadata['categories']:
        params = dict(gameId=GID, categoryId=category['id'], obsolete=1, video=0,
                      emulator=1, verified=1, values=[])
        page = client.v2('GetGameLeaderboard2', {'params': params, 'page': 1})
        row = dict(id=category['id'], name=category['name'], per_level=category['isPerLevel'],
                   archived=bool(category.get('archived')), v2_verified_count=int(page['pagination']['count']))
        if row['per_level']:
            row['v1_audit'] = 'Not applicable to full-game scope'
        elif row['v2_verified_count'] < 10000:
            runs = list(client.pages('runs', game=GID, category=category['id'], status='verified',
                                     orderby='submitted', direction='asc'))
            if len({run['id'] for run in runs}) != len(runs):
                raise RuntimeError('Duplicate v1 IDs')
            row['v1_verified_count'] = len(runs)
            row['v1_full_game_count'] = sum(run.get('level') is None for run in runs)
            row['v1_minus_v2'] = len(runs) - row['v2_verified_count']
            row['empty_player_run_ids'] = [run['id'] for run in runs if not run.get('players')]
            anomalies.extend(run for run in runs if not run.get('players'))
            save(ROOT / f'v1_{category["id"]}.json', runs)
        else:
            runs = client.get('runs', game=GID, category=category['id'], status='verified',
                              orderby='submitted', direction='asc', max=200, offset=0)['data']
            row['v1_early_sample_count'] = len(runs)
            row['early_empty_player_run_ids'] = [run['id'] for run in runs if not run.get('players')]
            anomalies.extend(run for run in runs if not run.get('players'))
        rows.append(row)
        print(json.dumps(row, ensure_ascii=False), flush=True)
        save(ROOT / 'phase1_progress.json', {'stats': summary['stats'], 'categories': rows, 'empty_player_runs': anomalies})
    report = {'completed_at': datetime.now(timezone.utc).isoformat(), 'stats': summary['stats'],
              'categories': rows, 'empty_player_runs': anomalies,
              'v2_full_game_verified_sum': sum(row['v2_verified_count'] for row in rows if not row['per_level'])}
    save(ROOT / 'phase1.json', report)
    print(json.dumps(report, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
