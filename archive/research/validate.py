"""Validate loaded dataset without making network requests."""
import json
import argparse
import sqlite3
from pathlib import Path
from collect import any_percent

parser = argparse.ArgumentParser()
parser.add_argument('--output', type=Path, default=Path('data'))
root = parser.parse_args().output
with sqlite3.connect(root / 'processed' / 'speedruns.sqlite') as db:
    assert db.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
    fields = {row[1] for row in db.execute('PRAGMA table_info(runs)')}
    all_statuses = 'status' in fields
    time_scope = "status='verified' AND " if all_statuses else ''
    count_column = 'total_full_game_runs' if all_statuses else 'verified_full_game_runs'
    for query in (
        'SELECT count(*) FROM runs r LEFT JOIN games g ON r.game_id=g.id WHERE g.id IS NULL',
        'SELECT count(*) FROM categories c LEFT JOIN games g ON c.game_id=g.id WHERE g.id IS NULL',
        'SELECT count(*) FROM run_players p LEFT JOIN runs r ON p.run_id=r.id WHERE r.id IS NULL',
        'SELECT count(*) FROM runs r JOIN categories c ON r.category_id=c.id WHERE c.selected=0 OR r.game_id!=c.game_id',
        'SELECT count(*) FROM runs WHERE ' + time_scope + '(primary_seconds IS NULL OR primary_seconds<0)',
        f'SELECT count(*) FROM categories c WHERE selected=1 AND {count_column}!=(SELECT count(*) FROM runs r WHERE r.category_id=c.id)',
    ):
        assert db.execute(query).fetchone()[0] == 0, query
    report = {'integrity': 'ok', 'games': db.execute('SELECT count(*) FROM games').fetchone()[0],
              'selected_categories': db.execute('SELECT count(*) FROM categories WHERE selected=1').fetchone()[0],
              'runs': db.execute('SELECT count(*) FROM runs').fetchone()[0],
              'any_percent_runs': db.execute("SELECT count(*) FROM runs r JOIN categories c ON r.category_id=c.id WHERE c.selection_reason='any_percent'").fetchone()[0]}
    assert report['runs'] == json.loads((root / 'manifest.json').read_text(encoding='utf-8'))['total_selected_runs']
    if all_statuses:
        assert db.execute('SELECT count(*) FROM runs WHERE level_id IS NOT NULL').fetchone()[0] == 0
        assert db.execute('SELECT count(*) FROM run_players p LEFT JOIN players u ON p.player_id=u.id WHERE u.id IS NULL').fetchone()[0] == 0
        report['status_counts'] = dict(db.execute('SELECT status,count(*) FROM runs GROUP BY status'))
        if 'statistics_eligible' in fields:
            assert db.execute("SELECT count(*) FROM runs WHERE statistics_eligible!=(CASE WHEN orphaned=0 AND source_api='v2' THEN 1 ELSE 0 END)").fetchone()[0] == 0
            assert db.execute('SELECT count(*) FROM runs r WHERE orphaned=0 AND NOT EXISTS (SELECT 1 FROM run_players p WHERE p.run_id=r.id)').fetchone()[0] == 0
            assert db.execute('SELECT count(*) FROM runs_for_statistics WHERE orphaned=1').fetchone()[0] == 0
            report['orphaned_runs'] = db.execute('SELECT count(*) FROM runs WHERE orphaned=1').fetchone()[0]
            report['statistics_runs'] = db.execute('SELECT count(*) FROM runs_for_statistics').fetchone()[0]
            report['statistics_status_counts'] = dict(db.execute('SELECT status,count(*) FROM runs_for_statistics GROUP BY status'))
        report['any_percent_runs'] = sum(count for name, count in db.execute('SELECT c.name,count(*) FROM runs r JOIN categories c ON r.category_id=c.id GROUP BY c.id') if any_percent(name))
        manifest = json.loads((root / 'manifest.json').read_text(encoding='utf-8'))
        assert manifest['status'] == 'complete'
        assert report['status_counts'] == manifest['status_counts']
        if 'statistics_eligible' in fields:
            assert report['orphaned_runs'] == manifest['orphaned_runs']
            assert report['statistics_runs'] == manifest['statistics_runs']
            assert report['statistics_status_counts'] == manifest['statistics_status_counts']
        for partition in manifest['partitions']:
            actual = db.execute('SELECT count(*) FROM runs WHERE category_id=? AND status=?', (partition['category_id'], partition['status'])).fetchone()[0]
            supplement = sum(a['v1_supplemental_runs'] for a in manifest['v1_audits'] if a['category_id']==partition['category_id'] and a['status']==partition['status'])
            assert actual == partition['api_count'] + supplement
            assert partition['api_count'] == partition['unique_runs']
print(json.dumps(report, indent=2))
(root / 'validation.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
