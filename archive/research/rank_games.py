"""Rank a saved game catalog by its site counter, keeping metric scope explicit."""
import argparse
import csv
from datetime import datetime, timezone
import json
from pathlib import Path

from collect import Client, save


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--catalog', type=Path, default=Path('data/games_catalog_20261010'))
    parser.add_argument('--output', type=Path, default=Path('data/games_ranking_20261010'))
    parser.add_argument('--compare-stats', action='store_true')
    args = parser.parse_args()
    with (args.catalog / 'processed/games.csv').open(encoding='utf-8-sig', newline='') as handle:
        games = list(csv.DictReader(handle))
    if len({game['id'] for game in games}) != len(games):
        raise RuntimeError('Duplicate catalog IDs')
    if any(not game['site_run_count'] for game in games):
        raise RuntimeError('Missing ranking counter')
    games.sort(key=lambda game: (-int(game['site_run_count']), game['id']))
    fields = ['rank', 'id', 'name', 'abbreviation', 'weblink', 'site_run_count',
              'site_total_player_count', 'site_active_player_count']
    rows = [dict(rank=rank, **{key: game[key] for key in fields if key != 'rank'})
            for rank, game in enumerate(games, 1)]
    args.output.mkdir(parents=True, exist_ok=True)
    for name, selected in [('ranking.csv', rows), ('top10.csv', rows[:10])]:
        with (args.output / name).open('w', encoding='utf-8-sig', newline='') as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows(selected)
    comparison = []
    client = Client(args.output / 'stats_check', 20)
    if args.compare_stats:
        for row in rows[:10]:
            response = client.v2('GetGameSummary', {'gameId': row['id']})
            stats = response['stats']
            if stats['gameId'] != row['id']:
                raise RuntimeError('Wrong game in stats response')
            entry = dict(row, summary_total_runs=int(stats['totalRuns']),
                         summary_full_game_runs=int(stats['totalRunsFG']),
                         summary_level_runs=int(stats['totalRunsIL']),
                         summary_challenge_runs=int(stats['totalRunsChallenge']))
            entry['summary_components_equal_total'] = (
                entry['summary_full_game_runs'] + entry['summary_level_runs'] +
                entry['summary_challenge_runs'] == entry['summary_total_runs'])
            comparison.append(entry)
            print(json.dumps(entry, ensure_ascii=False), flush=True)
        with (args.output / 'top10_stats_comparison.csv').open('w', encoding='utf-8-sig', newline='') as handle:
            writer = csv.DictWriter(handle, fieldnames=list(comparison[0]))
            writer.writeheader()
            writer.writerows(comparison)
    catalog_manifest = json.loads((args.catalog / 'manifest.json').read_text(encoding='utf-8'))
    save(args.output / 'manifest.json', {
        'created_at': datetime.now(timezone.utc).isoformat(),
        'metric': 'site_run_count (API game.runCount)',
        'scope': 'Catalog counter; not a global ranking of full-game runs across all verification statuses.',
        'catalog': str(args.catalog), 'catalog_snapshot_first_response_at': catalog_manifest.get('snapshot_first_response_at'),
        'catalog_snapshot_last_response_at': catalog_manifest.get('snapshot_last_response_at'),
        'ranked_games': len(rows), 'tie_breaker': 'game ID ascending', 'tenth_game': rows[9],
        'stats_comparison': comparison, 'stats_network_requests_this_execution': client.requests,
        'stats_snapshot_note': 'Summary counts fetched separately from catalog; differences can reflect newer data.',
        'runs_download_stage_executed': False,
    })
    table = ['| Место | Игра | Счётчик каталога |', '|---:|---|---:|']
    table.extend(f'| {row["rank"]} | [{row["name"]}]({row["weblink"]}) | {int(row["site_run_count"]):,} |'
                 for row in rows[:10])
    (args.output / 'README.md').write_text(
        '# Топ-10 по счётчику каталога\n\n' + '\n'.join(table) + '\n\n'
        'Этот рейтинг использует `runCount` из сохранённого каталога, а не число '
        'полных забегов во всех статусах. Level runs входят в счётчик сайта. '
        'Статистику `totalRunsFG`, `totalRunsIL`, `totalRunsChallenge` для этих '
        'десяти игр можно сравнить в `top10_stats_comparison.csv`. Это только '
        'десять лидеров по счётчику каталога, а не доказанный глобальный топ '
        'по полным забегам.\n\n'
        '`ranking.csv` содержит весь каталог в порядке счётчика, `top10.csv` '
        'содержит первые десять. Выгрузка всех ранов выбранной игры не запускалась.\n',
        encoding='utf-8')
    print(json.dumps({'top10': rows[:10], 'output': str(args.output)}, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
