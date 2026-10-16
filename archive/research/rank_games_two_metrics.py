"""Website top-10 and full-game-statistics top-10, with a checked search boundary."""
import argparse
import csv
from datetime import datetime, timezone
from pathlib import Path

from collect import Client, save


def write_csv(path, rows):
    with path.open('w', encoding='utf-8-sig', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def ranked(rows, key):
    return sorted(rows, key=lambda row: (-int(row[key]), row['id']))[:10]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=Path('data/games_ranking_20261010'))
    args = parser.parse_args()
    root = args.output
    root.mkdir(parents=True, exist_ok=True)
    client = Client(root / 'full_game_check', 20)
    types = client.get('gametypes')['data']
    extension_ids = {item['id'] for item in types if item['name'] == 'Category Extension'}
    if not extension_ids:
        raise RuntimeError('Category Extension game type not found')
    page = 1
    first = client.v2('GetGameList', {'orderType': 8, 'limit': 200, 'page': page})
    site_top = [dict(rank=i, id=game['id'], name=game['name'], abbreviation=game['url'],
                     weblink='https://www.speedrun.com/' + game['url'],
                     site_run_count=int(game['runCount']))
                for i, game in enumerate(first['gameList'][:10], 1)]
    write_csv(root / 'top10_site.csv', site_top)
    candidates, contenders, seen = [], [], set()
    previous_count = None
    boundary = None
    directory_count = int(first['pagination']['count'])
    response = first
    while True:
        if int(response['pagination']['count']) != directory_count:
            raise RuntimeError('Directory size changed during ranking')
        for game in response['gameList']:
            total = int(game['runCount'])
            if previous_count is not None and total > previous_count:
                raise RuntimeError('Directory order is not descending by runCount')
            previous_count = total
            if game['id'] in seen:
                raise RuntimeError('Repeated game ID')
            seen.add(game['id'])
            leaders = ranked(contenders, 'full_game_verified_site_count')
            if len(leaders) == 10 and total < leaders[-1]['full_game_verified_site_count']:
                boundary = dict(first_unqueried_game=game['id'], first_unqueried_name=game['name'],
                                remaining_site_run_count_upper_bound=total,
                                tenth_full_game_count=leaders[-1]['full_game_verified_site_count'])
                break
            summary = client.v2('GetGameSummary', {'gameId': game['id']})
            stats = summary['stats']
            if stats['gameId'] != game['id']:
                raise RuntimeError('Unexpected game in summary')
            fg, il, challenge = [int(stats[key]) for key in ('totalRunsFG', 'totalRunsIL', 'totalRunsChallenge')]
            summary_total = int(stats['totalRuns'])
            if fg + il + challenge != summary_total:
                raise RuntimeError('Stats components do not equal total')
            row = dict(id=game['id'], name=game['name'], abbreviation=game['url'],
                       weblink='https://www.speedrun.com/' + game['url'], site_run_count=total,
                       summary_total_runs=summary_total, full_game_verified_site_count=fg,
                       site_level_run_count=il, site_challenge_run_count=challenge,
                       category_extension=None, gametypes=None)
            # Only a possible contender needs game-type classification. Others
            # cannot enter the top ten regardless of their game type.
            if len(leaders) < 10 or fg >= leaders[-1]['full_game_verified_site_count']:
                metadata = client.get('games/' + game['id'])['data']
                if metadata['id'] != game['id']:
                    raise RuntimeError('Unexpected game metadata')
                row['gametypes'] = ','.join(metadata.get('gametypes', []))
                row['category_extension'] = bool(extension_ids.intersection(metadata.get('gametypes', [])))
                if not row['category_extension']:
                    contenders.append(row)
            candidates.append(row)
            print(f'Checked {len(candidates)}: {game["name"]}; catalog={total}, FG={fg}; extension={row["category_extension"]}', flush=True)
        if boundary:
            break
        if page >= int(response['pagination']['pages']):
            boundary = {'directory_exhausted': True}
            break
        page += 1
        response = client.v2('GetGameList', {'orderType': 8, 'limit': 200, 'page': page})
    full_top = [dict(rank=i, **row) for i, row in enumerate(ranked(contenders, 'full_game_verified_site_count'), 1)]
    if len(full_top) != 10:
        raise RuntimeError('Fewer than ten eligible games')
    write_csv(root / 'full_game_candidates.csv', candidates)
    write_csv(root / 'top10_full_game_verified.csv', full_top)

    # Check the interpretation against explicitly verified, non-obsolete-filtered
    # category pagination for SMO; fetch only the first page of each category.
    gid = '76r55vd8'
    data = client.v2('GetGameData', {'gameId': gid})
    category_counts = []
    for category in data['categories']:
        if category['isPerLevel']:
            continue
        params = dict(gameId=gid, categoryId=category['id'], obsolete=1,
                      video=0, emulator=1, verified=1, values=[])
        leaderboard = client.v2('GetGameLeaderboard2', {'params': params, 'page': 1})
        for run in leaderboard.get('runList', []):
            if run['gameId'] != gid or run['categoryId'] != category['id'] or run.get('levelId') or run['verified'] != 1:
                raise RuntimeError('Leaderboard ignored verification/full-game filters')
        category_counts.append(dict(category_id=category['id'], name=category['name'],
                                    archived=bool(category.get('archived')), verified_count=int(leaderboard['pagination']['count'])))
        print(f'Counter validation: {category["name"]}: {category_counts[-1]["verified_count"]}', flush=True)
    reference = next(row for row in candidates if row['id'] == gid)
    verified_sum = sum(row['verified_count'] for row in category_counts)
    validation = dict(game_id=gid, categories=category_counts, verified_category_sum=verified_sum,
                      summary_full_game_count=reference['full_game_verified_site_count'],
                      matches=verified_sum == reference['full_game_verified_site_count'],
                      summary_minus_category_counts=reference['full_game_verified_site_count'] - verified_sum,
                      discrepancy_cause='Not established' if verified_sum != reference['full_game_verified_site_count'] else None)
    save(root / 'two_metrics_manifest.json', {
        'completed_at': datetime.now(timezone.utc).isoformat(),
        'directory_count': directory_count, 'directory_order_type': 8,
        'website_metric': 'GetGameList game.runCount, preserving API website order',
        'full_game_metric': 'GetGameSummary stats.totalRunsFG; verified-counter interpretation checked against SMO category pagination.',
        'full_game_scope': 'Full-game site statistics; Category Extensions excluded by v1 gametypes, level and challenge runs excluded.',
        'full_game_candidates_checked': len(candidates), 'search_boundary': boundary,
        'bound_assumption': 'A game full-game verified count cannot exceed its catalog run count; directory sorted descending. Counts obtained over time, not an atomic snapshot.',
        'snapshot_drift_observed': [row['id'] for row in candidates if row['summary_total_runs'] != row['site_run_count']],
        'counter_validation': validation,
        'tie_breaker_full_game': 'game ID ascending',
        'network_requests_this_execution': client.requests,
        'website_top10': site_top, 'full_game_top10': full_top,
        'full_run_download_executed': False,
        'limitations': 'Uses website statistics, not an independent recount of every game. Public API leaderboard counts may omit orphan records. No claim of atomic historical snapshot.',
    })
    report = '# Два рейтинга игр\n\n'
    for title, rows, metric in [('По счётчику каталога сайта', site_top, 'site_run_count'),
                                ('По полным подтверждённым забегам (статистика сайта)', full_top, 'full_game_verified_site_count')]:
        report += f'## {title}\n\n| Место | Игра | Забеги |\n|---:|---|---:|\n'
        report += '\n'.join(f'| {row["rank"]} | [{row["name"]}]({row["weblink"]}) | {row[metric]:,} |' for row in rows) + '\n\n'
    report += (
        'Первый рейтинг сохраняет порядок API каталога сайта (`orderType=8`). '
        'Второй использует `stats.totalRunsFG`; Category Extensions исключены по '
        'официальным метаданным `gametypes`, включая записи без такого текста в названии. '
        'Архивные категории не исключаются; прежние подтверждённые результаты входят '
        'в область счётчика (проверочный запрос: `obsolete=1`).\n\n'
        f'Для второго рейтинга проверено {len(candidates)} игр. Граница поиска: `{boundary}`. '
        'У остальных игр весь счётчик каталога ниже десятого значения полных забегов, '
        'поэтому они не могут попасть в этот топ при неизменных счётчиках. '
        'Это сравнение статистики сайта, а не независимый пересчёт всех ранов. '
        'Ответы получены в разное время, поэтому возможны изменения счётчиков.\n\n'
        f'Проверка Super Mario Odyssey: сумма явно подтверждённых ранов по полным '
        f'категориям = {verified_sum}; статистика `totalRunsFG` = '
        f'{reference["full_game_verified_site_count"]}. '
        'Расхождение, если оно есть, не исправляется догадками: его причина '
        'не установлена, а рейтинг сохраняет исходную статистику сайта.\n\n'
        'Файлы: `top10_site.csv`, `top10_full_game_verified.csv`, '
        '`full_game_candidates.csv`, `two_metrics_manifest.json`; исходные ответы '
        'в `full_game_check/raw/`. Полная выгрузка ранов игры не запускалась.\n')
    (root / 'two_metrics.md').write_text(report, encoding='utf-8')
    (root / 'README.md').write_text(report, encoding='utf-8')
    print(report, flush=True)


if __name__ == '__main__':
    main()
