"""Prove which records explain the website stats/leaderboard count difference."""
from datetime import datetime, timezone
import bisect
import json

from audit_smo_stats import ROOT, GID
from audit_smo_any import CID
from collect import Client, save


def main():
    runs = json.loads((ROOT / 'any_v1_runs.json').read_text(encoding='utf-8'))
    phase1 = json.loads((ROOT / 'phase1.json').read_text(encoding='utf-8'))
    orphans = [run for run in runs if not run.get('players')]
    client = Client(ROOT, 20)
    checks, cards = [], []
    times = sorted(run['times']['primary_t'] for run in runs)
    any_v2_count = next(row['v2_verified_count'] for row in phase1['categories'] if row['id'] == CID)
    for orphan in orphans:
        card = client.v2('GetRun', {'runId': orphan['id']})['run']
        if card['id'] != orphan['id'] or card['verified'] != 1 or card.get('orphaned') is not True or 'no players' not in card.get('issues', []):
            raise RuntimeError('V2 run card does not confirm orphan/no-players issue')
        cards.append(card)
        value = orphan['times']['primary_t']
        predicted_page = bisect.bisect_left(times, value) // 100 + 1
        params = dict(gameId=GID, categoryId=CID, obsolete=1, video=0, emulator=1,
                      verified=1, values=[], timer=0)
        actual = {}
        pages = list(range(max(1, predicted_page - 1), predicted_page + 2))
        ordered_times = []
        for page in pages:
            response = client.v2('GetGameLeaderboard2', {'params': params, 'page': page})
            if int(response['pagination']['count']) != any_v2_count or int(response['pagination']['page']) != page:
                raise RuntimeError('Leaderboard count/page changed')
            for run in response.get('runList', []):
                if run['id'] in actual or run.get('gameId') != GID or run.get('categoryId') != CID or run.get('levelId') or run.get('verified') != 1:
                    raise RuntimeError('Unexpected filtered leaderboard run')
                actual[run['id']] = run
                ordered_times.append(float(run['time'][:-1]))
        if ordered_times != sorted(ordered_times):
            raise RuntimeError('Leaderboard is not ordered by realtime')
        low, high = min(ordered_times), max(ordered_times)
        if not low < value < high:
            raise RuntimeError('Leaderboard pages do not bracket the target time')
        # Strict interior excludes incomplete groups of equal times at page edges.
        expected = {run['id'] for run in runs if low < run['times']['primary_t'] < high}
        observed = {rid for rid, run in actual.items() if low < float(run['time'][:-1]) < high}
        missing, extra = expected - observed, observed - expected
        check = dict(run_id=orphan['id'], pages=pages, time_low=low, time_high=high,
                     v1_count=len(expected), v2_count=len(observed),
                     missing_from_v2=sorted(missing), extra_in_v2=sorted(extra),
                     only_empty_player_run_missing=missing == {orphan['id']} and not extra)
        checks.append(check)
        print(json.dumps(check, ensure_ascii=False), flush=True)
        save(ROOT / 'leaderboard_neighborhood_checks.json', checks)
        if not check['only_empty_player_run_missing']:
            raise RuntimeError('Leaderboard neighborhood did not isolate the orphan record')
    small_sum = sum(row['v1_full_game_count'] for row in phase1['categories'] if 'v1_full_game_count' in row)
    verified_total = len(runs) + small_sum
    if verified_total != int(phase1['stats']['totalRunsFG']):
        raise RuntimeError('V1 recount does not equal website stats')
    if len(runs) - len(orphans) != any_v2_count:
        raise RuntimeError('Empty-player records do not fully account for the Any% difference')
    after = Client(ROOT / 'after', 20)
    stats_after = after.v2('GetGameSummary', {'gameId': GID})['stats']
    any_after = after.v2('GetGameLeaderboard2', {'params': dict(gameId=GID, categoryId=CID,
                         obsolete=1, video=0, emulator=1, verified=1, values=[]), 'page': 1})['pagination']
    report = dict(completed_at=datetime.now(timezone.utc).isoformat(),
                  stats_before=phase1['stats'], stats_after=stats_after,
                  v1_verified_full_game_total=verified_total,
                  v2_verified_full_game_total=phase1['v2_full_game_verified_sum'],
                  v1_any_verified_count=len(runs), v2_any_verified_count=any_v2_count,
                  v2_any_verified_count_after=int(any_after['count']),
                  small_categories_v1_sum=small_sum, missing_runs=orphans,
                  leaderboard_neighborhood_checks=checks, v2_orphan_run_cards=cards,
                  conclusion='Three verified full-game Any% records with an empty players list exist in v1 and are counted in game stats, but omitted from v2 leaderboard responses.',
                  mechanism_limit='The empty-player/leaderboard omission is observed directly. The server query implementation and why these player associations became empty are not exposed by the public APIs.')
    save(ROOT / 'report.json', report)
    ranking = ROOT.parent / 'games_ranking_20261010'
    ranking_manifest = ranking / 'two_metrics_manifest.json'
    if ranking_manifest.exists():
        manifest = json.loads(ranking_manifest.read_text(encoding='utf-8'))
        manifest['counter_validation'].update(
            discrepancy_cause=report['conclusion'],
            missing_run_ids=[run['id'] for run in orphans],
            independently_recounted_v1_full_game_total=verified_total,
            audit_report=str(ROOT / 'report.json'))
        save(ranking_manifest, manifest)
    update = (
        '\n\n## Установленная причина расхождения SMO\n\n'
        'Независимый пересчёт подтверждённых полных ранов v1 дал **23502** — '
        'статистика сайта верна для этого снимка. В лидерборде v2 отсутствуют '
        'три Any% с пустыми участниками: `y6p5jo1m`, `z01ewg4y`, `yj686v3z`. '
        'Карточки v2 помечают их `orphaned=true`, `issues=["no players"]`. '
        'Каждый пропуск подтверждён сравнением ID соседних страниц лидерборда '
        'с записями v1. Доказательства: '
        '[отчёт](../smo_stats_audit_20261010/REPORT.md).\n')
    for name in ('README.md', 'two_metrics.md'):
        path = ranking / name
        if path.exists():
            old = path.read_text(encoding='utf-8')
            if '## Установленная причина расхождения SMO' not in old:
                path.write_text(old + update, encoding='utf-8')
    table = '\n'.join(f'| [{run["id"]}]({run["weblink"]}) | {run["date"]} | {run["times"]["primary"]} | `verified` | `[]` |' for run in orphans)
    (ROOT / 'REPORT.md').write_text(
        '# Причина расхождения Super Mario Odyssey\n\n'
        'В API v1 найдены три подтверждённых Any% полной игры без участников: '
        '`players: []`. Они учитываются статистикой игры, но отсутствуют в '
        'ответах лидерборда API v2.\n\n'
        '| Ран | Дата | Время | Статус | Участники |\n|---|---|---|---|---|\n' + table + '\n\n'
        f'- Any% в v1: **{len(runs)}**; в v2: **{any_v2_count}**.\n'
        f'- Остальные пять полных категорий: **{small_sum}** в обоих API.\n'
        f'- Все подтверждённые полные забеги в v1: **{verified_total}** — совпадает с `totalRunsFG`.\n'
        f'- Сумма лидербордов v2: **{phase1["v2_full_game_verified_sum"]}**.\n\n'
        'Для Any% прочитаны перекрывающиеся первые и последние страницы v1; '
        'пересечение подтверждает отсутствие разрыва посередине. Для каждого '
        'подозрительного забега дополнительно прочитаны три соседние страницы '
        'лидерборда v2, охватывающие его время. Проверена сортировка по времени. '
        'Множества ID внутри интервала совпали с v1 за исключением именно '
        'этого рана без участников. Результаты — в `leaderboard_neighborhood_checks.json`. '
        'Карточки всех трёх ранов в v2 дополнительно подтверждают '
        '`orphaned: true` и `issues: ["no players"]`.\n\n'
        'Статистика перепроверена после сверки (`after/raw/`). Исходные ответы '
        'с URL и временем сохранены в `raw/`, полный результат — `report.json`.\n\n'
        'Факт отсутствующей привязки к участникам подтверждён публичными данными. '
        'Установить, были ли аккаунты удалены или участников удалили из ранов '
        'другим способом, по этим ответам нельзя. Точный SQL запроса сайта '
        'также недоступен; утверждение об INNER JOIN было бы предположением.\n\n'
        'Для ETL лидерборд v2 нельзя использовать как единственный источник: '
        'записи v1 нужно добавлять по ID, сохраняя пустой список участников '
        'и исходный JSON, без выдумывания имени или страны раннера.\n', encoding='utf-8')
    print(json.dumps({key: value for key, value in report.items() if key not in ('missing_runs', 'stats_before', 'stats_after')}, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
