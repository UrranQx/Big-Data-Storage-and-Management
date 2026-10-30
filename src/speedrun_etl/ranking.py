"""Выбирает игры по свежей статистике full-game с проверенной границей каталога.

Сохраняет исходные ответы через общий клиент; каталог сам по себе не заменяет статистику."""

from .source import SourceError


def game_classification(game):
    # v2 GameType enum, corroborated by live mcce card: baseGameId + [8].
    # https://github.com/ManicJamie/speedruncompy/blob/master/src/speedruncompy/datatypes/enums.py
    types = game.get("gameTypeIds", [])
    if (
        game.get("type") != "game"
        or not isinstance(types, list)
        or any(type(t) is not int or t not in range(1, 14) for t in types)
    ):
        raise SourceError(
            "Unknown game classification; inspect source type/gameTypeIds"
        )
    return "category_extensions" if 8 in types else "in_scope"


def rank_games(client, top_n=10):
    if not 1 <= int(top_n) <= 100:
        raise ValueError("top_n must be 1..100")
    top_n = int(top_n)
    candidates, contenders, seen, excluded = [], [], set(), []
    previous = None
    baseline = None
    page = 1
    while True:
        directory = client.request(
            "GetGameList",
            {"orderType": 8, "limit": 200, "page": page},
            f"ranking/directory/{page}",
            {"kind": "ranking_directory"},
        )["body"]
        from .catalog import validate_catalog_page

        validate_catalog_page(directory, page, baseline)
        baseline = baseline or directory["pagination"]
        for game in directory.get("gameList", []):
            total = int(game["runCount"])
            if previous is not None and total > previous:
                raise SourceError("Ranking directory is not sorted by run count")
            if game["id"] in seen:
                raise SourceError("Duplicate ranking game")
            seen.add(game["id"])
            previous = total
            leaders = sorted(
                contenders, key=lambda g: (-g["verified_full_game_stats"], g["id"])
            )[:top_n]
            if (
                len(leaders) == top_n
                and total < leaders[-1]["verified_full_game_stats"]
            ):
                return {
                    "coverage": "verified_public_partitions",
                    "selected": leaders,
                    "top_n": top_n,
                    **({"top5": leaders} if top_n == 5 else {}),
                    "candidates": candidates,
                    "excluded": excluded,
                    "ranking_policy": "exclude_ce_v1",
                    "metric": "GetGameSummary.stats.totalRunsFG (site verified full-game counter)",
                    "boundary": {
                        "next_game_id": game["id"],
                        "catalog_run_count": total,
                        "cutoff_full_game_count": leaders[-1][
                            "verified_full_game_stats"
                        ],
                    },
                    "limitations": "Non-atomic ranking by site counters, not an independent recount. "
                    "Bound assumes total catalog runs >= full-game verified count. "
                    "Category Extensions excluded by v2 gameTypeIds=8; no name heuristic.",
                }
            classification = game_classification(game)
            if classification == "category_extensions":
                excluded.append(
                    {
                        "id": game["id"],
                        "url": game.get("url"),
                        "gameTypeIds": game.get("gameTypeIds"),
                        "baseGameId": game.get("baseGameId"),
                        "reason": classification,
                    }
                )
                print(
                    f"Ranking: excluded id={game['id']} gameTypeIds={game.get('gameTypeIds')}",
                    flush=True,
                )
                continue
            summary = client.request(
                "GetGameSummary",
                {"gameId": game["id"]},
                "ranking/stats/" + game["id"],
                {"game_id": game["id"], "kind": "ranking_stats"},
            )["body"]
            stats = summary.get("stats")
            if not isinstance(stats, dict):
                matches = [
                    s
                    for s in summary.get("gameStats", [])
                    if s.get("gameId") == game["id"]
                ]
                if len(matches) != 1:
                    raise SourceError("Unknown game statistics shape")
                stats = matches[0]
            if stats["gameId"] != game["id"]:
                raise SourceError("Ranking stats game mismatch")
            fg, il, challenge = (
                int(stats[k])
                for k in ("totalRunsFG", "totalRunsIL", "totalRunsChallenge")
            )
            if fg + il + challenge != int(stats["totalRuns"]):
                raise SourceError("Ranking stats components mismatch")
            row = {k: game.get(k) for k in ("id", "name", "url", "type")}
            row.update(
                catalog_run_count=total,
                verified_full_game_stats=fg,
                total_runs_stats=int(stats["totalRuns"]),
                level_runs_stats=il,
                challenge_runs_stats=challenge,
            )
            candidates.append(row)
            contenders.append(row)
            print(
                f"Ranking: {game['name']}: full-game stats={fg}, catalog={total}",
                flush=True,
            )
        if page >= int(baseline["pages"]):
            selected = sorted(
                contenders, key=lambda g: (-g["verified_full_game_stats"], g["id"])
            )[:top_n]
            return {
                "coverage": "verified_public_partitions"
                if len(selected) == top_n
                else "partial",
                "selected": selected,
                "top_n": top_n,
                "candidates": candidates,
                "excluded": excluded,
                "ranking_policy": "exclude_ce_v1",
                "boundary": "directory_exhausted",
                "metric": "GetGameSummary.stats.totalRunsFG",
                **({"top5": selected} if top_n == 5 else {}),
            }
        page += 1


def rank_top5(client):
    return rank_games(client, 5)
