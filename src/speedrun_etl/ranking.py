"""Five games by fresh public full-game stats, with a checked directory bound."""
from .source import SourceError


def rank_top5(client):
    candidates, contenders, seen = [], [], set()
    previous = None
    baseline = None
    page = 1
    while True:
        directory = client.request("GetGameList", {"orderType": 8, "limit": 200, "page": page},
                                   f"ranking/directory/{page}", {"kind": "ranking_directory"})["body"]
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
            leaders = sorted(contenders, key=lambda g: (-g["verified_full_game_stats"], g["id"]))[:5]
            if len(leaders) == 5 and total < leaders[-1]["verified_full_game_stats"]:
                return {"coverage": "verified_public_partitions", "top5": leaders,
                        "candidates": candidates,
                        "metric": "GetGameSummary.stats.totalRunsFG (site verified full-game counter)",
                        "boundary": {"next_game_id": game["id"], "catalog_run_count": total,
                                     "fifth_full_game_count": leaders[-1]["verified_full_game_stats"]},
                        "limitations": "Non-atomic ranking by site counters, not an independent recount. "
                                        "Bound assumes total catalog runs >= full-game verified count. "
                                        "V2 game.type is retained; no name-based type classification."}
            summary = client.request("GetGameSummary", {"gameId": game["id"]},
                                     "ranking/stats/" + game["id"], {"game_id": game["id"], "kind": "ranking_stats"})["body"]
            stats = summary["stats"]
            if stats["gameId"] != game["id"]:
                raise SourceError("Ranking stats game mismatch")
            fg, il, challenge = (int(stats[k]) for k in ("totalRunsFG", "totalRunsIL", "totalRunsChallenge"))
            if fg + il + challenge != int(stats["totalRuns"]):
                raise SourceError("Ranking stats components mismatch")
            row = {k: game.get(k) for k in ("id", "name", "url", "type")}
            row.update(catalog_run_count=total, verified_full_game_stats=fg,
                       total_runs_stats=int(stats["totalRuns"]), level_runs_stats=il, challenge_runs_stats=challenge)
            candidates.append(row)
            if game.get("type") != "game":
                raise SourceError("Unknown contender classification; ranking requires review")
            contenders.append(row)
            print(f"Ranking: {game['name']}: full-game stats={fg}, catalog={total}", flush=True)
        if page >= int(baseline["pages"]):
            raise SourceError("Directory exhausted before ranking bound was established")
        page += 1
