"""The reviewed Portal e2e and explicitly scoped full single-game collector."""
from pathlib import Path
import re

from .jsonutil import atomic_write, digest, read, utcnow
from .ods import Package, pack_response, resolve_payload, restore
from .source import Client, SourceError, staged_responses

STATUSES = {0: "new", 1: "verified", 2: "rejected"}
DATE_SPLIT_MIN_RUNS = 10000
DATE_SPLIT_MIN_PROFILES = 5000


def valid_game_slug(value):
    # These are source URL components, not local paths or IDs. The catalog also
    # contains leading/trailing dots and literal pipes; requests encode them in JSON.
    return (isinstance(value, str) and value not in (".", "..")
            and re.fullmatch(r"[a-zA-Z0-9_.|\-]{1,110}", value) is not None)


def validate_conf(conf):
    from .model import V2, V3
    result = _validate_conf(conf)
    model = conf.get('data_model', V2)
    if model not in (V2, V3):
        raise ValueError('Unknown ODS data model')
    if 'data_model' in conf:
        result['data_model'] = model
    if model == V3 and conf.get('repair_from_snapshot_id'):
        raise ValueError('Legacy repair must not mix with the new ODS contract')
    for key in ('game_label', 'display_game_id'):
        if key in conf:
            value = conf[key]
            if (not isinstance(value, str) or not 1 <= len(value) <= 256
                    or any(ord(c) < 32 for c in value)):
                raise ValueError('Invalid display identity')
            if key == 'display_game_id' and not re.fullmatch(r'[a-zA-Z0-9_-]{1,110}', value):
                raise ValueError('Invalid display game ID')
            if key == 'display_game_id' and result.get('game_id', value) != value:
                raise ValueError('Display identity differs from source game ID')
            result[key] = value
    if 'smoke' in conf:
        if type(conf['smoke']) is not bool or model != V3 or result['scope'] != 'single_game':
            raise ValueError('Smoke is an explicit entity-v3 single-game test only')
        result['smoke'] = conf['smoke']
    if 'smoke_category_id' in conf:
        cid=conf['smoke_category_id']
        if not result.get('smoke') or not isinstance(cid,str) or not re.fullmatch(r'[a-zA-Z0-9_-]{1,40}',cid):
            raise ValueError('A category limit is only valid for an explicit smoke test')
        result['smoke_category_id']=cid
    return result


def _validate_conf(conf):
    collection = conf.get("collection_id", "")
    if not re.fullmatch(r"[a-zA-Z0-9_-]{1,80}", collection):
        raise ValueError("collection_id must have 1..80 ASCII letters/digits/_/-")
    scope, game = conf.get("scope", "e2e"), conf.get("game", "portal")
    if scope == "single_game":
        identity = {k:conf[k] for k in ("game", "game_id") if k in conf}
        if len(identity) != 1 or any(not (valid_game_slug(v) if k == "game" else
                isinstance(v, str) and re.fullmatch(r"[a-zA-Z0-9_-]{1,110}", v)) for k,v in identity.items()):
            raise ValueError("Single game requires its explicit URL slug or source ID")
        if any(k in conf for k in ("selection_snapshot_id", "catalog_snapshot_id", "repair_from_snapshot_id", "repair_partitions")):
            raise ValueError("Single game cannot inherit a catalog/selection/repair snapshot")
        only = conf.get("metadata_only", False)
        if type(only) is not bool:
            raise ValueError("metadata_only must be boolean")
        budget = int(conf.get("request_budget", 2500))
        if not (5 if only else 100) <= budget <= 10000:
            raise ValueError("Single-game request budget is outside supported bounds")
        strategy = conf.get("run_partition_strategy", "standard")
        if strategy not in ("standard", "date_large_v1"):
            raise ValueError("Unknown fresh run partition strategy")
        return {"collection_id":collection,"scope":scope,**identity,"metadata_only":only,
                "request_budget":budget,"adaptive_rate":True,"run_partition_strategy":strategy}
    if scope == "ranking":
        parent = conf.get("catalog_snapshot_id", "")
        if not re.fullmatch(r"[a-zA-Z0-9_-]{1,110}", parent):
            raise ValueError("Explicit catalog snapshot required")
        top_n = int(conf.get("top_n", 10))
        if not 1 <= top_n <= 100:
            raise ValueError("top_n must be 1..100")
        policy = conf.get("ranking_policy")
        if policy not in (None, "exclude_ce_v1"):
            raise ValueError("Unknown ranking policy")
        return {"collection_id": collection, "scope": scope, "catalog_snapshot_id": parent,
                "top_n": top_n, "request_budget": 500, "adaptive_rate": True,
                **({"ranking_policy": policy} if policy else {})}
    if scope == "game":
        for key in ("game", "game_id", "selection_snapshot_id"):
            value = conf.get(key)
            if not (valid_game_slug(value) if key == "game" else
                    isinstance(value, str) and re.fullmatch(r"[a-zA-Z0-9_-]{1,110}", value)):
                raise ValueError("Game requires explicit source ID, URL and verified selection snapshot")
        budget = int(conf.get("request_budget", 5000))
        if not 100 <= budget <= 10000:
            raise ValueError("Game request budget must be 100..10000")
        result = {"collection_id": collection, "scope": scope,
                **{k:conf[k] for k in ("game", "game_id", "selection_snapshot_id")},
                "request_budget": budget, "adaptive_rate": True}
        if "repair_from_snapshot_id" in conf:
            parent = conf["repair_from_snapshot_id"]
            if not isinstance(parent, str) or not re.fullmatch(r"[a-zA-Z0-9_-]{1,105}-runs", parent) or parent == collection+"-runs":
                raise ValueError("Repair requires another explicit runs snapshot")
            parts = conf.get("repair_partitions", [])
            if not isinstance(parts, list) or not parts or any(
                not isinstance(p, dict) or set(p) != {"category_id", "status_code"} or
                not re.fullmatch(r"[a-zA-Z0-9_-]{1,40}", p.get("category_id", "")) or
                type(p.get("status_code")) is not int or p["status_code"] not in STATUSES for p in parts):
                raise ValueError("Repair requires explicit category/status partitions")
            if len({(p["category_id"], p["status_code"]) for p in parts}) != len(parts):
                raise ValueError("Duplicate repair partition")
            strategy = conf.get("run_partition_strategy")
            if strategy not in ("date_subdivision_v1", "standard"):
                raise ValueError("Repair requires an explicit standard or date subdivision strategy")
            result.update(repair_from_snapshot_id=parent, repair_partitions=parts,
                          run_partition_strategy=strategy)
        elif "repair_partitions" in conf:
            raise ValueError("Repair partitions require a base snapshot")
        elif "run_partition_strategy" in conf:
            if conf["run_partition_strategy"] not in ("standard", "date_large_v1"):
                raise ValueError("Unknown fresh run partition strategy")
            result["run_partition_strategy"] = conf["run_partition_strategy"]
        return result
    if scope == "ranked_game":
        for key in ("game", "game_id", "ranking_snapshot_id"):
            value = conf.get(key)
            if not (valid_game_slug(value) if key == "game" else
                    isinstance(value, str) and re.fullmatch(r"[a-zA-Z0-9_-]{1,110}", value)):
                raise ValueError("Ranked game needs explicit source IDs")
        return {"collection_id": collection, "scope": scope,
                **{k: conf[k] for k in ("game", "game_id", "ranking_snapshot_id")},
                "request_budget": 2500, "adaptive_rate": True,
                "deadline": "2026-10-16T20:00:00+00:00"}
    if scope == "e2e" and game == "portal" and conf.get("category", "Inbounds") == "Inbounds":
        return {"collection_id": collection, "scope": scope, "game": game, "category": "Inbounds", "request_budget": 30}
    if scope == "catalog" and 300 <= int(conf.get("request_budget", 0)) <= 1500:
        return {"collection_id": collection, "scope": scope, "request_budget": int(conf["request_budget"]), "adaptive_rate": True}
    if scope == "full_game" and game in ("half-life", "mc") and 30 <= int(conf.get("request_budget", 0)) <= (300 if game == "half-life" else 2500):
        return {"collection_id": collection, "scope": scope, "game": game,
                "request_budget": int(conf["request_budget"]), **({"adaptive_rate": True} if game == "mc" else {})}
    raise ValueError("Unknown collection scope; use single_game, catalog, ranking, game or a supported demonstration")


def snapshot_id(conf, kind):
    return conf["collection_id"] + "-" + kind


def location(state, conf, kind):
    return Path(state) / "snapshots" / snapshot_id(conf, kind)


def validate_page(body, page, baseline=None, game_id=None, category_id=None, status=None, date_range=None):
    p = body["pagination"]
    total, pages, per = (int(p[k]) for k in ("count", "pages", "per"))
    if per <= 0 or total < 0 or pages not in ({0, 1} if total == 0 else {(total + per - 1) // per}):
        raise SourceError("Invalid pagination totals")
    if int(p["page"]) != page:
        raise SourceError("Source returned another page")
    if page < 1 or page > max(1, pages):
        raise SourceError("Requested page is outside source pagination")
    if baseline is not None and any(p[k] != baseline[k] for k in ("count", "pages", "per")):
        raise SourceError("Pagination changed within a section")
    # The v2 endpoint omits runList entirely for a valid empty partition.
    rows = body.get("runList", [])
    if len(rows) != min(per, max(0, total - (page - 1) * per)):
        raise SourceError("Unexpected page length")
    ids = []
    for run in rows:
        if not run.get("id") or (game_id and run.get("gameId") != game_id) or (category_id and run.get("categoryId") != category_id) or run.get("levelId") or (status is not None and run.get("verified") != status):
            raise SourceError("Missing ID or game/category/status/IL filter violation")
        if date_range:
            from datetime import datetime, timezone
            try:
                instant = datetime.fromisoformat(run["performedAt"].replace("Z", "+00:00"))
                if instant.tzinfo is None:
                    raise ValueError("Missing date timezone")
                day = instant.astimezone(timezone.utc).date().isoformat()
                if not date_range[0] <= day <= date_range[1]:
                    raise ValueError("Outside requested interval")
            except (KeyError, TypeError, ValueError, AttributeError) as error:
                raise SourceError("Run date filter violation") from error
        ids.append(run["id"])
    if len(ids) != len(set(ids)):
        raise SourceError("Duplicate IDs within page")
    return ids


def boundary_signature(body):
    return digest({"pagination": body["pagination"], "ids": [r["id"] for r in body.get("runList", [])]})


def partition(client, root, game_id, category_id, status, cap=2, date_range=None, request_prefix=""):
    root = Path(root)
    name = category_id + "-" + STATUSES[status]
    request_name = request_prefix + "/" + name if request_prefix else name
    path = root / "checkpoints" / (name + ".json")
    progress = read(path) if path.exists() else {"pass": 0, "pages": {}, "complete": False, "resume": 0}
    if progress["complete"]:
        print(f"Runs: game={game_id} category={category_id} status={STATUSES[status]} checkpoint complete; skipped", flush=True)
        return progress["coverage"]
    drift_messages = ("Pagination changed within a section", "Boundary IDs changed on resume",
                      "Boundary IDs changed at end of section", "Repeated IDs across pages")
    def unstable():
        coverage = {"game_id": game_id, "category_id": category_id, "status": STATUSES[status],
            "status_code": status, "coverage": "partial", "intentional_limit": False,
            "diagnostic": "pagination_unstable_after_one_retry", "pass": progress["pass"],
            "errors": progress.get("errors", [])}
        progress.update(complete=True, coverage=coverage)
        atomic_write(path, progress)
        print(f"Runs: category={category_id} status={STATUSES[status]} partial: repeated pagination drift", flush=True)
        return coverage
    if progress["pass"] > 1:
        if all(e["error"] in drift_messages for e in progress.get("errors", [])):
            return unstable()
        raise SourceError("Previously failed section requires review of filter/source errors")
    params = {"gameId": game_id, "categoryId": category_id, "obsolete": 1,
              "video": 0, "emulator": 1, "verified": status, "values": []}
    context = {"game_id": game_id, "category_id": category_id, "status": STATUSES[status]}
    if date_range:
        from .date_partitions import date_params
        params.update(date_params(*date_range))
        context["run_date_range"] = list(date_range)
    while progress["pass"] <= 1:
        pass_no = progress["pass"]
        try:
            first = client.request("GetGameLeaderboard2", {"params": params, "page": 1}, f"{request_name}/pass{pass_no}/page1", context)
            baseline = first["body"]["pagination"]
            total_pages = max(1, int(baseline["pages"]))
            limit = total_pages if cap is None else min(total_pages, cap)
            drift = progress.setdefault("drift", {})

            def observe(response, page):
                nonlocal limit, total_pages
                body = response["body"]
                try:
                    batch = validate_page(body, page, game_id=game_id, category_id=category_id, status=status, date_range=date_range)
                except SourceError as error:
                    # A tail page scheduled before a live shrink can legitimately vanish.
                    # validate_page has already checked pagination shape and page number.
                    if str(error) != "Requested page is outside source pagination" or body.get("runList"):
                        raise
                    batch = None
                    drift["tail_shrank"] = True
                pagination = body["pagination"]
                if int(pagination["per"]) != int(baseline["per"]):
                    raise SourceError("Source pagination page size changed")
                if any(int(pagination[k]) != int(baseline[k]) for k in ("count", "pages")):
                    if not drift.get("pagination_changed"):
                        print(f"Runs: category={category_id} status={STATUSES[status]} "
                              f"pagination changed count={baseline['count']}->{pagination['count']}; "
                              "continuing traversal with partial coverage", flush=True)
                    counts = set(drift.get("observed_counts", [int(baseline["count"])]))
                    counts.add(int(pagination["count"]))
                    drift["observed_counts"] = sorted(counts)
                    drift["pagination_changed"] = True
                total_pages = max(total_pages, max(1, int(pagination["pages"])))
                current_pages = max(1, int(pagination["pages"]))
                limit = current_pages if cap is None else min(current_pages, cap)
                progress.update(source_pages=total_pages, pages_planned=limit,
                                source_count=int(baseline["count"]), drift=drift)
                return batch

            observe(first, 1)
            # Recheck incomplete boundaries using fresh request identities on each resume.
            if progress["pages"]:
                progress["resume"] += 1
                atomic_write(path, progress)
                for page in sorted({min(map(int, progress["pages"])), max(map(int, progress["pages"]))}):
                    fresh = client.request("GetGameLeaderboard2", {"params": params, "page": page},
                        f"{request_name}/pass{pass_no}/resume{progress['resume']}/{page}", context)
                    original = client.request("GetGameLeaderboard2", {"params": params, "page": page},
                        f"{request_name}/pass{pass_no}/page{page}", context)
                    current_ids = observe(fresh, page)
                    original_ids = [r["id"] for r in original["body"].get("runList", [])]
                    if current_ids != original_ids and not drift:
                        raise SourceError("Boundary IDs changed on resume")
                    if current_ids != original_ids:
                        drift["boundary_changed"] = True
            progress.update(source_pages=total_pages, pages_planned=limit,
                            source_count=int(baseline['count']))
            atomic_write(path, progress)
            print(f"Runs: game={game_id} category={category_id} status={STATUSES[status]} "
                  f"pages={limit}/{total_pages} source_count={baseline['count']} pass={pass_no}", flush=True)
            ids = set()
            window = getattr(client, "page_concurrency", 1) if hasattr(client, "request_pages") else 1
            print(f"Runs: page_window={window}; global HTTP pacing remains shared", flush=True)
            def responses():
                yield 1, first
                start = 2
                while start <= limit:
                    pages = list(range(start, min(limit + 1, start + window)))
                    specs = [({"params": params, "page": p}, f"{request_name}/pass{pass_no}/page{p}", context) for p in pages]
                    values = client.request_pages("GetGameLeaderboard2", specs) if window > 1 else [client.request("GetGameLeaderboard2", *s) for s in specs]
                    yield from zip(pages, values)
                    start = pages[-1] + 1
            for page, response in responses():
                batch = observe(response, page)
                if batch is None:
                    atomic_write(path, progress)
                    continue
                if ids.intersection(batch):
                    drift["repeated_ids_across_pages"] = True
                ids.update(batch)
                progress["pages"][str(page)] = {"response_id": response["response_id"], "signature": boundary_signature(response["body"])}
                progress["unique_runs"] = len(ids)
                atomic_write(path, progress)
                print(f"Runs: category={category_id} status={STATUSES[status]} page={page}/{limit} "
                      f"unique_runs={len(ids)} checking_boundaries={'next' if page == limit else 'pending'}", flush=True)
            for page in sorted({1, max(map(int, progress["pages"]))}):
                fresh = client.request("GetGameLeaderboard2", {"params": params, "page": page}, f"{request_name}/pass{pass_no}/end/{page}", context)
                observe(fresh, page)
                if boundary_signature(fresh["body"]) != progress["pages"][str(page)]["signature"]:
                    if not drift:
                        raise SourceError("Boundary IDs changed at end of section")
                    drift["boundary_changed"] = True
            limited = cap is not None and total_pages > cap
            coverage = {**context, "status_code": status, "coverage": "partial" if limited or drift else "verified_public_partitions",
                        "intentional_limit": limited, "api_count": int(baseline["count"]),
                        "api_pages": int(baseline["pages"]), "pages_received": len(progress["pages"]), "unique_runs": len(ids),
                        "pass": pass_no, "boundary_checks": "changed" if drift.get("boundary_changed") else "passed"}
            if drift:
                coverage.update(diagnostic="pagination_changed_during_traversal", drift=drift,
                                maximum_observed_pages=total_pages)
            progress.update(complete=True, coverage=coverage)
            atomic_write(path, progress)
            print(f"Runs: category={category_id} status={STATUSES[status]} completed coverage={coverage['coverage']}", flush=True)
            return coverage
        except SourceError as error:
            # HTTP/budget errors resume the same pass; only pagination/data drift restarts it.
            if "budget" in str(error).lower() or "attempt" in str(error).lower() or "request failed" in str(error).lower():
                raise
            if str(error) not in drift_messages:
                raise  # Wrong game/status/IL filters are failures, not coverage success.
            progress.setdefault("errors", []).append({"pass": pass_no, "error": str(error)})
            progress.update({"pass": pass_no + 1, "pages": {}, "resume": 0, "unique_runs": 0, "drift": {}})
            atomic_write(path, progress)
            if progress["pass"] > 1:
                return unstable()


def metadata(client, conf):
    slug = conf.get("game")
    direct = conf["scope"] == "single_game"
    query = {"gameId":conf["game_id"]} if direct and conf.get("game_id") else {"gameUrl":slug}
    print(f"Metadata: single game={slug or conf.get('game_id')}; GetGameData only; catalog not collected", flush=True)
    saved = client.request("GetGameData", query, "metadata", {"game_slug":slug,"game_id":conf.get("game_id")})
    body = saved["body"]
    game = body["game"]
    if conf["scope"] == "e2e":
        categories = [c for c in body["categories"] if c.get("isPerLevel") is False and c.get("name") == "Inbounds"]
        if len(categories) != 1:
            raise SourceError("Expected exactly one fresh full-game category named Inbounds; no substitution")
        return {"game_id": game["id"], "category_id": categories[0]["id"],
                "category_name": categories[0]["name"], "source_game_type": game.get("type"),
                "coverage": "partial", "intentional_limit": True, "catalog": "not_checked"}
    name = game.get("name", "").strip().casefold()
    if direct:
        matches = bool(game.get("id")) and (game["id"] == conf["game_id"] if conf.get("game_id") else game.get("url") == slug)
    elif conf["scope"] in ("ranked_game", "game"):
        matches = game["id"] == conf["game_id"]
    else:
        matches = name == {"half-life": "half-life", "mc": "minecraft: java edition"}[slug]
    if not matches:
        raise SourceError(f"Requested game identity mismatch: {game.get('name')!r}")
    if conf["scope"] in ("game", "single_game") and not conf.get("metadata_only"):
        from .ranking import game_classification
        if game_classification(game) != "in_scope":
            raise SourceError("Category Extensions are outside the selected run scope")
    categories = [c for c in body.get("categories", []) if c.get("isPerLevel") is False]
    unknown_category_count = sum("isPerLevel" not in c for c in body.get("categories", []))
    return {"game_id": game["id"], "game_name": game["name"], "source_game_type": game.get("type"),
            "categories": [{"id": c["id"], "name": c.get("name"), "archived": c.get("archived")}
                           for c in categories], "category_count": len(categories),
            "per_level_excluded": True, "unknown_category_count": unknown_category_count,
            "coverage": "metadata_observed" if not unknown_category_count else "partial_unknown_categories",
            "catalog": "not_checked"}


def collect_run_partition(client, root, gid, cid, status, conf, game):
    """Fresh opt-in routing; small runner lists retain the normal request path."""
    if conf.get("run_partition_strategy") == "date_large_v1":
        params={"gameId":gid,"categoryId":cid,"obsolete":1,"video":0,"emulator":1,"verified":status,"values":[]}
        first=client.request("GetGameLeaderboard2",{"params":params,"page":1},
            f"{cid}-{STATUSES[status]}/pass0/page1",{"game_id":gid,"category_id":cid,"status":STATUSES[status]})
        validate_page(first["body"],1,game_id=gid,category_id=cid,status=status)
        count=int(first["body"]["pagination"]["count"])
        profiles=len(first["body"].get("playerList",[]))
        year=(game.get("releaseDate") or {}).get("year")
        if count > DATE_SPLIT_MIN_RUNS and profiles >= DATE_SPLIT_MIN_PROFILES and year:
            from .date_partitions import date_partition
            print(f"Runs: date-large selected count={count} profiles={profiles}; reuse initial response",flush=True)
            return date_partition(client,root,gid,cid,status,int(year),baseline_response=first)
        print(f"Runs: standard path count={count} profiles={profiles} release_year={year}; initial response reused",flush=True)
    return partition(client,root,gid,cid,status,cap=None)


def collect_runs(client, root, metadata_package):
    from .entity_ods import EntityPackage
    if isinstance(metadata_package, EntityPackage):
        response = next(r for r in metadata_package.records("responses")
                        if r["metadata"]["endpoint"] == "GetGameData" and r["metadata"]["http_status"] == 200)
        body = metadata_package.decode(response["body"])
    else:
        response = next(r for r in metadata_package.records("responses") if r["endpoint"] == "GetGameData" and r["http_status"] == 200)
        body = restore(metadata_package.get, response)
    gid = body["game"]["id"]
    conf_path = Path(root) / "config.json"
    conf = read(conf_path)
    full_scope = conf["scope"] in ("full_game", "ranked_game", "game", "single_game")
    full_game = full_scope and not conf.get('smoke')
    category_rows = body.get("categories", [])
    if not isinstance(category_rows, list):
        raise SourceError("Invalid category list shape")
    if any(not isinstance(c, dict) or type(c.get("isPerLevel")) is not bool for c in category_rows):
        raise SourceError("Unknown full-game category classification")
    categories = [c for c in category_rows if c["isPerLevel"] is False and
                  (full_scope or c.get("name") == "Inbounds")]
    if conf.get('smoke'):
        if conf.get('smoke_category_id'):
            categories=[c for c in categories if c['id']==conf['smoke_category_id']]
            if not categories:raise SourceError('Requested smoke category absent from fresh full-game metadata')
        categories = categories[:1]
    if not categories and conf["scope"] in ("game", "single_game"):
        client.request("GetGameSummary", {"gameId":gid}, "stats-no-full-game-categories",
                       {"game_id":gid,"diagnostic":"no_full_game_categories_in_source_metadata"})
        return {"game_id": gid, "coverage": "unavailable", "partitions": [], "wr": [],
                "diagnostic": "no_full_game_categories_in_source_metadata",
                "category_list_present": "categories" in body, "source_category_count": len(category_rows)}
    if not categories or (not full_game and len(categories) != 1):
        raise SourceError("Requested full-game categories are missing or ambiguous")
    repair_result = None
    if conf.get("repair_from_snapshot_id"):
        from .repair import validate_base
        repair_result = validate_base(Path(root).parent.parent, conf)
        if repair_result["game_id"] != gid or set(repair_result["category_ids"]) != {c["id"] for c in categories}:
            raise SourceError("Repair metadata game/category set differs from the base snapshot")
        selected = {(p["category_id"], p["status_code"]) for p in conf["repair_partitions"]}
    client.request("GetGameSummary", {"gameId": gid}, "stats-before", {"game_id": gid, "phase": "before"})
    coverage = []
    for category_index, category in enumerate(categories, 1):
        cid = category["id"]
        print(f"Runs: game={gid} category={category_index}/{len(categories)} id={cid} name={category.get('name')}", flush=True)
        for status in (0, 1, 2):
            if repair_result and (cid, status) not in selected:
                inherited = next(p for p in repair_result["partitions"] if (p["category_id"], p["status_code"]) == (cid, status))
                coverage.append({**inherited, "retained_from_snapshot_id": conf["repair_from_snapshot_id"]})
                continue
            if repair_result and conf["run_partition_strategy"] == "date_subdivision_v1":
                from .date_partitions import date_partition
                release_year = body["game"].get("releaseDate", {}).get("year")
                if not release_year:
                    raise SourceError("Date repair requires an explicit release year in source metadata")
                coverage.append(date_partition(client, root, gid, cid, status, int(release_year)))
            else:
                coverage.append(collect_run_partition(client,root,gid,cid,status,conf,body["game"]) if full_game else
                                partition(client,root,gid,cid,status,cap=2))
    wr_coverage = [{**w, "retained_from_snapshot_id": conf["repair_from_snapshot_id"]}
                   for w in repair_result["wr"]] if repair_result else []
    from .model import version, V3
    for category in ([] if repair_result or version(conf) == V3 else categories):
        cid = category["id"]
        print(f"WR: game={gid} category={cid} fetching source history", flush=True)
        params = {"gameId": gid, "categoryId": cid, "obsolete": 1, "video": 0,
                  "emulator": 1, "verified": 1, "values": []}
        first = client.request("GetGameRecordHistory", {"params": params, "page": 1},
            f"wr/{cid}/page1", {"game_id": gid, "category_id": cid, "kind": "source_wr_history"})
        if first["body"] == {}:
            print(f"WR: category={cid} unavailable: empty object response", flush=True)
            # Observed v2 response for categories with no public record history.
            # Keep the exact empty response; it does not prove an empty history.
            wr_coverage.append({"category_id": cid, "pages_received": 1,
                "coverage": "unavailable", "diagnostic": "empty_object_response",
                "response_id": first["response_id"]})
            continue
        if not isinstance(first["body"].get("recordEventList"), list):
            raise SourceError("Unknown WR history shape: raw preserved, coverage not verified")
        pagination = first["body"].get("pagination")
        pages = int(pagination["pages"]) if pagination else 1
        for page in range(2, pages + 1):
            client.request("GetGameRecordHistory", {"params": params, "page": page},
                f"wr/{cid}/page{page}", {"game_id": gid, "category_id": cid, "kind": "source_wr_history"})
            print(f"WR: category={cid} page={page}/{pages}", flush=True)
        print(f"WR: category={cid} received_pages={pages} pagination={'present' if pagination else 'absent'}", flush=True)
        wr_coverage.append({"category_id": cid, "pages_received": pages,
            "coverage": "verified_public_partitions" if full_game and pagination else "partial"})
    client.request("GetGameSummary", {"gameId": gid}, "stats-after", {"game_id": gid, "phase": "after"})
    fully_verified = full_game and all(p["coverage"] == "verified_public_partitions" for p in coverage) and all(w["coverage"] == "verified_public_partitions" for w in wr_coverage)
    return {"game_id": gid, "category_ids": [c["id"] for c in categories],
            "coverage": "verified_public_partitions" if fully_verified else "partial",
            "intentional_limit": not full_game, "partitions": coverage, "wr": wr_coverage,
            **({"repair_from_snapshot_id": conf["repair_from_snapshot_id"],
                "repair_partitions": conf["repair_partitions"],
                "freshness": "retained_verified_partitions_and_WR_plus_fresh_repaired_partitions"} if repair_result else {})}


def copy_payload(source, target, key):
    item = source.get("payloads", key)
    for block in item.get("blocks", []):
        copy_payload(source, target, block)
    target.put("payloads", item)


def profiles(source, target):
    """Reuse original observations, never overwrite time/projection or manufacture HTTP."""
    participants = set()
    guests = []
    for observation in source.records("observations"):
        if observation["entity_type"] != "run":
            continue
        run = source.resolve(observation["payload"])
        for position, participant in enumerate(run.get("playerIds", [])):
            if isinstance(participant, str) and participant:
                participants.add(participant)
            else:
                guests.append({"run_id": run["id"], "position": position, "value": participant})
    found, copied, missing_name = set(), set(), set()
    key_count = 0
    for observation in source.records("observations"):
        if observation["entity_type"] != "player" or observation["source_id"] not in participants:
            continue
        profile = source.resolve(observation["payload"])
        # Guests with no registered identity are not merged by display name.
        if profile.get("isGuest") is True:
            continue
        found.add(observation["source_id"])
        if not profile.get("name"):
            missing_name.add(observation["source_id"])
        if observation["payload"] not in copied:
            copy_payload(source, target, observation["payload"])
            copied.add(observation["payload"])
        target.put("observations", observation)
        key_count += 1
    key_payload = target.payload_array(target.keys("observations"))
    target.flush()
    return {"coverage": "partial" if participants - found or missing_name else "verified_public_partitions",
            "scope": "participants_in_input_snapshot_only", "participants": len(participants),
            "available_profiles": len(found), "profile_observations": key_count,
            "unavailable_or_unresolved": sorted(participants - found), "missing_name": sorted(missing_name),
            "guests_without_id": guests, "profile_observation_keys_digest": key_payload.split(":", 1)[1],
            "profile_observation_keys_payload": key_payload,
            "profile_fields": "All embedded fields preserved; missing country is valid"}


def seal(state, conf, kind, result, error=None):
    root = location(state, conf, kind)
    package = Package(root / "package")
    for staged in staged_responses(root):
        pack_response(package, staged, snapshot_id(conf, kind), staged.get("context", {}))
    manifest = {"key": snapshot_id(conf, kind) + ":1", "snapshot_id": snapshot_id(conf, kind), "revision": 1,
        "collection_id": conf["collection_id"], "kind": kind, "scope": conf,
        "collector_version": "0.1.0", "format_version": "json-v1", "result": result,
        "execution_status": "failed" if error else "extracted", "error": error,
        "input_snapshot_ids": ([conf[k] for k in ("catalog_snapshot_id", "ranking_snapshot_id") if k in conf]
                               if kind == "games_metadata" else [snapshot_id(conf, "games_metadata" if kind == "runs" else "runs")]),
        "records": package.summary(("payloads", "responses", "observations"))}
    package.put("manifests", manifest)
    reference = {"snapshot_id": snapshot_id(conf, kind), "package_uri": str(package.root), "digest": digest(package.summary())}
    # The derived package can be regenerated from durable raw. Flush it as a
    # group before publishing extracted.json; raw/checkpoints still fsync per write.
    import os
    if hasattr(os, "sync"):
        os.sync()
    return reference


def extract(state, conf, kind):
    conf = validate_conf(conf)
    root = location(state, conf, kind)
    completed = root / "extracted.json"
    if completed.exists():
        return read(completed)
    if conf.get("game") == "mc" or conf["scope"] == "ranked_game":
        package_root = root / "package"
        if not package_root.exists():
            prepared = Package(package_root, compact=True)
            prepared.db.close()
    client = Client(state, conf["collection_id"], snapshot_id(conf, kind), budget=conf["request_budget"],
                    adaptive_rate=conf.get("adaptive_rate", False), deadline=conf.get("deadline"))
    if kind == "games_metadata":
        if conf["scope"] == "catalog":
            from .catalog import collect_catalog
            result = collect_catalog(client, root)
        elif conf["scope"] == "ranking":
            from .ranking import rank_top5
            result = rank_top5(client)
        else:
            result = metadata(client, conf)
    elif kind == "runs":
        from .source import BudgetExceeded
        try:
            result = collect_runs(client, root, Package(location(state, conf, "games_metadata") / "package"))
        except BudgetExceeded as error:
            result = {"coverage": "partial", "stop_reason": str(error), "intentional_limit": True,
                      "partitions": [read(p) for p in (root / "checkpoints").glob("*.json")]}
    elif kind == "players":
        result = profiles(Package(location(state, conf, "runs") / "package"), Package(root / "package"))
    else:
        raise ValueError(kind)
    reference = seal(state, conf, kind, result)
    atomic_write(completed, reference)
    return reference
