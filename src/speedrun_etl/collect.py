"""The reviewed Portal e2e and explicitly scoped full single-game collector."""
from pathlib import Path
import re

from .jsonutil import atomic_write, digest, read, utcnow
from .ods import Package, pack_response, resolve_payload, restore
from .source import Client, SourceError, staged_responses

STATUSES = {0: "new", 1: "verified", 2: "rejected"}


def validate_conf(conf):
    collection = conf.get("collection_id", "")
    if not re.fullmatch(r"[a-zA-Z0-9_-]{1,80}", collection):
        raise ValueError("collection_id must have 1..80 ASCII letters/digits/_/-")
    scope, game = conf.get("scope", "e2e"), conf.get("game", "portal")
    if scope == "ranking":
        parent = conf.get("catalog_snapshot_id", "")
        if not re.fullmatch(r"[a-zA-Z0-9_-]{1,110}", parent):
            raise ValueError("Explicit catalog snapshot required")
        top_n = int(conf.get("top_n", 10))
        if not 1 <= top_n <= 100:
            raise ValueError("top_n must be 1..100")
        return {"collection_id": collection, "scope": scope, "catalog_snapshot_id": parent,
                "top_n": top_n, "request_budget": 500, "adaptive_rate": True}
    if scope == "game":
        for key in ("game", "game_id", "selection_snapshot_id"):
            if not re.fullmatch(r"[a-zA-Z0-9_-]{1,110}", conf.get(key, "")):
                raise ValueError("Game requires explicit source ID, URL and verified selection snapshot")
        budget = int(conf.get("request_budget", 5000))
        if not 100 <= budget <= 10000:
            raise ValueError("Game request budget must be 100..10000")
        return {"collection_id": collection, "scope": scope,
                **{k:conf[k] for k in ("game", "game_id", "selection_snapshot_id")},
                "request_budget": budget, "adaptive_rate": True}
    if scope == "ranked_game":
        for key in ("game", "game_id", "ranking_snapshot_id"):
            if not re.fullmatch(r"[a-zA-Z0-9_-]{1,110}", conf.get(key, "")):
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
    raise ValueError("Allowed scopes: Portal e2e, Half-Life, Minecraft Java Edition, catalog")


def snapshot_id(conf, kind):
    return conf["collection_id"] + "-" + kind


def location(state, conf, kind):
    return Path(state) / "snapshots" / snapshot_id(conf, kind)


def validate_page(body, page, baseline=None, game_id=None, category_id=None, status=None):
    p = body["pagination"]
    total, pages, per = (int(p[k]) for k in ("count", "pages", "per"))
    if per <= 0 or total < 0 or pages not in ({0, 1} if total == 0 else {(total + per - 1) // per}):
        raise SourceError("Invalid pagination totals")
    if int(p["page"]) != page:
        raise SourceError("Source returned another page")
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
        ids.append(run["id"])
    if len(ids) != len(set(ids)):
        raise SourceError("Duplicate IDs within page")
    return ids


def boundary_signature(body):
    return digest({"pagination": body["pagination"], "ids": [r["id"] for r in body.get("runList", [])]})


def partition(client, root, game_id, category_id, status, cap=2):
    root = Path(root)
    name = category_id + "-" + STATUSES[status]
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
    while progress["pass"] <= 1:
        pass_no = progress["pass"]
        try:
            # Recheck incomplete boundaries using fresh request identities on each resume.
            if progress["pages"]:
                progress["resume"] += 1
                atomic_write(path, progress)
                for page in sorted({min(map(int, progress["pages"])), max(map(int, progress["pages"]))}):
                    fresh = client.request("GetGameLeaderboard2", {"params": params, "page": page},
                        f"{name}/pass{pass_no}/resume{progress['resume']}/{page}", context)
                    if boundary_signature(fresh["body"]) != progress["pages"][str(page)]["signature"]:
                        raise SourceError("Boundary IDs changed on resume")
            first = client.request("GetGameLeaderboard2", {"params": params, "page": 1}, f"{name}/pass{pass_no}/page1", context)
            baseline = first["body"]["pagination"]
            total_pages = max(1, int(baseline["pages"]))
            limit = total_pages if cap is None else min(total_pages, cap)
            print(f"Runs: game={game_id} category={category_id} status={STATUSES[status]} "
                  f"pages={limit}/{total_pages} source_count={baseline['count']} pass={pass_no}", flush=True)
            ids = set()
            for page in range(1, limit + 1):
                response = first if page == 1 else client.request("GetGameLeaderboard2", {"params": params, "page": page}, f"{name}/pass{pass_no}/page{page}", context)
                batch = validate_page(response["body"], page, baseline, game_id, category_id, status)
                if ids.intersection(batch):
                    raise SourceError("Repeated IDs across pages")
                ids.update(batch)
                progress["pages"][str(page)] = {"response_id": response["response_id"], "signature": boundary_signature(response["body"])}
                atomic_write(path, progress)
                print(f"Runs: category={category_id} status={STATUSES[status]} page={page}/{limit} "
                      f"unique_runs={len(ids)} checking_boundaries={'next' if page == limit else 'pending'}", flush=True)
            for page in sorted({1, limit}):
                fresh = client.request("GetGameLeaderboard2", {"params": params, "page": page}, f"{name}/pass{pass_no}/end/{page}", context)
                validate_page(fresh["body"], page, baseline, game_id, category_id, status)
                if boundary_signature(fresh["body"]) != progress["pages"][str(page)]["signature"]:
                    raise SourceError("Boundary IDs changed at end of section")
            limited = cap is not None and total_pages > cap
            coverage = {**context, "status_code": status, "coverage": "partial" if limited else "verified_public_partitions",
                        "intentional_limit": limited, "api_count": int(baseline["count"]),
                        "api_pages": int(baseline["pages"]), "pages_received": limit, "unique_runs": len(ids),
                        "pass": pass_no, "boundary_checks": "passed"}
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
            progress.update({"pass": pass_no + 1, "pages": {}, "resume": 0})
            atomic_write(path, progress)
            if progress["pass"] > 1:
                return unstable()


def metadata(client, conf):
    slug = conf["game"]
    saved = client.request("GetGameData", {"gameUrl": slug}, "metadata", {"game_slug": slug})
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
    if conf["scope"] in ("ranked_game", "game"):
        matches = game["id"] == conf["game_id"]
    else:
        matches = name == {"half-life": "half-life", "mc": "minecraft: java edition"}[slug]
    if not matches:
        raise SourceError(f"Requested game identity mismatch: {game.get('name')!r}")
    categories = [c for c in body.get("categories", []) if c.get("isPerLevel") is False]
    unknown_category_count = sum("isPerLevel" not in c for c in body.get("categories", []))
    return {"game_id": game["id"], "game_name": game["name"], "source_game_type": game.get("type"),
            "categories": [{"id": c["id"], "name": c.get("name"), "archived": c.get("archived")}
                           for c in categories], "category_count": len(categories),
            "per_level_excluded": True, "unknown_category_count": unknown_category_count,
            "coverage": "metadata_observed" if not unknown_category_count else "partial_unknown_categories",
            "catalog": "not_checked"}


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
    full_game = conf["scope"] in ("full_game", "ranked_game", "game")
    categories = [c for c in body["categories"] if c.get("isPerLevel") is False and
                  (full_game or c.get("name") == "Inbounds")]
    if not categories and conf["scope"] == "game":
        return {"game_id": gid, "coverage": "unavailable", "partitions": [], "wr": [],
                "diagnostic": "no_full_game_categories_in_source_metadata"}
    if not categories or (not full_game and len(categories) != 1):
        raise SourceError("Requested full-game categories are missing or ambiguous")
    client.request("GetGameSummary", {"gameId": gid}, "stats-before", {"game_id": gid, "phase": "before"})
    coverage = []
    for category_index, category in enumerate(categories, 1):
        cid = category["id"]
        print(f"Runs: game={gid} category={category_index}/{len(categories)} id={cid} name={category.get('name')}", flush=True)
        for status in (0, 1, 2):
            coverage.append(partition(client, root, gid, cid, status, cap=None if full_game else 2))
    wr_coverage = []
    for category in categories:
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
            "intentional_limit": not full_game, "partitions": coverage, "wr": wr_coverage}


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
