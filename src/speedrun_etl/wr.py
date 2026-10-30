"""Собирает исходную историю WR отдельным этапом после runs.

Сохраняет ответы и checkpoints категорий; unavailable и partial не означают пустую историю."""

from pathlib import Path

from .jsonutil import atomic_write, read
from .source import SourceError


def collect_wr(client, root, metadata_package, conf):
    """Обходит категории свежих метаданных и возвращает coverage истории WR.

    Каждый ответ журналируется клиентом; checkpoints сохраняют готовность по категории."""
    response = next(
        r
        for r in metadata_package.records("responses")
        if r["metadata"]["endpoint"] == "GetGameData"
        and r["metadata"]["http_status"] == 200
    )
    body = metadata_package.decode(response["body"])
    gid = body["game"]["id"]
    rows = body.get("categories", [])
    if not isinstance(rows, list) or any(
        not isinstance(c, dict) or type(c.get("isPerLevel")) is not bool for c in rows
    ):
        raise SourceError("Unknown full-game category classification")
    categories = [
        c
        for c in rows
        if not c["isPerLevel"]
        and (conf["scope"] != "e2e" or c.get("name") == "Inbounds")
    ]
    if conf.get("smoke"):
        if conf.get("smoke_category_id"):
            categories = [c for c in categories if c["id"] == conf["smoke_category_id"]]
            if not categories:
                raise SourceError(
                    "Requested smoke category absent from fresh full-game metadata"
                )
        categories = categories[:1]
    if not categories:
        return {
            "game_id": gid,
            "coverage": "unavailable",
            "categories": [],
            "diagnostic": "no_full_game_categories_in_source_metadata",
        }
    results = []
    for index, category in enumerate(categories, 1):
        cid = category["id"]
        checkpoint = Path(root) / "checkpoints" / (cid + ".json")
        if checkpoint.exists() and read(checkpoint).get("complete"):
            results.append(read(checkpoint)["coverage"])
            print(
                f"WR: category={index}/{len(categories)} id={cid} checkpoint reused",
                flush=True,
            )
            continue
        params = {
            "gameId": gid,
            "categoryId": cid,
            "obsolete": 1,
            "video": 0,
            "emulator": 1,
            "verified": 1,
            "values": [],
        }
        saved_pages = {}
        result = {
            "category_id": cid,
            "category_name": category.get("name"),
            "filters": params,
            "pages_received": 0,
            "events_received": 0,
            "coverage": "partial",
        }
        pages = None
        initial_signature = None
        page = 1
        while True:
            print(
                f"WR: category={index}/{len(categories)} name={category.get('name')} id={cid} page={page}/{pages or '?'}",
                flush=True,
            )
            saved = client.request(
                "GetGameRecordHistory",
                {"params": params, "page": page},
                f"wr/{cid}/page{page}",
                {"game_id": gid, "category_id": cid, "kind": "source_wr_history"},
                allow_statuses=(404, 410),
            )
            saved_pages[str(page)] = {"response_id": saved["response_id"]}
            result["pages_received"] += 1
            value = saved["body"]
            if saved["http_status"] in (404, 410) or value == {}:
                result.update(
                    coverage="unavailable" if page == 1 else "partial",
                    diagnostic="http_unavailable"
                    if saved["http_status"] != 200
                    else "empty_object_response",
                    http_status=saved["http_status"],
                )
                break
            if not isinstance(value, dict) or not isinstance(
                value.get("recordEventList"), list
            ):
                result["diagnostic"] = "unknown_response_shape"
                break
            events = value["recordEventList"]
            if any(
                not isinstance(e, dict)
                or not isinstance(e.get("runList"), list)
                or any(
                    not isinstance(r, dict)
                    or not r.get("id")
                    or r.get("gameId", gid) != gid
                    or r.get("categoryId", cid) != cid
                    for r in e["runList"]
                )
                for e in events
            ):
                raise SourceError("WR history entity/filter mismatch; raw preserved")
            result["events_received"] += len(events)
            pagination = value.get("pagination")
            if pagination is None:
                result["diagnostic"] = "pagination_absent"
                break
            try:
                signature = tuple(int(pagination[k]) for k in ("pages", "per", "count"))
                valid = (
                    int(pagination["page"]) == page
                    and signature[0] >= 0
                    and signature[1] > 0
                    and signature[2] >= 0
                )
                valid = (
                    valid
                    and page <= max(1, signature[0])
                    and len(events) <= signature[1]
                )
            except (KeyError, TypeError, ValueError):
                valid = False
            if not valid:
                result["diagnostic"] = "invalid_pagination"
                break
            if initial_signature is None:
                initial_signature = signature
            elif initial_signature != signature:
                result["diagnostic"] = "pagination_changed"
                break
            pages = max(1, signature[0])
            if conf.get("smoke") or conf["scope"] == "e2e":
                result.update(
                    diagnostic="intentional_smoke_limit", intentional_limit=True
                )
                break
            if page == pages:
                if result["events_received"] == signature[2]:
                    result["coverage"] = "verified_public_partitions"
                else:
                    result["diagnostic"] = "event_count_mismatch"
                break
            page += 1
            atomic_write(
                checkpoint,
                {"complete": False, "pages": saved_pages, "coverage": result},
            )
        atomic_write(
            checkpoint, {"complete": True, "pages": saved_pages, "coverage": result}
        )
        results.append(result)
        print(
            f"WR: category={index}/{len(categories)} events={result['events_received']} coverage={result['coverage']}",
            flush=True,
        )
    return {
        "game_id": gid,
        "categories": results,
        "coverage": "verified_public_partitions"
        if all(r["coverage"] == "verified_public_partitions" for r in results)
        else "unavailable"
        if all(r["coverage"] == "unavailable" for r in results)
        else "partial",
        "intentional_limit": bool(conf.get("smoke") or conf["scope"] == "e2e"),
    }
