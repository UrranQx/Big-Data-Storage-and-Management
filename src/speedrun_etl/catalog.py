"""Sequential fresh directory cards; checkpoints contain IDs, never full pages."""
from pathlib import Path

from .jsonutil import atomic_write, digest, read
from .source import SourceError


def validate_catalog_page(body, page, baseline=None):
    p = body["pagination"]
    count, pages, per = (int(p[k]) for k in ("count", "pages", "per"))
    if count < 0 or per <= 0 or pages != max(1, (count + per - 1) // per) or int(p["page"]) != page:
        raise SourceError("Invalid catalog pagination")
    if baseline and any(p[k] != baseline[k] for k in ("count", "pages", "per")):
        raise SourceError("Catalog pagination drift")
    ids = [g.get("id") for g in body.get("gameList", [])]
    if len(ids) != min(per, max(0, count - (page - 1) * per)) or not all(ids) or len(set(ids)) != len(ids):
        raise SourceError("Invalid catalog page length or IDs")
    return ids, digest({"pagination": p, "ids": ids})


def collect_catalog(client, root):
    path = Path(root) / "checkpoints" / "catalog.json"
    progress = read(path) if path.exists() else {"pass": 0, "pages": {}, "resume": 0, "complete": False}
    if progress["complete"]:
        return progress["result"]
    while progress["pass"] <= 1:
        prefix = f"catalog/pass{progress['pass']}"
        def request(page, label):
            return client.request("GetGameList", {"orderType": 1, "limit": 200, "page": page},
                                  f"{prefix}/{label}/{page}", {"kind": "catalog", "page": page})
        # Network failures are resumable, not evidence of catalog drift.
        try:
            if progress["pages"]:
                progress["resume"] += 1
                atomic_write(path, progress)
                for number in sorted({1, max(map(int, progress["pages"]))}):
                    fresh = request(number, f"resume{progress['resume']}")
                    _, signature = validate_catalog_page(fresh["body"], number)
                    if signature != progress["pages"][str(number)]["signature"]:
                        raise SourceError("Catalog boundary drift")
            first = request(1, "page")
            baseline = first["body"]["pagination"]
            validate_catalog_page(first["body"], 1)
            pages = int(baseline["pages"])
            progress.update(source_pages=pages, pages_planned=pages, source_count=int(baseline['count']))
            atomic_write(path, progress)
            seen = set()
            for number in range(1, pages + 1):
                cached = progress["pages"].get(str(number))
                if cached:
                    ids = cached["ids"]
                else:
                    saved = first if number == 1 else request(number, "page")
                    ids, signature = validate_catalog_page(saved["body"], number, baseline)
                    cached = {"ids": ids, "signature": signature, "response_id": saved["response_id"]}
                if seen.intersection(ids):
                    raise SourceError("Duplicate catalog IDs across pages")
                seen.update(ids)
                progress["pages"][str(number)] = cached
                progress["unique_games"] = len(seen)
                atomic_write(path, progress)
                if number == 1 or number % 20 == 0 or number == pages:
                    print(f"Catalog: {number}/{pages} pages, {len(seen)} games", flush=True)
            for number in sorted({1, pages}):
                fresh = request(number, "end")
                _, signature = validate_catalog_page(fresh["body"], number, baseline)
                if signature != progress["pages"][str(number)]["signature"]:
                    raise SourceError("Catalog boundary drift")
            if len(seen) != int(baseline["count"]):
                raise SourceError("Catalog count mismatch")
            result = {"coverage": "verified_public_partitions", "unique_games": len(seen),
                      "pages": pages, "pass": progress["pass"], "boundary_checks": "passed",
                      "scope": "Global directory cards, no game type filter; not per-game details"}
            progress.update(complete=True, result=result)
            atomic_write(path, progress)
            return result
        except SourceError as error:
            if not str(error).startswith(("Catalog", "Invalid catalog", "Duplicate catalog")):
                raise
            progress.setdefault("errors", []).append(str(error))
            progress.update({"pass": progress["pass"] + 1, "pages": {}, "resume": 0})
            atomic_write(path, progress)
            if progress["pass"] > 1:
                raise SourceError("Catalog remains partial after one restart") from error
