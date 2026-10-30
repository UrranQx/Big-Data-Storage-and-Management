"""Делит раздел категории/статуса на диапазоны дат по явному запросу.

Сверяет исходные counts и уникальные ID; пробелы дат и дрейф не скрываются за verified."""

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from .collect import STATUSES, partition, validate_page
from .jsonutil import atomic_write, read
from .source import SourceError


def date_params(start, end):
    first, last = date.fromisoformat(start), date.fromisoformat(end)
    if first > last:
        raise ValueError("Invalid run date interval")

    def value(day):
        return {"year": day.year, "month": day.month, "day": day.day}

    return {"minRunDate": value(first), "maxRunDate": value(last)}


def annual_ranges(release_year, current_year):
    first = max(1901, min(int(release_year), current_year))
    if not 1901 <= current_year < 2100:
        raise ValueError("Date subdivision requires a reviewed calendar horizon")
    return (
        [("1900-01-01", f"{first - 1}-12-31")]
        + [
            (f"{year}-01-01", f"{year}-12-31")
            for year in range(first, current_year + 1)
        ]
        + [(f"{current_year + 1}-01-01", "2100-12-31")]
    )


def fallback_for_date_gap(client, root, game_id, category_id, status, point):
    """A stable deficit needs an independent unfiltered traversal, not a waiver."""
    result = point["coverage"]
    count = result.get("api_count", 0)
    if not (
        result.get("diagnostic") == "date_partition_coverage_mismatch"
        and count == result.get("api_count_after")
        and result.get("unique_runs") == result.get("leaf_count_sum")
        and result["unique_runs"] < count
        and not result.get("overlapping_ids")
        and all(
            p["coverage"] == "verified_public_partitions"
            for p in point.get("leaves", [])
        )
    ):
        return result
    name = category_id + "-" + STATUSES[status]
    root = Path(root)
    atomic_write(root / "date-parts" / name / "attempt.json", point)
    print(
        f"Date split: category={category_id} status={STATUSES[status]} "
        f"missing={count - result['unique_runs']}; checking full unfiltered partition",
        flush=True,
    )
    fallback_root = root / "date-fallback" / name
    checked = partition(
        client,
        fallback_root,
        game_id,
        category_id,
        status,
        cap=None,
        request_prefix="date-fallback",
    )
    fallback_point = read(fallback_root / "checkpoints" / (name + ".json"))
    coverage = {
        **checked,
        "strategy": "unfiltered_fallback_v1",
        "date_subdivision_attempt": result,
    }
    # The selected pages now refer to the unfiltered pass. Old dated answers stay raw/history.
    atomic_write(
        root / "checkpoints" / (name + ".json"),
        {**fallback_point, "coverage": coverage, "strategy": "unfiltered_fallback_v1"},
    )
    return coverage


def date_partition(
    client,
    root,
    game_id,
    category_id,
    status,
    release_year,
    max_runs=1500,
    baseline_response=None,
):
    root = Path(root)
    name = category_id + "-" + STATUSES[status]
    checkpoint = root / "checkpoints" / (name + ".json")
    if checkpoint.exists() and read(checkpoint).get("complete"):
        return fallback_for_date_gap(
            client, root, game_id, category_id, status, read(checkpoint)
        )
    plan_path = root / "date-parts" / name / "plan.json"
    config = {
        "game_id": game_id,
        "category_id": category_id,
        "status_code": status,
        "max_runs": max_runs,
        "release_year": int(release_year),
    }
    if plan_path.exists():
        plan = read(plan_path)
        if plan["config"] != config:
            raise ValueError("Date split configuration changed during resume")
    else:
        plan = {
            "config": config,
            "ranges": [
                list(v)
                for v in annual_ranges(release_year, datetime.now(timezone.utc).year)
            ],
        }
        atomic_write(plan_path, plan)
    params = {
        "gameId": game_id,
        "categoryId": category_id,
        "verified": status,
        "obsolete": 1,
        "video": 0,
        "emulator": 1,
        "values": [],
    }
    context = {
        "game_id": game_id,
        "category_id": category_id,
        "status": STATUSES[status],
        "strategy": "date_subdivision_v1",
    }
    before = (
        baseline_response
        if baseline_response is not None
        else client.request(
            "GetGameLeaderboard2",
            {"params": params, "page": 1},
            name + "/date-baseline-before",
            context,
        )
    )
    validate_page(
        before["body"], 1, game_id=game_id, category_id=category_id, status=status
    )
    before_count = int(before["body"]["pagination"]["count"])
    leaves, saved_pages, all_ids = [], {}, set()
    overlaps = set()

    def visit(start, end):
        filtered = {**params, **date_params(start, end)}
        print(
            f"Date split: probing category={category_id} status={STATUSES[status]} range={start}..{end}",
            flush=True,
        )
        # Same key as partition pass 0: a successful probe is not downloaded again.
        first = client.request(
            "GetGameLeaderboard2",
            {"params": filtered, "page": 1},
            name + "/pass0/page1",
            {**context, "run_date_range": [start, end]},
        )
        validate_page(
            first["body"],
            1,
            game_id=game_id,
            category_id=category_id,
            status=status,
            date_range=(start, end),
        )
        count = int(first["body"]["pagination"]["count"])
        print(
            f"Date split: category={category_id} status={STATUSES[status]} range={start}..{end} source_count={count}",
            flush=True,
        )
        if count > max_runs and start != end:
            a, b = date.fromisoformat(start), date.fromisoformat(end)
            middle = a + (b - a) // 2
            visit(start, middle.isoformat())
            visit((middle + timedelta(days=1)).isoformat(), end)
            return
        leaf_root = root / "date-parts" / name / (start + "_" + end)
        coverage = partition(
            client,
            leaf_root,
            game_id,
            category_id,
            status,
            cap=None,
            date_range=(start, end),
        )
        point = read(leaf_root / "checkpoints" / (name + ".json"))
        leaf_ids = set()
        for page, saved in point["pages"].items():
            response = client.request(
                "GetGameLeaderboard2",
                {"params": filtered, "page": int(page)},
                f"{name}/pass{point['pass']}/page{page}",
                context,
            )
            if response["response_id"] != saved["response_id"]:
                raise SourceError("Date checkpoint response identity changed")
            leaf_ids.update(r["id"] for r in response["body"].get("runList", []))
            saved_pages[start + "_" + end + "/" + page] = saved
        overlaps.update(all_ids & leaf_ids)
        all_ids.update(leaf_ids)
        leaves.append({**coverage, "date_range": [start, end]})
        atomic_write(
            checkpoint,
            {
                "complete": False,
                "pages": saved_pages,
                "unique_runs": len(all_ids),
                "source_count": before_count,
                "leaves": leaves,
                "strategy": "date_subdivision_v1",
            },
        )

    for start, end in plan["ranges"]:
        visit(start, end)
    after = client.request(
        "GetGameLeaderboard2",
        {"params": params, "page": 1},
        name + "/date-baseline-after",
        context,
    )
    validate_page(
        after["body"], 1, game_id=game_id, category_id=category_id, status=status
    )
    after_count = int(after["body"]["pagination"]["count"])
    leaf_count = sum(p.get("api_count", 0) for p in leaves)
    complete = (
        not overlaps
        and before_count == after_count == leaf_count == len(all_ids)
        and all(p["coverage"] == "verified_public_partitions" for p in leaves)
    )
    coverage = {
        **context,
        "status_code": status,
        "intentional_limit": False,
        "coverage": "verified_public_partitions" if complete else "partial",
        "api_count": before_count,
        "api_count_after": after_count,
        "leaf_count_sum": leaf_count,
        "unique_runs": len(all_ids),
        "pages_received": len(saved_pages),
        "leaf_partitions": len(leaves),
        "overlapping_ids": sorted(overlaps),
        "date_horizon": ["1900-01-01", "2100-12-31"],
    }
    if not complete:
        coverage["diagnostic"] = "date_partition_coverage_mismatch"
    point = {
        "complete": True,
        "pages": saved_pages,
        "unique_runs": len(all_ids),
        "coverage": coverage,
        "leaves": leaves,
        "strategy": "date_subdivision_v1",
    }
    atomic_write(checkpoint, point)
    return fallback_for_date_gap(client, root, game_id, category_id, status, point)
