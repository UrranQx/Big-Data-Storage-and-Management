"""Bounded crossover study on real pages; uses the production journal/limiter."""
import argparse
import os
from pathlib import Path
from statistics import median
from datetime import datetime
from urllib.request import urlopen

from .collect import validate_page
from .http_transport import PooledTransport
from .jsonutil import atomic_write, digest, dumps, read
from .source import Client, SourceError


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--collection", required=True)
    parser.add_argument("--source-collection", required=True)
    parser.add_argument("--pages", type=int, nargs="+", default=[100, 101, 102, 103])
    parser.add_argument("--post", action="store_true")
    parser.add_argument("--window", type=int, choices=(1,2,4), default=1)
    args = parser.parse_args()
    import re
    if any(not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", c) for c in (args.collection, args.source_collection)):
        parser.error("Invalid collection ID")
    if not 2 <= len(args.pages) <= 8 or len(set(args.pages)) != len(args.pages) or min(args.pages) < 1:
        parser.error("Provide 2..8 distinct positive pages")
    if args.window > 1 and (len(args.pages) % (args.window*2) or args.post):
        parser.error("Window study needs complete serial/parallel groups and no --post")
    state = Path(os.environ["ETL_STATE"])
    source = state / "snapshots" / (args.source_collection + "-runs")
    choices = [read(p) for p in (source / "checkpoints").glob("*.json")]
    candidates = [c for c in choices if c.get("source_pages", 0) >= max(args.pages)]
    if not candidates:
        raise ValueError("No observed source section has the requested pages")
    section = max(candidates, key=lambda c: c["source_pages"])
    # Read the durable first response identified by the checkpoint, not a guessed game/category.
    first_id = section["pages"]["1"]["response_id"]
    first = next(read(p) for p in (source / "requests").glob("*/*.json") if first_id in p.name)
    params = first["params"]["params"]
    root = state / "http-studies" / args.collection
    root.mkdir(parents=True, exist_ok=True)
    config = {"source_collection": args.source_collection, "params": params, "pages": args.pages, "post": args.post,
              **({"window": args.window} if args.window > 1 else {})}
    if (root / "config.json").exists() and read(root / "config.json") != config:
        raise ValueError("Cannot change an existing study scope")
    atomic_write(root / "config.json", config)
    pool = PooledTransport()
    clients = {mode: Client(state, args.collection, args.collection + "-" + mode,
        budget=32, initial_pause=60, max_rpm=20, max_attempts=1,
        transport=urlopen if mode == "urlopen" else pool,
        http_method="POST" if mode == "pooled_post" else "GET")
        for mode in ("urlopen", "pooled_get", "pooled_post")}
    plan = [(mode, page) for index, page in enumerate(args.pages)
            for mode in (("urlopen", "pooled_get") if index % 2 == 0 else ("pooled_get", "urlopen"))]
    if args.post:
        plan += [("pooled_post", page) for page in args.pages[:2]]
    rows = []
    report = {"config": config, "rows": rows, "limitations": "Small crossover; cache/server load may differ. No API limit claim."}
    def record(mode, page, saved):
        ids = validate_page(saved["body"], page, game_id=params["gameId"],
                            category_id=params["categoryId"], status=params["verified"])
        row = {"transport": mode, "page": page, "run_count": len(ids),
            "run_ids_digest": digest(ids), "body_digest": digest(saved["body"]),
            **{k: saved.get(k) for k in ("elapsed_ms", "headers_ms", "download_ms", "network_bytes", "limiter_wait_ms", "http_status", "response_id", "pacing_interval_ms")}}
        rows.append(row)
        print(dumps(row), flush=True)
        atomic_write(root / "results.json", report)
    try:
        if args.window > 1:
            report["groups"] = []
            client = clients["urlopen"]
            size = args.window
            for block in range(len(args.pages)//(size*2)):
                group = args.pages[block*size*2:(block+1)*size*2]
                modes = ("sequential", f"window{size}") if block % 2 == 0 else (f"window{size}", "sequential")
                for index, mode in enumerate(modes):
                    pages = group[index*size:index*size+size]
                    specs = [({"params": params, "page": page}, f"window-study/page{page}",
                        {"kind": "http_acceleration_study", "mode": mode}) for page in pages]
                    print(f"Probe: mode={mode} block={block} pages={pages}", flush=True)
                    saved = client.request_pages("GetGameLeaderboard2", specs) if mode != "sequential" else [client.request("GetGameLeaderboard2", *s) for s in specs]
                    for page, value in zip(pages, saved): record(mode, page, value)
                    span = (max(datetime.fromisoformat(r["fetched_at"]) for r in saved) - min(datetime.fromisoformat(r["requested_at"]) for r in saved)).total_seconds()*1000
                    report["groups"].append({"block": block, "mode": mode, "pages": pages,
                        "network_sum_ms": sum(r["elapsed_ms"] for r in saved), "service_span_ms": round(span),
                        "serial_isolated_estimate_ms": sum(max(r["elapsed_ms"], r["pacing_interval_ms"]) for r in saved[:-1]) + saved[-1]["elapsed_ms"]})
                    before = read(state / "collections" / args.collection / "budget.json")["used"]
                    replay = client.request_pages("GetGameLeaderboard2", specs)
                    assert saved == replay
                    assert before == read(state / "collections" / args.collection / "budget.json")["used"]
            report["replay_without_http_verified"] = True
            plan = []
        for mode, page in plan:
            print(f"Probe: transport={mode} page={page}", flush=True)
            saved = clients[mode].request("GetGameLeaderboard2", {"params": params, "page": page},
                f"{mode}/page{page}", {"kind": "http_acceleration_study", "transport": mode})
            record(mode, page, saved)
    except SourceError as error:
        report["stop_reason"] = str(error)
        print("Probe stopped:", type(error).__name__, str(error), flush=True)
    finally:
        pool.close()
        report["summary"] = {mode: {"samples": len(sample), "median_response_ms": median(r["elapsed_ms"] for r in sample),
            "median_headers_ms": median(r["headers_ms"] for r in sample)}
            for mode in sorted({r["transport"] for r in rows}) if (sample := [r for r in rows if r["transport"] == mode])}
        if args.window == 1:
            report["paired_run_ids_match"] = all(len({r["run_ids_digest"] for r in rows if r["page"] == p}) == 1 for p in args.pages)
        atomic_write(root / "results.json", report)
        print(dumps(report), flush=True)


if __name__ == "__main__":
    main()
