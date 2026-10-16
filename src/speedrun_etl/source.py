"""Sequential v2 HTTP with a durable shared limiter, budgets and response journal."""
import base64
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import os
from pathlib import Path
import random
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import uuid4

from .jsonutil import atomic_write, digest, dumps, file_lock, loads, read, utcnow

ENDPOINTS = {"GetGameData", "GetGameSummary", "GetGameLeaderboard2", "GetGameRecordHistory", "GetGameList"}


class SourceError(RuntimeError):
    pass


class BudgetExceeded(SourceError):
    pass


def retry_after(value, now=None):
    now = now or datetime.now(timezone.utc)
    try:
        return max(0, float(value))
    except (TypeError, ValueError):
        try:
            return max(0, (parsedate_to_datetime(value) - now).total_seconds())
        except (TypeError, ValueError, OverflowError):
            return 0


class Client:
    def __init__(self, state, collection_id, snapshot_id, budget=30, transport=urlopen,
                 clock=time.time, sleep=time.sleep, initial_pause=60, interval=3.1, adaptive_rate=False, deadline=None):
        self.root = Path(state)
        self.collection = self.root / "collections" / collection_id
        self.snapshot = self.root / "snapshots" / snapshot_id
        self.budget = budget
        self.transport, self.clock, self.sleep = transport, clock, sleep
        self.initial_pause, self.interval = initial_pause, interval
        self.adaptive_rate = adaptive_rate
        self.deadline = datetime.fromisoformat(deadline).timestamp() if deadline else None

    def request(self, endpoint, params, logical_key, context=None):
        if endpoint not in ENDPOINTS:
            raise ValueError("Unapproved endpoint")
        identity = digest([endpoint, params, logical_key])
        request_dir = self.snapshot / "requests" / identity
        # Same logical request after a crash reuses an already saved success.
        for path in sorted(request_dir.glob("*.json")):
            saved = read(path)
            if saved["http_status"] == 200 and saved.get("error") is None:
                return saved
        attempts_path = request_dir / "attempts.state"
        for _ in range(5):
            with file_lock(self.root / "source.lock"):
                if self.deadline is not None and self.clock() >= self.deadline:
                    raise BudgetExceeded("Collection deadline reached; preserved partial snapshot")
                # A concurrent retry may have saved this request while we waited.
                for path in sorted(request_dir.glob("*.json")):
                    saved = read(path)
                    if saved["http_status"] == 200 and saved.get("error") is None:
                        return saved
                attempts = read(attempts_path)["count"] if attempts_path.exists() else 0
                if attempts >= 5:
                    raise SourceError("Five persisted attempts exhausted for this logical request")
                budget_path = self.collection / "budget.json"
                budget_state = read(budget_path) if budget_path.exists() else {"used": 0, "limit": self.budget}
                if budget_state["limit"] != self.budget:
                    raise SourceError("A retry cannot change the collection budget")
                if budget_state["used"] >= self.budget:
                    raise BudgetExceeded("Collection HTTP attempt budget exhausted")
                limiter_path = self.root / "limiter.json"
                try:
                    limiter = read(limiter_path)
                    if limiter.get("inflight"):
                        limiter["next_at"] = max(float(limiter["next_at"]), self.clock() + self.initial_pause)
                except (FileNotFoundError, ValueError, KeyError):
                    limiter = {"next_at": self.clock() + self.initial_pause}
                self.sleep(max(0, float(limiter["next_at"]) - self.clock()))
                rate = limiter.get("rate", {"rpm": 30, "successes": 0, "throttled": False})
                interval = 60 / rate["rpm"] if self.adaptive_rate else self.interval
                if rate["throttled"]:
                    interval = max(interval, 3.1)
                start = self.clock()
                budget_state["used"] += 1
                atomic_write(budget_path, budget_state)
                atomic_write(attempts_path, {"count": attempts + 1})
                atomic_write(limiter_path, {"next_at": start + interval, "inflight": True, "rate": rate})
                encoded = base64.urlsafe_b64encode(dumps(params).encode()).decode().rstrip("=")
                request = Request("https://www.speedrun.com/api/v2/" + endpoint + "?_r=" + encoded,
                                  headers={"Accept": "application/json", "User-Agent": "speedrun-etl-lab1/0.1"})
                saved = {"response_id": str(uuid4()), "endpoint": endpoint, "params": params,
                         "requested_at": utcnow(), "attempt": attempts + 1, "logical_key": logical_key,
                         "context": context or {}, "http_status": None, "network_bytes": 0, "body": None,
                         "pacing_interval_ms": round(interval * 1000)}
                delay = 0
                network_start = self.clock()
                saved["requested_at"] = utcnow()
                try:
                    with self.transport(request, timeout=60) as response:
                        raw = response.read()
                        saved["http_status"] = response.status
                    saved["network_bytes"] = len(raw)
                    try:
                        saved["body"] = loads(raw)
                    except (ValueError, UnicodeError):
                        saved["body"] = {"non_json_body_base64": base64.b64encode(raw).decode()}
                        saved["error"] = "invalid_json"
                except HTTPError as error:
                    raw = error.read()
                    saved["http_status"] = error.code
                    saved["network_bytes"] = len(raw)
                    try:
                        saved["body"] = loads(raw)
                    except (ValueError, UnicodeError):
                        saved["body"] = {"non_json_body_base64": base64.b64encode(raw).decode()}
                    saved["error"] = f"http_{error.code}"
                    if error.code in (420, 429):
                        delay = max(60, retry_after(error.headers.get("Retry-After")))
                        rate.update(rpm=20, successes=0, throttled=True)
                    elif error.code >= 500:
                        delay = 2 ** attempts + random.random()
                except (URLError, TimeoutError, OSError) as error:
                    saved["error"] = type(error).__name__  # no URLs, headers or credentials in errors
                    delay = 2 ** attempts + random.random()
                saved["fetched_at"] = utcnow()
                saved["elapsed_ms"] = round((self.clock() - network_start) * 1000)
                if self.adaptive_rate and not rate["throttled"]:
                    if saved.get("error") is None:
                        rate["successes"] += 1
                        if rate["rpm"] < 60 and rate["successes"] >= rate["rpm"]:
                            rate.update(rpm=min(60, rate["rpm"] + 15), successes=0)
                    else:
                        rate.update(rpm=30, successes=0)
                # Response is durable BEFORE the rate state becomes clean or checkpoints advance.
                atomic_write(request_dir / (f"{attempts + 1:02d}-" + saved["response_id"] + ".json"), saved)
                # Shared lock covers HTTP and journaling. Pace from actual transport
                # start; server backoff is always measured from completion.
                atomic_write(limiter_path, {"next_at": max(network_start + interval, self.clock() + delay),
                                            "inflight": False, "rate": rate})
            if saved.get("error") is None and saved["http_status"] == 200:
                return saved
            if delay == 0:
                raise SourceError(f"Source request failed: {saved.get('error', saved['http_status'])}")
        raise SourceError("HTTP attempts exhausted")


def staged_responses(snapshot_path):
    for path in sorted((Path(snapshot_path) / "requests").glob("*/*.json")):
        yield read(path)
