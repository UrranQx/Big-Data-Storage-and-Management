"""V2 HTTP with bounded page overlap, durable shared pacing and journaling."""
import base64
import gzip
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
from .capacity import CapacityPaused, check_capacity

ENDPOINTS = {"GetGameData", "GetGameSummary", "GetGameLeaderboard2", "GetGameRecordHistory", "GetGameList",
             "GetGameLeaderboard"}  # Secondary endpoint: bounded research only, not the DAG source.
ENDPOINTS.update({"GetUserSummary", "GetUserPopoverData"})  # Public profile capability probes.


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


def advance_rate(rate, now, max_rpm, success):
    """Recover slowly after throttling, with shared durable probe checkpoints."""
    if not success:
        rate["successes"] = 0
        if not rate["throttled"]:
            rate["rpm"] = min(rate["rpm"], 30, max_rpm)
        return
    rate["successes"] += 1
    if rate["throttled"]:
        if now >= rate["recover_not_before"] and rate["successes"] >= 20:
            rate.update(rpm=min(max_rpm, 25), successes=0, throttled=False,
                        recovering=True, probe_at=now)
    elif rate.get("recovering"):
        if rate["successes"] >= 20 and now >= rate["probe_at"] + 120:
            rate.update(rpm=min(max_rpm, rate["rpm"] + 5), successes=0, probe_at=now)
        if rate["rpm"] >= max_rpm:
            rate["recovering"] = False
    elif rate["rpm"] < max_rpm and rate["successes"] >= rate["rpm"]:
        rate.update(rpm=min(max_rpm, rate["rpm"] + 15), successes=0)


class Client:
    def __init__(self, state, collection_id, snapshot_id, budget=30, transport=urlopen,
                 clock=time.time, sleep=time.sleep, initial_pause=60, interval=3.1, adaptive_rate=False, deadline=None,
                 max_rpm=None, http_method="GET", max_attempts=5):
        self.root = Path(state)
        self.collection = self.root / "collections" / collection_id
        self.snapshot = self.root / "snapshots" / snapshot_id
        self.budget = budget
        self.transport, self.clock, self.sleep = transport, clock, sleep
        self.initial_pause, self.interval = initial_pause, interval
        self.adaptive_rate = adaptive_rate
        self.deadline = datetime.fromisoformat(deadline).timestamp() if deadline else None
        self.max_rpm = int(max_rpm if max_rpm is not None else os.environ.get("ETL_MAX_RPM", "60"))
        if not 20 <= self.max_rpm <= 90:
            raise ValueError("ETL_MAX_RPM must be between 20 and 90")
        if http_method not in ("GET", "POST"):
            raise ValueError("Source HTTP method must be GET or POST")
        self.http_method = http_method
        if not 1 <= max_attempts <= 5:
            raise ValueError("HTTP attempts must be 1..5")
        self.max_attempts = max_attempts
        self.page_concurrency = int(os.environ.get("ETL_PAGE_CONCURRENCY", "4"))
        if not 1 <= self.page_concurrency <= 4:
            raise ValueError("ETL_PAGE_CONCURRENCY must be 1..4")

    def _attempt_state(self, path):
        state = read(path) if path.exists() else {"count": 0}
        extra = state.get("reviewed_extra_attempts", 0)
        if type(extra) is not int or extra not in (0, 1):
            raise SourceError("Invalid reviewed retry allowance")
        return state, self.max_attempts + extra

    def request(self, endpoint, params, logical_key, context=None, allow_statuses=()):
        if endpoint not in ENDPOINTS:
            raise ValueError("Unapproved endpoint")
        if allow_statuses and (endpoint != 'GetGameRecordHistory' or set(allow_statuses) - {404,410}):
            raise ValueError('Only explicit WR history 404/410 may be unavailable results')
        identity = digest([endpoint, params, logical_key] + ([self.http_method] if self.http_method != "GET" else []))
        request_dir = self.snapshot / "requests" / identity
        # Same logical request after a crash reuses an already saved success.
        for path in sorted(request_dir.glob("*.json")):
            saved = read(path)
            if (saved["http_status"] == 200 and saved.get("error") is None) or saved['http_status'] in allow_statuses:
                return saved
        attempts_path = request_dir / "attempts.state"
        for _ in range(self.max_attempts):
            with file_lock(self.root / "source.lock"):
                if self.deadline is not None and self.clock() >= self.deadline:
                    raise BudgetExceeded("Collection deadline reached; preserved partial snapshot")
                # A concurrent retry may have saved this request while we waited.
                for path in sorted(request_dir.glob("*.json")):
                    saved = read(path)
                    if (saved["http_status"] == 200 and saved.get("error") is None) or saved['http_status'] in allow_statuses:
                        return saved
                check_capacity(self.root, check_virtual=False, now=self.clock())
                attempt_state, attempt_limit = self._attempt_state(attempts_path)
                attempts = attempt_state["count"]
                if attempts >= attempt_limit:
                    raise SourceError("Persisted HTTP attempts exhausted for this logical request")
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
                limiter_wait = max(0, float(limiter["next_at"]) - self.clock())
                self.sleep(limiter_wait)
                check_capacity(self.root, check_virtual=False, now=self.clock())
                rate = limiter.get("rate", {"rpm": 30, "successes": 0, "throttled": False})
                rate["rpm"] = min(rate["rpm"], self.max_rpm)
                if rate["throttled"]:
                    rate.setdefault("recover_not_before", self.clock() + 300)
                previous_rate = (rate["rpm"], rate["throttled"])
                interval = 60 / rate["rpm"] if self.adaptive_rate else self.interval
                if rate["throttled"]:
                    interval = max(interval, 3.1)
                start = self.clock()
                budget_state["used"] += 1
                atomic_write(budget_path, budget_state)
                atomic_write(attempts_path, {**attempt_state, "count": attempts + 1})
                atomic_write(limiter_path, {"next_at": start + interval, "inflight": True, "rate": rate})
                saved, delay, network_start = self._fetch(endpoint, params, logical_key, context,
                    attempts + 1, interval, limiter_wait)
                if saved["http_status"] in (420, 429):
                    rate.update(rpm=20, successes=0, throttled=True, recovering=True,
                                last_throttled_at=self.clock(), recover_not_before=self.clock() + max(300, delay))
                if self.adaptive_rate:
                    advance_rate(rate, self.clock(), self.max_rpm, saved.get("error") is None)
                if (rate["rpm"], rate["throttled"]) != previous_rate:
                    print(f"Rate: ceiling={previous_rate[0]}->{rate['rpm']} rpm "
                          f"mode={'cooldown' if rate['throttled'] else 'probe' if rate.get('recovering') else 'adaptive'}", flush=True)
                # Response is durable BEFORE the rate state becomes clean or checkpoints advance.
                self._record(request_dir, saved, network_start, budget_state)
                # Shared lock covers HTTP and journaling. Pace from actual transport
                # start; server backoff is always measured from completion.
                atomic_write(limiter_path, {"next_at": max(network_start + interval, self.clock() + delay),
                                            "inflight": False, "rate": rate})
            if (saved.get("error") is None and saved["http_status"] == 200) or saved['http_status'] in allow_statuses:
                return saved
            if delay == 0:
                raise SourceError(f"Source request failed: {saved.get('error', saved['http_status'])}")
        raise SourceError("HTTP attempts exhausted")


    def _record(self, request_dir, saved, network_start, budget_state):
        atomic_write(request_dir / (f"{saved['attempt']:02d}-" + saved["response_id"] + ".json"), saved)
        metrics_path = self.collection / "telemetry.json"
        metrics = read(metrics_path) if metrics_path.exists() else {"attempts": 0, "network_bytes": 0, "status_counts": {}, "recent": []}
        metrics["attempts"] += 1
        metrics["network_bytes"] += saved["network_bytes"]
        status = str(saved["http_status"] or "network_error")
        metrics["status_counts"][status] = metrics["status_counts"].get(status, 0) + 1
        metrics.update(rate_policy="recover-v1", last_http_status=status, last_error=saved.get("error"),
                       last_pacing_interval_ms=saved["pacing_interval_ms"], configured_page_window=self.page_concurrency)
        metrics["recent"] = sorted(metrics["recent"] + [{"start_ms": round(network_start * 1000),
            "elapsed_ms": saved["elapsed_ms"], "limiter_wait_ms": saved["limiter_wait_ms"]}], key=lambda r: r["start_ms"])[-60:]
        recent = metrics["recent"]
        span = recent[-1]["start_ms"] - recent[0]["start_ms"]
        metrics["observed_rpm"] = round(60000 * (len(recent) - 1) / span, 2) if span > 0 else None
        short = recent[-10:]
        short_span = short[-1]["start_ms"] - short[0]["start_ms"]
        metrics["recent_rpm"] = round(60000 * (len(short) - 1) / short_span, 2) if short_span > 0 else None
        atomic_write(metrics_path, metrics, durable=False)
        if metrics["attempts"] % 10 == 0 or saved.get("error"):
            print(f"HTTP: endpoint={saved['endpoint']} attempts={metrics['attempts']} "
                          f"budget={budget_state['used']}/{self.budget} recent_rpm={metrics['recent_rpm']} mean_rpm_60_requests={metrics['observed_rpm']} "
                  f"pacing_ms={saved['pacing_interval_ms']} response_ms={saved['elapsed_ms']} "
                  f"headers_ms={saved.get('headers_ms')} download_ms={saved.get('download_ms')} "
                  f"limiter_wait_ms={saved['limiter_wait_ms']} status={status}", flush=True)

    def request_pages(self, endpoint, requests):
        """Overlap up to four page reads; retain the global lock, pacing and budget.

        Responses are durable separately. Any retry uses request() and its same
        identities/counters. Other collectors cannot overlap this batch.
        """
        from concurrent.futures import ThreadPoolExecutor, Future, as_completed
        if endpoint not in ENDPOINTS or not 1 <= len(requests) <= 4:
            raise ValueError("One to four approved requests required")
        prepared = []
        for params, key, context in requests:
            identity = digest([endpoint, params, key] + ([self.http_method] if self.http_method != "GET" else []))
            directory = self.snapshot / "requests" / identity
            cached = any((r := read(p))["http_status"] == 200 and not r.get("error") for p in directory.glob("*.json"))
            prepared.append((params, key, context, directory, cached))
        # Never spend network attempts for successful pages when resuming.
        if len(prepared) < 2 or any(p[4] for p in prepared):
            return [self.request(endpoint, *r) for r in requests]
        with file_lock(self.root / "source.lock"):
            check_capacity(self.root, check_virtual=False, now=self.clock())
            budget_path = self.collection / "budget.json"
            budget = read(budget_path) if budget_path.exists() else {"used": 0, "limit": self.budget}
            if budget["limit"] != self.budget:
                raise SourceError("A retry cannot change the collection budget")
            cached_now = any(any((r := read(path))["http_status"] == 200 and not r.get("error")
                for path in p[3].glob("*.json")) for p in prepared)
            if budget["used"] + len(prepared) <= self.budget and not cached_now:
                limiter_path = self.root / "limiter.json"
                try:
                    limiter = read(limiter_path)
                    if limiter.get("inflight"):
                        limiter["next_at"] = max(float(limiter["next_at"]), self.clock() + self.initial_pause)
                except (FileNotFoundError, ValueError, KeyError):
                    limiter = {"next_at": self.clock() + self.initial_pause}
                rate = limiter.get("rate", {"rpm": 30, "successes": 0, "throttled": False})
                rate["rpm"] = min(rate["rpm"], self.max_rpm)
                if rate["throttled"]:
                    rate.setdefault("recover_not_before", self.clock() + 300)
                futures, consumed = {}, set()
                failed = False

                def consume(future):
                    nonlocal failed
                    saved, delay, started = future.result()
                    directory = futures[future]
                    failed |= saved.get("error") is not None
                    previous_rate = (rate["rpm"], rate["throttled"])
                    if saved["http_status"] in (420, 429):
                        rate.update(rpm=20, successes=0, throttled=True, recovering=True,
                            last_throttled_at=self.clock(), recover_not_before=self.clock() + max(300, delay))
                    if self.adaptive_rate and started >= float(rate.get("last_throttled_at", 0)):
                        advance_rate(rate, self.clock(), self.max_rpm, saved.get("error") is None)
                    if previous_rate != (rate["rpm"], rate["throttled"]):
                        print(f"Rate: ceiling={previous_rate[0]}->{rate['rpm']} rpm "
                              f"mode={'cooldown' if rate['throttled'] else 'probe' if rate.get('recovering') else 'adaptive'}", flush=True)
                    self._record(directory, saved, started, budget)
                    limiter["next_at"] = max(float(limiter["next_at"]), self.clock() + delay)
                    consumed.add(future)

                try:
                    with ThreadPoolExecutor(max_workers=len(prepared)) as executor:
                        for params, key, context, directory, _ in prepared:
                            wait = max(0, float(limiter["next_at"]) - self.clock())
                            self.sleep(wait)
                            check_capacity(self.root, check_virtual=False, now=self.clock())
                            for future in list(futures):
                                if future.done() and future not in consumed:
                                    consume(future)
                            if failed:
                                break  # Observe 429/backoff before scheduling another request.
                            if self.deadline is not None and self.clock() >= self.deadline:
                                break
                            attempts_path = directory / "attempts.state"
                            attempt_state, attempt_limit = self._attempt_state(attempts_path)
                            attempts = attempt_state["count"]
                            if attempts >= attempt_limit:
                                break
                            interval = 60 / rate["rpm"] if self.adaptive_rate else self.interval
                            if rate["throttled"]:
                                interval = max(interval, 3.1)
                            budget["used"] += 1
                            atomic_write(budget_path, budget)
                            atomic_write(attempts_path, {**attempt_state, "count": attempts + 1})
                            atomic_write(limiter_path, {"next_at": self.clock() + interval, "inflight": True, "rate": rate})
                            started = Future()
                            future = executor.submit(self._fetch, endpoint, params, key, context,
                                attempts + 1, interval, wait, started.set_result)
                            futures[future] = directory
                            # Pace actual transport starts, including fsync/thread setup time.
                            limiter["next_at"] = started.result(timeout=65) + interval
                        for future in as_completed(futures):
                            if future not in consumed:
                                consume(future)
                except CapacityPaused:
                    # A guard may stop scheduling after an earlier page has started.
                    # Journal every in-flight response before propagating the pause.
                    for future in as_completed(futures):
                        if future not in consumed:
                            consume(future)
                    raise
                finally:
                    # A crash before journaling keeps inflight=True for restart cooldown.
                    if len(consumed) == len(futures):
                        atomic_write(limiter_path, {"next_at": limiter["next_at"], "inflight": False, "rate": rate})
        # Cached successes return immediately; failures retain the same retry budget.
        return [self.request(endpoint, *r) for r in requests]


    def _fetch(self, endpoint, params, logical_key, context, attempt, interval, limiter_wait, on_start=None):
        encoded = base64.urlsafe_b64encode(dumps(params).encode()).decode().rstrip("=")
        url = "https://www.speedrun.com/api/v2/" + endpoint
        headers = {"Accept": "application/json", "Accept-Encoding": "gzip", "User-Agent": "speedrun-etl-lab1/0.1"}
        if self.http_method == "GET":
            url += "?_r=" + encoded
        else:
            headers["Content-Type"] = "application/json"
        request = Request(url, data=dumps(params).encode() if self.http_method == "POST" else None,
                          headers=headers, method=self.http_method)
        saved = {"response_id": str(uuid4()), "endpoint": endpoint, "params": params,
                 "requested_at": utcnow(), "attempt": attempt, "logical_key": logical_key,
                 "http_method": self.http_method,
                 "context": context or {}, "http_status": None, "network_bytes": 0, "body": None,
                 "pacing_interval_ms": round(interval * 1000), "limiter_wait_ms": round(limiter_wait * 1000)}
        delay = 0
        network_start = self.clock()
        saved["requested_at"] = utcnow()
        if on_start is not None:
            on_start(network_start)
        try:
            with self.transport(request, timeout=60) as response:
                headers_at = self.clock()
                raw = response.read()
                read_at = self.clock()
                saved["http_status"] = response.status
                encoding = getattr(response, "headers", {}).get("Content-Encoding", "identity").lower()
            saved["network_bytes"] = len(raw)
            saved["content_encoding"] = encoding
            wire_raw = raw
            try:
                if encoding == "gzip":
                    raw = gzip.decompress(raw)
                elif encoding != "identity":
                    raise ValueError("Unsupported source content encoding")
                saved["decoded_bytes"] = len(raw)
                saved["body"] = loads(raw)
                saved["headers_ms"] = round((headers_at - network_start) * 1000)
                saved["download_ms"] = round((read_at - headers_at) * 1000)
            except (ValueError, UnicodeError, OSError, EOFError):
                saved["body"] = {"non_json_body_base64": base64.b64encode(wire_raw).decode()}
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
                saved["retry_after_seconds"] = retry_after(error.headers.get("Retry-After"))
                delay = max(min(900, 60 * 2 ** (attempt - 1)), saved["retry_after_seconds"])
            elif error.code >= 500:
                delay = 2 ** (attempt - 1) + random.random()
        except (URLError, TimeoutError, OSError) as error:
            saved["error"] = type(error).__name__  # no URLs, headers or credentials in errors
            delay = 2 ** (attempt - 1) + random.random()
        saved["fetched_at"] = utcnow()
        saved["elapsed_ms"] = round((self.clock() - network_start) * 1000)
        saved["backoff_seconds"] = delay
        return saved, delay, network_start


def staged_responses(snapshot_path):
    for path in sorted((Path(snapshot_path) / "requests").glob("*/*.json")):
        yield read(path)
