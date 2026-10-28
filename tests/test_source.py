from datetime import datetime, timezone
import io
import multiprocessing
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import time
import unittest
from urllib.error import HTTPError

from speedrun_etl.jsonutil import atomic_write, read
from speedrun_etl.source import BudgetExceeded, Client, SourceError, retry_after, advance_rate


class Clock:
    def __init__(self):
        self.time = 1000.0
        self.starts = []

    def now(self):
        return self.time

    def sleep(self, seconds):
        self.time += seconds


class Response(io.BytesIO):
    status = 200


def concurrent_call(root, snapshot):
    def transport(request, timeout):
        # The append is inside the shared source lock held over the entire request.
        with (Path(root) / "starts.txt").open("a") as handle:
            handle.write(str(time.time()) + "\n")
        return Response(b'{"ok":true}')
    Client(root, "collection", snapshot, transport=transport, initial_pause=0,
           interval=0.08).request("GetGameData", {}, "same")


def concurrent_pages(root, snapshot):
    def transport(request, timeout):
        with (Path(root) / "starts.txt").open("a") as handle:
            handle.write(str(time.time()) + "\n")
        time.sleep(0.15)
        return Response(b'{}')
    Client(root, "collection", snapshot, transport=transport, initial_pause=0,
           interval=0.08).request_pages("GetGameData", [({"page": n}, str(n), {}) for n in (1, 2)])


class RetryAfterTests(unittest.TestCase):
    def test_repeated_429_backoff_and_one_reviewed_retry_keep_attempt_history(self):
        clock=Clock()
        def blocked(request,timeout):
            raise HTTPError(request.full_url,429,'rate limited',{},io.BytesIO(b'{"error":"rate limited"}'))
        with TemporaryDirectory() as directory:
            client=Client(directory,'c','s',transport=blocked,clock=clock.now,sleep=clock.sleep,initial_pause=0)
            with self.assertRaises(SourceError): client.request('GetGameData',{},'blocked')
            request_dir=next((Path(directory)/'snapshots/s/requests').iterdir())
            self.assertEqual(5,read(request_dir/'attempts.state')['count'])
            attempts=sorted((read(p) for p in request_dir.glob('*.json')),key=lambda r:r['attempt'])
            self.assertEqual([60,120,240,480,900],[r['backoff_seconds'] for r in attempts])
            with self.assertRaises(SourceError): client.request('GetGameData',{},'blocked')
            self.assertEqual(5,read(Path(directory)/'collections/c/budget.json')['used'])
            atomic_write(request_dir/'attempts.state',{'count':5,'reviewed_extra_attempts':1,
                                                      'review_reason':'single retry after verified cooldown'})
            client.transport=lambda request,timeout: Response(b'{}')
            result=client.request('GetGameData',{},'blocked')
            self.assertEqual(6,result['attempt'])
            self.assertEqual(6,read(request_dir/'attempts.state')['count'])
            self.assertEqual(6,read(Path(directory)/'collections/c/budget.json')['used'])
            self.assertEqual(6,len(list(request_dir.glob('*.json'))))
            self.assertEqual(result,client.request('GetGameData',{},'blocked'))
            client.transport=blocked
            with self.assertRaises(SourceError): client.request('GetGameData',{},'another-page')
            self.assertEqual(11,read(Path(directory)/'collections/c/budget.json')['used'])

    def test_recovery_waits_for_time_and_clean_responses_then_probes_in_small_steps(self):
        rate = {"rpm": 20, "successes": 0, "throttled": True, "recover_not_before": 1300}
        for _ in range(30): advance_rate(rate, 1200, 60, True)
        self.assertEqual(20, rate["rpm"])
        advance_rate(rate, 1300, 60, True)
        self.assertEqual(25, rate["rpm"])
        self.assertFalse(rate["throttled"])
        for _ in range(30): advance_rate(rate, 1350, 60, True)
        self.assertEqual(25, rate["rpm"])
        advance_rate(rate, 1420, 60, True)
        self.assertEqual(30, rate["rpm"])
        advance_rate(rate, 1540, 60, False)
        self.assertEqual(0, rate["successes"])
        self.assertEqual(30, rate["rpm"])

    def test_seconds_and_http_date(self):
        self.assertEqual(120, retry_after("120"))
        now = datetime(2026, 10, 15, tzinfo=timezone.utc)
        self.assertEqual(60, retry_after("Thu, 15 Oct 2026 00:01:00 GMT", now))
        self.assertEqual(0, retry_after("garbage", now))


@unittest.skipUnless(os.name == "posix", "Production limiter requires Linux flock; run in Docker")
class SourceTests(unittest.TestCase):
    def test_window_overlaps_four_reads_but_paces_starts_and_replays_without_http(self):
        import threading
        with TemporaryDirectory() as temp:
            guard = threading.Lock()
            active = 0
            maximum = 0
            starts = []
            def transport(request, timeout):
                nonlocal active, maximum
                with guard:
                    active += 1
                    maximum = max(maximum, active)
                    starts.append(time.time())
                time.sleep(0.15)
                with guard: active -= 1
                return Response(b'{"ok":true}')
            client = Client(temp, "c", "s", budget=10, initial_pause=0, interval=0.05, transport=transport)
            requests = [({"page": n}, f"page{n}", {}) for n in (1, 2, 3, 4)]
            result = client.request_pages("GetGameLeaderboard2", requests)
            self.assertGreaterEqual(maximum, 2)
            self.assertLessEqual(maximum, 4)
            self.assertTrue(all(b-a>=0.049 for a,b in zip(starts,starts[1:])))
            self.assertEqual(4, read(Path(temp)/"collections/c/budget.json")["used"])
            self.assertEqual(result, client.request_pages("GetGameLeaderboard2", requests))
            self.assertEqual(4, len(starts))
            self.assertFalse(read(Path(temp)/"limiter.json")["inflight"])

    def test_fast_429_prevents_second_request_and_preserves_backoff(self):
        with TemporaryDirectory() as temp:
            starts = []
            def transport(request, timeout):
                starts.append(time.time())
                raise HTTPError(request.full_url,429,"limit",{"Retry-After":"60"},io.BytesIO(b'{}'))
            client = Client(temp,"c","s",initial_pause=0,interval=0.05,transport=transport,max_attempts=1)
            with self.assertRaises(SourceError):
                client.request_pages("GetGameLeaderboard2",[({"page":n},str(n),{}) for n in (1,2)])
            self.assertEqual(1,len(starts))
            limiter=read(Path(temp)/"limiter.json")
            self.assertTrue(limiter["rate"]["throttled"])
            self.assertFalse(limiter["inflight"])
            self.assertGreaterEqual(float(limiter["next_at"]),starts[0]+60)

    def test_pair_late_success_cannot_undo_429_and_is_durable(self):
        import base64
        from speedrun_etl.jsonutil import loads
        with TemporaryDirectory() as temp:
            starts=[]
            def transport(request,timeout):
                params=loads(base64.urlsafe_b64decode(request.full_url.split('_r=')[1]+'=='))
                starts.append(params['page'])
                time.sleep(0.08 if params['page']==1 else 0.15)
                if params['page']==1:
                    raise HTTPError(request.full_url,429,"limit",{"Retry-After":"60"},io.BytesIO(b'{}'))
                return Response(b'{"saved":true}')
            client=Client(temp,"c","s",initial_pause=0,interval=0.04,transport=transport,max_attempts=1,adaptive_rate=False)
            with self.assertRaises(SourceError):
                client.request_pages("GetGameLeaderboard2",[({"page":n},str(n),{}) for n in (1,2)])
            limiter=read(Path(temp)/"limiter.json")
            self.assertEqual(20,limiter["rate"]["rpm"])
            self.assertTrue(limiter["rate"]["throttled"])
            self.assertGreater(float(limiter['next_at']),time.time()+59)
            self.assertEqual({"saved":True},client.request("GetGameLeaderboard2",{"page":2},"2",{})['body'])
            self.assertEqual([1,2],starts)

    def test_batch_and_another_process_share_start_spacing(self):
        with TemporaryDirectory() as temp:
            ctx=multiprocessing.get_context("fork")
            processes=[ctx.Process(target=concurrent_pages,args=(temp,"batch")),
                       ctx.Process(target=concurrent_call,args=(temp,"single"))]
            for process in processes:process.start()
            for process in processes:
                process.join(10)
                self.assertEqual(0,process.exitcode)
            starts=sorted(map(float,(Path(temp)/"starts.txt").read_text().splitlines()))
            self.assertEqual(3,len(starts))
            self.assertTrue(all(b-a>=0.078 for a,b in zip(starts,starts[1:])))

    def test_partial_budget_keeps_success_and_never_exceeds_attempt_cap(self):
        with TemporaryDirectory() as temp:
            clock=Clock()
            starts=[]
            def transport(request,timeout):
                starts.append(clock.now())
                return Response(b'{"kept":true}')
            client=Client(temp,"c","s",budget=1,initial_pause=0,transport=transport,clock=clock.now,sleep=clock.sleep)
            with self.assertRaises(BudgetExceeded):
                client.request_pages("GetGameData",[({"page":n},str(n),{}) for n in (1,2)])
            self.assertEqual(1,len(starts))
            self.assertEqual({"kept":True},client.request("GetGameData",{"page":1},"1",{})['body'])
            self.assertEqual(1,read(Path(temp)/"collections/c/budget.json")['used'])

    def test_post_request_provenance_and_cache_identity_are_distinct_from_get(self):
        from speedrun_etl.jsonutil import loads
        with TemporaryDirectory() as temp:
            clock = Clock()
            methods = []
            def transport(request, timeout):
                methods.append(request.get_method())
                if request.get_method() == "POST":
                    self.assertEqual({"gameUrl": "example"}, loads(request.data))
                    self.assertNotIn("?_r=", request.full_url)
                return Response(b'{}')
            for method in ("GET", "POST"):
                client = Client(temp, "c", "s", transport=transport, clock=clock.now,
                    sleep=clock.sleep, initial_pause=0, http_method=method)
                saved = client.request("GetGameData", {"gameUrl": "example"}, "same")
                self.assertEqual(method, saved["http_method"])
                self.assertEqual(saved, client.request("GetGameData", {"gameUrl": "example"}, "same"))
            self.assertEqual(["GET", "POST"], methods)

    def test_throttle_recovery_persists_across_client_restarts_and_rethrottles(self):
        with TemporaryDirectory() as temp:
            clock = Clock()
            fail = [True]
            def transport(request, timeout):
                if fail[0]:
                    fail[0] = False
                    raise HTTPError(request.full_url, 429, "limit", {"Retry-After": "90"}, io.BytesIO(b'{}'))
                return Response(b'{}')
            def client():
                return Client(temp, "c", "s", budget=100, transport=transport, clock=clock.now,
                    sleep=clock.sleep, initial_pause=0, adaptive_rate=True)
            client().request("GetGameData", {}, "initial-throttle")
            before = read(Path(temp)/"limiter.json")["rate"]
            self.assertTrue(before["throttled"])
            clock.sleep(300)
            for n in range(20): client().request("GetGameData", {}, "clean"+str(n))
            self.assertEqual(25, read(Path(temp)/"limiter.json")["rate"]["rpm"])
            fail[0] = True
            client().request("GetGameData", {}, "throttle-again")
            after = read(Path(temp)/"limiter.json")["rate"]
            self.assertEqual(20, after["rpm"])
            self.assertTrue(after["throttled"])
            self.assertGreater(after["recover_not_before"], clock.now())

    def test_gzip_exact_json_cache_and_wire_size(self):
        import gzip
        from decimal import Decimal
        with TemporaryDirectory() as temp:
            raw = b'{"value":0.1234567890123456789012345,"unknown":[null,2,2]}'
            wire = gzip.compress(raw, mtime=0)
            def transport(request, timeout):
                self.assertEqual("gzip", request.get_header("Accept-encoding"))
                response = Response(wire)
                response.headers = {"Content-Encoding": "gzip"}
                return response
            client = Client(temp, "c", "s", transport=transport, initial_pause=0)
            saved = client.request("GetGameData", {}, "gzip")
            self.assertEqual(Decimal("0.1234567890123456789012345"), saved["body"]["value"])
            self.assertEqual(len(wire), saved["network_bytes"])
            self.assertEqual(len(raw), saved["decoded_bytes"])
            self.assertEqual(saved, client.request("GetGameData", {}, "gzip"))
            self.assertEqual(1, read(Path(temp)/"collections/c/telemetry.json")["attempts"])

    def test_optional_90_rpm_ceiling_uses_gradual_steps(self):
        with TemporaryDirectory() as temp:
            clock = Clock()
            client = Client(temp,"c","s",budget=220,transport=lambda *a,**k:Response(b'{}'),
                            clock=clock.now,sleep=clock.sleep,initial_pause=0,adaptive_rate=True,max_rpm=90)
            for n in range(211):
                client.request("GetGameData",{},str(n))
            self.assertEqual(90,read(Path(temp)/"limiter.json")["rate"]["rpm"])
            self.assertGreaterEqual(read(Path(temp)/"collections/c/telemetry.json")["observed_rpm"],75)
        with self.assertRaises(ValueError):
            Client("unused","c","s",max_rpm=100)

    def test_transport_time_counts_towards_spacing(self):
        with TemporaryDirectory() as temp:
            clock = Clock()
            def transport(request, timeout):
                clock.starts.append(clock.now())
                clock.sleep(0.7)
                return Response(b'{}')
            client = Client(temp, "c", "s", transport=transport, clock=clock.now,
                            sleep=clock.sleep, initial_pause=0, interval=1)
            for n in range(3):
                client.request("GetGameData", {}, str(n))
            self.assertEqual([1000.0, 1001.0, 1002.0], clock.starts)

    def test_adaptive_rate_and_persistent_throttle(self):
        with TemporaryDirectory() as temp:
            clock = Clock()
            def transport(request, timeout):
                clock.starts.append(clock.now())
                if len(clock.starts) == 77:
                    raise HTTPError(request.full_url, 429, "limit", {"Retry-After": "90"}, io.BytesIO(b'{}'))
                return Response(b'{}')
            client = Client(temp, "c", "s", budget=100, transport=transport,
                            clock=clock.now, sleep=clock.sleep, initial_pause=0, adaptive_rate=True, max_rpm=60)
            for n in range(76):
                client.request("GetGameData", {}, str(n))
            self.assertEqual(60, read(Path(temp) / "limiter.json")["rate"]["rpm"])
            client.request("GetGameData", {}, "throttle")
            self.assertGreaterEqual(clock.starts[77] - clock.starts[76], 90)
            client = Client(temp, "c", "s", budget=100, transport=transport,
                            clock=clock.now, sleep=clock.sleep, adaptive_rate=True, max_rpm=60)
            client.request("GetGameData", {}, "restart")
            self.assertEqual(20, read(Path(temp) / "limiter.json")["rate"]["rpm"])
            self.assertGreaterEqual(clock.starts[-1] - clock.starts[-2], 3)

    def test_retry_budget_resume_and_initial_cooldown(self):
        with TemporaryDirectory() as temp:
            clock = Clock()
            def transport(request, timeout):
                clock.starts.append(clock.now())
                if len(clock.starts) == 1:
                    raise HTTPError(request.full_url, 429, "limit", {"Retry-After": "75"}, io.BytesIO(b'{}'))
                return Response(b'{"game":{"id":"g"}}')
            client = Client(temp, "c", "s", budget=3, transport=transport, clock=clock.now, sleep=clock.sleep)
            first = client.request("GetGameData", {}, "first")
            self.assertEqual([1060.0, 1135.0], clock.starts)
            restarted = Client(temp, "c", "s", budget=3, transport=transport, clock=clock.now, sleep=clock.sleep)
            self.assertEqual(first, restarted.request("GetGameData", {}, "first"))
            restarted.request("GetGameData", {}, "fresh")
            with self.assertRaises(BudgetExceeded):
                restarted.request("GetGameData", {}, "over-budget")
            self.assertEqual(3, read(Path(temp) / "collections/c/budget.json")["used"])
            self.assertAlmostEqual(3.1, clock.starts[2] - clock.starts[1])

    def test_persisted_five_attempt_limit_and_inflight_restart(self):
        with TemporaryDirectory() as temp:
            clock = Clock()
            def transport(request, timeout):
                raise TimeoutError()
            client = Client(temp, "c", "s", transport=transport, clock=clock.now, sleep=clock.sleep)
            with self.assertRaises(SourceError):
                client.request("GetGameData", {}, "fail")
            with self.assertRaises(SourceError):
                client.request("GetGameData", {}, "fail")
            self.assertEqual(5, read(Path(temp) / "collections/c/budget.json")["used"])
            atomic_write(Path(temp) / "limiter.json", {"next_at": clock.now(), "inflight": True})
            before = clock.now()
            client.transport = lambda request, timeout: Response(b'{}')
            client.request("GetGameData", {}, "new")
            self.assertGreaterEqual(clock.now() - before, 60)

    def test_two_processes_share_one_start_interval(self):
        with TemporaryDirectory() as temp:
            ctx = multiprocessing.get_context("fork")
            processes = [ctx.Process(target=concurrent_call, args=(temp, f"s{i}")) for i in range(3)]
            for process in processes:
                process.start()
            for process in processes:
                process.join(10)
                self.assertEqual(0, process.exitcode)
            starts = sorted(map(float, (Path(temp) / "starts.txt").read_text().splitlines()))
            self.assertEqual(3, len(starts))
            self.assertTrue(all(b - a >= 0.078 for a, b in zip(starts, starts[1:])))


if __name__ == "__main__":
    unittest.main()
