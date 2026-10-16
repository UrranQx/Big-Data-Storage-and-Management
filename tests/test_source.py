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
from speedrun_etl.source import BudgetExceeded, Client, SourceError, retry_after


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


class RetryAfterTests(unittest.TestCase):
    def test_seconds_and_http_date(self):
        self.assertEqual(120, retry_after("120"))
        now = datetime(2026, 10, 15, tzinfo=timezone.utc)
        self.assertEqual(60, retry_after("Thu, 15 Oct 2026 00:01:00 GMT", now))
        self.assertEqual(0, retry_after("garbage", now))


@unittest.skipUnless(os.name == "posix", "Production limiter requires Linux flock; run in Docker")
class SourceTests(unittest.TestCase):
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
                            clock=clock.now, sleep=clock.sleep, initial_pause=0, adaptive_rate=True)
            for n in range(76):
                client.request("GetGameData", {}, str(n))
            self.assertEqual(60, read(Path(temp) / "limiter.json")["rate"]["rpm"])
            client.request("GetGameData", {}, "throttle")
            self.assertGreaterEqual(clock.starts[77] - clock.starts[76], 90)
            client = Client(temp, "c", "s", budget=100, transport=transport,
                            clock=clock.now, sleep=clock.sleep, adaptive_rate=True)
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
