"""Остановка по реальному диску хоста и сохранение уже полученных ответов.

Проверяет устаревшие/повреждённые замеры и отсутствие потраченного HTTP-бюджета при блокировке."""

import io
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from speedrun_etl.capacity import CapacityPaused, check_capacity
from speedrun_etl.jsonutil import atomic_write, read
from speedrun_etl.source import Client


class CapacityTests(TestCase):
    def sample(self, root, **extra):
        atomic_write(
            Path(root) / "safety/host-disk.json",
            {
                "measured_at_unix": 1000,
                "free_bytes": 63 * 1024**3,
                "minimum_free_bytes": 20 * 1024**3,
                **extra,
            },
        )

    def test_full_requires_real_host_report_and_refuses_stale_report(self):
        with TemporaryDirectory() as root:
            with self.assertRaisesRegex(CapacityPaused, "requires"):
                check_capacity(root, require_host=True, check_virtual=False)
            self.sample(root)
            check_capacity(root, require_host=True, check_virtual=False, now=1000)
            with self.assertRaisesRegex(CapacityPaused, "stale"):
                check_capacity(root, require_host=True, check_virtual=False, now=1121)

    def test_large_docker_virtual_disk_does_not_hide_low_host_disk(self):
        from types import SimpleNamespace

        with (
            TemporaryDirectory() as root,
            patch(
                "speedrun_etl.capacity.shutil.disk_usage",
                return_value=SimpleNamespace(total=1024**4, free=900 * 1024**3),
            ),
        ):
            self.sample(root, free_bytes=19 * 1024**3)
            with self.assertRaisesRegex(CapacityPaused, "Host disk low"):
                check_capacity(root, now=1000)

    def test_real_fractional_timestamp_survives_the_exact_json_decimal_reader(self):
        with TemporaryDirectory() as root:
            self.sample(root, measured_at_unix=1000.25)
            check_capacity(root, require_host=True, check_virtual=False, now=1000.5)

    def test_blocked_source_does_not_consume_http_attempts_and_resumes_same_identity(
        self,
    ):
        for window in (1, 2):
            with self.subTest(window=window), TemporaryDirectory() as root:
                self.sample(root, free_bytes=1)
                calls = []

                def transport(request, timeout):
                    calls.append(request.full_url)
                    result = io.BytesIO(b"{}")
                    result.status = 200
                    return result

                client = Client(
                    root,
                    "c",
                    "s",
                    transport=transport,
                    initial_pause=0,
                    interval=0,
                    clock=lambda: 1000,
                )

                def run():
                    return (
                        client.request("GetGameData", {}, "one")
                        if window == 1
                        else client.request_pages(
                            "GetGameData", [({"page": p}, str(p), {}) for p in (1, 2)]
                        )
                    )

                with self.assertRaises(CapacityPaused):
                    run()
                self.assertEqual([], calls)
                self.assertFalse((Path(root) / "collections/c/budget.json").exists())
                self.sample(root)
                run()
                self.assertEqual(window, len(calls))

    def test_corrupt_host_measurement_blocks_instead_of_silently_disabling_guard(self):
        with TemporaryDirectory() as root:
            path = Path(root) / "safety/host-disk.json"
            path.parent.mkdir()
            path.write_text("{")
            with self.assertRaisesRegex(CapacityPaused, "invalid"):
                check_capacity(root, check_virtual=False)

    def test_pause_between_window_starts_journals_the_already_received_response(self):
        with TemporaryDirectory() as root:

            def transport(request, timeout):
                response = io.BytesIO(b'{"preserved":true}')
                response.status = 200
                return response

            client = Client(
                root,
                "c",
                "s",
                transport=transport,
                initial_pause=0,
                interval=0,
                clock=lambda: 1000,
            )
            with patch(
                "speedrun_etl.source.check_capacity",
                side_effect=[None, None, CapacityPaused("Host disk low")],
            ):
                with self.assertRaises(CapacityPaused):
                    client.request_pages(
                        "GetGameData", [({"page": p}, str(p), {}) for p in (1, 2)]
                    )
            saved = list((Path(root) / "snapshots/s/requests").glob("*/*.json"))
            self.assertEqual(1, len(saved))
            self.assertEqual({"preserved": True}, read(saved[0])["body"])
            self.assertEqual(1, read(Path(root) / "collections/c/budget.json")["used"])
            self.assertFalse(read(Path(root) / "limiter.json")["inflight"])
