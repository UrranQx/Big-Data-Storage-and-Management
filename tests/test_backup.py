"""Проверяет выбор актуальной ODS и состав переносимой копии без Docker.

Пустой новый namespace остаётся валидным; CSV отчёта нужны для офлайн-показа,
а сырые данные и повторные копии exports не должны попасть в ZIP исходников.
"""

import importlib.util
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import MagicMock


def load_infra(name):
    path = Path(__file__).resolve().parents[1] / "infra" / (name + ".py")
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class BackupTests(unittest.TestCase):
    def test_portable_zip_rejects_windows_and_posix_escape_paths(self):
        module = load_infra("restore_portable")
        for name in (
            "../outside",
            "/absolute",
            "C:/outside",
            "a/../../outside",
            "..\\outside",
            "file:stream",
        ):
            with self.subTest(name=name), self.assertRaises(ValueError):
                module.safe_member(name)
        self.assertEqual("infra/init.py", module.safe_member("infra/init.py"))

    def test_current_namespace_preferred(self):
        module = load_infra("backup_inventory")
        conn = MagicMock()
        conn.execute.return_value.fetchone.return_value = ("entity_v3.manifests",)
        self.assertEqual("entity_v3", module.ods_namespace(conn))
        self.assertEqual(1, conn.execute.call_count)

    def test_legacy_namespace_fallback(self):
        module = load_infra("backup_inventory")
        conn = MagicMock()
        conn.execute.return_value.fetchone.side_effect = [
            (None,),
            ("entity_v2.manifests",),
        ]
        self.assertEqual("entity_v2", module.ods_namespace(conn))

    def test_missing_namespace_is_an_error(self):
        module = load_infra("backup_inventory")
        conn = MagicMock()
        conn.execute.return_value.fetchone.return_value = (None,)
        with self.assertRaisesRegex(ValueError, "No supported ODS"):
            module.ods_namespace(conn)

    def test_queue_order_and_ties(self):
        module = load_infra("backup_inventory")
        rows = [(1, "a", "A", "10"), (2, "b", "B", "10"), (3, "c", "C", "5")]
        result = module.queue_summary(rows)
        self.assertTrue(result["descending_catalog_run_count"])
        self.assertEqual(3, result["queued"])
        self.assertFalse(
            module.queue_summary(rows[::-1])["descending_catalog_run_count"]
        )

    def test_source_zip_keeps_report_data_and_local_credentials(self):
        module = load_infra("cold_backup")
        with TemporaryDirectory() as temp:
            root = Path(temp)
            keep = [
                "compose.yaml",
                ".env",
                "infra/garage.local.toml",
                "reports/lab1/comparison/data/summary.csv",
            ]
            skip = [
                "data/raw.json",
                "exports/old/data.json",
                ".git/config",
                "src/__pycache__/cached.pyc",
                ".venv/bin/python",
                ".aws/credentials",
                ".codex/auth.json",
                ".agents/session.json",
            ]
            for name in keep + skip:
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("fixture", encoding="utf-8")
            self.assertEqual(
                set(keep),
                {p.relative_to(root).as_posix() for p in module.source_files(root)},
            )
