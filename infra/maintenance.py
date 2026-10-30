"""Удаляет только устаревшие технические task logs Airflow.

Исходные ответы, ODS и staging не затрагиваются."""

from pathlib import Path
import time

root = Path("/opt/airflow/logs").resolve()
cutoff = time.time() - 30 * 86400
removed = 0
for path in root.rglob("*.log"):
    if path.is_symlink() or not path.resolve().is_relative_to(root):
        continue
    if path.is_file() and path.stat().st_mtime < cutoff:
        path.unlink()
        removed += 1
print(f"Expired technical logs removed: {removed}; ODS/staging untouched")
