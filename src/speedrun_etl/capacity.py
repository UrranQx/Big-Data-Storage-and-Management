"""Проверяет свободное место перед новой работой ETL.

Читает замер диска хоста и staging; при нехватке или неверном замере останавливает сбор."""

import shutil
import time
from pathlib import Path

from .jsonutil import read


class CapacityPaused(RuntimeError):
    pass


def check_capacity(state, *, require_host=False, check_virtual=True, now=None):
    if check_virtual:
        disk = shutil.disk_usage(state)
        if disk.free < max(10 * 1024**3, disk.total * 0.2):
            raise CapacityPaused(
                "Insufficient free staging space; queue paused without deleting data"
            )
    path = Path(state) / "safety" / "host-disk.json"
    if not path.exists():
        if require_host:
            raise CapacityPaused(
                "Full collection requires a running infra/host_disk_watch.py on the Docker host"
            )
        return
    try:
        report = read(path)
        age = (time.time() if now is None else now) - float(report["measured_at_unix"])
        if not -30 <= age <= 120:
            raise CapacityPaused(
                "Host disk measurement is stale; resume the host disk watcher"
            )
        minimum = report["minimum_free_bytes"]
        free = report["free_bytes"]
        if (
            type(minimum) is not int
            or type(free) is not int
            or minimum < 10 * 1024**3
            or free < 0
        ):
            raise ValueError("Invalid host disk bounds")
        if free < minimum:
            raise CapacityPaused(
                f"Host disk low: {free / 1024**3:.1f} GiB free, "
                f"{minimum / 1024**3:.1f} GiB reserve; source paused"
            )
    except CapacityPaused:
        raise
    except (KeyError, TypeError, ValueError, OSError) as error:
        raise CapacityPaused(
            "Host disk measurement invalid; refresh the host disk watcher"
        ) from error
