"""Снимает грубый профиль ресурсов трёх тестовых backend во время опыта.

Docker stats усредняет CPU; короткие операции могут не попасть в выборку.
CSV не подменяет таймеры запросов и не объявляется точным пиком памяти.
"""

import argparse
import csv
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import subprocess
import time


def output(args):
    return subprocess.check_output(args, text=True, encoding="utf-8")


def byte_value(value):
    match = re.fullmatch(r"([\d.]+)([A-Za-z]+)", value.strip())
    if not match:
        return None
    scales = {
        "B": 1,
        "kB": 1000,
        "KB": 1000,
        "MB": 1000**2,
        "GB": 1000**3,
        "KiB": 1024,
        "MiB": 1024**2,
        "GiB": 1024**3,
    }
    return int(float(match[1]) * scales[match[2]])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session", type=Path, required=True)
    args = parser.parse_args()
    state = args.session / "session.json"
    fields = [
        "at",
        "size",
        "repeat",
        "operation",
        "active_store",
        "container",
        "cpu_percent_one_core",
        "memory_bytes",
        "memory_limit_bytes",
        "block_read_bytes",
        "block_write_bytes",
    ]
    with (args.session / "backend_resources.csv").open(
        "x", encoding="utf-8", newline=""
    ) as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        while json.loads(state.read_text())["phase"] in (
            "measuring",
            "waiting_safe_pause",
        ):
            runners = output(
                [
                    "docker",
                    "ps",
                    "--filter",
                    "label=com.docker.compose.project=speedrun-comparison-20261030",
                    "--filter",
                    "label=com.docker.compose.service=runner",
                    "--format",
                    "{{.ID}}",
                ]
            ).split()
            if not runners:
                time.sleep(2)
                continue
            code = "import json;from pathlib import Path;p=sorted(Path('/opt/airflow/state/analytics').glob('results_*/status.json'));print(p[-1].read_text() if p else '{}')"
            status = json.loads(
                output(["docker", "exec", runners[0], "python", "-c", code])
            )
            names = [
                "speedrun-comparison-20261030-" + name + "-1"
                for name in ("postgres", "mongo", "garage")
            ]
            result = output(
                ["docker", "stats", "--no-stream", "--format", "{{json .}}", *names]
            )
            for line in result.splitlines():
                row = json.loads(line)
                memory = row["MemUsage"].split("/")
                io = row["BlockIO"].split("/")
                writer.writerow(
                    {
                        "at": datetime.now(timezone.utc).isoformat(),
                        "size": status.get("size"),
                        "repeat": status.get("repeat"),
                        "operation": status.get("operation"),
                        "active_store": status.get("store"),
                        "container": row["Name"],
                        "cpu_percent_one_core": row["CPUPerc"].rstrip("%"),
                        "memory_bytes": byte_value(memory[0]),
                        "memory_limit_bytes": byte_value(memory[1]),
                        "block_read_bytes": byte_value(io[0]),
                        "block_write_bytes": byte_value(io[1]),
                    }
                )
            handle.flush()
            time.sleep(2)


if __name__ == "__main__":
    main()
