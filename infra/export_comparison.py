"""Выгружает небольшие результаты сравнения, исключая SQLite и сырые пакеты.

Копирует CSV, планы, манифесты и логи из отдельного state в каталог отчёта;
секреты/production volumes этот скрипт не читает.
"""

import argparse
from pathlib import Path
import shutil


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not (args.state / "isolated-experiment.json").exists():
        raise ValueError("Only an isolated comparison state is accepted")
    count = 0
    for root in (args.state / "benchmarks").iterdir():
        if not (root / "completed.json").exists():
            target = args.output / "resource_probe" / root.name
            target.mkdir(parents=True, exist_ok=True)
            for name in (
                "config.json",
                "measurements.csv",
                "status.json",
                "driver.log",
            ):
                if (root / name).is_file():
                    shutil.copyfile(root / name, target / name)
            continue  # Resource-limited/aborted series are not final measurements.
        for path in root.rglob("*"):
            if path.is_file() and path.suffix in (".csv", ".json", ".log"):
                target = args.output / "raw" / root.name / path.relative_to(root)
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(path, target)
                count += 1
    for name in ("frozen-inputs.json", "isolated-experiment.json"):
        (args.output / "evidence").mkdir(exist_ok=True)
        shutil.copyfile(args.state / name, args.output / "evidence" / name)
    print("Exported result files:", count)


if __name__ == "__main__":
    main()
