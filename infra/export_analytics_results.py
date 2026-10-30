"""Экспортирует только завершённые CSV/JSON аналитического опыта в отчёт.

Исходные SQLite, нормализованные записи, секреты и private.env не копируются.
"""

import argparse
from pathlib import Path
import shutil


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    completed = sorted(args.root.glob("results_*/completed.json"))
    if not completed:
        raise ValueError("No completed analytics experiment")
    source = completed[-1].parent
    raw = args.output / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    for path in source.iterdir():
        if path.is_file() and path.suffix in (".json", ".csv"):
            shutil.copyfile(path, raw / path.name)
    print("Exported completed experiment", source.name)


if __name__ == "__main__":
    main()
