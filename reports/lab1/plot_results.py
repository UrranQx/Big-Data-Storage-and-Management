"""Render the actual benchmark CSV; never substitute synthetic measurements."""
import csv
from pathlib import Path
import statistics
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

source = Path(sys.argv[1])
rows = list(csv.DictReader(source.open(encoding="utf-8")))
stores = ["postgres", "mongo", "garage"]
labels = ["PostgreSQL", "MongoDB", "Garage S3"]
summary = []
for metric, operation, field, divisor, title, filename in [
    ("verified_load_seconds", "load_verified", "seconds", 1, "Загрузка с проверкой, секунды", "load_time.png"),
    ("reported_MiB", "size", "reported_bytes", 1024**2, "Объём по метрике сервиса, МиБ", "storage_size.png"),
]:
    groups = [[float(r[field]) / divisor for r in rows if r["store"] == store and r["operation"] == operation] for store in stores]
    if any(len(values) != 3 for values in groups):
        raise ValueError("Exactly three measured repetitions per store required")
    medians = [statistics.median(values) for values in groups]
    lower = [m - min(v) for m,v in zip(medians, groups)]
    upper = [max(v) - m for m,v in zip(medians, groups)]
    fig, ax = plt.subplots(figsize=(8, 4.8))
    ax.bar(labels, medians, yerr=[lower, upper], capsize=5, color=["#336791", "#4b9c46", "#e2972c"])
    ax.set_ylabel(title)
    ax.set_title("Одинаковые данные: медиана и диапазон трёх повторов")
    ax.grid(axis="y", alpha=0.2)
    if operation == "size":
        fig.text(0.02, 0.02, "PG: таблицы + индексы; Mongo: storageSize + indexSize; S3: размеры объектов.\nФизический объём Garage с метаданными в последнюю метрику не входит.", fontsize=8)
        fig.tight_layout(rect=(0, 0.09, 1, 1))
    else:
        fig.tight_layout()
    fig.savefig(source.parent / filename, dpi=160)
    plt.close(fig)
    for store,values in zip(stores,groups):
        summary.append({"metric": metric, "store": store, "median": statistics.median(values), "min": min(values), "max": max(values)})
with (source.parent / "summary.csv").open("w", newline="", encoding="utf-8") as output:
    writer = csv.DictWriter(output, fieldnames=["metric", "store", "median", "min", "max"])
    writer.writeheader()
    writer.writerows(summary)
