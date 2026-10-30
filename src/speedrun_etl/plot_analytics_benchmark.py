"""Строит таблицы и Matplotlib-рисунки полного аналитического сравнения.

Использует только опубликованные измерения. Пропуски не дополняются числами;
память — наблюдавшийся RSS клиента, не память всех сервисов вместе.
"""

import argparse
from collections import Counter, defaultdict
import csv
import json
from pathlib import Path
import statistics


STORES = ("postgres", "mongo", "garage")
NAMES = {"postgres": "PostgreSQL", "mongo": "MongoDB", "garage": "Garage S3"}
COLORS = {"postgres": "#4477AA", "mongo": "#228833", "garage": "#EE6677"}
LABELS = {
    "normalization_load": "Загрузка 6 связанных наборов",
    "indexes": "Индексы входа",
    "activity": "Активность всех игр",
    "geography": "Глобальный JOIN и география",
    "best": "Лучший результат участника",
    "ranked": "Ранги внутри зачёта",
    "features": "Признаки и веса зачёта",
    "scores": "Сумма баллов игрока",
    "rating": "Глобальный рейтинг",
    "publish_views": "Подготовка быстрого чтения",
    "cached_top20": "Чтение готового top-20",
    "synthetic_patch": "Тестовые изменения",
    "full_rating_refresh": "Полный пересчёт рейтинга",
    "incremental_rating_refresh": "Частичное обновление рейтинга",
}
BUILD = (
    "normalization_load",
    "indexes",
    "activity",
    "geography",
    "best",
    "ranked",
    "features",
    "scores",
    "rating",
    "publish_views",
)


def read_csv(path):
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path, values):
    if not values:
        raise ValueError("No values for " + str(path))
    fields = list(dict.fromkeys(k for row in values for k in row))
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(values)


def summarize(values):
    groups = defaultdict(list)
    for r in values:
        groups[int(r["size"]), r["store"], r["operation"]].append(r)
    result = []
    for (size, store, op), group in sorted(groups.items()):
        times = [float(v["seconds"]) for v in group]
        q = (
            statistics.quantiles(times, n=4, method="inclusive")
            if len(times) > 1
            else [times[0]] * 3
        )
        result.append(
            {
                "size": size,
                "store": store,
                "operation": op,
                "n": len(times),
                "median_seconds": statistics.median(times),
                "min_seconds": min(times),
                "max_seconds": max(times),
                "q25_seconds": q[0],
                "q75_seconds": q[2],
                "median_client_cpu_seconds": statistics.median(
                    float(v["client_cpu_seconds"]) for v in group
                ),
                "max_sampled_client_rss_bytes": max(
                    int(v["sampled_client_peak_rss_bytes"]) for v in group
                ),
            }
        )
    return result


def verify_results(root, values):
    """Отказывает при неполной серии или расхождении результатов, включая delta."""
    completed = json.loads((root / "raw/completed.json").read_text(encoding="utf-8"))
    expected_ops = Counter({op: 1 for op in LABELS})
    expected_ops["cached_top20"] = 3
    trials = defaultdict(list)
    proofs = defaultdict(set)
    for row in values:
        trials[int(row["size"]), int(row["repeat"]), row["store"]].append(row)
        if row.get("digest"):
            proofs[int(row["size"]), row["operation"]].add(
                (row["count"], row["digest"])
            )
    expected_trials = {
        (size, repeat, store)
        for size in completed["sizes"]
        for repeat in range(1, completed["repeats"] + 1)
        for store in STORES
    }
    if set(trials) != expected_trials:
        raise ValueError("Incomplete size/repeat/store coverage")
    for key, trial in trials.items():
        if Counter(row["operation"] for row in trial) != expected_ops:
            raise ValueError(f"Incomplete operations: {key}")
    for key, signatures in proofs.items():
        if len(signatures) != 1:
            raise ValueError(f"Different readback results across stores/repeats: {key}")
    for size in completed["sizes"]:
        if (
            proofs[size, "full_rating_refresh"]
            != proofs[size, "incremental_rating_refresh"]
        ):
            raise ValueError(f"Full/incremental mismatch: {size}")
    records = list((root / "raw").glob("verified_*.json"))
    if len(records) != len(expected_trials):
        raise ValueError("Missing trial verification records")
    if not all(
        json.loads(p.read_text(encoding="utf-8"))["all_input_relations_verified"]
        for p in records
    ):
        raise ValueError("Unverified input relations")
    return {
        "measurement_rows": len(values),
        "trials": len(trials),
        "proof_groups": len(proofs),
        "cross_store_delta_equal": True,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", type=Path, required=True)
    args = parser.parse_args()
    root = args.results
    assert (root / "raw/completed.json").exists()
    data = root / "data"
    figures = root / "figures"
    data.mkdir(exist_ok=True)
    figures.mkdir(exist_ok=True)
    values = read_csv(root / "raw/measurements.csv")
    proof = verify_results(root, values)
    (root / "raw/postprocessing_verification.json").write_text(
        json.dumps(proof, indent=2) + "\n", encoding="utf-8"
    )
    write_csv(data / "measurements.csv", values)
    groups = defaultdict(list)
    for r in values:
        groups[int(r["size"]), r["repeat"], r["store"]].append(r)
    totals = []
    for (size, repeat, store), group in groups.items():
        build = [r for r in group if r["operation"] in BUILD]
        if len(build) != len(BUILD):
            raise ValueError("Incomplete build pipeline")
        totals.append(
            {
                "size": size,
                "repeat": repeat,
                "store": store,
                "operation": "total_pipeline",
                "seconds": sum(float(r["seconds"]) for r in build),
                "client_cpu_seconds": sum(
                    float(r["client_cpu_seconds"]) for r in build
                ),
                "sampled_client_peak_rss_bytes": max(
                    int(r["sampled_client_peak_rss_bytes"]) for r in build
                ),
            }
        )
    summary = summarize(values + totals)
    write_csv(data / "summary.csv", summary)
    write_csv(data / "pipeline_totals.csv", totals)
    sizes = sorted({int(r["size"]) for r in values})
    maximum = max(sizes)
    indexed = {(r["size"], r["store"], r["operation"]): r for r in summary}

    def stat(size, store, op):
        return indexed[size, store, op]

    inputs = []
    for size in sizes:
        m = json.loads((root / f"raw/input_{size}.json").read_text(encoding="utf-8"))
        inputs.append(
            {
                "size": size,
                **m["counts"],
                **m["diagnostics"],
                "common_preparation_seconds": m["normalization_seconds"],
            }
        )
    write_csv(data / "input_stats.csv", inputs)
    if (root / "raw/input_distribution.csv").exists():
        write_csv(
            data / "input_distribution.csv",
            read_csv(root / "raw/input_distribution.csv"),
        )
    tasks = []
    for op in [
        *BUILD,
        "total_pipeline",
        "full_rating_refresh",
        "incremental_rating_refresh",
        "cached_top20",
    ]:
        tasks.append(
            {
                "operation": op,
                "task": LABELS.get(op, "Весь конвейер"),
                **{
                    s + "_seconds": stat(maximum, s, op)["median_seconds"]
                    for s in STORES
                },
            }
        )
    write_csv(data / "task_comparison.csv", tasks)

    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "font.size": 10,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "svg.fonttype": "none",
            "axes.titleweight": "bold",
        }
    )

    def save(fig, name, note):
        fig.tight_layout(rect=(0, 0.09, 1, 0.96))
        fig.text(0.015, 0.015, note, fontsize=8, color="#536173", wrap=True)
        fig.savefig(figures / (name + ".png"), dpi=200, bbox_inches="tight")
        svg = figures / (name + ".svg")
        fig.savefig(svg, bbox_inches="tight")
        svg.write_text(
            "\n".join(s.rstrip() for s in svg.read_text(encoding="utf-8").splitlines())
            + "\n",
            encoding="utf-8",
        )
        plt.close(fig)

    def line(ax, op):
        for s in STORES:
            vv = [stat(n, s, op) for n in sizes]
            ax.plot(
                sizes,
                [r["median_seconds"] for r in vv],
                "-o",
                label=NAMES[s],
                color=COLORS[s],
            )
            ax.fill_between(
                sizes,
                [r["min_seconds"] for r in vv],
                [r["max_seconds"] for r in vv],
                color=COLORS[s],
                alpha=0.12,
            )
        ax.set(xscale="log", yscale="log", xlabel="Ранов во входе", ylabel="Секунды")
        ax.grid(alpha=0.2)

    fig, ax = plt.subplots(figsize=(10, 5.5))
    line(ax, "total_pipeline")
    ax.set_title("Полный конвейер: связанные наборы → аналитические витрины")
    ax.legend(frameon=False)
    save(
        fig,
        "01_full_pipeline",
        "Медиана и min–max трёх повторов. Общая подготовка фиксированного входа вынесена отдельно; readback вне таймеров. Кеш не сбрасывался.",
    )
    ops = (
        "normalization_load",
        "activity",
        "geography",
        "best",
        "ranked",
        "features",
        "scores",
        "rating",
    )
    fig, axes = plt.subplots(2, 4, figsize=(15, 8))
    fig.suptitle(f"Все игры входа: {maximum:,} ранов".replace(",", " "))
    for ax, op in zip(axes.flat, ops):
        vv = [stat(maximum, s, op) for s in STORES]
        med = [r["median_seconds"] for r in vv]
        ax.bar(
            range(3),
            med,
            color=[COLORS[s] for s in STORES],
            yerr=[
                [v - r["min_seconds"] for v, r in zip(med, vv)],
                [r["max_seconds"] - v for v, r in zip(med, vv)],
            ],
            capsize=3,
        )
        ax.set(title=LABELS[op], yscale="log", ylabel="Секунды")
        ax.set_xticks(range(3), ["PG", "Mongo", "S3"])
        ax.grid(axis="y", alpha=0.15)
        for i, r in enumerate(vv):
            ax.annotate(
                f"{r['median_seconds']:.3f}",
                (i, r["max_seconds"]),
                xytext=(0, 5),
                textcoords="offset points",
                ha="center",
                fontsize=8,
            )
    save(
        fig,
        "02_maximum_tasks",
        "Медиана и min–max. SQL/Mongo считают на сервере; S3 читает и считает клиентом. Технические группы и веса не являются официальным рейтингом.",
    )
    fig, axes = plt.subplots(1, 3, figsize=(14, 5))
    for ax, s in zip(axes, STORES):
        for op, style, label in [
            ("full_rating_refresh", "-o", "Полный"),
            ("incremental_rating_refresh", "--s", "Частичный"),
        ]:
            vv = [stat(n, s, op) for n in sizes]
            ax.plot(
                sizes,
                [r["median_seconds"] for r in vv],
                style,
                label=label,
                color=COLORS[s],
                alpha=1 if op.startswith("full") else 0.65,
            )
        ax.set(
            title=NAMES[s], xscale="log", yscale="log", xlabel="Ранов", ylabel="Секунды"
        )
        ax.grid(alpha=0.2)
        ax.legend(frameon=False)
    save(
        fig,
        "03_refresh",
        "Одинаковые синтетические изменения тестовых копий; частичный результат проверен против полного. Заключительное глобальное ранжирование остаётся необходимым.",
    )
    fig, ax = plt.subplots(figsize=(10, 5.5))
    for s in STORES:
        vv = []
        for n in sizes:
            peaks = [
                int(r["sampled_client_peak_rss_bytes"]) / 1024**2
                for r in totals
                if int(r["size"]) == n and r["store"] == s
            ]
            vv.append(statistics.median(peaks))
        ax.plot(sizes, vv, "-o", label=NAMES[s], color=COLORS[s])
    ax.set(
        xscale="log",
        xlabel="Ранов",
        ylabel="Наблюдавшийся RSS клиента, МиБ",
        title="Память клиента во время построения витрин",
    )
    ax.legend(frameon=False)
    ax.grid(alpha=0.2)
    save(
        fig,
        "04_client_memory",
        "Медиана максимального наблюдения по повтору; выборка RSS каждые 50 мс, включая базовую память процесса. Backend сюда не включён; лимит клиента 3072 МиБ.",
    )
    fig, axes = plt.subplots(1, 2, figsize=(11, 5))
    for ax, op, title in zip(
        axes,
        ["total_pipeline", "cached_top20"],
        ["Построение всех витрин", "Чтение готового top-20"],
    ):
        v = [stat(maximum, s, op)["median_seconds"] for s in STORES]
        ax.bar(range(3), v, color=[COLORS[s] for s in STORES])
        ax.set_xticks(range(3), [NAMES[s] for s in STORES])
        ax.set(title=title, ylabel="Секунды", yscale="log")
        ax.grid(axis="y", alpha=0.2)
        for i, value in enumerate(v):
            ax.annotate(
                f"{value:.4f}",
                (i, value),
                xytext=(0, 5),
                textcoords="offset points",
                ha="center",
            )
    save(
        fig,
        "05_build_vs_read",
        "Разные этапы. Для S3 top-20 опубликован отдельным объектом с учтённой ценой подготовки; для PG/Mongo созданы индексы. Это не сравнение сырого сканирования с кешем.",
    )

    storage_path = root / "raw/storage_sizes.csv"
    storage_summary = []
    if storage_path.exists():
        storage = read_csv(storage_path)
        write_csv(data / "storage_sizes.csv", storage)
        for n in sizes:
            for s in STORES:
                group = [r for r in storage if int(r["size"]) == n and r["store"] == s]
                if len(group) != 3:
                    raise ValueError("Incomplete storage size measurements")
                fields = (
                    "data_bytes",
                    "index_bytes",
                    "reported_bytes",
                    "retained_all_objects_bytes",
                )
                storage_summary.append(
                    {
                        "size": n,
                        "store": s,
                        "method": group[0]["method"],
                        **{
                            field: statistics.median(float(r[field]) for r in group)
                            if all(r[field] != "" for r in group)
                            else ""
                            for field in fields
                        },
                    }
                )
        write_csv(data / "storage_summary.csv", storage_summary)
        fig, axes = plt.subplots(1, 2, figsize=(12, 5))
        for s in STORES:
            group = [r for r in storage_summary if r["store"] == s]
            axes[0].plot(
                [r["size"] for r in group],
                [r["reported_bytes"] / 1024**2 for r in group],
                "-o",
                label=NAMES[s],
                color=COLORS[s],
            )
        axes[0].set(
            xscale="log",
            yscale="log",
            xlabel="Ранов",
            ylabel="МиБ",
            title="Доступные показатели занятого объёма",
        )
        axes[0].legend(frameon=False)
        for i, s in enumerate(STORES):
            row = next(
                r for r in storage_summary if r["size"] == maximum and r["store"] == s
            )
            body = row["data_bytes"] / 1024**2
            index = (row["index_bytes"] or 0) / 1024**2
            axes[1].bar(i, body, color=COLORS[s])
            axes[1].bar(i, index, bottom=body, color=COLORS[s], alpha=0.45, hatch="//")
        axes[1].set_xticks(range(3), ["PG", "Mongo", "S3"])
        axes[1].set(ylabel="МиБ", title="Максимальный вход: данные и индексы (штрих)")
        for ax in axes:
            ax.grid(alpha=0.2, axis="y")
        save(
            fig,
            "06_reported_storage",
            "После тестовых изменений, без *_full. PG: таблицы/TOAST + индексы; Mongo: storageSize + индексы после fsync; S3: тела текущих объектов. Различные метрики, не полный физический объём volumes.",
        )

    def cell(size, s, op):
        if s == "garage" and op == "indexes":
            return "— (не создаются)"
        r = stat(size, s, op)
        return (
            f"{r['median_seconds']:.4f} [{r['min_seconds']:.4f}–{r['max_seconds']:.4f}]"
        )

    text = [
        "# Расширенное сравнение: результаты\n",
        "Время: секунды, **медиана [min–max]**. Три повтора; для чтения top-20 — девять значений.\n",
        "## Полный конвейер\n",
        "| Ранов | PostgreSQL | MongoDB | Garage S3 |",
        "|---:|---:|---:|---:|",
    ]
    for n in sizes:
        text.append(
            "| "
            + str(n)
            + " | "
            + " | ".join(cell(n, s, "total_pipeline") for s in STORES)
            + " |"
        )
    text += [
        "\n## Максимальный вход\n",
        "| Задача | PostgreSQL | MongoDB | Garage S3 |",
        "|---|---:|---:|---:|",
    ]
    for op in [
        *BUILD,
        "full_rating_refresh",
        "incremental_rating_refresh",
        "cached_top20",
    ]:
        text.append(
            "| "
            + LABELS[op]
            + " | "
            + " | ".join(cell(maximum, s, op) for s in STORES)
            + " |"
        )
    text += [
        "\n## Состав входа и общая подготовка\n",
        "| Ранов | Игр | Категорий | Технических групп | Профилей/placeholder | Участий | Подготовка, с |",
        "|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for r in inputs:
        text.append(
            f"| {r['size']} | {r['games']} | {r['categories']} | {r['boards']} | {r['players']} | {r['run_players']} | {r['common_preparation_seconds']:.2f} |"
        )
    text += [
        "\nПодготовка — общий клиентский разбор/отбор фиксированного входа и создание связанных проекций; не сравнение нативных JSON-парсеров серверов. Она не включена в таблицу конвейера. Исходный ODS не нормализуется на месте.\n",
        "Ранги/веса экспериментальные, raw и coverage источника не меняются. При частичном обновлении проверяется рейтинг; прочие ранее построенные витрины не объявляются обновлёнными. Кеш не сбрасывался.\n",
    ]
    if storage_summary:
        text += [
            "\n## Объём после тестовых изменений\n",
            "МиБ, медиана трёх namespaces. Доступные метрики различаются; это не одинаковый физический учёт. S3 retention указан отдельно.\n",
            "| Ранов | PostgreSQL, таблицы + индексы | MongoDB, storage + индексы | S3, текущие тела | S3, все сохранённые тела |",
            "|---:|---:|---:|---:|---:|",
        ]
        for n in sizes:
            group = {r["store"]: r for r in storage_summary if r["size"] == n}
            fields = [group[s]["reported_bytes"] for s in STORES] + [
                group["garage"]["retained_all_objects_bytes"]
            ]
            text.append(
                "| "
                + str(n)
                + " | "
                + " | ".join(f"{v / 1024**2:.2f}" for v in fields)
                + " |"
            )
    (root / "TABLES.md").write_text("\n".join(text), encoding="utf-8")
    print(
        "Saved",
        len(values),
        "measurements and",
        6 if storage_summary else 5,
        "PNG/SVG figures",
    )


if __name__ == "__main__":
    main()
