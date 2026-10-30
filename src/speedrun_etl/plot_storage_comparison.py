"""Сводит реальные CSV замеров в таблицы и экспортируемые графики Matplotlib.

Читает выгруженные результаты отдельного стенда. Не обращается к СУБД или API,
не дополняет отсутствующие измерения; PNG/SVG и CSV пишет в каталог отчёта.
"""

import argparse
from collections import defaultdict
import csv
import json
import math
from pathlib import Path
import statistics


STORES = ("postgres", "mongo", "garage")
NAMES = {"postgres": "PostgreSQL", "mongo": "MongoDB", "garage": "Garage S3"}
COLORS = {"postgres": "#4477AA", "mongo": "#228833", "garage": "#EE6677"}
LABELS = {
    "history_100": "Наблюдения 100 ранов",
    "latest_100": "Последнее состояние 100 ранов",
    "filter": "Раны категории и статуса",
    "wr_events": "События WR игры",
    "game_rules": "Карточка игры и правила",
    "profiles_100": "100 профилей игроков",
    "batch_100": "Чтение 100 ранов по ID",
    "scope_top_100": "Фильтр и сортировка: top-100",
    "json_filter": "Фильтр по полю JSON",
    "player_runs": "Раны одного игрока",
    "monthly_counts": "Количество ранов по месяцам",
    "cumulative_counts": "Накопительный итог / окно",
    "join_1000": "Связь 1000 ранов с профилями",
    "offset_page": "Страница через OFFSET",
    "keyset_page": "Страница по ключу",
}


def read_csv(path):
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path, rows):
    if not rows:
        raise ValueError("No measurements for " + str(path))
    fields = list(dict.fromkeys(key for row in rows for key in row))
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def summarize(rows):
    groups = defaultdict(list)
    for row in rows:
        if row.get("seconds"):
            key = (
                row["suite"],
                row.get("size", ""),
                row["store"],
                row.get("phase", ""),
                row["operation"],
            )
            groups[key].append(row)
    result = []
    for key, group in sorted(groups.items()):
        times = [float(row["seconds"]) for row in group]
        if len(times) >= 2:
            q25, _, q75 = statistics.quantiles(times, n=4, method="inclusive")
        else:
            q25 = q75 = times[0]
        item = dict(zip(("suite", "size", "store", "phase", "operation"), key))
        item.update(
            n=len(times),
            median_seconds=statistics.median(times),
            min_seconds=min(times),
            max_seconds=max(times),
            q25_seconds=q25,
            q75_seconds=q75,
            mean_seconds=statistics.mean(times),
            stdev_seconds=statistics.stdev(times) if len(times) > 1 else 0,
        )
        for field in (
            "get_calls",
            "read_bytes",
            "put_calls",
            "written_bytes",
            "copy_calls",
            "copied_bytes",
            "delete_calls",
        ):
            values = [float(row[field]) for row in group if row.get(field)]
            if values:
                item[field] = statistics.median(values)
        result.append(item)
    return result


def summarize_plans(root):
    """Отделяет диагностику explain от клиентских измерений времени."""
    result = []
    for path in sorted(root.glob("plans_*.json")):
        _, size, repeat, store, *phase = path.stem.split("_")
        for operation, explain in json.loads(path.read_text(encoding="utf-8")).items():
            item = {
                "size": int(size),
                "repeat": int(repeat) + 1,
                "store": store,
                "phase": "_".join(phase),
                "operation": operation,
            }
            if store == "postgres":
                document = explain[0]
                plan = document["Plan"]
                item.update(
                    execution_ms=document.get("Execution Time"),
                    planning_ms=document.get("Planning Time"),
                    rows_returned=plan.get("Actual Rows"),
                    shared_hit_blocks=plan.get("Shared Hit Blocks"),
                    shared_read_blocks=plan.get("Shared Read Blocks"),
                )
                stage_key = "Node Type"
            else:
                cursor = explain
                for stage in explain.get("stages", []):
                    if "$cursor" in stage:
                        cursor = stage["$cursor"]
                        break
                stats = cursor.get("executionStats", {})
                item.update(
                    execution_ms=stats.get("executionTimeMillis"),
                    rows_returned=stats.get("nReturned"),
                    primary_docs_examined=stats.get("totalDocsExamined"),
                    primary_keys_examined=stats.get("totalKeysExamined"),
                )
                plan = stats.get("executionStages") or cursor.get(
                    "queryPlanner", {}
                ).get("winningPlan", {})
                stage_key = "stage"
            stages = set()

            def visit(value):
                if isinstance(value, dict):
                    if stage_key in value:
                        stages.add(value[stage_key])
                    for child in value.values():
                        visit(child)
                elif isinstance(value, list):
                    for child in value:
                        visit(child)

            visit(plan)
            item["plan_stages"] = ";".join(sorted(stages))
            result.append(item)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", type=Path, required=True)
    args = parser.parse_args()
    root = args.results.resolve()
    data = root / "data"
    data.mkdir(exist_ok=True)
    raw = root / "raw"
    ods_root = next(raw.glob("entity_v3_*"))
    workload_root = next(raw.glob("workloads_*"))
    bulk_root = next(raw.glob("pg_bulk_*"))
    for directory in (ods_root, workload_root, bulk_root):
        if not (directory / "completed.json").exists():
            raise ValueError("Experiment is not complete: " + str(directory))
    rows = []
    for suite, directory in (
        ("ods", ods_root),
        ("workload", workload_root),
        ("pg_bulk", bulk_root),
    ):
        for row in read_csv(directory / "measurements.csv"):
            row.update(suite=suite, store=row.get("store") or "postgres")
            if suite == "pg_bulk":
                row["phase"] = row["method"]
            rows.append(row)
    write_csv(data / "measurements.csv", rows)
    summary = summarize(rows)
    write_csv(data / "summary.csv", summary)
    write_csv(data / "query_plans.csv", summarize_plans(workload_root))
    write_csv(
        data / "s3_io.csv",
        [
            row
            for row in summary
            if row["suite"] == "workload"
            and row["store"] == "garage"
            and ("get_calls" in row or "put_calls" in row)
        ],
    )
    lookup = {
        (r["suite"], r["size"], r["store"], r["phase"], r["operation"]): r
        for r in summary
    }

    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "font.size": 10,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "axes.titleweight": "bold",
            "figure.facecolor": "white",
            "axes.labelcolor": "#273444",
            "text.color": "#273444",
            "svg.fonttype": "none",
        }
    )
    figures = root / "figures"
    figures.mkdir(exist_ok=True)

    def save(fig, name, note):
        fig.text(0.02, 0.015, note, fontsize=8, color="#536170", va="bottom")
        fig.tight_layout(rect=(0, 0.09, 1, 0.94))
        fig.savefig(figures / (name + ".png"), dpi=200, bbox_inches="tight")
        svg = figures / (name + ".svg")
        fig.savefig(svg, bbox_inches="tight")
        # Matplotlib leaves trailing spaces in path attributes; line breaks
        # already delimit coordinates, so remove them for clean Git diffs.
        svg.write_text(
            "\n".join(
                line.rstrip() for line in svg.read_text(encoding="utf-8").splitlines()
            )
            + "\n",
            encoding="utf-8",
        )
        plt.close(fig)

    def timing(suite, store, op, size="", phase=""):
        return lookup[suite, str(size), store, phase, op]

    def bars(ax, items, title, *, milliseconds=False, log=False):
        scale = 1000 if milliseconds else 1
        values = [item["median_seconds"] * scale for item in items]
        errors = [
            [
                max(0, value - item["min_seconds"] * scale)
                for value, item in zip(values, items)
            ],
            [
                max(0, item["max_seconds"] * scale - value)
                for value, item in zip(values, items)
            ],
        ]
        ax.bar(
            range(len(items)),
            values,
            color=[COLORS[item["store"]] for item in items],
            yerr=errors,
            capsize=4,
            width=0.63,
        )
        ax.set_xticks(range(len(items)), [NAMES[item["store"]] for item in items])
        ax.set(title=title, ylabel="Время, мс" if milliseconds else "Время, с")
        if log:
            ax.set_yscale("log")
        ax.grid(axis="y", alpha=0.18)
        for i, value in enumerate(values):
            ax.annotate(
                f"{value:.2f}",
                (i, items[i]["max_seconds"] * scale),
                xytext=(0, 7),
                textcoords="offset points",
                ha="center",
                fontsize=9,
            )

    fig, axes = plt.subplots(1, 2, figsize=(11, 4.9))
    for ax, op, title in zip(
        axes,
        ("load_verified", "replay_verified"),
        ("Первая загрузка", "Идемпотентный повтор"),
    ):
        bars(ax, [timing("ods", store, op) for store in STORES], title)
    fig.suptitle("ODS entity-v3: сохранённый снимок Portal", fontsize=15)
    save(
        fig,
        "01_ods_load",
        "Медиана и min–max трёх повторов. Включены загрузка, полная сверка чтения и receipt; это не чистое время INSERT.",
    )

    size_rows = [r for r in rows if r["suite"] == "ods" and r["operation"] == "size"]
    fig, ax = plt.subplots(figsize=(9, 5.3))
    size_summary = []
    for i, store in enumerate(STORES):
        group = [r for r in size_rows if r["store"] == store]
        reported = (
            statistics.median(float(r["reported_bytes"]) for r in group) / 1024**2
        )
        indexes = [float(r["index_bytes"]) for r in group if r.get("index_bytes")]
        index = statistics.median(indexes) / 1024**2 if indexes else 0
        ax.bar(
            i,
            reported - index,
            color=COLORS[store],
            width=0.6,
            label="Данные / тела объектов" if i == 0 else None,
        )
        if index:
            ax.bar(
                i,
                index,
                bottom=reported - index,
                color=COLORS[store],
                hatch="///",
                alpha=0.55,
                width=0.6,
                label="Индексы" if i == 0 else None,
            )
        ax.annotate(
            f"{reported:.1f}",
            (i, reported),
            xytext=(0, 7),
            textcoords="offset points",
            ha="center",
        )
        size_summary.append(
            {
                "store": store,
                "reported_MiB": reported,
                "index_MiB": index if indexes else "",
                "method": group[0]["size_method"],
            }
        )
    ax.set_xticks(range(3), [NAMES[s] for s in STORES])
    ax.set(ylabel="МиБ", title="Объём одного одинакового ODS-входа")
    ax.legend(frameon=False)
    ax.grid(axis="y", alpha=0.18)
    save(
        fig,
        "02_ods_size",
        "PG: таблицы с TOAST + индексы; Mongo: storageSize + индексы; S3: сумма тел объектов, без overhead volume. Метрики различаются.",
    )
    write_csv(data / "storage_size.csv", size_summary)

    operations = (
        "history_100",
        "latest_100",
        "filter",
        "wr_events",
        "game_rules",
        "profiles_100",
    )
    fig, axes = plt.subplots(2, 3, figsize=(13, 8.4))
    for ax, op in zip(axes.flat, operations):
        bars(
            ax,
            [timing("ods", store, op) for store in STORES],
            LABELS[op],
            milliseconds=True,
            log=True,
        )
        ax.tick_params(axis="x", labelsize=8)
    fig.suptitle(
        "Чтение ODS: одинаковые результаты, разные способы доступа", fontsize=15
    )
    save(
        fig,
        "03_ods_reads",
        "Медиана и min–max девяти проходов. Логарифмическая шкала. Первый проход не считается холодным кешем; история — в рамках снимка.",
    )

    sizes = sorted({int(r["size"]) for r in summary if r["suite"] == "workload"})
    queries = (
        "batch_100",
        "scope_top_100",
        "json_filter",
        "player_runs",
        "monthly_counts",
        "cumulative_counts",
        "join_1000",
        "offset_page",
        "keyset_page",
    )
    fig, axes = plt.subplots(3, 3, figsize=(14, 12))
    for ax, op in zip(axes.flat, queries):
        for store in STORES:
            items = [timing("workload", store, op, size, "indexed") for size in sizes]
            med = [r["median_seconds"] * 1000 for r in items]
            ax.plot(
                sizes,
                med,
                marker="o",
                label=NAMES[store],
                color=COLORS[store],
                linewidth=2,
            )
            ax.fill_between(
                sizes,
                [r["min_seconds"] * 1000 for r in items],
                [r["max_seconds"] * 1000 for r in items],
                color=COLORS[store],
                alpha=0.1,
            )
        ax.set(
            xscale="log",
            yscale="log",
            title=LABELS[op],
            xlabel="Число ранов",
            ylabel="Время, мс",
        )
        ax.grid(alpha=0.18)
    axes[0, 0].legend(frameon=False, fontsize=8)
    fig.suptitle("Запросы на 1 000, 10 000 и 100 000 ранов", fontsize=16)
    save(
        fig,
        "04_query_scaling",
        "Одинаковые временные проекции; PG/Mongo с индексами, S3 читает блоки клиентом. Медиана и min–max девяти чтений; заливка — диапазон.",
    )

    size = max(sizes)
    fig, axes = plt.subplots(1, 2, figsize=(12, 5.2))
    index_ops = ("scope_top_100", "json_filter", "player_runs")
    for ax, store in zip(axes, STORES[:2]):
        for i, phase in enumerate(("pk_only", "indexed")):
            values = [
                timing("workload", store, op, size, phase)["median_seconds"] * 1000
                for op in index_ops
            ]
            ax.bar(
                [j + (i - 0.5) * 0.34 for j in range(3)],
                values,
                width=0.34,
                label="Только PK" if i == 0 else "Доп. индексы",
                color="#AAB7C4" if i == 0 else COLORS[store],
            )
        ax.set_xticks(range(3), ["Фильтр + sort", "Поле JSON", "Участник"])
        ax.set(yscale="log", ylabel="Время, мс", title=NAMES[store])
        ax.legend(frameon=False)
        ax.grid(axis="y", alpha=0.18)
    fig.suptitle("Эффект дополнительных индексов: 100 000 ранов", fontsize=15)
    save(
        fig,
        "05_index_effect",
        "Кеш не сбрасывался. Построение индексов измерено отдельно; планы PG/Mongo сохранены вне таймеров пользовательских запросов.",
    )

    table_ops = (
        "clone_table",
        "rename_clone",
        "update_1000",
        "append_100",
        "delete_100",
        "cache_build",
        "cached_counts",
    )
    titles = (
        "Копирование",
        "Переименование",
        "UPDATE 1000",
        "INSERT 100",
        "DELETE 100",
        "Расчёт сводки",
        "Чтение сводки",
    )
    fig, ax = plt.subplots(figsize=(13, 5.8))
    for i, store in enumerate(STORES):
        ax.bar(
            [j + (i - 1) * 0.25 for j in range(len(table_ops))],
            [
                timing("workload", store, op, size, "table_ops")["median_seconds"]
                * 1000
                for op in table_ops
            ],
            width=0.25,
            label=NAMES[store],
            color=COLORS[store],
        )
    ax.set_xticks(range(len(table_ops)), titles, rotation=12)
    ax.set(
        yscale="log",
        ylabel="Время, мс",
        title="Операции с тестовыми наборами на 100 000 ранов",
    )
    ax.legend(frameon=False)
    ax.grid(axis="y", alpha=0.18)
    save(
        fig,
        "06_table_operations",
        "Медиана трёх повторов. PG/Mongo: серверные операции; S3: copy/delete объектов и copy-on-write JSONL с манифестом. Это разные механизмы.",
    )

    latency_rows = []
    for path in workload_root.glob("concurrent_*.json"):
        value = json.loads(path.read_text(encoding="utf-8"))
        parts = path.stem.split("_")
        for i, seconds in enumerate(value["latency_seconds"]):
            latency_rows.append(
                {
                    "size": int(parts[1]),
                    "repeat": int(parts[2]) + 1,
                    "store": parts[3],
                    "call": i + 1,
                    "seconds": seconds,
                }
            )
    write_csv(data / "concurrent_latencies.csv", latency_rows)
    latency_summary = []
    for count in sizes:
        for store in STORES:
            values = sorted(
                float(r["seconds"]) * 1000
                for r in latency_rows
                if r["size"] == count and r["store"] == store
            )
            latency_summary.append(
                {
                    "size": count,
                    "store": store,
                    "n": len(values),
                    "p50_ms": statistics.median(values),
                    "p95_nearest_rank_ms": values[math.ceil(0.95 * len(values)) - 1],
                    "min_ms": values[0],
                    "max_ms": values[-1],
                }
            )
    write_csv(data / "concurrent_summary.csv", latency_summary)
    fig, ax = plt.subplots(figsize=(9, 5.6))
    for store in STORES:
        values = sorted(
            float(r["seconds"]) * 1000
            for r in latency_rows
            if r["size"] == size and r["store"] == store
        )
        ax.step(
            values,
            [(i + 1) / len(values) for i in range(len(values))],
            where="post",
            label=NAMES[store],
            color=COLORS[store],
            linewidth=2,
        )
    ax.set(
        xscale="log",
        xlabel="Задержка запроса, мс",
        ylabel="Доля завершённых запросов",
        title="Четыре читателя: задержки на 100 000 ранов",
    )
    ax.legend(frameon=False)
    ax.grid(alpha=0.18)
    save(
        fig,
        "07_concurrent_reads",
        "По 20 вызовов в каждом из трёх повторов, прогрев исключён. Небольшой опыт, не тест насыщения и не гарантия production p95.",
    )

    fig, ax = plt.subplots(figsize=(9, 5.2))
    bulk_keys = (
        ("executemany", "initial_load"),
        ("copy", "initial_load"),
        ("copy", "idempotent_copy_merge"),
    )
    items = [
        timing("pg_bulk", "postgres", op, phase=method) for method, op in bulk_keys
    ]
    med = [r["median_seconds"] for r in items]
    ax.bar(
        range(3),
        med,
        color=["#4477AA", "#228833", "#77AADD"],
        width=0.6,
        yerr=[
            [y - r["min_seconds"] for y, r in zip(med, items)],
            [r["max_seconds"] - y for y, r in zip(med, items)],
        ],
        capsize=4,
    )
    ax.set_xticks(range(3), ["Пакетные INSERT", "COPY", "COPY + merge\n(повтор)"])
    ax.set(ylabel="Время до commit, с", title="PostgreSQL: загрузка 100 000 ранов")
    ax.grid(axis="y", alpha=0.18)
    save(
        fig,
        "08_postgres_bulk",
        "Одинаковый вход, PK заранее, три повтора. Полная сверка содержимого выполняется после таймера. Merge — для уже загруженного набора.",
    )

    recommendations = []
    for op in queries:
        ordered = sorted(
            (timing("workload", s, op, size, "indexed") for s in STORES),
            key=lambda r: r["median_seconds"],
        )
        recommendations.append(
            {
                "task": LABELS[op],
                "operation": op,
                "size": size,
                "fastest_measured": NAMES[ordered[0]["store"]],
                **{
                    s + "_median_ms": timing("workload", s, op, size, "indexed")[
                        "median_seconds"
                    ]
                    * 1000
                    for s in STORES
                },
                "scope": "Только измеренный вход и реализации; функциональные ограничения учитывать отдельно",
            }
        )
    write_csv(data / "task_comparison.csv", recommendations)

    def measured(item, scale=1000):
        return f"{item['median_seconds'] * scale:.3f} [{item['min_seconds'] * scale:.3f}–{item['max_seconds'] * scale:.3f}]"

    tables = [
        "# Таблицы измеренного сравнения\n",
        "Значения: **медиана [min–max]**; время чтения — мс, загрузки — с. "
        "У загрузок/операций три повтора, у обычного чтения девять проходов.\n",
        "## Загрузка полного ODS Portal\n",
        "| Операция | PostgreSQL, с | MongoDB, с | Garage S3, с |",
        "|---|---:|---:|---:|",
    ]
    for op, title in (
        ("load_verified", "Первая загрузка + проверка"),
        ("replay_verified", "Повтор + проверка"),
    ):
        tables.append(
            "| "
            + title
            + " | "
            + " | ".join(measured(timing("ods", s, op), 1) for s in STORES)
            + " |"
        )
    tables += [
        "\n## Объём ODS\n",
        "| Сервис | Доступная метрика, МиБ | Индексы, МиБ |",
        "|---|---:|---:|",
    ]
    for item in size_summary:
        index = (
            f"{item['index_MiB']:.2f}"
            if item["index_MiB"] != ""
            else "не выделено отдельно"
        )
        tables.append(
            f"| {NAMES[item['store']]} | {item['reported_MiB']:.2f} | {index} |"
        )
    tables += [
        "\nPG: таблицы/TOAST + индексы; Mongo: storageSize + индексы; S3: тела объектов без overhead volume. Метрики не тождественны.\n",
        "\n## Чтение ODS\n",
        "| Операция | PostgreSQL, мс | MongoDB, мс | Garage S3, мс |",
        "|---|---:|---:|---:|",
    ]
    for op in operations:
        tables.append(
            "| "
            + LABELS[op]
            + " | "
            + " | ".join(measured(timing("ods", s, op)) for s in STORES)
            + " |"
        )
    tables += [
        f"\n## Запросы на {size:,} ранов\n",
        "| Операция | PostgreSQL, мс | MongoDB, мс | Garage S3, мс |",
        "|---|---:|---:|---:|",
    ]
    for op in queries:
        tables.append(
            "| "
            + LABELS[op]
            + " | "
            + " | ".join(
                measured(timing("workload", s, op, size, "indexed")) for s in STORES
            )
            + " |"
        )
    tables += [
        "\n## Влияние индексов\n",
        "| Сервис | Запрос | Только PK, мс | С индексами, мс | Отношение PK / indexed |",
        "|---|---|---:|---:|---:|",
    ]
    for store in STORES[:2]:
        for op in index_ops:
            before = timing("workload", store, op, size, "pk_only")
            after = timing("workload", store, op, size, "indexed")
            tables.append(
                f"| {NAMES[store]} | {LABELS[op]} | {before['median_seconds'] * 1000:.3f} | {after['median_seconds'] * 1000:.3f} | {before['median_seconds'] / after['median_seconds']:.2f} |"
            )
    tables += [
        "\n## Операции с тестовыми копиями\n",
        "| Операция | PostgreSQL, мс | MongoDB, мс | Garage S3, мс |",
        "|---|---:|---:|---:|",
    ]
    for op, title in zip(table_ops, titles):
        tables.append(
            "| "
            + title
            + " | "
            + " | ".join(
                measured(timing("workload", s, op, size, "table_ops")) for s in STORES
            )
            + " |"
        )
    tables += [
        "\n## Четыре читателя\n",
        "| Сервис | Вызовов | p50, мс | Наблюдаемый p95, мс | Min–max, мс |",
        "|---|---:|---:|---:|---:|",
    ]
    for item in latency_summary:
        if item["size"] == size:
            tables.append(
                f"| {NAMES[item['store']]} | {item['n']} | {item['p50_ms']:.3f} | {item['p95_nearest_rank_ms']:.3f} | {item['min_ms']:.3f}–{item['max_ms']:.3f} |"
            )
    tables += [
        "\n## PostgreSQL: способы массовой загрузки\n",
        "| Метод | Операция | Секунды, медиана [min–max] |",
        "|---|---|---:|",
    ]
    for method in ("executemany", "copy"):
        for operation in ("initial_load", "idempotent_copy_merge"):
            item = timing("pg_bulk", "postgres", operation, phase=method)
            tables.append(f"| {method} | {operation} | {measured(item, 1)} |")
    tables.append(
        "\nНаблюдаемый p95 вычислен nearest-rank по 60 вызовам; это небольшой опыт, не production SLA. "
        "Кеш не сбрасывался. SQL/Mongo фильтруют сервером; S3 — клиентом. "
        "Различия механизмов UPDATE/copy/rename и учёта объёма раскрыты в README.md.\n"
    )
    (root / "TABLES.md").write_text("\n".join(tables), encoding="utf-8")
    print(
        "Saved summaries and",
        len(list(figures.glob("*.png"))),
        "PNG/SVG figures in",
        root,
    )


if __name__ == "__main__":
    main()
