"""Plot the saved observational request-speed study; no Docker or network."""
import csv
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def main():
    root = Path(__file__).resolve().parent
    report = json.loads((root / "source_speed_correlation_20261028.json").read_text(encoding="utf-8"))
    windows = report["clean_windows"]
    with (root / "source_speed_windows_20261028.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(windows[0]))
        writer.writeheader()
        writer.writerows(windows)
    fig, ax = plt.subplots(figsize=(10.5, 6.4), layout="constrained")
    points = ax.scatter([w["median_decoded_bytes"] / 1024 for w in windows],
        [w["observed_rpm"] for w in windows],
        c=[w["median_headers_ms"] / 1000 for w in windows], cmap="plasma", s=49, alpha=.8,
        edgecolors="white", linewidths=.4)
    ax.set_xscale("log")
    ax.set_xlabel("Медианный размер распакованного ответа, КиБ")
    ax.set_ylabel("Фактические HTTP-запросы в минуту")
    ax.set_title("Размер ответа и скорость получения страниц Speedrun.com")
    ax.set_ylim(0, 66)
    ax.axhline(60, linestyle="--", color="#718096", linewidth=1, label="Общий настроенный потолок: 60 RPM")
    for value in (256, 1024):
        ax.axvline(value, linestyle=":", color="#a0aec0", linewidth=.8)
    ax.grid(alpha=.16)
    ax.legend(loc="upper right", frameon=False, fontsize=9)
    fig.colorbar(points, ax=ax, label="Медианное ожидание заголовков ответа, с")
    fig.supxlabel(f"{len(windows)} окна по 10–20 запросов; одинаковый pacing, без ошибок. Наблюдение, не причинный эксперимент.", fontsize=9)
    fig.savefig(root / "source_speed_correlation_20261028.png", dpi=150)
    plt.close(fig)
    print(f"Saved {len(windows)} windows as CSV and PNG")


if __name__ == "__main__":
    main()
