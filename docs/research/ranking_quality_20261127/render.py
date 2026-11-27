"""Render shareable research artifacts; needs matplotlib only for the figure."""

import argparse
import csv
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from experiment import aggregate, quality


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=Path("exports/ranking-quality-20261127"))
    args = parser.parse_args()
    data = json.loads((args.input / "results.json").read_text(encoding="utf-8"))
    output = Path(__file__).resolve().parent / "evidence"
    output.mkdir(exist_ok=True)
    (output / "results.json").write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    with (output / "comparison.csv").open("w",encoding="utf-8",newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(["variant","rank","name","points","games","released_rank"])
        for variant in data["variants"]:
            for player in variant["top20"]:
                writer.writerow([variant["name"],player["rank"],player["name"],player["points"],player["games"],player["released_rank"]])

    fig, axes = plt.subplots(1, 2, figsize=(14, 5), constrained_layout=True)
    places = list(range(1, 10001))
    axes[0].plot(places, [100*quality(10000,r,kind="baseline") for r in places],label="Текущая оценка результата",color="#64748b")
    axes[0].plot(places, [100*quality(10000,r)**2 for r in places],label="Кандидат: логарифм места, квадрат",color="#2563eb")
    axes[0].set(xscale="log",xlabel="Место среди 10 000 участников (без ничьих)",ylabel="Баллы за первую игру",title="Высокие места получают больший вес",ylim=(0,105))
    axes[0].legend(fontsize=9)
    games = list(range(1,41))
    for q,color in [(.95,"#15803d"),(.75,"#2563eb"),(.55,"#c2410c"),(.2,"#64748b")]:
        axes[1].plot(games,[aggregate([q]*n,2,.9) for n in games],label=f"Все результаты q = {q}",color=color)
    axes[1].axhline(aggregate([.95]*3,2,.9),linestyle="--",color="#15803d",alpha=.7,label="Три результата q = 0,95")
    axes[1].set(xlabel="Число разных игр",ylabel="Сумма баллов",title="Все игры учитываются; вклад убывает",ylim=(0,950))
    axes[1].legend(fontsize=9)
    for ax in axes:
        ax.grid(alpha=.2)
    fig.suptitle("Эксперимент, не релиз: γ = 2, d = 0,9, k = 20",fontsize=14)
    fig.savefig(output / "formula_tradeoffs.png",dpi=170)
    fig.savefig(output / "formula_tradeoffs.svg")
    plt.close(fig)
    print(f"Research artifacts: {output}")


if __name__ == "__main__":
    main()
