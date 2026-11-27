"""Save compact density evidence and a standalone figure (Matplotlib)."""

import csv
import json
import math
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE=Path(__file__).resolve().parent
SOURCE=Path("exports/ranking-density-20261127")
INPUT=Path("exports/ranking-quality-20261127/input.csv")
IDS=["3bdbfeeed92f46ede11247498cbc90b0","5aadc7e1e30d81561da01db633eca651","b07129e954966eb86cfac7a4af78787e"]


def main():
    output=HERE/"evidence"
    output.mkdir(exist_ok=True)
    results=json.loads((SOURCE/"results.json").read_text(encoding="utf-8"))
    fields=json.loads((SOURCE/"fields.json").read_text(encoding="utf-8"))
    selected={f["scope_id"]:f for f in fields if f["scope_id"] in IDS}
    for f in selected.values():
        f["top25_gap_pct"]=[]
    with INPUT.open(encoding="utf-8",newline="") as stream:
        for row in csv.DictReader(stream):
            if row["scope_id"] in selected:
                points=selected[row["scope_id"]]["top25_gap_pct"]
                if len(points)<25:
                    points.append(100*math.expm1(float(row["log_gap"])))
    results["examples"]=[selected[sid] for sid in IDS]
    results["tied10_with_nonzero25"]=sum(f["metrics"]["10"]["distinct"]==1 and f["density"]>0 for f in fields)
    (output/"results.json").write_text(json.dumps(results,ensure_ascii=False,indent=2),encoding="utf-8")
    with (output/"top20_comparison.csv").open("w",encoding="utf-8",newline="") as stream:
        writer=csv.writer(stream)
        writer.writerow(["variant","name","rank","previous_rank","points"])
        for v in results["variants"]:
            for p in v["top20"]:
                writer.writerow([v["name"],p["name"],p["rank"],p["previous_rank"],p["points"]])
    fig,axes=plt.subplots(1,2,figsize=(13,4.4),constrained_layout=True)
    colors=["#2563eb","#15803d","#c2410c"]
    labels=["SMB Any% NTSC","Refunct Any% Normal","Celeste Any%"]
    for i,(sid,color,label) in enumerate(zip(IDS,colors,labels)):
        f=selected[sid]
        gaps=f["top25_gap_pct"]
        axes[0].plot([gaps[0],gaps[-1]],[i,i],color=color,alpha=.35)
        axes[0].scatter(gaps,[i]*25,color=color,s=22,alpha=.65)
        ranks=range(1,101)
        axes[1].plot(ranks,[25*f["density"]*math.exp(-max(0,r-25)/25) for r in ranks],color=color,label=label)
    axes[0].set(yticks=range(3),yticklabels=labels,xlabel="Отставание от WR, %",title="Первые 25 результатов: совпадения накладываются")
    axes[0].set_ylim(-.5,2.5)
    axes[1].set(xlabel="Место (без ничьих)",ylabel="Добавка к игровым баллам, %",title="Бонус сосредоточен около вершины",ylim=(0,26))
    axes[1].axvline(25,linestyle="--",color="#64748b",alpha=.4)
    axes[1].legend(fontsize=9)
    for ax in axes:
        ax.grid(alpha=.2)
    fig.suptitle("Эксперимент: плотность топ-10/25, срез 30.10.2026; максимум бонуса 25%")
    fig.savefig(output/"density.png",dpi=170)
    fig.savefig(output/"density.svg")
    plt.close(fig)
    print("Density research artifacts saved")


if __name__=="__main__":
    main()
