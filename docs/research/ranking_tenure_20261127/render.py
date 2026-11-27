"""Compact tenure evidence and a synthetic illustration of retained awards."""

import csv
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from replay import bonus,replay

HERE=Path(__file__).resolve().parent
SOURCE=Path("exports/ranking-tenure-20261127/analysis_lifetime")


def main():
    output=HERE/"evidence"
    output.mkdir(exist_ok=True)
    result=json.loads((SOURCE/"results.json").read_text(encoding="utf-8"))
    day=86400
    seed=[(0,chr(65+i),100+10*i) for i in range(10)]
    seed += [(0,f"extra_{i:04}",1000+i) for i in range(990)]
    events=seed+[(5*day,"A",95),(10*day,"D",90),(20*day,"E",80),
                 (30*day,"F",70),(40*day,"A",60),(50*day,"A",55)]
    samples=[]
    for d in range(61):
        s,_=replay([e for e in events if e[0]<=d*day],d*day)
        a=s["A"]
        samples.append(dict(day=d,rank=a["current_rank"] or 4,
                            quality_days=a["quality_total"],bonus=bonus(a["quality_total"])))
    assert samples[30]["bonus"]==samples[39]["bonus"]
    assert samples[60]["bonus"]>samples[40]["bonus"]>0
    result["illustration"]={"synthetic":True,"constant_field_size":1000,"samples":samples,
                            "self_improvement_days":[5,50]}
    (output/"results.json").write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding="utf-8")
    with (output/"top20_comparison.csv").open("w",encoding="utf-8",newline="") as stream:
        writer=csv.writer(stream)
        writer.writerow(["variant","name","rank","previous_rank","points"])
        for v in result["variants"]:
            for p in v["top20"]:
                writer.writerow([v["name"],p["name"],p["rank"],p["previous_rank"],p["points"]])
    fig,axes=plt.subplots(2,1,figsize=(10,6),sharex=True,constrained_layout=True)
    x=[s["day"] for s in samples]
    axes[0].step(x,[s["rank"] for s in samples],where="post",color="#2563eb",linewidth=2)
    axes[0].set(ylim=(4.5,.5),yticks=[1,2,3,4],yticklabels=["1-е","2-е","3-е","Вне топ-3"],ylabel="Место")
    axes[1].plot(x,[s["bonus"] for s in samples],color="#15803d",linewidth=2)
    axes[1].set(xlabel="Дни с начала условного примера",ylabel="Накопленные бонусные баллы")
    for ax in axes:
        ax.axvspan(30,40,color="#94a3b8",alpha=.15)
        for d in (5,50):ax.axvline(d,color="#c2410c",linestyle=":",alpha=.8)
        ax.grid(alpha=.2)
    axes[0].set_title("Собственные улучшения на 5-й и 50-й день сохраняют удержание")
    axes[1].set_title("Вне тройки начисление останавливается, заработанные баллы остаются")
    fig.suptitle("Искусственный пример: 1000 участников, бонус за все прошлые периоды")
    fig.savefig(output/"retained_bonus.png",dpi=170)
    fig.savefig(output/"retained_bonus.svg")
    plt.close(fig)
    print("Tenure evidence and illustration saved")


if __name__=="__main__":main()
