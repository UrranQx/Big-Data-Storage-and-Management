"""Offline follow-up: bounded top-10/25 density bonus, no database access.

Uses the immutable CSV from ranking_quality_20261127. Standard library only.
"""

import argparse
import csv
import hashlib
import importlib.util
import json
import math
import statistics
import time
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
PREVIOUS = HERE.parent / "ranking_quality_20261127"
spec = importlib.util.spec_from_file_location("previous_experiment", PREVIOUS / "experiment.py")
previous = importlib.util.module_from_spec(spec)
spec.loader.exec_module(previous)


def window(rows, k, start=0):
    """Positional top-k; a tie at the boundary is truncated only for statistics.

    All tied players receive identical scores via the original midpoint rank.
    Distinct values use exact source score strings, not rounded log gaps.
    """
    if len(rows) < k:
        return None
    selected = rows[start:k]
    gaps = [r["gap"] for r in selected]
    spread = gaps[-1] - gaps[0]
    distinct = len({r["score"] for r in selected})
    return dict(spread=spread, ratio_pct=100*math.expm1(spread),
                unique_fraction=(distinct-1)/(len(selected)-1),
                distinct=distinct, participants=len(selected),
                median_step=statistics.median(b-a for a,b in zip(gaps,gaps[1:])))


def density(metrics, tau):
    if metrics is None:
        return 0.0  # absent window means no bonus, not a measured sparse field
    return metrics["unique_fraction"] * tau / (tau + metrics["spread"])


def locality(midrank):
    return math.exp(-max(0,midrank-25)/25)


def variants():
    return [
        ("previous",0,.01,"both",False),
        ("top10",.25,.01,"10",False),
        ("top25",.25,.01,"25",False),
        ("top10_25",.25,.01,"both",False),
        ("inner_pack",.25,.01,"inner",False),
        ("bonus10",.10,.01,"both",False),
        ("bonus50",.50,.01,"both",False),
        ("scale005",.25,.005,"both",False),
        ("scale02",.25,.02,"both",False),
        ("lower_better_only",.25,.01,"both",True),
    ]


def combined(metrics, mode, tau):
    if mode in ("10","25"):
        return density(metrics[mode],tau)
    keys = ("inner10","inner25") if mode=="inner" else ("10","25")
    return sum(density(metrics[key],tau) for key in keys)/2


def fixtures():
    def field(values):
        return [dict(gap=math.log(t/values[0]),score=str(t)) for t in values]
    tight = field([100+i*.01 for i in range(25)])
    wide = field([100+i for i in range(25)])
    tied = field([100]*25)
    isolated = field([80]+[100+i*.001 for i in range(24)])
    tau = math.log1p(.01)
    assert density(window(tight,25),tau)>density(window(wide,25),tau)
    assert density(window(tied,25),tau)==0
    assert window(tight[:10],25) is None
    assert density(window(tight[:10],25),tau)==0
    assert density(window(isolated,25,1),tau)>density(window(isolated,25),tau)
    scaled = field([(100+i*.01)*1000 for i in range(25)])
    assert math.isclose(window(tight,25)["spread"],window(scaled,25)["spread"],abs_tol=1e-12)
    # A higher-better leaderboard with reciprocal scores has the same gaps.
    reciprocal = [dict(gap=abs(math.log((1/(100+i*.01))/(1/100))),score=str(1/(100+i*.01))) for i in range(25)]
    assert math.isclose(window(tight,25)["spread"],window(reciprocal,25)["spread"],abs_tol=1e-12)
    assert locality(25)==1 and locality(100)<.05 and locality(1000)<1e-15
    sample = {}
    for name,rows in [("tight",tight),("wide",wide),("all_ties",tied),("isolated_WR",isolated)]:
        d= (density(window(rows,10),tau)+density(window(rows,25),tau))/2
        sample[name] = dict(top10=window(rows,10),top25=window(rows,25),
                           inner25=window(rows,25,1),density=d,
                           bonus_top25_pct=25*d,bonus_rank100_pct=25*d*locality(100))
        assert 0<=d<=1
    return sample


def main(input_dir,output):
    started=time.monotonic()
    if output.exists():
        raise SystemExit("Use a new output directory to preserve previous evidence")
    metadata=json.loads((input_dir/"input_metadata.json").read_text(encoding="utf-8"))
    assert hashlib.sha256((input_dir/"input.csv").read_bytes()).hexdigest()==metadata["input_sha256"]
    scopes=defaultdict(list)
    people={}
    with (input_dir/"input.csv").open(encoding="utf-8",newline="") as stream:
        for raw in csv.DictReader(stream):
            n,place,ties=[int(raw[c]) for c in ("n","place","ties")]
            scopes[raw["scope_id"]].append(dict(player=raw["holder_id"],game=raw["game_id"],
                game_name=raw["game_name"],scope=raw["scope_name"],direction=int(raw["direction"]),
                q=previous.quality(n,place,ties),midrank=place+(ties-1)/2,
                place=place,n=n,ties=ties,gap=float(raw["log_gap"]),score=raw["score"]))
            people[raw["holder_id"]]=raw["player_name"]
    metrics={}
    for sid,rows in scopes.items():
        assert len(rows)==rows[0]["n"]  # do not score density on incomplete field
        assert all(a["gap"]<=b["gap"] for a,b in zip(rows,rows[1:]))
        metrics[sid]={"10":window(rows,10),"25":window(rows,25),
                      "inner10":window(rows,10,1),"inner25":window(rows,25,1)}

    totals,ranks,orders,portfolios={},{},{},{}
    field_summary=[]
    for sid,ms in metrics.items():
        first=scopes[sid][0]
        field_summary.append(dict(scope_id=sid,game=first["game_name"],scope=first["scope"],
            n=first["n"],direction=first["direction"],metrics=ms,
            density=combined(ms,"both",math.log1p(.01)),
            first_second_gap_pct=100*math.expm1(scopes[sid][1]["gap"])))
    checks={"complete_fields":True,"sorted_input":True,"within_bonus_bounds":True,"within_field_monotone":True,"equal_scores_for_ties":True}
    for name,alpha,scale,mode,lower_only in variants():
        best={}
        for sid,rows in scopes.items():
            d=combined(metrics[sid],mode,math.log1p(scale))
            if lower_only and rows[0]["direction"]!=1:
                d=0
            last_points=None
            last_score=None
            for row in rows:
                multiplier=1+alpha*d*locality(row["midrank"])
                points=100*row["q"]**2*multiplier
                assert 1<=multiplier<=1+alpha+1e-12
                assert last_points is None or points<=last_points+1e-10
                assert last_score!=row["score"] or abs(points-last_points)<1e-10
                last_score,last_points=row["score"],points
                key=row["player"],row["game"]
                if key not in best or points>best[key][0] or (points==best[key][0] and sid<best[key][1]):
                    best[key]=(points,sid,row,multiplier,d)
        per_player=defaultdict(list)
        for (person,game),entry in best.items():
            per_player[person].append((game,*entry))
        portfolios[name]={p:sorted(vals,key=lambda v:(-v[1],v[0])) for p,vals in per_player.items()}
        totals[name]={p:sum(.9**i*v[1] for i,v in enumerate(vals)) for p,vals in portfolios[name].items()}
        orders[name],ranks[name]=previous.rank_map(totals[name])
    baseline=json.loads((input_dir/"results.json").read_text(encoding="utf-8"))
    expected=next(v for v in baseline["variants"] if v["name"]=="log_square_decay")
    for p in expected["top20"]:
        assert ranks["previous"][p["player_id"]]==p["rank"]
        assert abs(totals["previous"][p["player_id"]]-p["points"])<1e-6
    assert len(people)==baseline["people"]
    checks["previous_top20_reproduced"]=True
    checks["same_population"]=True
    candidate="top10_25"

    def profile(person,name):
        contributions=[]
        for i,(game,points,sid,row,mult,d) in enumerate(portfolios[name][person]):
            contributions.append(dict(game=row["game_name"],scope_id=sid,scope=row["scope"],
                place=row["place"],ties=row["ties"],n=row["n"],base_game_points=100*row["q"]**2,
                density=d,multiplier=mult,weight=.9**i,contribution=points*.9**i))
        return dict(id=person,name=people[person],rank=ranks[name][person],
            previous_rank=ranks["previous"][person],points=totals[name][person],
            previous_points=totals["previous"][person],games=len(contributions),contributions=contributions)

    comparisons=[]
    for name,alpha,scale,mode,lower_only in variants():
        comparisons.append(dict(name=name,alpha=alpha,scale=scale,mode=mode,lower_only=lower_only,
            overlap_previous100=len(set(orders[name][:100])&set(orders["previous"][:100])),
            overlap_candidate100=len(set(orders[name][:100])&set(orders[candidate][:100])),
            top20=[{k:v for k,v in profile(p,name).items() if k!="contributions"} for p in orders[name][:20]]))
    pool=set(orders["previous"][:1000])|set(orders[candidate][:1000])
    movers=sorted(pool,key=lambda p:(ranks[candidate][p]-ranks["previous"][p],p))
    selected=set(orders["previous"][:10]+orders[candidate][:20]+movers[:5]+movers[-5:])
    profiles=[profile(p,candidate) for p in sorted(selected,key=lambda p:ranks[candidate][p])]
    output.mkdir(parents=True)
    with (output/"all_rankings.csv").open("w",encoding="utf-8",newline="") as stream:
        writer=csv.writer(stream)
        writer.writerow(["id","name"]+[x for name,*_ in variants() for x in (name+"_rank",name+"_points")])
        for p in orders[candidate]:
            writer.writerow([p,people[p]]+[x for name,*_ in variants() for x in (ranks[name][p],totals[name][p])])
    with (output/"fields.json").open("w",encoding="utf-8") as stream:
        json.dump(field_summary,stream,ensure_ascii=False,indent=2)
    results=dict(metadata=metadata,people=len(people),fields=len(scopes),
        full25_fields=sum(m["25"] is not None for m in metrics.values()),
        all_tied_top10=sum(m["10"]["distinct"]==1 for m in metrics.values()),
        checks=checks,fixtures=fixtures(),variants=comparisons,profiles=profiles,
        seconds=round(time.monotonic()-started,3),
        limitation="Static density is not recent activity or validated intrinsic difficulty")
    (output/"results.json").write_text(json.dumps(results,ensure_ascii=False,indent=2),encoding="utf-8")
    print(json.dumps({k:v for k,v in results.items() if k in ["people","fields","full25_fields","all_tied_top10","seconds","checks"]}))
    print(json.dumps(comparisons[3],ensure_ascii=False))


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input",type=Path,default=Path("exports/ranking-quality-20261127"))
    parser.add_argument("--output",type=Path,default=Path("exports/ranking-density-20261127"))
    args=parser.parse_args()
    fixtures()
    main(args.input,args.output)
