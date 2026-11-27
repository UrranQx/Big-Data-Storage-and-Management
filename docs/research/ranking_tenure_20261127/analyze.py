"""Offline lifetime podium bonus experiment; never changes published rankings."""

import argparse
import csv
import hashlib
import importlib.util
import json
import math
import time
from collections import Counter, defaultdict
from datetime import datetime
from itertools import groupby
from pathlib import Path

from replay import bonus, checks, replay

HERE=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location("density_experiment",HERE.parent/"ranking_density_20261127/experiment.py")
density=importlib.util.module_from_spec(spec)
spec.loader.exec_module(density)


def configurations():
    return [
        ("density_baseline","verified_at","total",0,365),
        ("lifetime","verified_at","total",25,365),
        ("podium_streak","verified_at","streak",25,365),
        ("exact_position_streak","verified_at","exact",25,365),
        ("half_three_years","verified_at","total",25,1095),
        ("maximum10","verified_at","total",10,365),
        ("maximum50","verified_at","total",50,365),
        ("submission_basis","submitted_at","total",25,365),
    ]


def main(history_dir,output):
    started=time.monotonic()
    if output.exists():raise SystemExit("Choose a fresh output directory")
    metadata=json.loads((history_dir/"metadata.json").read_text(encoding="utf-8"))
    assert hashlib.sha256((history_dir/"history.csv").read_bytes()).hexdigest()==metadata["input_sha256"]
    quality_dir=Path("exports/ranking-quality-20261127")
    qmeta=json.loads((quality_dir/"input_metadata.json").read_text(encoding="utf-8"))
    assert metadata["context"]==qmeta["context"]
    assert hashlib.sha256((quality_dir/"input.csv").read_bytes()).hexdigest()==qmeta["input_sha256"]
    end=datetime.fromisoformat(metadata["context"][0]["as_of"]).timestamp()
    fields=defaultdict(list)
    names={}
    with (quality_dir/"input.csv").open(encoding="utf-8",newline="") as stream:
        for r in csv.DictReader(stream):
            n,rank,ties=(int(r[k]) for k in ("n","place","ties"))
            row=dict(player=r["holder_id"],game=r["game_id"],scope_id=r["scope_id"],
                scope=r["scope_name"],game_name=r["game_name"],n=n,rank=rank,ties=ties,
                q=density.previous.quality(n,rank,ties),score=int(r["score"]),gap=float(r["log_gap"]))
            fields[row["scope_id"]].append(row)
            names[row["player"]]=r["player_name"]
    field_density={}
    for sid,rows in fields.items():
        metrics={"10":density.window(rows,10),"25":density.window(rows,25)}
        field_density[sid]=density.combined(metrics,"both",math.log1p(.01))

    histories={}
    audit=[]
    missing=Counter()
    history_rows=0
    with (history_dir/"history.csv").open(encoding="utf-8",newline="") as stream:
        for sid,group in groupby(csv.DictReader(stream),key=lambda r:r["scope_id"]):
            rows=list(group)
            history_rows+=len(rows)
            if sid not in fields:raise AssertionError("Unexpected historical field")
            current={r["player"]:r["score"] for r in fields[sid]}
            for basis in ("verified_at","submitted_at"):
                events=[]
                unknown=future=0
                for r in rows:
                    if not r[basis]:
                        unknown+=1
                        continue
                    at=datetime.fromisoformat(r[basis]).timestamp()
                    if at>end:
                        future+=1
                        continue
                    events.append((at,r["holder_id"],int(r["score"])))
                stats,final=replay(events,end)
                mismatch=sum(final.get(p)!=score for p,score in current.items())
                extra=len(final.keys()-current.keys())
                reasons=[]
                if unknown:reasons.append("missing_date")
                if future:reasons.append("future_date")
                if mismatch or extra:reasons.append("end_state_mismatch")
                valid=not reasons
                audit.append(dict(scope_id=sid,basis=basis,valid=valid,reasons=reasons,
                    rows=len(rows),missing_dates=unknown,future_dates=future,
                    mismatched_current_players=mismatch,extra_historical_players=extra,
                    history_podium_people=len(stats)))
                missing[basis]+=unknown
                if valid:
                    for p,s in stats.items():histories[basis,sid,p]=s
    assert {a["scope_id"] for a in audit}==fields.keys()
    audited={(a["basis"],a["scope_id"]):a for a in audit}
    totals,ranks,orders,portfolios={},{},{},{}
    for name,basis,kind,maximum,half in configurations():
        best={}
        for sid,rows in fields.items():
            d=field_density[sid]
            valid=audited[basis,sid]["valid"]
            for row in rows:
                s=histories.get((basis,sid,row["player"]),{})
                days=s.get("quality_"+kind,0)
                extra=bonus(days,maximum,half) if valid else 0
                base=100*row["q"]**2*(1+.25*d*density.locality(row["rank"]+(row["ties"]-1)/2))
                points=base+extra
                assert base<=points<=base+maximum+1e-9
                key=row["player"],row["game"]
                value=(points,sid,row,extra,valid,s)
                old=best.get(key)
                if old is None or points>old[0] or (points==old[0] and sid<old[1]):best[key]=value
        by_player=defaultdict(list)
        for (p,g),v in best.items():by_player[p].append((g,*v))
        portfolios[name]={p:sorted(vals,key=lambda v:(-v[1],v[0])) for p,vals in by_player.items()}
        totals[name]={p:sum(.9**i*v[1] for i,v in enumerate(vals)) for p,vals in portfolios[name].items()}
        orders[name],ranks[name]=density.previous.rank_map(totals[name])
    previous=json.loads(Path("exports/ranking-density-20261127/results.json").read_text(encoding="utf-8"))
    expected=next(v for v in previous["variants"] if v["name"]=="top10_25")
    for p in expected["top20"]:
        assert ranks["density_baseline"][p["id"]]==p["rank"]
        assert abs(totals["density_baseline"][p["id"]]-p["points"])<1e-6
    assert len(names)==previous["people"]

    def profile(p,name):
        contributions=[]
        for i,(game,points,sid,row,b,valid,stats) in enumerate(portfolios[name][p]):
            contributions.append(dict(scope_id=sid,scope=row["scope"],game=row["game_name"],
                place=row["rank"],field_size=row["n"],history_valid=valid,
                tenure_bonus_points=b,portfolio_weight=.9**i,weighted_points=points*.9**i,stats=stats))
        return dict(id=p,name=names[p],rank=ranks[name][p],previous_rank=ranks["density_baseline"][p],
            points=totals[name][p],previous_points=totals["density_baseline"][p],
            games=len(contributions),contributions=contributions)

    comparisons=[]
    for name,basis,kind,maximum,half in configurations():
        comparisons.append(dict(name=name,basis=basis,kind=kind,maximum=maximum,half_days=half,
            overlap_previous100=len(set(orders[name][:100])&set(orders["density_baseline"][:100])),
            overlap_lifetime100=len(set(orders[name][:100])&set(orders["lifetime"][:100])),
            top20=[{k:v for k,v in profile(p,name).items() if k!="contributions"} for p in orders[name][:20]]))
    pool=set(orders["density_baseline"][:1000])|set(orders["lifetime"][:1000])
    movers=sorted(pool,key=lambda p:(ranks["lifetime"][p]-ranks["density_baseline"][p],p))
    selected=set(orders["density_baseline"][:10]+orders["lifetime"][:20]+movers[:5]+movers[-5:])
    coverage={}
    for basis in ("verified_at","submitted_at"):
        audits=[a for a in audit if a["basis"]==basis]
        good={a["scope_id"] for a in audits if a["valid"]}
        coverage[basis]=dict(valid_fields=len(good),unknown_fields=len(fields)-len(good),
            reasons=dict(Counter(reason for a in audits for reason in a["reasons"])),
            missing_date_runs=missing[basis],
            current_pb_with_valid_history=sum(len(fields[sid]) for sid in good),
            bonus_people=len({p for (b,sid,p),s in histories.items() if b==basis and s["total"]>0 and p in names}),
            historical_podium_holders=sum(s["total"]>0 for (b,_,_),s in histories.items() if b==basis))
    results=dict(metadata=metadata,current_pb_sha256=qmeta["input_sha256"],
        history_rows=history_rows,people=len(names),fields=len(fields),coverage=coverage,
        policy=dict(lifetime_preserved=True,historical_minimum_field=10,medal_weights=[1,.6,.3],
            half_days=365,maximum_bonus_points=25,historical_quality_weighted=True,
            unknown_history="no additional bonus; keep base score",
            reconstruction="retrospective currently verified archive; not historical official snapshots"),
        variants=comparisons,profiles=[profile(p,"lifetime") for p in sorted(selected,key=lambda p:ranks["lifetime"][p])],
        checks=dict(previous_top20_matches=True,population_matches=True,bonus_bounds=True,
                    self_improvements_keep_reign=True,simultaneous_batches=True),
        seconds=round(time.monotonic()-started,3))
    output.mkdir(parents=True)
    (output/"results.json").write_text(json.dumps(results,ensure_ascii=False,indent=2),encoding="utf-8")
    (output/"field_audit.json").write_text(json.dumps(audit,ensure_ascii=False,indent=2),encoding="utf-8")
    with (output/"tenure.csv").open("w",encoding="utf-8",newline="") as stream:
        writer=csv.writer(stream)
        writer.writerow(["basis","scope_id","player_id","total_weighted_days","streak_weighted_days","exact_weighted_days","quality_total","quality_streak","quality_exact","days_first","days_second","days_third","episodes","self_improvements","current_rank"])
        for (basis,sid,p),s in sorted(histories.items()):
            writer.writerow([basis,sid,p,s["total"],s["streak"],s["exact"],s["quality_total"],s["quality_streak"],s["quality_exact"],*s["days"],s["episodes"],s["self_improvements"],s["current_rank"]])
    with (output/"all_rankings.csv").open("w",encoding="utf-8",newline="") as stream:
        writer=csv.writer(stream)
        writer.writerow(["id","name"]+[x for name,*_ in configurations() for x in (name+"_rank",name+"_points")])
        for p in orders["lifetime"]:
            writer.writerow([p,names[p]]+[x for name,*_ in configurations() for x in (ranks[name][p],totals[name][p])])
    print(json.dumps({k:results[k] for k in ("history_rows","people","coverage","seconds")},ensure_ascii=False))
    print(json.dumps(comparisons[1],ensure_ascii=False))


if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("--input",type=Path,default=Path("exports/ranking-tenure-20261127/input"))
    parser.add_argument("--output",type=Path,default=Path("exports/ranking-tenure-20261127/analysis_lifetime"))
    args=parser.parse_args()
    checks()
    main(args.input,args.output)
