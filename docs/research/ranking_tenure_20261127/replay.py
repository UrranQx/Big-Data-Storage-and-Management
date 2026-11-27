"""Retrospective solo podium tenure; simultaneous events are applied together."""

from bisect import bisect_left, insort
from collections import defaultdict
from itertools import groupby
import math

WEIGHTS=(1.0,.6,.3)


def new_stats():
    return dict(total=0.0,streak=0.0,exact=0.0,days=[0.0,0.0,0.0],
                quality_total=0.0,quality_streak=0.0,quality_exact=0.0,
                streak_start=None,exact_start=None,current_rank=None,
                episodes=0,self_improvements=0)


def replay(events, end, minimum_field=10):
    """events=(timestamp_seconds, player, oriented_integer_score).

    Scores are smaller-is-better. No mixing of date bases. Tied players share
    occupied medal weights; historical field size is unique PB holders.
    """
    best,ordered,active={},[],{}
    stats=defaultdict(new_stats)
    last=None

    def accrue(until):
        if last is None:return
        days=(until-last)/86400
        assert days>=0
        for player,(rank,weight,q) in active.items():
            s=stats[player]
            s["days"][rank-1]+=days
            s["total"]+=days*weight
            s["streak"]+=days*weight
            s["exact"]+=days*weight
            s["quality_total"]+=days*weight*q*q
            s["quality_streak"]+=days*weight*q*q
            s["quality_exact"]+=days*weight*q*q

    for at,group in groupby(sorted(events),key=lambda e:e[0]):
        assert at<=end
        accrue(at)
        improved=set()
        for _,player,score in group:
            old=best.get(player)
            if old is not None and score>=old:continue
            if old is not None:
                ordered.pop(bisect_left(ordered,(old,player)))
                improved.add(player)
            best[player]=score
            insort(ordered,(score,player))
        new_active={}
        if len(ordered)>=minimum_field:
            i=0
            while i<min(3,len(ordered)):
                j=i+1
                while j<len(ordered) and ordered[j][0]==ordered[i][0]:j+=1
                rank=i+1
                weight=sum(WEIGHTS[i:min(j,3)])/(j-i)
                n=len(ordered)
                midrank=rank+(j-i-1)/2
                q=(n-1)/(n-1+20)*(1-math.log(midrank)/math.log(n))
                for _,player in ordered[i:j]:new_active[player]=(rank,weight,q)
                i=j
        for p in active.keys()|new_active.keys():
            s=stats[p]
            old_rank=active.get(p,(None,))[0]
            new_rank=new_active.get(p,(None,))[0]
            if new_rank is None:
                s.update(streak=0.0,exact=0.0,quality_streak=0.0,quality_exact=0.0,
                         streak_start=None,exact_start=None,current_rank=None)
            elif old_rank is None:
                s.update(streak=0.0,exact=0.0,quality_streak=0.0,quality_exact=0.0,
                         streak_start=at,exact_start=at,current_rank=new_rank)
                s["episodes"]+=1
            elif old_rank!=new_rank:
                s.update(exact=0.0,quality_exact=0.0,exact_start=at,current_rank=new_rank)
            if p in improved and p in active and p in new_active:
                s["self_improvements"]+=1
        active=new_active
        last=at
    accrue(end)
    return dict(stats),best


def bonus(equivalent_days,alpha=25,half_days=365):
    return alpha*equivalent_days/(equivalent_days+half_days)


def checks():
    day=86400
    seed=[(0,chr(65+i),100+i*10) for i in range(10)]
    # Self-improvement keeps the whole reign; own publications do not add days.
    s,_=replay(seed+[(10*day,"A",90),(20*day,"A",80)],30*day)
    assert s["A"]["total"]==30 and s["A"]["streak"]==30
    assert s["A"]["exact"]==30 and s["A"]["self_improvements"]==2
    assert s["B"]["total"]==18 and s["C"]["total"]==9
    # Losing WR but staying on podium retains podium history, exact rank resets.
    s,_=replay(seed+[(10*day,"D",90)],30*day)
    assert s["A"]["total"]==22 and s["A"]["streak"]==22 and s["A"]["exact"]==12
    # Three challengers arrive simultaneously; no fictitious intermediate reigns.
    events=seed+[(10*day,p,v) for p,v in [("D",90),("E",80),("F",70)]]+[(20*day,"A",60)]
    s,_=replay(events,30*day)
    assert s["A"]["total"]==20 and s["A"]["streak"]==10 and s["A"]["episodes"]==2
    assert s["F"]["total"]==16
    lost,_=replay(events[:-1],30*day)
    before_loss,_=replay(seed,10*day)
    assert lost["A"]["current_rank"] is None
    assert lost["A"]["quality_total"]==before_loss["A"]["quality_total"]
    assert bonus(lost["A"]["quality_total"])>0
    assert s["A"]["quality_total"]>lost["A"]["quality_total"]
    # An early lone leader gets no credit until the tenth competitor arrives.
    s,_=replay([(0,"A",100)]+[(10*day,p,v) for _,p,v in seed[1:]],20*day)
    assert s["A"]["total"]==10
    # Ten identical first places share the three medal slots fairly.
    s,_=replay([(0,str(i),100) for i in range(10)],10*day)
    assert abs(sum(v["total"] for v in s.values())-19)<1e-10
    assert len({round(v["total"],9) for v in s.values()})==1
    # Source orientation also works for higher-better results (negative scores).
    s,_=replay([(0,str(i),-100+i) for i in range(10)],10*day)
    assert s["0"]["total"]==10
    assert bonus(0)==0 and bonus(365)==12.5 and bonus(3650)<25
    print("Tenure behavioral checks passed")


if __name__=="__main__":checks()
