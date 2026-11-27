"""Offline ranking experiment. No writes to DB, Superset, images or DAGs.

python experiment.py extract --output exports/ranking-quality-20261127
python experiment.py analyze --output exports/ranking-quality-20261127
python experiment.py check

extract uses an existing worker's DB credentials without exposing them. Raw
data stay in ignored exports/. analyze needs only Python's standard library.
"""

import argparse
import csv
import hashlib
import json
import math
import subprocess
import statistics
import time
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent


def write_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")


def extract(output, worker):
    output.mkdir(parents=True, exist_ok=True)
    target = output / "input.csv"
    if target.exists():
        raise SystemExit("Input already exists; choose a new output directory.")
    query = (HERE / "extract.sql").read_text(encoding="utf-8")
    remote = f'''
import json, os, sys
from speedrun_etl.connections import pg_connect
from psycopg.rows import dict_row
if os.environ.get("ETL_NAMESPACE"):
    raise RuntimeError("Research extraction requires the normal read-only marts connection")
with pg_connect() as conn:
    conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
    conn.execute("SET LOCAL statement_timeout='5min'")
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT * FROM speedrun_marts.context")
        context = cur.fetchall()
    readonly = conn.execute("SHOW transaction_read_only").fetchone()[0]
    print(json.dumps({{"context": context, "transaction_read_only": readonly}}, default=str), flush=True)
    with conn.cursor().copy("COPY (" + {query!r} + ") TO STDOUT WITH CSV HEADER") as cp:
        for block in cp:
            sys.stdout.buffer.write(block)
'''
    started = time.monotonic()
    wire = output / "extraction.tmp"
    with wire.open("wb") as stream:
        subprocess.run(
            ["docker", "exec", "--user", "airflow", "-e", "PYTHONPATH=/opt/project/src",
             "-i", worker, "python", "-"],
            input=remote.encode(), stdout=stream, check=True,
        )
    with wire.open("rb") as src, target.open("wb") as dst:
        meta = json.loads(src.readline())
        while block := src.read(1024 * 1024):
            dst.write(block)
    wire.unlink()
    meta.update(extracted_at=datetime.now(UTC).isoformat(),
                extract_seconds=round(time.monotonic() - started, 3),
                input_sha256=hashlib.sha256(target.read_bytes()).hexdigest(),
                sql_sha256=hashlib.sha256(query.encode()).hexdigest())
    write_json(output / "input_metadata.json", meta)
    print(json.dumps(meta, ensure_ascii=False))


def quality(n, place, ties=1, k=20, kind="log", gap=0, scale=0, gap_weight=0.3):
    """All coefficients are research policy choices, not fitted probabilities."""
    midrank = place + (ties - 1) / 2
    field_weight = (n - 1) / (n - 1 + k)
    if kind == "baseline":
        return (n - midrank) / (n - 1 + k)
    rank_component = max(0.0, 1 - math.log(midrank) / math.log(n))
    if kind == "gap":
        # Degenerate/fully tied distributions use rank alone. The 0.001 floor
        # is a sensitivity parameter, not measured timer resolution.
        if scale <= 1e-12:
            return field_weight * rank_component
        gap_component = math.exp(-gap / max(scale, 0.001))
        rank_component = (1-gap_weight)*rank_component + gap_weight*gap_component
    return field_weight * rank_component


def aggregate(scores, gamma=1, decay=1, limit=None):
    ordered = sorted(scores, reverse=True)
    if limit is not None:
        ordered = ordered[:limit]
    return 100 * sum(decay**i * q**gamma for i, q in enumerate(ordered))


def configs():
    # Separate changes to quality, cap, nonlinear reward and portfolio weights.
    result = [
        ("released_top10", "baseline", 20, 1, 1, 10, 0),
        ("baseline_all", "baseline", 20, 1, 1, None, 0),
        ("log_top10", "log", 20, 1, 1, 10, 0),
        ("log_all", "log", 20, 1, 1, None, 0),
        ("log_square_all", "log", 20, 2, 1, None, 0),
        ("log_square_decay", "log", 20, 2, .9, None, 0),
        ("gap_square_decay", "gap", 20, 2, .9, None, .3),
    ]
    for gamma in (1.5, 3):
        result.append((f"gamma_{gamma}", "log", 20, gamma, .9, None, 0))
    for decay in (.8, .95):
        result.append((f"decay_{decay}", "log", 20, 2, decay, None, 0))
    for k in (5, 50):
        result.append((f"k_{k}", "log", k, 2, .9, None, 0))
    for weight in (.1, .5):
        result.append((f"gap_weight_{weight}", "gap", 20, 2, .9, None, weight))
    return result


def checks():
    # Behavioral checks, including a documented failure of naive accumulation.
    for n in (10, 100, 10000):
        qs = [quality(n, r) for r in range(1, n+1)]
        assert all(0 <= q <= 1 for q in qs)
        assert all(a >= b for a, b in zip(qs, qs[1:]))
        assert quality(n, 1, n) < quality(n, 1)
    assert aggregate([.95]*3) < aggregate([.55]*10)
    assert aggregate([.95]*3, 2, .9) > aggregate([.55]*10, 2, .9)
    assert aggregate([.95]*3, 2, .9) < aggregate([.95]*10, 2, .9)
    assert aggregate([.8]*11, 2, .9) > aggregate([.8]*10, 2, .9)
    assert aggregate([1]*1000, 2, .9) <= 1000+1e-9
    # Gaps are invariant to units and reverse direction; ranks tie identically.
    assert math.isclose(abs(math.log(110/100)), abs(math.log(11000/10000)))
    assert math.isclose(abs(math.log(110/100)), abs(math.log(100/110)))
    assert quality(100, 2) > quality(100, 2, 3) > quality(100, 5)
    assert quality(100, 1, kind="gap", scale=0) == quality(100, 1)
    print("Behavior checks passed")


def rank_map(totals):
    ordered = sorted(totals, key=lambda p: (-totals[p], p))
    ranks, last, rank = {}, None, 0
    for i, p in enumerate(ordered, 1):
        if totals[p] != last:
            rank = i
        ranks[p] = rank
        last = totals[p]
    return ordered, ranks


def analyze(output):
    started = time.monotonic()
    meta = json.loads((output / "input_metadata.json").read_text(encoding="utf-8"))
    assert hashlib.sha256((output / "input.csv").read_bytes()).hexdigest() == meta["input_sha256"]
    variants = configs()
    # Only one max per (runner, game). Each model may select a different category.
    models = list(dict.fromkeys((kind, k, weight) for _,kind,k,_,_,_,weight in variants))
    best = {model: {} for model in models}
    players, scopes, row_count = {}, {}, 0
    with (output / "input.csv").open(encoding="utf-8", newline="") as stream:
        for row in csv.DictReader(stream):
            row_count += 1
            player, game = row["holder_id"], row["game_id"]
            players[player] = row["player_name"]
            n, place, ties = (int(row[c]) for c in ("n", "place", "ties"))
            gap, scale = float(row["log_gap"]), float(row["top_decile_gap"])
            scope = row["scope_id"]
            scopes[scope] = {c:row[c] for c in ("game_id", "game_name", "scope_name", "direction")}
            scopes[scope].update(n=n, top_decile_gap=scale)
            evidence = (scope, place, n, ties, gap, row["run_id"])
            for kind,k,weight in models:
                q = quality(n, place, ties, k, kind, gap, scale, weight)
                old = best[kind,k,weight].get((player,game))
                if old is None or q > old[0] or (q == old[0] and scope < old[1][0]):
                    best[kind,k,weight][player,game] = (q,evidence)
    portfolios = {}
    for model, entries in best.items():
        per_person = defaultdict(list)
        for (person,game), (q,evidence) in entries.items():
            per_person[person].append((q,game,evidence))
        portfolios[model] = {p:sorted(vals,key=lambda v:(-v[0],v[1])) for p,vals in per_person.items()}
    del best
    totals, order, ranks = {}, {}, {}
    for name,kind,k,gamma,decay,limit,weight in variants:
        totals[name] = {p:aggregate((v[0] for v in vals),gamma,decay,limit)
                        for p,vals in portfolios[kind,k,weight].items()}
        order[name], ranks[name] = rank_map(totals[name])
    base = "released_top10"
    candidate = "log_square_decay"
    archived = HERE.parents[2] / "reports/lab3/runner_ranking10_20261127/validation.json"
    released_evidence = json.loads(archived.read_text(encoding="utf-8"))
    assert meta["context"][0]["build_id"] == released_evidence["summary"][0]["build_id"]
    assert len(players) == released_evidence["summary"][0]["ranked_people"]
    for expected in released_evidence["top_universal"]:
        person = expected["holder_id"]
        assert ranks[base][person] == expected["portfolio_rank"]
        assert abs(totals[base][person]-float(expected["portfolio_points"])) < 1e-6

    def details(person, name, max_games=15):
        _,kind,k,gamma,decay,limit,weight = next(v for v in variants if v[0]==name)
        vals = portfolios[kind,k,weight][person]
        parts = []
        for i,(q,game,e) in enumerate(vals):
            scope,place,n,ties,gap,run = e
            points = 100 * q**gamma * decay**i if limit is None or i<limit else 0
            parts.append(dict(game=game,game_name=scopes[scope]["game_name"],
                              scope=scopes[scope]["scope_name"],direction=scopes[scope]["direction"],
                              place=place,n=n,ties=ties,log_gap=gap,q=round(q,6),
                              points=round(points,6),run_id=run))
        return dict(player_id=person,name=players[person],rank=ranks[name][person],
                    released_rank=ranks[base][person],points=round(totals[name][person],6),
                    games=len(vals),strong_games=sum(q>=.7 for q,_,_ in vals),
                    elite_games=sum(q>=.9 for q,_,_ in vals),
                    tail_after_10=round(sum(p["points"] for p in parts[10:]),6),
                    contributions=parts[:max_games])

    comparisons = []
    reference_top = set(order[candidate][:100])
    for name,kind,k,gamma,decay,limit,weight in variants:
        comparisons.append(dict(name=name,quality=kind,k=k,gamma=gamma,decay=decay,
            limit=limit,gap_weight=weight,
            overlap_released_top100=len(set(order[name][:100]) & set(order[base][:100])),
            overlap_candidate_top100=len(set(order[name][:100]) & reference_top),
            mean_games_top20=round(sum(len(portfolios[kind,k,weight][p]) for p in order[name][:20])/20,2),
            top20=[details(p,name,3) for p in order[name][:20]]))

    # Compare only a disclosed union of either top 1000, avoiding trivial tail moves.
    pool = set(order[base][:1000]) | set(order[candidate][:1000])
    movers = sorted(pool, key=lambda p:(ranks[candidate][p]-ranks[base][p],p))
    selected = set(order[base][:10] + order[candidate][:20] + movers[:10] + movers[-10:])
    profiles = [{"released":details(p,base),"candidate":details(p,candidate)}
                for p in sorted(selected,key=lambda p:ranks[candidate][p])]

    synthetic = []
    for label,qs in [("3 elite", [.95]*3),("10 moderate", [.55]*10),
                     ("30 moderate", [.55]*30),("10 elite", [.95]*10),
                     ("1 elite", [.95]),("10 weak", [.2]*10)]:
        synthetic.append(dict(profile=label,quality=qs[0],games=len(qs),
            linear_all=round(aggregate(qs),3),
            square_all=round(aggregate(qs,2),3),
            square_decay=round(aggregate(qs,2,.9),3)))

    sensitivity = []
    family = ["log_square_decay","gamma_1.5","gamma_3","decay_0.8","decay_0.95","k_5","k_50"]
    for p in order[candidate][:20]:
        rs = {name:ranks[name][p] for name in family}
        sensitivity.append(dict(name=players[p],player_id=p,ranks=rs,min_rank=min(rs.values()),max_rank=max(rs.values())))

    # Counterexamples are reported, not hidden behind a successful check label.
    n_games = [len(v) for v in portfolios["log",20,0].values()]
    adversarial = {
        "winner_100_points":100*quality(100,1)**2,
        "winner_10000_points":100*quality(10000,1)**2,
        "rank10_1000_points":100*quality(1000,10)**2,
        "rank10_10000_points":100*quality(10000,10)**2,
        "three_elite_q095":aggregate([.95]*3,2,.9),
        "unlimited_moderate_q055_bound":100*.55**2/(1-.9),
        "three_elite_gamma3":aggregate([.95]*3,3,.9),
        "unlimited_moderate_gamma3_bound":100*.55**3/(1-.9),
        "one_elite_q095":aggregate([.95],2,.9),
        "unlimited_weak_q020_bound":100*.2**2/(1-.9),
    }
    invariants = {
        "released_top20_reproduced_to_1e6":True,
        "population_matches_release":True,
        "all_points_finite_nonnegative":all(math.isfinite(x) and x>=0 for vals in totals.values() for x in vals.values()),
        "candidate_below_theoretical_1000":max(totals[candidate].values())<1000,
        "all_games_never_less_than_top10":all(totals["log_all"][p]+1e-9>=totals["log_top10"][p] for p in players),
        "portfolio_unique_game":all(len(vals)==len({v[1] for v in vals}) for vals in portfolios["log",20,0].values()),
    }
    assert all(invariants.values()), invariants

    with (output / "all_rankings.csv").open("w",encoding="utf-8",newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(["player_id","name","eligible_games"] + [x for name,*_ in variants for x in (name+"_rank",name+"_points")])
        for p in order[candidate]:
            writer.writerow([p,players[p],len(portfolios["log",20,0][p])] +
                [x for name,*_ in variants for x in (ranks[name][p],round(totals[name][p],6))])
    with (output / "candidate_contributions.csv").open("w",encoding="utf-8",newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(["player_id","name","game","scope","place","field_size","ties","quality","weight","points","direction","log_gap"])
        for p in order[candidate]:
            for i,(q,game,e) in enumerate(portfolios["log",20,0][p]):
                scope,place,n,ties,gap,_ = e
                writer.writerow([p,players[p],scopes[scope]["game_name"],scopes[scope]["scope_name"],place,n,ties,q,.9**i,100*q*q*.9**i,scopes[scope]["direction"],gap])
    result = dict(metadata=meta,rows=row_count,people=len(players),fields=len(scopes),
                  games=len({s["game_id"] for s in scopes.values()}),
                  reverse_fields=sum(s["direction"]=="-1" for s in scopes.values()),
                  degenerate_top_decile_fields=sum(s["top_decile_gap"]<=1e-12 for s in scopes.values()),
                  portfolio_statistics=dict(median_games=statistics.median(n_games),max_games=max(n_games),
                      people_over10_games=sum(n>10 for n in n_games)),
                  variants=comparisons,profiles=profiles,synthetic=synthetic,sensitivity=sensitivity,
                  checks=invariants,adversarial=adversarial,
                  analysis_seconds=round(time.monotonic()-started,3),
                  policy="Exploratory, not a calibrated measure of intrinsic difficulty or skill")
    write_json(output / "results.json",result)
    print(json.dumps({"rows":row_count,"people":len(players),"seconds":result["analysis_seconds"],
                      "candidate_top10":[(players[p],round(totals[candidate][p],3),ranks[base][p]) for p in order[candidate][:10]]},ensure_ascii=False))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action",choices=["extract","analyze","check"])
    parser.add_argument("--output",type=Path,default=Path("exports/ranking-quality-20261127"))
    parser.add_argument("--worker",default="speedrun-lab3-worker")
    args = parser.parse_args()
    if args.action == "extract":
        extract(args.output,args.worker)
    elif args.action == "analyze":
        checks()
        analyze(args.output)
    else:
        checks()
