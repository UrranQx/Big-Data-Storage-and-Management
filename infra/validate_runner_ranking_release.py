"""Compare the live mart with the frozen experiment, without publishing anything."""

import csv
import io
import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEST = ROOT / "reports/lab3/runner_ranking_v2_20261128"


def worker(code):
    return subprocess.run(
        [
            "docker",
            "exec",
            "--user",
            "airflow",
            "-e",
            "PYTHONPATH=/opt/project/src",
            "-i",
            "speedrun-lab3-worker",
            "python",
            "-",
        ],
        input=code.encode("utf-8"),
        stdout=subprocess.PIPE,
        check=True,
    ).stdout


def main():
    DEST.mkdir(parents=True, exist_ok=True)
    raw = worker(
        "import runpy; runpy.run_path('/opt/project/infra/inspect_runner_ranking.py',run_name='__main__')"
    )
    actual = json.loads(raw)
    reference = json.loads(
        (
            ROOT / "docs/research/ranking_tenure_20261127/evidence/results.json"
        ).read_text(encoding="utf-8")
    )
    expected = next(v for v in reference["variants"] if v["name"] == "lifetime")
    assert actual["summary"][0]["ranked_people"] == reference["people"]
    observed = {p["holder_id"]: p for p in actual["top_universal"]}
    for p in expected["top20"]:
        row = observed[p["id"]]
        assert row["portfolio_rank"] == p["rank"]
        assert abs(float(row["portfolio_points"]) - p["points"]) < 1e-6
    summary = {"top20_matches_experiment": True, "same_population": True}
    local = ROOT / "exports/ranking-tenure-20261127/analysis_lifetime/all_rankings.csv"
    if local.exists():
        code = """
import sys
from pathlib import Path
from speedrun_etl.connections import pg_connect
query=Path('/opt/project/infra/runner_ranking.sql').read_text()
with pg_connect() as conn:
    conn.execute('SET TRANSACTION READ ONLY')
    sql='COPY (SELECT holder_id,portfolio_rank,portfolio_points FROM ('+query+') x WHERE portfolio_rank IS NOT NULL) TO STDOUT WITH CSV HEADER'
    with conn.cursor().copy(sql) as cp:
        for block in cp:sys.stdout.buffer.write(block)
"""
        rows = list(csv.DictReader(io.StringIO(worker(code).decode("utf-8"))))
        with local.open(encoding="utf-8", newline="") as stream:
            expected_rows = {r["id"]: r for r in csv.DictReader(stream)}
        assert len(rows) == len(expected_rows)
        maximum_delta = 0
        changed_places = []
        for row in rows:
            original = expected_rows[row["holder_id"]]
            delta = abs(
                float(row["portfolio_points"]) - float(original["lifetime_points"])
            )
            maximum_delta = max(maximum_delta, delta)
            assert delta < 1e-6, (row["holder_id"], delta)
            if int(row["portfolio_rank"]) != int(original["lifetime_rank"]):
                changed_places.append(
                    {
                        "holder": row["holder_id"],
                        "old": int(original["lifetime_rank"]),
                        "new": int(row["portfolio_rank"]),
                    }
                )
        summary.update(
            full_population_scores_checked=len(rows),
            maximum_score_delta=maximum_delta,
            ranks_changed_by_decimal_quantization=changed_places,
        )
    else:
        summary["full_population_scores_checked"] = None
    (DEST / "validation.json").write_text(
        json.dumps(actual, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (DEST / "experiment_comparison.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(
        json.dumps(
            {
                **summary,
                "ranks_changed_by_decimal_quantization": len(
                    summary.get("ranks_changed_by_decimal_quantization", [])
                ),
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
