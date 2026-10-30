"""Выполняет три серии замеров в отдельном стенде и возобновляет очередь.

Перед измерениями проверяет мягкую паузу production и отсутствие активных
DAG/tasks. После сравнивает бюджеты источника; finally возобновляет только
ту паузу, которая принадлежит этому эксперименту. Production-код не меняется.
"""

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--production-root", type=Path, required=True)
    parser.add_argument("--production-collection", required=True)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--skip-ods",
        action="store_true",
        help="Retain a completed ODS series; rerun all workload sizes",
    )
    args = parser.parse_args()
    code_root = Path(__file__).resolve().parents[1]
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    bench = [
        "docker",
        "compose",
        "--project-name",
        "speedrun-comparison-20261030",
        "--env-file",
        str(args.env_file.resolve()),
        "-f",
        str(code_root / "infra/compose.comparison.yaml"),
    ]
    prod = [
        "docker",
        "compose",
        "-f",
        str(args.production_root.resolve() / "compose.yaml"),
    ]
    status = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "phase": "preflight",
        "resumed": False,
        "client_memory_policy_MiB": {"ods": 1536, "workloads": 3072, "pg_bulk": 3072},
    }

    def save():
        status["updated_at"] = datetime.now(timezone.utc).isoformat()
        temporary = output / "session.pending.json"
        temporary.write_text(
            json.dumps(status, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        temporary.replace(output / "session.json")

    def production():
        code = """import os,json
from pathlib import Path
import psycopg
from speedrun_etl.jsonutil import read
from speedrun_etl.progress import driver_running
state=Path(os.environ['ETL_STATE'])
root=state/'batches'/COLLECTION
with psycopg.connect(os.environ['AIRFLOW__DATABASE__SQL_ALCHEMY_CONN'].replace('postgresql+psycopg2://','postgresql://')) as c:
 c.execute('SET TRANSACTION READ ONLY')
 active_dags=c.execute("SELECT count(*) FROM dag_run WHERE state IN ('running','queued')").fetchone()[0]
 active_tasks=c.execute("SELECT count(*) FROM task_instance WHERE state IN ('running','queued','restarting')").fetchone()[0]
print(json.dumps({'status':read(root/'status.json'),'control':read(root/'control.json'),
 'driver_running':driver_running(root/'driver.lock'),'active_dags':active_dags,'active_tasks':active_tasks,
 'source_budgets':{p.parent.name:int(read(p)['used']) for p in (state/'collections').glob('*/budget.json')}},default=str))
""".replace("COLLECTION", repr(args.production_collection))
        value = subprocess.check_output(
            prod + ["exec", "-T", "scheduler", "python", "-c", code],
            cwd=args.production_root,
            text=True,
        )
        return json.loads(value)

    before = production()
    if (
        before["driver_running"]
        or before["active_dags"]
        or before["active_tasks"]
        or before["status"]["phase"] != "paused"
    ):
        raise RuntimeError("Production has not reached its safe pause boundary")
    request = before["control"]["requested_at"]
    status["production_before"] = before
    save()

    def run(phase, arguments):
        status["phase"] = phase
        save()
        print("Comparison phase:", phase, flush=True)
        with (output / (phase + ".log")).open("w", encoding="utf-8") as log:
            subprocess.run(
                bench + ["run", "--rm", "--no-deps", "runner"] + arguments,
                cwd=code_root,
                env={
                    **os.environ,
                    "BENCH_RUNNER_MEMORY": "1536m" if phase == "ods" else "3072m",
                },
                stdout=log,
                stderr=subprocess.STDOUT,
                check=True,
            )

    try:
        if not args.skip_ods:
            run(
                "ods",
                [
                    "-m",
                    "speedrun_etl.benchmark",
                    "--collection",
                    "lab1-popular500-20261030-g-4pd0n31e",
                    "--log",
                ],
            )
        run(
            "workloads",
            [
                "-m",
                "speedrun_etl.storage_workloads",
                "--collections",
                "lab1-popular500-20261030-g-4pd0n31e",
                "lab1-popular500-20261030-g-o1y9wo6q",
                "lab1-popular500-20261030-g-j1npme6p",
                "--sizes",
                "1000",
                "10000",
                "100000",
                "--log",
            ],
        )
        find = "from pathlib import Path; import os; p=Path(os.environ['ETL_STATE'])/'benchmarks'; print(sorted(p.glob('workloads_*/input_100000'))[-1])"
        bulk_input = (
            subprocess.check_output(
                bench + ["run", "--rm", "--no-deps", "runner", "-c", find],
                cwd=code_root,
                text=True,
            )
            .strip()
            .splitlines()[-1]
        )
        run(
            "pg_bulk",
            ["-m", "speedrun_etl.storage_workloads", "--pg-bulk-input", bulk_input],
        )
        after = production()
        status["production_after"] = after
        if before["source_budgets"] != after["source_budgets"]:
            raise RuntimeError("Source budgets changed during measurement window")
        status.update(phase="measurements_complete", source_HTTP_during_measurements=0)
    except BaseException as error:
        status.update(phase="failed", error=repr(error))
        raise
    finally:
        try:
            current = production()
            may_resume = (
                current["status"]["phase"] == "paused"
                and current["control"].get("pause_requested")
                and current["control"].get("requested_at") == request
                and not (output / "hold-production-resume").exists()
            )
            if may_resume:
                subprocess.run(
                    prod
                    + [
                        "exec",
                        "-d",
                        "scheduler",
                        "python",
                        "-u",
                        "-m",
                        "speedrun_etl.batch",
                        "resume",
                        "--collection",
                        args.production_collection,
                        "--log",
                    ],
                    cwd=args.production_root,
                    check=True,
                )
                status["resumed"] = True
                print(
                    "Production resume submitted with saved configuration", flush=True
                )
            else:
                status["resume_skipped"] = (
                    "Pause changed, not paused, or explicit hold marker exists"
                )
        except Exception as error:
            status["resume_error"] = repr(error)
        save()


if __name__ == "__main__":
    main()
