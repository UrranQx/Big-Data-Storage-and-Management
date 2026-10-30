"""Проводит расширенные замеры в отдельном Compose и восстанавливает сбор.

Дожидается безопасной паузы, сохраняет бюджет/прежний deadline и выключает
Airflow только на окно опыта. finally возобновляет только собственную паузу.
"""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import time


ROOT = Path(__file__).resolve().parents[1]


def run(args, **kwargs):
    return subprocess.run(args, cwd=ROOT, check=True, **kwargs)


def source(batch):
    code = """import os,psycopg
from pathlib import Path
from speedrun_etl.jsonutil import read,dumps
from speedrun_etl.progress import driver_running
s=Path(os.environ['ETL_STATE']); p=s/'batches'/BATCH
uri=os.environ['AIRFLOW__DATABASE__SQL_ALCHEMY_CONN'].replace('postgresql+psycopg2://','postgresql://')
with psycopg.connect(uri) as c:
 c.execute('SET TRANSACTION READ ONLY')
 active=c.execute("SELECT count(*) FROM dag_run WHERE state IN ('running','queued')").fetchone()[0]
 tasks=c.execute("SELECT count(*) FROM task_instance WHERE state IN ('running','queued','restarting')").fetchone()[0]
print(dumps({'status':read(p/'status.json'),'control':read(p/'control.json'),'driver':driver_running(p/'driver.lock'),'controller':driver_running(p/'keep-running.lock'),'continuation':read(p/'keep-running.json'),'active':active,'tasks':tasks,'budgets':{f.parent.name:read(f)['used'] for f in (s/'collections').glob('*/budget.json')}}))
""".replace("BATCH", repr(batch))
    result = run(
        ["docker", "compose", "exec", "-T", "scheduler", "python", "-c", code],
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return json.loads(result.stdout)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--private-env", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--batch", default="lab1-popular500-20261030")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    path = args.output / "session.json"
    if path.exists():
        parser.error("New output directory required")
    record = {"phase": "waiting_safe_pause", "production_resumed": False}

    def save():
        record["updated_at"] = datetime.now(timezone.utc).isoformat()
        tmp = path.with_suffix(".pending.json")
        tmp.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
        tmp.replace(path)

    before = source(args.batch)
    if not before["control"].get("pause_requested"):
        parser.error("Request the authorised maintenance pause first")
    pause = before["control"]["requested_at"]
    until = before["continuation"]["until"]
    record.update(pause_request=pause, until=until)
    save()
    while (
        before["driver"] or before["controller"] or before["active"] or before["tasks"]
    ):
        time.sleep(10)
        before = source(args.batch)
        if before["control"].get("requested_at") != pause:
            raise RuntimeError("Pause changed during preflight")
    record["source_before"] = before
    save()
    bench = [
        "docker",
        "compose",
        "--project-name",
        "speedrun-comparison-20261030",
        "--env-file",
        str(args.private_env.resolve()),
        "-f",
        "infra/compose.comparison.yaml",
        "-f",
        "infra/compose.analytics.yaml",
    ]
    try:
        run(
            [
                "docker",
                "compose",
                "stop",
                "--timeout",
                "120",
                "scheduler",
                "api-server",
                "dag-processor",
            ]
        )
        record["phase"] = "measuring"
        save()
        with (args.output / "driver.log").open("w", encoding="utf-8") as log:
            run(
                bench
                + [
                    "run",
                    "--rm",
                    "--no-deps",
                    "--user",
                    "0",
                    "runner",
                    "-m",
                    "speedrun_etl.analytics_benchmark",
                    "--root",
                    "/opt/airflow/state/analytics",
                ],
                stdout=log,
                stderr=subprocess.STDOUT,
            )
        record["phase"] = "measurements_complete"
        save()
    except BaseException as error:
        record.update(phase="failed", error=repr(error))
        save()
        raise
    finally:
        run(bench + ["stop", "--timeout", "60"])
        run(
            [
                "docker",
                "compose",
                "up",
                "-d",
                "--no-build",
                "--pull",
                "never",
                "--wait",
                "--wait-timeout",
                "240",
            ]
        )
        after = source(args.batch)
        record["source_after"] = after
        record["source_budgets_unchanged"] = before["budgets"] == after["budgets"]
        if before["budgets"] != after["budgets"]:
            record.update(
                phase="failed", error="Unexpected source HTTP during measurement window"
            )
        if (
            after["control"].get("pause_requested")
            and after["control"].get("requested_at") == pause
            and datetime.fromisoformat(until) > datetime.now(timezone.utc)
        ):
            run(
                [
                    "docker",
                    "compose",
                    "exec",
                    "-d",
                    "scheduler",
                    "python",
                    "-u",
                    "-m",
                    "speedrun_etl.batch",
                    "resume",
                    "--collection",
                    args.batch,
                    "--log",
                ]
            )
            time.sleep(3)
            run(
                [
                    "docker",
                    "compose",
                    "exec",
                    "-d",
                    "scheduler",
                    "python",
                    "-u",
                    "-m",
                    "speedrun_etl.keep_running",
                    "--collection",
                    args.batch,
                    "--until",
                    until,
                    "--log",
                ]
            )
            time.sleep(3)
            current = source(args.batch)
            record["resumed_status"] = {
                k: current[k] for k in ("driver", "controller", "status", "control")
            }
            record["production_resumed"] = current["driver"] and current["controller"]
        else:
            record["resume_skipped"] = "Pause changed or original deadline passed"
        save()


if __name__ == "__main__":
    main()
