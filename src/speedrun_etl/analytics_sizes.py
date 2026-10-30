"""Читает размер шести входных наборов и витрин после замеров, вне таймеров.

S3 показывает тела текущих объектов и отдельно все сохранённые версии;
метрики не равны выделенному месту Docker volume.
"""

import argparse
import csv
import json
from pathlib import Path
import sqlite3
import statistics

from .analytics_benchmark import BASE, PIPELINE
from .jsonutil import read, loads
from .storage_workloads import connect, s3_get


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    args = parser.parse_args()
    result = sorted(args.root.glob("results_*/completed.json"))[-1].parent
    config = read(result / "config.json")
    stamp = result.name[len("results_") :]
    output = []
    distributions = []
    flushed = False
    for size in config["sizes"]:
        db = sqlite3.connect(
            f"file:{(args.root / ('input_v2_' + str(size)) / 'input.sqlite3').as_posix()}?mode=ro",
            uri=True,
        )
        logical = db.execute(
            "SELECT sum(length(CAST(document AS BLOB)))+count(*) FROM records"
        ).fetchone()[0]
        statuses = dict(
            db.execute(
                "SELECT json_extract(document,'$.status'),count(*) FROM records WHERE dataset='runs' GROUP BY 1"
            )
        )
        boards = [
            r[0]
            for r in db.execute(
                "SELECT count(*) FROM records WHERE dataset='runs' GROUP BY json_extract(document,'$.board_id')"
            )
        ]
        eligible = db.execute(
            "SELECT count(*) FROM records WHERE dataset='runs' AND json_extract(document,'$.eligible')=1"
        ).fetchone()[0]
        distributions.append(
            {
                "size": size,
                "eligible_runs": eligible,
                "status_counts_json": json.dumps(
                    {str(k): v for k, v in statuses.items()}, sort_keys=True
                ),
                "technical_group_runs_min": min(boards),
                "technical_group_runs_median": statistics.median(boards),
                "technical_group_runs_max": max(boards),
            }
        )
        db.close()
        for repeat in range(config["repeats"]):
            ns = f"test_wl_an_{stamp}_{size}_{repeat}"
            for name in ("postgres", "mongo", "garage"):
                row = {
                    "size": size,
                    "repeat": repeat + 1,
                    "store": name,
                    "input_canonical_bytes": logical,
                    "data_bytes": 0,
                    "index_bytes": 0,
                    "reported_bytes": 0,
                    "retained_all_objects_bytes": "",
                }
                with connect(name, ns) as h:
                    if name == "postgres":
                        counts = (
                            h["conn"]
                            .execute(
                                "SELECT coalesce(sum(pg_table_size(c.oid)),0),coalesce(sum(pg_indexes_size(c.oid)),0) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=%s AND c.relname=ANY(%s) AND c.relkind='r'",
                                [ns, BASE + PIPELINE],
                            )
                            .fetchone()
                        )
                        row.update(
                            data_bytes=int(counts[0]),
                            index_bytes=int(counts[1]),
                            method="table/TOAST + indexes, excludes *_full; after test updates",
                        )
                    elif name == "mongo":
                        if not flushed:
                            h["client"].admin.command("fsync")
                            flushed = True
                        for dataset in BASE + PIPELINE:
                            info = h["db"].command("collStats", dataset)
                            row["data_bytes"] += int(info["storageSize"])
                            row["index_bytes"] += int(info["totalIndexSize"])
                        row["method"] = (
                            "storageSize + indexes after fsync, excludes *_full; after test updates"
                        )
                    else:
                        index_key = ns + "/workloads/analytics/index.json"
                        index = loads(s3_get(h, index_key))
                        keys = {
                            key for dataset in BASE + PIPELINE for key in index[dataset]
                        }
                        total = 0
                        for page in (
                            h["s3"]
                            .client.get_paginator("list_objects_v2")
                            .paginate(
                                Bucket=h["s3"].bucket,
                                Prefix=ns + "/workloads/analytics/",
                            )
                        ):
                            for obj in page.get("Contents", []):
                                total += obj["Size"]
                                if obj["Key"] in keys or obj["Key"] == index_key:
                                    row["data_bytes"] += obj["Size"]
                        row.update(
                            index_bytes="",
                            retained_all_objects_bytes=total,
                            method="current referenced object bodies + snapshot index; all retained bodies separately; no volume overhead",
                        )
                row["reported_bytes"] = row["data_bytes"] + int(row["index_bytes"] or 0)
                output.append(row)
    with (result / "storage_sizes.csv").open(
        "w", encoding="utf-8", newline=""
    ) as handle:
        writer = csv.DictWriter(handle, fieldnames=list(output[0]))
        writer.writeheader()
        writer.writerows(output)
    print("Recorded", len(output), "storage size measurements")
    with (result / "input_distribution.csv").open(
        "w", encoding="utf-8", newline=""
    ) as handle:
        writer = csv.DictWriter(handle, fieldnames=list(distributions[0]))
        writer.writeheader()
        writer.writerows(distributions)


if __name__ == "__main__":
    main()
