"""Export only the historical input, in a single read-only snapshot."""

import argparse
import hashlib
import json
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path

HERE=Path(__file__).resolve().parent


def main(output):
    if output.exists():
        raise SystemExit("Choose a new directory; previous exports are immutable")
    output.mkdir(parents=True)
    query=(HERE/"extract.sql").read_text(encoding="utf-8")
    remote=f'''
import json,os,sys
from psycopg.rows import dict_row
from speedrun_etl.connections import pg_connect
if os.environ.get("ETL_NAMESPACE"):
    raise RuntimeError("Unexpected namespace")
with pg_connect() as conn:
    conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
    conn.execute("SET LOCAL statement_timeout='5min'")
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT * FROM speedrun_marts.context")
        context=cur.fetchall()
    readonly=conn.execute("SHOW transaction_read_only").fetchone()[0]
    print(json.dumps({{"context":context,"transaction_read_only":readonly}},default=str),flush=True)
    with conn.cursor().copy("COPY ("+{query!r}+") TO STDOUT WITH CSV HEADER") as cp:
        for block in cp:sys.stdout.buffer.write(block)
'''
    started=time.monotonic()
    wire=output/"wire.tmp"
    with wire.open("wb") as stream:
        subprocess.run(["docker","exec","--user","airflow","-e","PYTHONPATH=/opt/project/src",
                        "-i","speedrun-lab3-worker","python","-"],input=remote.encode(),stdout=stream,check=True)
    target=output/"history.csv"
    with wire.open("rb") as src,target.open("wb") as dst:
        metadata=json.loads(src.readline())
        while chunk:=src.read(1024*1024):dst.write(chunk)
    wire.unlink()
    metadata.update(extracted_at=datetime.now(UTC).isoformat(),
                    seconds=round(time.monotonic()-started,3),
                    input_sha256=hashlib.sha256(target.read_bytes()).hexdigest(),
                    sql_sha256=hashlib.sha256(query.encode()).hexdigest())
    (output/"metadata.json").write_text(json.dumps(metadata,ensure_ascii=False,indent=2),encoding="utf-8")
    print(json.dumps(metadata))


if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("--output",type=Path,default=Path("exports/ranking-tenure-20261127/input"))
    main(parser.parse_args().output)
