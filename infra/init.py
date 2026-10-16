import json
import os
from pathlib import Path
import subprocess

from speedrun_etl.storage import init_stores

auth = Path(os.environ["AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE"])
auth.parent.mkdir(parents=True, exist_ok=True)
if not auth.exists():
    auth.write_text(json.dumps({"admin": os.environ["AIRFLOW_ADMIN_PASSWORD"]}))
    auth.chmod(0o600)
subprocess.run(["airflow", "db", "migrate"], check=True)
for name, description in (("source_api", "Sequential source requests plus durable pacing"),
                          ("ods_load", "Serialize store loads to limit RAM and I/O")):
    subprocess.run(["airflow", "pools", "set", name, "1", description], check=True)
init_stores()
print("Airflow and three ODS stores initialized")
