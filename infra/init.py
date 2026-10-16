import json
import os
from pathlib import Path
import subprocess

from speedrun_etl.entity_storage import init_entities

auth = Path(os.environ["AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE"])
auth.parent.mkdir(parents=True, exist_ok=True)
if not auth.exists():
    auth.write_text(json.dumps({"admin": os.environ["AIRFLOW_ADMIN_PASSWORD"]}))
    auth.chmod(0o600)
subprocess.run(["airflow", "db", "migrate"], check=True)
for name, slots, description in (("source_api", 1, "Sequential source requests plus durable pacing"),
                                 ("ods_load", 2, "Two concurrent batched ODS loads")):
    subprocess.run(["airflow", "pools", "set", name, str(slots), description], check=True)
init_entities()
print("Airflow and three ODS stores initialized")
