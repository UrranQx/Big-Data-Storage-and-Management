"""Однократно создаёт локальные секреты и конфигурацию стенда.

Не печатает значения и не перезаписывает существующие файлы."""

import argparse
from pathlib import Path
import secrets

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument(
    "--lab1", action="store_true", help="Configure historical PostgreSQL/Mongo/S3 stack"
)
args = parser.parse_args()

root = Path(__file__).resolve().parents[1]
env_path = root / ".env"
config_path = root / "infra" / "garage.local.toml"
if env_path.exists() or (args.lab1 and config_path.exists()):
    raise SystemExit("Local configuration already exists; refusing to overwrite")
values = {
    name: secrets.token_hex(24)
    for name in (
        "POSTGRES_PASSWORD",
        "AIRFLOW_DB_PASSWORD",
        "ODS_DB_PASSWORD",
        "CONTROL_DB_PASSWORD",
        "AIRFLOW_JWT_SECRET",
        "AIRFLOW_ADMIN_PASSWORD",
    )
}
if args.lab1:
    values["MONGO_PASSWORD"] = secrets.token_hex(24)
    values["S3_SECRET_KEY"] = secrets.token_hex(24)
    values["S3_ACCESS_KEY"] = "GK" + secrets.token_hex(16)
values["ETL_STORES"] = "postgres,mongo,garage" if args.lab1 else "postgres"
values["ETL_STAGING_CLEANUP"] = "0" if args.lab1 else "1"
values["ETL_MAX_RPM"] = "40"
values["ETL_PAGE_CONCURRENCY"] = "4"
garage_config = (
    '''metadata_dir = "/var/lib/garage/meta"
data_dir = "/var/lib/garage/data"
db_engine = "sqlite"
replication_factor = 1
rpc_bind_addr = "[::]:3901"
rpc_public_addr = "garage:3901"
rpc_secret = "'''
    + secrets.token_hex(32)
    + """"
[s3_api]
s3_region = "garage"
api_bind_addr = "[::]:3900"
root_domain = ".s3.garage.localhost"
[admin]
api_bind_addr = "[::]:3903"
"""
)
if args.lab1:
    config_path.write_text(garage_config, encoding="utf-8")
env_path.write_text(
    "".join(f"{key}={value}\n" for key, value in values.items()), encoding="utf-8"
)
print("Created local configuration; secret values hidden")
