"""Generate local credentials once; never print them or overwrite existing files."""
from pathlib import Path
import secrets

root = Path(__file__).resolve().parents[1]
env_path = root / ".env"
config_path = root / "infra" / "garage.local.toml"
if env_path.exists() or config_path.exists():
    raise SystemExit("Local configuration already exists; refusing to overwrite")
values = {name: secrets.token_hex(24) for name in (
    "POSTGRES_PASSWORD", "AIRFLOW_DB_PASSWORD", "ODS_DB_PASSWORD",
    "CONTROL_DB_PASSWORD", "MONGO_PASSWORD", "S3_SECRET_KEY",
    "AIRFLOW_JWT_SECRET", "AIRFLOW_ADMIN_PASSWORD",
)}
values["S3_ACCESS_KEY"] = "GK" + secrets.token_hex(16)
config_path.write_text('''metadata_dir = "/var/lib/garage/meta"
data_dir = "/var/lib/garage/data"
db_engine = "sqlite"
replication_factor = 1
rpc_bind_addr = "[::]:3901"
rpc_public_addr = "garage:3901"
rpc_secret = "''' + secrets.token_hex(32) + '''"
[s3_api]
s3_region = "garage"
api_bind_addr = "[::]:3900"
root_domain = ".s3.garage.localhost"
[admin]
api_bind_addr = "[::]:3903"
''', encoding="utf-8")
env_path.write_text("".join(f"{key}={value}\n" for key, value in values.items()), encoding="utf-8")
print("Created .env and infra/garage.local.toml (values hidden)")
