"""Создаёт отдельные секреты и Garage-конфигурацию временного стенда замеров.

Production .env не читается. Файлы записываются только в новый каталог
эксперимента; повторный запуск отказывается их перезаписывать.
"""

import argparse
from pathlib import Path
import secrets


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--state-volume", required=True)
    parser.add_argument("--experiment-id", required=True)
    args = parser.parse_args()
    target = args.output.resolve()
    target.mkdir(parents=True, exist_ok=False)
    config = target / "garage.toml"
    config.write_text(
        'metadata_dir = "/var/lib/garage/meta"\n'
        'data_dir = "/var/lib/garage/data"\n'
        'db_engine = "sqlite"\nreplication_factor = 1\n'
        'rpc_bind_addr = "[::]:3901"\nrpc_public_addr = "garage:3901"\n'
        f'rpc_secret = "{secrets.token_hex(32)}"\n'
        '[s3_api]\ns3_region = "garage"\napi_bind_addr = "[::]:3900"\n'
        'root_domain = ".s3.garage.localhost"\n'
        '[admin]\napi_bind_addr = "[::]:3903"\n',
        encoding="utf-8",
    )
    values = {
        name: secrets.token_hex(24)
        for name in (
            "POSTGRES_PASSWORD",
            "AIRFLOW_DB_PASSWORD",
            "ODS_DB_PASSWORD",
            "CONTROL_DB_PASSWORD",
            "MONGO_PASSWORD",
            "S3_SECRET_KEY",
        )
    }
    values.update(
        S3_ACCESS_KEY="GK" + secrets.token_hex(16),
        BENCH_GARAGE_CONFIG=config.as_posix(),
        BENCH_STATE_VOLUME=args.state_volume,
        ETL_EXPERIMENT_ID=args.experiment_id,
    )
    (target / "private.env").write_text(
        "".join(f"{key}={value}\n" for key, value in values.items()), encoding="utf-8"
    )
    print("Created isolated comparison configuration; secrets hidden:", target)


if __name__ == "__main__":
    main()
