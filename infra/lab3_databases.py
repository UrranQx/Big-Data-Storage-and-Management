"""Idempotent database and read-only role setup on an existing PostgreSQL volume."""

import os
from pathlib import Path

import psycopg
from psycopg import sql


def main():
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    key_dir = Path("/opt/project/keys")
    key_dir.mkdir(parents=True, exist_ok=True)
    private_path = key_dir / "private_key.der"
    if not private_path.exists():
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        private_path.write_bytes(
            key.private_bytes(
                serialization.Encoding.DER,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            )
        )
        (key_dir / "public_key.der").write_bytes(
            key.public_key().public_bytes(
                serialization.Encoding.DER,
                serialization.PublicFormat.SubjectPublicKeyInfo,
            )
        )
    with psycopg.connect(
        host="postgres",
        dbname="postgres",
        user="postgres",
        password=os.environ["POSTGRES_PASSWORD"],
        autocommit=True,
    ) as conn:
        for role, database, variable in (
            ("superset", "superset", "SUPERSET_DB_PASSWORD"),
            ("openmetadata", "openmetadata", "OPENMETADATA_DB_PASSWORD"),
            ("analytics_reader", None, "ANALYTICS_READER_PASSWORD"),
        ):
            if not os.environ.get(variable):
                raise ValueError(f"Configure {variable} with infra/configure_lab3.py")
            if not conn.execute(
                "SELECT 1 FROM pg_roles WHERE rolname=%s", (role,)
            ).fetchone():
                conn.execute(
                    sql.SQL("CREATE ROLE {} LOGIN").format(sql.Identifier(role))
                )
            conn.execute(
                sql.SQL("ALTER ROLE {} PASSWORD {}").format(
                    sql.Identifier(role),
                    sql.Literal(os.environ[variable]),
                )
            )
            if (
                database
                and not conn.execute(
                    "SELECT 1 FROM pg_database WHERE datname=%s", (database,)
                ).fetchone()
            ):
                conn.execute(
                    sql.SQL("CREATE DATABASE {} OWNER {}").format(
                        sql.Identifier(database),
                        sql.Identifier(role),
                    )
                )
        conn.execute("GRANT CONNECT ON DATABASE speedrun_ods TO analytics_reader")
    print("Lab 3 databases and roles ready")


if __name__ == "__main__":
    main()
