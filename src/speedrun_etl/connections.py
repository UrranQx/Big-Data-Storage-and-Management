"""Общие подключения PG/Mongo/S3 без операций прежнего формата ODS."""

import os
import re

from .jsonutil import loads


def namespace():
    name = os.environ.get("ETL_NAMESPACE", "")
    if name and not re.fullmatch(r"test_[a-z0-9_]{1,40}", name):
        raise ValueError("Only explicit test_* namespaces are supported")
    return name


def pg_connect(control=False):
    import psycopg
    from psycopg.types.json import set_json_loads

    conn = psycopg.connect(
        host=os.environ.get("PGHOST", "postgres"),
        dbname="speedrun_control"
        if control
        else os.environ.get("PGDATABASE", "speedrun_ods"),
        user="speedrun_control"
        if control
        else os.environ.get("PGUSER", "speedrun_ods"),
        password=os.environ["CONTROL_DB_PASSWORD" if control else "PGPASSWORD"],
    )
    set_json_loads(loads, conn)
    if not control and namespace():
        from psycopg import sql

        conn.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                sql.Identifier(namespace())
            )
        )
        conn.execute(
            sql.SQL("SET search_path TO {}").format(sql.Identifier(namespace()))
        )
    return conn


class Mongo:
    def __enter__(self):
        from pymongo import MongoClient
        from pymongo.write_concern import WriteConcern

        self.client = MongoClient(
            "mongodb://mongo:27017",
            username="ods",
            password=os.environ["MONGO_PASSWORD"],
            serverSelectionTimeoutMS=10000,
        )
        self.db = self.client.get_database(
            namespace() or "speedrun_ods", write_concern=WriteConcern(w=1, j=True)
        )
        return self

    def __exit__(self, *args):
        self.client.close()


class S3:
    def __enter__(self):
        import boto3
        from botocore.config import Config

        self.bucket = "speedrun-ods"
        self.prefix = (namespace() + "/") if namespace() else ""
        self.client = boto3.client(
            "s3",
            endpoint_url="http://garage:3900",
            region_name="garage",
            aws_access_key_id=os.environ["S3_ACCESS_KEY"],
            aws_secret_access_key=os.environ["S3_SECRET_KEY"],
            config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 3}),
        )
        return self

    def __exit__(self, *args):
        self.client.close()
