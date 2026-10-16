#!/bin/bash
set -euo pipefail
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
  -v airflow_password="$AIRFLOW_DB_PASSWORD" \
  -v ods_password="$ODS_DB_PASSWORD" \
  -v control_password="$CONTROL_DB_PASSWORD" <<'SQL'
CREATE USER airflow PASSWORD :'airflow_password';
CREATE USER speedrun_ods PASSWORD :'ods_password';
CREATE USER speedrun_control PASSWORD :'control_password';
CREATE DATABASE airflow OWNER airflow;
CREATE DATABASE speedrun_ods OWNER speedrun_ods;
CREATE DATABASE speedrun_control OWNER speedrun_control;
REVOKE ALL ON DATABASE airflow FROM PUBLIC;
REVOKE ALL ON DATABASE speedrun_ods FROM PUBLIC;
REVOKE ALL ON DATABASE speedrun_control FROM PUBLIC;
SQL
