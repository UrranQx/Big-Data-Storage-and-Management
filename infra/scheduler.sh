#!/bin/bash
set -euo pipefail
python /opt/project/maintenance.py
exec airflow scheduler
