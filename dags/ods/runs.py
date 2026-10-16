"""Airflow DAG: runs, source statistics and WR history."""
from speedrun_etl.workflow import make_dag

dag = make_dag("runs", "players")
