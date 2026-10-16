"""Airflow DAG: participant profile observations from the exact runs snapshot."""
from speedrun_etl.workflow import make_dag

dag = make_dag("players")
