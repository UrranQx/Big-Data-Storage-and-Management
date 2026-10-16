"""Airflow DAG: fresh game metadata and its three ODS copies."""
from speedrun_etl.workflow import make_dag

dag = make_dag("games_metadata", "runs")
