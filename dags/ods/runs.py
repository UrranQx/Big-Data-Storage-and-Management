"""Airflow DAG: one next-stage trigger routes to WR in v3 or players in v2."""
from speedrun_etl.workflow import make_dag

dag = make_dag("runs", "wr_history")
