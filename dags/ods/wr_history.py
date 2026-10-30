"""Airflow DAG: source WR events, category coverage and participant references."""
from speedrun_etl.workflow import make_dag

dag = make_dag('wr_history', 'players')
