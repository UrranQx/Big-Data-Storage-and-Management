"""Airflow dag истории WR: исходные события, coverage категорий и ссылки участников."""

from speedrun_etl.workflow import make_dag

dag = make_dag("wr_history", "players")
