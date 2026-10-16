FROM apache/airflow:3.3.2-python3.11@sha256:a6685a7a708ed6fa910c2c05458d7f2b0db9cca5c3bf58a72ded389767ad3e65
COPY --chown=airflow:root infra/airflow-constraints-3.11.txt /opt/airflow/constraints.txt
COPY --chown=airflow:root pyproject.toml /opt/project/pyproject.toml
COPY --chown=airflow:root src /opt/project/src
RUN pip install --no-cache-dir "apache-airflow==3.3.2" "apache-airflow-providers-standard==1.19.0" /opt/project --constraint /opt/airflow/constraints.txt && pip check
COPY --chown=airflow:root sql /opt/project/sql
COPY --chown=airflow:root tests /opt/project/tests
COPY --chown=airflow:root infra/init.py /opt/project/init.py
COPY --chown=airflow:root infra/maintenance.py infra/scheduler.sh /opt/project/
COPY --chown=airflow:root dags/ods /opt/airflow/dags/ods
