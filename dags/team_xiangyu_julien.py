"""
Lab 4 - copy to dags/team_<yourname>.py and complete the capstone.

Mandatory:
  - >= 5 Airflow tasks in your dag
  - 3 Spark transforms in include/team_<yourname>_spark.py
  - Try to be creative with the tasks

Steps:
  1. Change dag_id below.
  2. Copy include/team_spark_TEMPLATE.py -> include/team_<yourname>_spark.py
  3. Define 5 tasks
  4. Wire spark task to YOUR run_daily() in include/team_<yourname>_spark.py
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.sensors.filesystem import FileSensor

from include.ingest import ingest_day, validate_silver
from lab4_student.include.team_xiangyu_julien_spark import run_daily


DEFAULT_ARGS = {
    "owner": "team_yan",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}


with DAG(
    dag_id="team_yan",
    description="Retail KPI pipeline",
    start_date=datetime(2026, 6, 1),
    end_date=datetime(2026, 6, 14),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["lab4", "capstone"],
) as dag:

    wait_csv = FileSensor(
        task_id="wait_csv",
        filepath="/opt/airflow/data/incoming/transactions_{{ ds }}.csv",
        poke_interval=10,
        timeout=300,
        mode="reschedule",
    )

    @task
    def ingest_task(ds=None):
        ingest_day(ds)

    @task
    def validate_task(ds=None):
        validate_silver(ds)

    @task
    def spark_task(ds=None):
        run_daily(ds)

    @task
    def report_task(ds=None):
        print(f"Dashboard report generated for {ds}")

    @task
    def notify_task(ds=None):
        print(f"Pipeline completed successfully for {ds}")

    wait_csv >> ingest_task() >> validate_task() >> spark_task() >> report_task() >> notify_task()


