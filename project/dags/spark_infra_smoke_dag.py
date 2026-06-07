"""Airflow DAG: quick Spark smoke test in current infra."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from airflow_monitoring import (
    dagrun_failure_metrics,
    dagrun_success_metrics,
    task_failure_alert,
    task_retry_alert,
)

try:
    from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
except Exception:  # pragma: no cover - optional provider in local environments
    SparkSubmitOperator = None

SPARK_APP = "/opt/airflow/dags/spark_infra_smoke_job.py"

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": task_failure_alert,
    "on_retry_callback": task_retry_alert,
}


def _raise_missing_spark_provider() -> None:
    raise RuntimeError(
        "airflow.providers.apache.spark is not installed. "
        "Install Spark provider to run SparkSubmitOperator tasks."
    )


with DAG(
    dag_id="spark_infra_smoke_check",
    description="Simple DAG that verifies Spark is operational in current infrastructure",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["spark", "smoke", "infra"],
    on_success_callback=dagrun_success_metrics,
    on_failure_callback=dagrun_failure_metrics,
) as dag:
    start = EmptyOperator(task_id="start")
    finish = EmptyOperator(task_id="finish")

    if SparkSubmitOperator is not None:
        spark_smoke = SparkSubmitOperator(
            task_id="run_spark_smoke_job",
            conn_id="spark_default",
            application=SPARK_APP,
            conf={
                "spark.executor.instances": "1",
                "spark.sql.shuffle.partitions": "4",
            },
            application_args=[
                "--task-id",
                "spark_infra_smoke_job",
                "--dataset-size",
                "small",
                "--executors",
                "1",
                "--shuffle-partitions",
                "4",
                "--dag-run-id",
                "{{ run_id }}",
            ],
            verbose=True,
        )
    else:
        spark_smoke = PythonOperator(
            task_id="run_spark_smoke_job",
            python_callable=_raise_missing_spark_provider,
        )

    start >> spark_smoke >> finish
