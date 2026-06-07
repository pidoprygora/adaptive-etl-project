"""Airflow DAG для генерації, S3 завантаження та ETL обробки."""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path
from time import perf_counter

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

# Додаємо корінь проєкту в sys.path, щоб імпорти працювали з папки dags.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from etl_pipeline import (  # noqa: E402
    PROCESSED_FILES,
    RAW_DATASET_FILES,
    extract_data,
    load_data,
    save_metrics,
    transform_data,
    validate_data,
)

STAGE_DIR = PROJECT_ROOT / "output" / "_stage"


def _save_frames(frames: dict[str, pd.DataFrame], prefix: str) -> None:
    STAGE_DIR.mkdir(parents=True, exist_ok=True)
    for name, frame in frames.items():
        frame.to_csv(STAGE_DIR / f"{prefix}_{name}.csv", index=False)


def _load_frames(names: list[str], prefix: str) -> dict[str, pd.DataFrame]:
    loaded: dict[str, pd.DataFrame] = {}
    for name in names:
        stage_path = STAGE_DIR / f"{prefix}_{name}.csv"
        loaded[name] = pd.read_csv(stage_path)
    return loaded


def extract_data_task_callable(**context) -> None:
    start_time = perf_counter()
    datasets = extract_data()
    _save_frames(datasets, prefix="raw")
    elapsed = round(perf_counter() - start_time, 3)
    task_instance = context["ti"]
    task_instance.xcom_push(key="extract_time_sec", value=elapsed)
    task_instance.xcom_push(key="rows_clients", value=len(datasets["clients"]))
    task_instance.xcom_push(key="rows_offers", value=len(datasets["offers"]))
    task_instance.xcom_push(key="rows_client_offers", value=len(datasets["client_offers"]))
    task_instance.xcom_push(key="rows_events", value=len(datasets["mailing_events"]))


def transform_data_task_callable(**context) -> None:
    start_time = perf_counter()
    raw_datasets = _load_frames(list(RAW_DATASET_FILES.keys()), prefix="raw")
    transformed = transform_data(raw_datasets)
    _save_frames(transformed, prefix="processed")
    elapsed = round(perf_counter() - start_time, 3)
    context["ti"].xcom_push(key="transform_time_sec", value=elapsed)


def load_results_to_s3_task_callable(**context) -> None:
    start_time = perf_counter()
    transformed = _load_frames(list(PROCESSED_FILES.keys()), prefix="processed")
    load_data(transformed=transformed)
    elapsed = round(perf_counter() - start_time, 3)
    context["ti"].xcom_push(key="load_time_sec", value=elapsed)


def validate_results_task_callable(**context) -> None:
    validate_start_time = perf_counter()
    status = "success"
    try:
        validate_data()
    except Exception:
        status = "failed"
        raise
    finally:
        ti = context["ti"]
        dag_run = context.get("dag_run")
        extract_time = ti.xcom_pull(task_ids="extract_data", key="extract_time_sec") or 0.0
        transform_time = ti.xcom_pull(task_ids="transform_data", key="transform_time_sec") or 0.0
        load_time = ti.xcom_pull(task_ids="load_results_to_s3", key="load_time_sec") or 0.0
        validate_time = round(perf_counter() - validate_start_time, 3)
        metrics_record = {
            "dag_run_id": dag_run.run_id if dag_run else "manual_airflow",
            "execution_date": context.get("ts"),
            "rows_clients": ti.xcom_pull(task_ids="extract_data", key="rows_clients") or 0,
            "rows_offers": ti.xcom_pull(task_ids="extract_data", key="rows_offers") or 0,
            "rows_client_offers": ti.xcom_pull(task_ids="extract_data", key="rows_client_offers") or 0,
            "rows_events": ti.xcom_pull(task_ids="extract_data", key="rows_events") or 0,
            "extract_time_sec": extract_time,
            "transform_time_sec": transform_time,
            "load_time_sec": load_time,
            "total_execution_time_sec": round(extract_time + transform_time + load_time + validate_time, 3),
            "status": status,
        }
        save_metrics(metrics_record)


default_args = {
    "owner": "bank-etl",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bank_etl_dag",
    default_args=default_args,
    description="ETL-процес формування маркетингових банківських вибірок",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["bank", "etl", "s3", "adaptive", "diploma"],
) as dag:
    extract_data_operator = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data_task_callable,
        do_xcom_push=False,
    )

    transform_data_operator = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data_task_callable,
        do_xcom_push=False,
    )

    load_results_to_s3_operator = PythonOperator(
        task_id="load_results_to_s3",
        python_callable=load_results_to_s3_task_callable,
        do_xcom_push=False,
    )

    validate_results_operator = PythonOperator(
        task_id="validate_results",
        python_callable=validate_results_task_callable,
        do_xcom_push=False,
    )

    (
        extract_data_operator
        >> transform_data_operator
        >> load_results_to_s3_operator
        >> validate_results_operator
    )
