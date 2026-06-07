"""Airflow DAG example for adaptive ETL parallelization with PySpark."""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path
from time import perf_counter

from airflow import DAG
from airflow.operators.python import PythonOperator

try:
    from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
except Exception:  # pragma: no cover - optional provider in local environments
    SparkSubmitOperator = None

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROJECT_SRC = PROJECT_ROOT / "project" / "src"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))
if str(PROJECT_SRC) not in sys.path:
    sys.path.append(str(PROJECT_SRC))

from adaptive_scheduler import AdaptiveScheduler, ResourceSnapshot, TaskProfile  # noqa: E402
from adaptive_scheduler.resource_monitor import collect_local_resource_snapshot  # noqa: E402
from adaptive_scheduler.storage import AdaptiveStorage  # noqa: E402
from adaptive_scheduler.time_prediction import update_prediction_with_smoothing  # noqa: E402


def _read_task_history() -> dict[str, list[float]]:
    """
    History is usually read from historical metrics storage.
    Here we provide deterministic fallback values for a standalone diploma example.
    """
    fallback_history = {
        "extract_raw": [24.2, 21.8, 22.5],
        "transform_features": [130.0, 142.0, 138.4],
        "load_results": [38.0, 36.3, 40.1],
        "q21_adaptive_etl_example": [114.5, 120.4, 112.1],
    }
    try:
        storage = AdaptiveStorage.from_env()
        loaded_history: dict[str, list[float]] = {}
        for task_id, fallback in fallback_history.items():
            history = storage.read_task_time_history(task_id=task_id, limit_values=20)
            loaded_history[task_id] = history if history else fallback
        return loaded_history
    except Exception:
        return fallback_history


def load_task_profiles(**context) -> None:
    """Prepare task profiles from data-source metadata."""
    dag_run = context.get("dag_run")
    dataset_size = (dag_run.conf or {}).get("dataset_size", "medium") if dag_run else "medium"

    size_factor = {"small": 0.5, "medium": 1.0, "large": 1.7}.get(str(dataset_size).lower(), 1.0)
    task_profiles = [
        TaskProfile(
            task_id="extract_raw",
            data_volume=300.0 * size_factor,
            transformation_complexity=120.0,
            sequential_time_sec=30.0 * size_factor,
            parallelizable_fraction=0.35,
            stage="extract",
        ),
        TaskProfile(
            task_id="transform_features",
            data_volume=600.0 * size_factor,
            transformation_complexity=700.0,
            sequential_time_sec=180.0 * size_factor,
            parallelizable_fraction=0.85,
            stage="transform",
        ),
        TaskProfile(
            task_id="load_results",
            data_volume=250.0 * size_factor,
            transformation_complexity=150.0,
            sequential_time_sec=45.0 * size_factor,
            parallelizable_fraction=0.5,
            stage="load",
        ),
        TaskProfile(
            task_id="q21_adaptive_etl_example",
            data_volume=700.0 * size_factor,
            transformation_complexity=900.0,
            sequential_time_sec=220.0 * size_factor,
            parallelizable_fraction=0.9,
            stage="transform",
        ),
    ]
    context["ti"].xcom_push(key="dataset_size", value=dataset_size)
    context["ti"].xcom_push(key="task_profiles", value=[profile.__dict__ for profile in task_profiles])


def compute_adaptive_plan(**context) -> None:
    """Calculate task complexity and adaptive parallelism plan."""
    ti = context["ti"]
    task_profiles_raw = ti.xcom_pull(task_ids="load_task_profiles", key="task_profiles") or []
    task_profiles = [TaskProfile(**item) for item in task_profiles_raw]
    history = _read_task_history()

    snapshot = collect_local_resource_snapshot()
    resources = {
        profile.task_id: ResourceSnapshot(
            cpu_used=snapshot.cpu_used,
            cpu_total=snapshot.cpu_total,
            ram_used=snapshot.ram_used,
            ram_total=snapshot.ram_total,
        )
        for profile in task_profiles
    }

    task_graph = {
        "extract_raw": ["transform_features"],
        "transform_features": ["load_results", "q21_adaptive_etl_example"],
        "q21_adaptive_etl_example": ["load_results"],
        "load_results": [],
    }

    scheduler = AdaptiveScheduler(alpha=0.4, beta=0.3, gamma=0.3, p_max=8)
    summary = scheduler.build_schedule(
        task_profiles=task_profiles,
        task_history=history,
        task_resources=resources,
        task_graph=task_graph,
    )

    decisions = {item.task_id: item for item in summary.decisions}
    adaptive_task = decisions["q21_adaptive_etl_example"]
    recommended_parallelism = adaptive_task.adapted_parallelism

    ti.xcom_push(key="spark_executor_instances", value=recommended_parallelism)
    ti.xcom_push(key="spark_shuffle_partitions", value=max(recommended_parallelism * 4, 4))
    ti.xcom_push(
        key="scheduler_summary",
        value={
            "avg_load": summary.avg_load,
            "max_load": summary.max_load,
            "balance_coefficient": summary.balance_coefficient,
            "critical_path_time_sec": summary.dag_critical_path_sec,
            "etl_time_sec": summary.etl_time_sec,
            "parallel_time_sec": summary.parallel_time_sec,
            "sequential_time_sec": summary.sequential_time_sec,
        },
    )
    ti.xcom_push(
        key="q21_decision",
        value={
            "task_id": adaptive_task.task_id,
            "predicted_time_sec": adaptive_task.predicted_time_sec,
            "task_load": adaptive_task.task_load,
            "avg_task_load": adaptive_task.avg_task_load,
            "recommended_parallelism": adaptive_task.recommended_parallelism,
            "adapted_parallelism": adaptive_task.adapted_parallelism,
            "cpu_utilization": adaptive_task.cpu_utilization,
            "ram_utilization": adaptive_task.ram_utilization,
            "speedup": adaptive_task.speedup,
            "efficiency": adaptive_task.efficiency,
            "amdahl_speedup": adaptive_task.amdahl_speedup,
        },
    )


def _raise_missing_spark_provider() -> None:
    raise RuntimeError(
        "airflow.providers.apache.spark is not installed. "
        "Install Spark provider to run SparkSubmitOperator tasks."
    )


def collect_metrics(**context) -> None:
    """Collect run metrics and prepare records for storage."""
    ti = context["ti"]
    dag_run = context.get("dag_run")
    run_id = dag_run.run_id if dag_run else f"manual_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
    dataset_size = ti.xcom_pull(task_ids="load_task_profiles", key="dataset_size") or "medium"
    decision = ti.xcom_pull(task_ids="compute_adaptive_plan", key="q21_decision") or {}
    summary = ti.xcom_pull(task_ids="compute_adaptive_plan", key="scheduler_summary") or {}

    task_start = perf_counter()
    # This task records orchestration-level metrics; Spark task-level metrics are logged by the PySpark job.
    orchestration_time = round(perf_counter() - task_start, 6)
    total_parallel_time = float(summary.get("parallel_time_sec", 0.0))
    total_sequential_time = float(summary.get("sequential_time_sec", 0.0))
    speedup_run = (total_sequential_time / total_parallel_time) if total_parallel_time > 0 else 0.0
    p_for_run = int(decision.get("adapted_parallelism", 1) or 1)
    efficiency_run = speedup_run / p_for_run if p_for_run > 0 else 0.0

    metrics_payload = {
        "task_id": "q21_adaptive_etl_example",
        "dag_run_id": run_id,
        "execution_date": context.get("ts"),
        "dataset_size": dataset_size,
        "parallel_tasks_count": p_for_run,
        "task_load": float(decision.get("task_load", 0.0)),
        "avg_task_load": float(decision.get("avg_task_load", 0.0)),
        "cpu_utilization": float(decision.get("cpu_utilization", 0.0)),
        "ram_utilization": float(decision.get("ram_utilization", 0.0)),
        "extract_time_sec": total_sequential_time * 0.2,
        "transform_time_sec": total_sequential_time * 0.6,
        "load_time_sec": total_sequential_time * 0.2,
        "total_execution_time_sec": total_parallel_time + orchestration_time,
        "speedup": float(decision.get("speedup", speedup_run)),
        "efficiency": float(decision.get("efficiency", efficiency_run)),
        "amdahl_speedup": float(decision.get("amdahl_speedup", 0.0)),
        "critical_path_time_sec": float(summary.get("critical_path_time_sec", 0.0)),
        "etl_time_sec": float(summary.get("etl_time_sec", 0.0)),
        "load_balance_coeff": float(summary.get("balance_coefficient", 0.0)),
        "status": "success",
    }
    ti.xcom_push(key="metrics_payload", value=metrics_payload)


def update_time_prediction(**context) -> None:
    """Update T_pred after receiving actual execution metrics."""
    ti = context["ti"]
    decision = ti.xcom_pull(task_ids="compute_adaptive_plan", key="q21_decision") or {}
    metrics = ti.xcom_pull(task_ids="collect_metrics", key="metrics_payload") or {}

    predicted_old = float(decision.get("predicted_time_sec", 0.0))
    actual_time = float(metrics.get("total_execution_time_sec", predicted_old))
    predicted_new = update_prediction_with_smoothing(
        actual_time_sec=actual_time,
        predicted_old_sec=predicted_old,
        smoothing_lambda=0.3,
    )
    ti.xcom_push(key="predicted_time_new_sec", value=predicted_new)


def persist_metrics_to_s3(**context) -> None:
    """Persist execution log and metrics for next scheduler iterations."""
    ti = context["ti"]
    metrics_payload = ti.xcom_pull(task_ids="collect_metrics", key="metrics_payload") or {}
    predicted_new = ti.xcom_pull(task_ids="update_time_prediction", key="predicted_time_new_sec") or 0.0
    metrics_payload["predicted_time_new_sec"] = predicted_new

    storage = AdaptiveStorage.from_env()
    metrics_path = storage.save_metrics_record(metrics_payload)
    log_path = storage.save_execution_log(
        run_id=metrics_payload.get("dag_run_id", "manual_run"),
        payload=metrics_payload,
    )
    ti.xcom_push(key="metrics_s3_path", value=metrics_path)
    ti.xcom_push(key="log_s3_path", value=log_path)


default_args = {
    "owner": "adaptive-etl",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="adaptive_pyspark_etl_dag",
    default_args=default_args,
    description="Adaptive parallel ETL scheduling with Airflow and PySpark",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["adaptive", "etl", "pyspark", "diploma"],
) as dag:
    load_task_profiles_op = PythonOperator(
        task_id="load_task_profiles",
        python_callable=load_task_profiles,
    )

    compute_adaptive_plan_op = PythonOperator(
        task_id="compute_adaptive_plan",
        python_callable=compute_adaptive_plan,
    )

    if SparkSubmitOperator is not None:
        run_pyspark_etl_task = SparkSubmitOperator(
            task_id="run_q21_adaptive_etl_task",
            conn_id="spark_default",
            application=str(PROJECT_SRC / "pyspark_dag_queries" / "q21_adaptive_etl_example.py"),
            conf={
                "spark.executor.instances": "{{ ti.xcom_pull(task_ids='compute_adaptive_plan', key='spark_executor_instances') }}",
                "spark.sql.shuffle.partitions": "{{ ti.xcom_pull(task_ids='compute_adaptive_plan', key='spark_shuffle_partitions') }}",
            },
            application_args=[
                "--task-id",
                "q21_adaptive_etl_example",
                "--dataset-size",
                "{{ ti.xcom_pull(task_ids='load_task_profiles', key='dataset_size') }}",
                "--executors",
                "{{ ti.xcom_pull(task_ids='compute_adaptive_plan', key='spark_executor_instances') }}",
                "--shuffle-partitions",
                "{{ ti.xcom_pull(task_ids='compute_adaptive_plan', key='spark_shuffle_partitions') }}",
                "--pred-time",
                "{{ ti.xcom_pull(task_ids='compute_adaptive_plan', key='q21_decision')['predicted_time_sec'] }}",
            ],
            verbose=True,
        )
    else:
        run_pyspark_etl_task = PythonOperator(
            task_id="run_q21_adaptive_etl_task",
            python_callable=_raise_missing_spark_provider,
        )

    collect_metrics_op = PythonOperator(
        task_id="collect_metrics",
        python_callable=collect_metrics,
    )

    update_time_prediction_op = PythonOperator(
        task_id="update_time_prediction",
        python_callable=update_time_prediction,
    )

    persist_metrics_op = PythonOperator(
        task_id="persist_metrics_to_s3",
        python_callable=persist_metrics_to_s3,
    )

    (
        load_task_profiles_op
        >> compute_adaptive_plan_op
        >> run_pyspark_etl_task
        >> collect_metrics_op
        >> update_time_prediction_op
        >> persist_metrics_op
    )
