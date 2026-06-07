"""Production Airflow DAG orchestrating PySpark jobs q01..q20.

Execution flow
--------------
start
  └─► load_task_profiles          # resolves dataset_size from env
        └─► compute_adaptive_plan  # AdaptiveScheduler → per-task decisions pushed to XCom
              ├─► q01 ─┐
              ├─► q02  │
              ├─► q03  │  (up to 4 tasks in parallel via max_active_tasks)
              ├─► q04  │
              ├─► q05  │
              ├─► q06 ─┼──► q16 ──► q17 ─┐
              ├─► q07  │          └► q20 ─┤
              ├─► q08  │                  │
              ├─► q09  │                  │
              ├─► q10  │                  ▼
              ├─► q11  ┼──────────────► q18 ──► q19 ──► finish
              ├─► q12  │
              ├─► q13  │
              ├─► q14  │
              └─► q15 ─┘

Each SparkSubmitOperator receives runtime parameters via Airflow XCom:
  --task-id, --dataset-size, --executors, --shuffle-partitions,
  --pred-time, --dag-run-id, --cpu-utilization, --ram-utilization

Data quality gates (fail-fast):
  - dq_after_audience_stage
  - dq_after_mailing_stage
  - dq_after_metrics_stage
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from airflow_monitoring import (
    dagrun_failure_metrics,
    dagrun_success_metrics,
    task_failure_alert,
    task_retry_alert,
)

SPARK_CONN_ID = "spark_default"
PYSPARK_JOBS_ROOT = "/opt/airflow/project/src/pyspark_dag_queries"
ADAPTIVE_SCHEDULER_SRC = "/opt/airflow/project/src"
AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")
# Airflow task slots per DAG run. The scheduler can update this variable between runs.
DAG_MAX_ACTIVE_TASKS_VAR = "ADAPTIVE_DAG_MAX_ACTIVE_TASKS"
DEFAULT_DAG_MAX_ACTIVE_TASKS = max(int(os.getenv("DAG_MAX_ACTIVE_TASKS", "4")), 1)
TASK_PRIORITY_RANKS_VAR = "ADAPTIVE_TASK_PRIORITY_RANKS"
SCHEDULER_MODE_VAR = "ADAPTIVE_SCHEDULER_MODE"
GLUE_METASTORE_FACTORY = (
    "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
)
AWS_CREDENTIALS_PROVIDER = "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
GLUE_CLIENT_JAR_NAMES = (
    "aws-glue-datacatalog-spark-client.jar",
    "aws-glue-datacatalog-hive3-client.jar",
)
PATCHED_HIVE_JAR_NAMES = (
    "hive-exec-2.3.9-core.jar",
    "hive-common-2.3.9.jar",
)


def _resolve_java_truststore_path() -> str | None:
    candidates: list[str] = []
    env_path = os.getenv("JAVA_SSL_TRUSTSTORE", "")
    if env_path:
        candidates.append(env_path)
    java_home = os.getenv("JAVA_HOME", "")
    if java_home:
        candidates.append(str(Path(java_home) / "lib/security/cacerts"))
    candidates.extend(
        [
            "/opt/spark-conf/cacerts",
            "/etc/pki/java/cacerts",
            "/usr/lib/jvm/java-17-amazon-corretto/lib/security/cacerts",
            "/usr/lib/jvm/java-17-amazon-corretto.x86_64/lib/security/cacerts",
        ]
    )
    for candidate in candidates:
        if candidate and Path(candidate).is_file():
            return candidate
    return None


def _spark_java_extra_options() -> str:
    opts = f"-Dhive.metastore.client.factory.class={GLUE_METASTORE_FACTORY}"
    truststore = _resolve_java_truststore_path()
    if truststore:
        opts += (
            f" -Djavax.net.ssl.trustStore={truststore}"
            " -Djavax.net.ssl.trustStorePassword=changeit"
            " -Djavax.net.ssl.trustStoreType=JKS"
        )
    return opts


def _resolve_glue_client_jar_path() -> str:
    """Pick the Glue metastore client JAR from env or known mount locations."""
    env_candidate = os.getenv("SPARK_GLUE_CLIENT_JAR", "")
    candidates: list[str] = []
    if env_candidate:
        candidates.append(env_candidate)
    for jar_name in GLUE_CLIENT_JAR_NAMES:
        candidates.append(f"/opt/spark-jars/{jar_name}")
        candidates.append(f"/tmp/{jar_name}")
    for candidate in candidates:
        if candidate and Path(candidate).is_file():
            return candidate
    return f"/opt/spark-jars/{GLUE_CLIENT_JAR_NAMES[0]}"


def _spark_support_jar_paths() -> list[str]:
    jars_dir = Path("/opt/spark-jars")
    if not jars_dir.is_dir():
        return []
    paths: list[str] = []
    for pattern in (
        "aws-glue-datacatalog-spark-client.jar",
        "hadoop-aws-*.jar",
        "aws-java-sdk-bundle-*.jar",
    ):
        for jar_path in sorted(jars_dir.glob(pattern)):
            jar_str = str(jar_path)
            if jar_path.is_file() and jar_str not in paths:
                paths.append(jar_str)
    return paths


def _glue_extra_classpath() -> str:
    paths = _spark_support_jar_paths()
    return ":".join(paths) if paths else _resolve_glue_client_jar_path()


GLUE_SUPPORT_JARS = _spark_support_jar_paths()
GLUE_CLIENT_JAR_PATH = _resolve_glue_client_jar_path()
GLUE_SPARK_CLASSPATH = _glue_extra_classpath()
GLUE_SPARK_JARS = ",".join(GLUE_SUPPORT_JARS) if GLUE_SUPPORT_JARS else GLUE_CLIENT_JAR_PATH


def _glue_catalog_conf() -> dict[str, str]:
    return {
        "spark.sql.catalogImplementation": "hive",
        "spark.sql.hive.metastore.sharedPrefixes": "com.amazonaws.glue,com.amazonaws",
        "spark.driver.userClassPathFirst": "false",
        "spark.executor.userClassPathFirst": "false",
        "spark.driver.extraJavaOptions": _spark_java_extra_options(),
        "spark.executor.extraJavaOptions": _spark_java_extra_options(),
        "spark.hive.imetastoreclient.factory.class": GLUE_METASTORE_FACTORY,
        "hive.metastore.client.factory.class": GLUE_METASTORE_FACTORY,
        "spark.hadoop.hive.metastore.client.factory.class": GLUE_METASTORE_FACTORY,
        "spark.hadoop.aws.region": AWS_REGION,
        "spark.hadoop.fs.s3a.endpoint.region": AWS_REGION,
        "spark.hadoop.fs.s3a.endpoint": f"s3.{AWS_REGION}.amazonaws.com",
        "spark.hadoop.fs.s3a.aws.credentials.provider": AWS_CREDENTIALS_PROVIDER,
        "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3.aws.credentials.provider": AWS_CREDENTIALS_PROVIDER,
        "spark.hadoop.fs.s3.endpoint": f"s3.{AWS_REGION}.amazonaws.com",
        "spark.hadoop.fs.s3.endpoint.region": AWS_REGION,
        "spark.sql.parquet.enableVectorizedReader": "false",
        "spark.jars": GLUE_SPARK_JARS,
        "spark.driver.extraClassPath": GLUE_SPARK_CLASSPATH,
        "spark.executor.extraClassPath": GLUE_SPARK_CLASSPATH,
    }

# ---------------------------------------------------------------------------
# Static task profiles: task_id → (data_volume, transform_complexity, seq_time_sec, parallel_frac)
# ---------------------------------------------------------------------------
_TASK_PROFILES_STATIC: dict[str, tuple[float, float, float, float]] = {
    "q01_credit_campaign_target_audience":     (3.0, 4.0, 120.0, 0.85),
    "q02_deposit_campaign_target_audience":    (3.0, 4.0, 110.0, 0.85),
    "q03_insurance_cross_sell_audience":       (2.5, 3.5, 100.0, 0.85),
    "q04_premium_upgrade_audience":            (2.5, 3.5,  90.0, 0.85),
    "q05_retention_inactive_clients":          (2.0, 3.0,  80.0, 0.80),
    "q06_client_profile_scoring":              (4.0, 5.0, 150.0, 0.85),
    "q07_client_best_channel":                 (3.0, 4.0, 100.0, 0.80),
    "q08_campaign_performance_dashboard":      (3.5, 4.5, 130.0, 0.85),
    "q09_product_conversion_analysis":         (2.5, 3.0,  80.0, 0.80),
    "q10_delivery_failure_analysis":           (2.0, 2.5,  60.0, 0.80),
    "q11_app_behavior_offer_recommendation":   (2.0, 2.5,  70.0, 0.80),
    "q12_transaction_behavior_segmentation":   (3.5, 4.0, 120.0, 0.85),
    "q13_high_value_clients_top1000":          (3.5, 4.5, 130.0, 0.85),
    "q14_duplicate_client_offers":             (1.5, 2.0,  40.0, 0.75),
    "q15_expired_offer_cleanup_candidate":     (1.0, 1.5,  30.0, 0.75),
    "q16_mailing_base":                        (4.5, 5.0, 160.0, 0.85),
    "q17_mailing_schedule_optimization":       (2.0, 2.5,  50.0, 0.80),
    "q18_etl_metrics_aggregation":             (1.5, 2.0,  40.0, 0.75),
    "q19_adaptive_parallelism_recommendation": (1.0, 1.5,  30.0, 0.75),
    "q20_campaign_readiness_check":            (3.0, 4.0, 110.0, 0.85),
}

# DAG topology: task_id → downstream task_ids (used for critical-path calculation).
_TASK_GRAPH: dict[str, list[str]] = {
    "q01_credit_campaign_target_audience":     ["q18_etl_metrics_aggregation"],
    "q02_deposit_campaign_target_audience":    ["q18_etl_metrics_aggregation"],
    "q03_insurance_cross_sell_audience":       ["q18_etl_metrics_aggregation"],
    "q04_premium_upgrade_audience":            ["q18_etl_metrics_aggregation"],
    "q05_retention_inactive_clients":          ["q18_etl_metrics_aggregation"],
    "q06_client_profile_scoring":              ["q16_mailing_base", "q18_etl_metrics_aggregation"],
    "q07_client_best_channel":                 ["q18_etl_metrics_aggregation"],
    "q08_campaign_performance_dashboard":      ["q18_etl_metrics_aggregation"],
    "q09_product_conversion_analysis":         ["q18_etl_metrics_aggregation"],
    "q10_delivery_failure_analysis":           ["q18_etl_metrics_aggregation"],
    "q11_app_behavior_offer_recommendation":   ["q18_etl_metrics_aggregation"],
    "q12_transaction_behavior_segmentation":   ["q18_etl_metrics_aggregation"],
    "q13_high_value_clients_top1000":          ["q18_etl_metrics_aggregation"],
    "q14_duplicate_client_offers":             ["q18_etl_metrics_aggregation"],
    "q15_expired_offer_cleanup_candidate":     ["q18_etl_metrics_aggregation"],
    "q16_mailing_base":                        ["q17_mailing_schedule_optimization", "q20_campaign_readiness_check"],
    "q17_mailing_schedule_optimization":       ["q18_etl_metrics_aggregation"],
    "q18_etl_metrics_aggregation":             ["q19_adaptive_parallelism_recommendation"],
    "q19_adaptive_parallelism_recommendation": [],
    "q20_campaign_readiness_check":            ["q18_etl_metrics_aggregation"],
}

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": task_failure_alert,
    "on_retry_callback": task_retry_alert,
}


# ---------------------------------------------------------------------------
# PythonOperator callables
# ---------------------------------------------------------------------------

def load_task_profiles_fn(**context: Any) -> None:
    """Resolve dataset_size from environment and push to XCom."""
    dataset_size = os.getenv("DATASET_SIZE", "medium")
    context["ti"].xcom_push(key="dataset_size", value=dataset_size)


def resolve_graph_mode(**context: Any) -> str:
    """Resolve scheduler mode from dag_run.conf, Variable, ENV, then fallback.

    Preferred modes:
      - adaptive: current adaptive strategy
      - sequential: force single executor for each task
      - parallel: force max executors for each task

    Legacy modes are still accepted for backward compatibility:
      - recommended / conservative / aggressive
    """
    dag_run = context.get("dag_run")
    conf_mode = None
    if dag_run and getattr(dag_run, "conf", None):
        conf_mode = (
            dag_run.conf.get("scheduler_mode")
            or dag_run.conf.get("planning_mode")
            or dag_run.conf.get("graph_mode")
        )

    variable_mode = Variable.get(SCHEDULER_MODE_VAR, default_var=None) or Variable.get(
        "ADAPTIVE_GRAPH_MODE", default_var=None
    )
    env_mode = os.getenv("ADAPTIVE_SCHEDULER_MODE") or os.getenv("ADAPTIVE_GRAPH_MODE")

    mode_raw = str(conf_mode or variable_mode or env_mode or "adaptive").strip().lower()
    allowed_modes = {
        "adaptive",
        "sequential",
        "parallel",
        "recommended",
        "conservative",
        "aggressive",
    }
    if mode_raw not in allowed_modes:
        return "adaptive"
    return mode_raw


def resolve_dag_max_active_tasks() -> int:
    """Resolve DAG-level task concurrency from Variable, then ENV fallback."""
    raw_value = Variable.get(DAG_MAX_ACTIVE_TASKS_VAR, default_var=str(DEFAULT_DAG_MAX_ACTIVE_TASKS))
    try:
        parsed = int(str(raw_value).strip())
    except ValueError:
        parsed = DEFAULT_DAG_MAX_ACTIVE_TASKS
    return max(1, min(8, parsed))


def priority_weight_for_task(task_id: str) -> int:
    """Resolve Airflow priority weight from stored adaptive priority ranks.

    Larger weight means higher execution priority. If variable is missing or malformed,
    keep neutral priority to avoid DAG import failures.
    """
    raw = Variable.get(TASK_PRIORITY_RANKS_VAR, default_var="{}")
    try:
        ranks = json.loads(raw) if raw else {}
    except json.JSONDecodeError:
        ranks = {}

    if not isinstance(ranks, dict):
        return 1
    if task_id not in ranks:
        return 1

    valid_ranks = [int(v) for v in ranks.values() if isinstance(v, int) or str(v).isdigit()]
    if not valid_ranks:
        return 1

    max_rank = max(valid_ranks)
    current_rank = ranks.get(task_id, max_rank)
    try:
        current_rank = int(current_rank)
    except (TypeError, ValueError):
        return 1

    # Convert rank (1 is best) to descending weight (bigger is better for Airflow).
    return max(1, max_rank - current_rank + 1)


def choose_executors(mode: str, recommended: int, adapted: int, p_max: int) -> int:
    """Select final executor count based on selected scheduling mode."""
    if mode == "sequential":
        return 1
    if mode == "parallel":
        return max(1, p_max)
    if mode == "recommended":
        return max(1, min(p_max, recommended))
    if mode == "conservative":
        return max(1, min(p_max, adapted - 1))
    if mode == "aggressive":
        return max(1, min(p_max, adapted + 1))
    return max(1, min(p_max, adapted))


def choose_task_concurrency(mode: str, adaptive_recommendation: int, p_max: int) -> int:
    """Select DAG-level task concurrency recommendation for next runs."""
    if mode == "sequential":
        return 1
    if mode == "parallel":
        return max(1, min(1, p_max))
    return max(1, min(1, adaptive_recommendation))


def build_run_level_report(
    dag_run_id: str,
    dataset_size: str,
    graph_mode: str,
    summary: Any,
    selected_task_concurrency: int,
    selected_executors: dict[str, int],
    resource_snapshot_source: str,
    resource_snapshot: dict[str, float],
) -> dict[str, Any]:
    """Assemble run-level scheduler report with requested analytical metrics."""
    decisions = summary.decisions or []
    executor_values = list(selected_executors.values())
    run_parallelism = (sum(executor_values) / len(executor_values)) if executor_values else 1.0

    sequential_time = float(summary.sequential_time_sec or 0.0)
    parallel_time = float(summary.parallel_time_sec or 0.0)
    speedup_value = float(summary.run_speedup or ((sequential_time / parallel_time) if parallel_time > 0 else 0.0))
    efficiency_value = float(summary.run_efficiency or ((speedup_value / run_parallelism) if run_parallelism > 0 else 0.0))
    avg_amdahl = float(summary.run_amdahl_speedup or 0.0)

    return {
        "run_id": dag_run_id,
        "dataset_size": dataset_size,
        "graph_mode": graph_mode,
        "recommended_task_concurrency": int(selected_task_concurrency),
        "max_execution_wave_width": int(summary.max_execution_wave_width),
        "metrics": {
            "L_i": {d.task_id: round(float(d.task_load), 6) for d in decisions},
            "L_avg": round(float(summary.avg_load), 6),
            "B": round(float(summary.balance_coefficient), 6),
            "S": round(float(speedup_value), 6),
            "E": round(float(efficiency_value), 6),
            "S_Amdahl": round(float(avg_amdahl), 6),
            "T_DAG": round(float(summary.dag_critical_path_sec), 6),
            "T_ETL": round(float(summary.etl_time_sec), 6),
        },
        "execution_plan": {
            "order": list(summary.execution_order or []),
            "waves": list(summary.execution_waves or []),
        },
        "selected_parallelism": selected_executors,
        "resource_signal": {
            "source": resource_snapshot_source,
            "snapshot": resource_snapshot,
        },
        "task_metrics": [
            {
                "task_id": d.task_id,
                "L_i": round(float(d.task_load), 6),
                "recommended_parallelism": int(d.recommended_parallelism),
                "adapted_parallelism": int(d.adapted_parallelism),
                "selected_parallelism": int(selected_executors.get(d.task_id, d.adapted_parallelism)),
                "S": round(float(d.speedup), 6),
                "E": round(float(d.efficiency), 6),
                "S_Amdahl": round(float(d.amdahl_speedup), 6),
                "predicted_time_sec": round(float(d.predicted_time_sec), 6),
                "cpu_utilization": round(float(d.cpu_utilization), 6),
                "ram_utilization": round(float(d.ram_utilization), 6),
                "priority_score": round(float(d.priority_score), 6),
                "priority_rank": int(d.priority_rank),
                "execution_wave": int(d.execution_wave),
            }
            for d in decisions
        ],
    }


def compute_adaptive_plan_fn(**context: Any) -> None:
    """Run AdaptiveScheduler for all tasks and push per-task decisions to XCom.

    Each decision is stored under key ``{task_id}_decision`` and contains:
      executors, shuffle_partitions, predicted_time_sec,
      cpu_utilization, ram_utilization.
    """
    if ADAPTIVE_SCHEDULER_SRC not in sys.path:
        sys.path.insert(0, ADAPTIVE_SCHEDULER_SRC)

    from adaptive_scheduler import AdaptiveScheduler, ResourceSnapshot, TaskProfile  # noqa: PLC0415
    from adaptive_scheduler.resource_monitor import (  # noqa: PLC0415
        collect_preferred_resource_snapshot,
        snapshot_to_dict,
    )
    from adaptive_scheduler.storage import AdaptiveStorage  # noqa: PLC0415

    ti = context["ti"]
    dag_run_id: str = context["run_id"]
    dataset_size: str = ti.xcom_pull(task_ids="load_task_profiles", key="dataset_size") or "medium"
    graph_mode = resolve_graph_mode(**context)

    task_profiles = [
        TaskProfile(
            task_id=tid,
            data_volume=vals[0],
            transformation_complexity=vals[1],
            sequential_time_sec=vals[2],
            parallelizable_fraction=vals[3],
        )
        for tid, vals in _TASK_PROFILES_STATIC.items()
    ]

    storage = AdaptiveStorage.from_env()
    task_history = {p.task_id: storage.read_task_time_history(p.task_id) for p in task_profiles}
    host_snapshot, resource_snapshot_source = collect_preferred_resource_snapshot()
    host_snapshot_dict = snapshot_to_dict(host_snapshot)
    task_resources: dict[str, ResourceSnapshot] = {p.task_id: host_snapshot for p in task_profiles}

    p_max = 6
    scheduler = AdaptiveScheduler(p_max=p_max)
    summary = scheduler.build_schedule(
        task_profiles=task_profiles,
        task_history=task_history,
        task_resources=task_resources,
        task_graph=_TASK_GRAPH,
    )

    selected_executors: dict[str, int] = {}
    for decision in summary.decisions:
        executors = choose_executors(
            mode=graph_mode,
            recommended=decision.recommended_parallelism,
            adapted=decision.adapted_parallelism,
            p_max=p_max,
        )
        selected_executors[decision.task_id] = executors
        shuffle_partitions = max(executors * 4, 4)
        ti.xcom_push(
            key=f"{decision.task_id}_decision",
            value={
                "executors": executors,
                "shuffle_partitions": shuffle_partitions,
                "predicted_time_sec": round(decision.predicted_time_sec, 3),
                "cpu_utilization": round(decision.cpu_utilization, 4),
                "ram_utilization": round(decision.ram_utilization, 4),
                "task_load": round(decision.task_load, 6),
                "avg_task_load": round(summary.avg_load, 6),
                "balance_coefficient": round(summary.balance_coefficient, 6),
                "speedup": round(decision.speedup, 6),
                "efficiency": round(decision.efficiency, 6),
                "amdahl_speedup": round(decision.amdahl_speedup, 6),
                "priority_score": round(decision.priority_score, 6),
                "priority_rank": int(decision.priority_rank),
                "execution_wave": int(decision.execution_wave),
                "critical_path_time_sec": round(summary.dag_critical_path_sec, 6),
                "etl_time_sec": round(summary.etl_time_sec, 6),
            },
        )

    selected_task_concurrency = choose_task_concurrency(
        mode=graph_mode,
        adaptive_recommendation=int(summary.recommended_task_concurrency),
        p_max=p_max,
    )

    run_level_report = build_run_level_report(
        dag_run_id=dag_run_id,
        dataset_size=dataset_size,
        graph_mode=graph_mode,
        summary=summary,
        selected_task_concurrency=selected_task_concurrency,
        selected_executors=selected_executors,
        resource_snapshot_source=resource_snapshot_source,
        resource_snapshot=host_snapshot_dict,
    )
    run_level_metrics_record = {
        "run_id": run_level_report["run_id"],
        "dataset_size": run_level_report["dataset_size"],
        "graph_mode": run_level_report["graph_mode"],
        "recommended_task_concurrency": run_level_report["recommended_task_concurrency"],
        "max_execution_wave_width": run_level_report["max_execution_wave_width"],
        "resource_signal_source": run_level_report["resource_signal"]["source"],
        "L_avg": run_level_report["metrics"]["L_avg"],
        "B": run_level_report["metrics"]["B"],
        "S": run_level_report["metrics"]["S"],
        "E": run_level_report["metrics"]["E"],
        "S_Amdahl": run_level_report["metrics"]["S_Amdahl"],
        "T_DAG": run_level_report["metrics"]["T_DAG"],
        "T_ETL": run_level_report["metrics"]["T_ETL"],
        "execution_order": run_level_report["execution_plan"]["order"],
    }
    run_level_report_uri = storage.save_execution_log(run_id=dag_run_id, payload=run_level_report)
    run_level_metrics_uri = storage.save_run_metrics_record(run_level_metrics_record)

    ti.xcom_push(key="dag_run_id", value=dag_run_id)
    ti.xcom_push(key="dataset_size", value=dataset_size)
    ti.xcom_push(key="scheduler_mode", value=graph_mode)
    ti.xcom_push(key="graph_mode", value=graph_mode)
    ti.xcom_push(key="recommended_task_concurrency", value=selected_task_concurrency)
    # Apply recommendation for next DAG runs (parse-time concurrency parameter).
    Variable.set(DAG_MAX_ACTIVE_TASKS_VAR, str(selected_task_concurrency))
    Variable.set(
        TASK_PRIORITY_RANKS_VAR,
        json.dumps({d.task_id: int(d.priority_rank) for d in summary.decisions}, ensure_ascii=True),
    )
    ti.xcom_push(key="run_level_report", value=run_level_report)
    ti.xcom_push(key="run_level_report_uri", value=run_level_report_uri)
    ti.xcom_push(key="run_level_metrics_uri", value=run_level_metrics_uri)


# ---------------------------------------------------------------------------
# Helper: build SparkSubmitOperator with full runtime args from XCom
# ---------------------------------------------------------------------------

def _xcom(key: str, task_id: str = "compute_adaptive_plan") -> str:
    return "{{ ti.xcom_pull(task_ids='" + task_id + "', key='" + key + "') }}"


def spark_task(task_id: str, script_name: str) -> SparkSubmitOperator:
    """Create SparkSubmitOperator with adaptive runtime parameters from XCom."""
    decision_key = f"{task_id}_decision"
    d = f"ti.xcom_pull(task_ids='compute_adaptive_plan', key='{decision_key}')"

    return SparkSubmitOperator(
        task_id=task_id,
        application=f"{PYSPARK_JOBS_ROOT}/{script_name}",
        conn_id=SPARK_CONN_ID,
        verbose=False,
        priority_weight=priority_weight_for_task(task_id),
        weight_rule="absolute",
        conf={
            "spark.executor.instances":      "{{{{ {d}['executors'] }}}}".format(d=d),
            "spark.sql.shuffle.partitions":  "{{{{ {d}['shuffle_partitions'] }}}}".format(d=d),
            **_glue_catalog_conf(),
        },
        application_args=[
            "--task-id",           task_id,
            "--dataset-size",      _xcom("dataset_size", "load_task_profiles"),
            "--executors",         "{{{{ {d}['executors'] }}}}".format(d=d),
            "--shuffle-partitions","{{{{ {d}['shuffle_partitions'] }}}}".format(d=d),
            "--pred-time",         "{{{{ {d}['predicted_time_sec'] }}}}".format(d=d),
            "--dag-run-id",        "{{ run_id }}",
            "--cpu-utilization",   "{{{{ {d}['cpu_utilization'] }}}}".format(d=d),
            "--ram-utilization",   "{{{{ {d}['ram_utilization'] }}}}".format(d=d),
            "--task-load",         "{{{{ {d}['task_load'] }}}}".format(d=d),
            "--avg-task-load",     "{{{{ {d}['avg_task_load'] }}}}".format(d=d),
            "--balance-coeff",     "{{{{ {d}['balance_coefficient'] }}}}".format(d=d),
            "--speedup",           "{{{{ {d}['speedup'] }}}}".format(d=d),
            "--efficiency",        "{{{{ {d}['efficiency'] }}}}".format(d=d),
            "--amdahl-speedup",    "{{{{ {d}['amdahl_speedup'] }}}}".format(d=d),
            "--priority-score",    "{{{{ {d}['priority_score'] }}}}".format(d=d),
            "--priority-rank",     "{{{{ {d}['priority_rank'] }}}}".format(d=d),
            "--execution-wave",    "{{{{ {d}['execution_wave'] }}}}".format(d=d),
            "--critical-path-time-sec", "{{{{ {d}['critical_path_time_sec'] }}}}".format(d=d),
            "--etl-time-sec",      "{{{{ {d}['etl_time_sec'] }}}}".format(d=d),
        ],
    )


def quality_task(task_id: str, check_group: str) -> SparkSubmitOperator:
    """Create fail-fast Spark quality check task for one DAG stage."""
    d = "ti.xcom_pull(task_ids='compute_adaptive_plan', key='q18_etl_metrics_aggregation_decision')"
    return SparkSubmitOperator(
        task_id=task_id,
        application=f"{PYSPARK_JOBS_ROOT}/data_quality_checks.py",
        conn_id=SPARK_CONN_ID,
        verbose=False,
        conf={
            "spark.executor.instances": "{{{{ {d}['executors'] }}}}".format(d=d),
            "spark.sql.shuffle.partitions": "{{{{ {d}['shuffle_partitions'] }}}}".format(d=d),
            **_glue_catalog_conf(),
        },
        application_args=[
            "--check-group", check_group,
            "--task-id", task_id,
            "--dataset-size", _xcom("dataset_size", "load_task_profiles"),
            "--executors", "{{{{ {d}['executors'] }}}}".format(d=d),
            "--shuffle-partitions", "{{{{ {d}['shuffle_partitions'] }}}}".format(d=d),
            "--pred-time", "{{{{ {d}['predicted_time_sec'] }}}}".format(d=d),
            "--dag-run-id", "{{ run_id }}",
            "--cpu-utilization", "{{{{ {d}['cpu_utilization'] }}}}".format(d=d),
            "--ram-utilization", "{{{{ {d}['ram_utilization'] }}}}".format(d=d),
            "--task-load", "{{{{ {d}['task_load'] }}}}".format(d=d),
            "--avg-task-load", "{{{{ {d}['avg_task_load'] }}}}".format(d=d),
            "--balance-coeff", "{{{{ {d}['balance_coefficient'] }}}}".format(d=d),
            "--speedup", "{{{{ {d}['speedup'] }}}}".format(d=d),
            "--efficiency", "{{{{ {d}['efficiency'] }}}}".format(d=d),
            "--amdahl-speedup", "{{{{ {d}['amdahl_speedup'] }}}}".format(d=d),
            "--priority-score", "{{{{ {d}['priority_score'] }}}}".format(d=d),
            "--priority-rank", "{{{{ {d}['priority_rank'] }}}}".format(d=d),
            "--execution-wave", "{{{{ {d}['execution_wave'] }}}}".format(d=d),
            "--critical-path-time-sec", "{{{{ {d}['critical_path_time_sec'] }}}}".format(d=d),
            "--etl-time-sec", "{{{{ {d}['etl_time_sec'] }}}}".format(d=d),
        ],
    )


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="adaptive_etl_bank_q01_q20_production",
    description="Production ETL DAG for q01..q20 with adaptive runtime parameters",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 2 * * *",
    catchup=False,
    max_active_runs=6,
    max_active_tasks=resolve_dag_max_active_tasks(),
    tags=["production", "adaptive-etl", "q01-q20"],
    on_success_callback=dagrun_success_metrics,
    on_failure_callback=dagrun_failure_metrics,
) as dag:

    start = EmptyOperator(task_id="start")
    finish = EmptyOperator(task_id="finish")

    load_profiles = PythonOperator(
        task_id="load_task_profiles",
        python_callable=load_task_profiles_fn,
    )

    compute_plan = PythonOperator(
        task_id="compute_adaptive_plan",
        python_callable=compute_adaptive_plan_fn,
    )

    q01 = spark_task("q01_credit_campaign_target_audience",     "q01_credit_campaign_target_audience.py")
    q02 = spark_task("q02_deposit_campaign_target_audience",    "q02_deposit_campaign_target_audience.py")
    q03 = spark_task("q03_insurance_cross_sell_audience",       "q03_insurance_cross_sell_audience.py")
    q04 = spark_task("q04_premium_upgrade_audience",            "q04_premium_upgrade_audience.py")
    q05 = spark_task("q05_retention_inactive_clients",          "q05_retention_inactive_clients.py")
    q06 = spark_task("q06_client_profile_scoring",              "q06_client_profile_scoring.py")
    q07 = spark_task("q07_client_best_channel",                 "q07_client_best_channel.py")
    q08 = spark_task("q08_campaign_performance_dashboard",      "q08_campaign_performance_dashboard.py")
    q09 = spark_task("q09_product_conversion_analysis",         "q09_product_conversion_analysis.py")
    q10 = spark_task("q10_delivery_failure_analysis",           "q10_delivery_failure_analysis.py")
    q11 = spark_task("q11_app_behavior_offer_recommendation",   "q11_app_behavior_offer_recommendation.py")
    q12 = spark_task("q12_transaction_behavior_segmentation",   "q12_transaction_behavior_segmentation.py")
    q13 = spark_task("q13_high_value_clients_top1000",          "q13_high_value_clients_top1000.py")
    q14 = spark_task("q14_duplicate_client_offers",             "q14_duplicate_client_offers.py")
    q15 = spark_task("q15_expired_offer_cleanup_candidate",     "q15_expired_offer_cleanup_candidate.py")
    q16 = spark_task("q16_mailing_base",                        "q16_mailing_base.py")
    q17 = spark_task("q17_mailing_schedule_optimization",       "q17_mailing_schedule_optimization.py")
    q18 = spark_task("q18_etl_metrics_aggregation",             "q18_etl_metrics_aggregation.py")
    q19 = spark_task("q19_adaptive_parallelism_recommendation", "q19_adaptive_parallelism_recommendation.py")
    q20 = spark_task("q20_campaign_readiness_check",            "q20_campaign_readiness_check.py")
    dq_after_audience = quality_task("dq_after_audience_stage", "post_audience")
    dq_after_mailing = quality_task("dq_after_mailing_stage", "post_mailing")
    dq_after_metrics = quality_task("dq_after_metrics_stage", "post_metrics")

    parallel_roots = [q01, q02, q03, q04, q05, q06, q07, q08, q09, q10, q11, q12, q13, q14, q15]

    # Planning phase runs before any Spark job.
    start >> load_profiles >> compute_plan >> parallel_roots
    parallel_roots >> dq_after_audience

    # q16 requires client scoring from q06.
    [q06, dq_after_audience] >> q16

    # Operational branch: mailing pipeline.
    q16 >> [q17, q20]
    [q17, q20] >> dq_after_mailing

    # Final aggregation waits for all productive tasks.
    [q01, q02, q03, q04, q05, q06, q07, q08, q09, q10,
     q11, q12, q13, q14, q15, q16, q17, q20, dq_after_audience, dq_after_mailing] >> q18

    q18 >> q19 >> dq_after_metrics >> finish
