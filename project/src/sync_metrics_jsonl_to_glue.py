"""
Збирає per-task метрики з S3 JSONL (metrics/bank_data/etl_metrics_*.jsonl)
у Parquet для Glue-таблиці adaptive_etl_bank.etl_execution_metrics.

Після синхронізації запустіть q18 (etl_metrics_aggregation), щоб заповнити
другу таблицю агрегатів.

Usage:
  cd project/src
  python sync_metrics_jsonl_to_glue.py
  python sync_metrics_jsonl_to_glue.py --upload
  python sync_metrics_jsonl_to_glue.py --upload --dag-run-id 'manual__2026-06-04T20:24:10.385702+00:00'
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from adaptive_scheduler.storage import AdaptiveStorage
from config import PARQUET_DIR
from glue_parquet_types import coerce_dataframe_to_glue_schema, normalize_parquet_glue_types
from upload_to_s3 import normalize_parquet_timestamps, upload_table_parquet

TABLE_NAME = "etl_execution_metrics"
METRICS_FILE_PREFIX = "etl_metrics_"


def list_task_metrics_keys(storage: AdaptiveStorage, limit_files: int | None) -> list[str]:
    """List S3 keys for etl_metrics_*.jsonl under metrics/bank_data/."""
    prefix = storage._key(area="metrics", filename="")
    keys: list[str] = []
    paginator = storage.client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=storage.bucket, Prefix=prefix):
        for entry in page.get("Contents", []):
            key = entry["Key"]
            name = key.rsplit("/", 1)[-1]
            if name.startswith(METRICS_FILE_PREFIX) and name.endswith(".jsonl"):
                keys.append(key)
    keys.sort(key=lambda k: k.rsplit("/", 1)[-1])
    if limit_files is not None and limit_files > 0:
        keys = keys[-limit_files:]
    return keys


def load_task_metrics_records(
    storage: AdaptiveStorage,
    *,
    limit_files: int | None = None,
    dag_run_id: str | None = None,
) -> list[dict[str, Any]]:
    """Read all task-level metric rows from JSONL files in S3."""
    keys = list_task_metrics_keys(storage, limit_files)
    rows: list[dict[str, Any]] = []
    for key in keys:
        body = storage.client.get_object(Bucket=storage.bucket, Key=key)["Body"].read().decode("utf-8")
        for line in body.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if dag_run_id and row.get("dag_run_id") != dag_run_id:
                continue
            rows.append(row)
    return rows


def dedupe_latest_per_task(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep one row per (dag_run_id, task_id) — newest metric_id wins."""
    best: dict[tuple[str, str], dict[str, Any]] = {}
    for row in records:
        run_id = str(row.get("dag_run_id", ""))
        task_id = str(row.get("task_id", ""))
        metric_id = int(row.get("metric_id") or 0)
        key = (run_id, task_id)
        prev = best.get(key)
        if prev is None or int(prev.get("metric_id") or 0) <= metric_id:
            best[key] = row
    return list(best.values())


def records_to_dataframe(records: list[dict[str, Any]]) -> pd.DataFrame:
    """Map JSONL payloads to Glue schema columns."""
    if not records:
        return pd.DataFrame()

    normalized: list[dict[str, Any]] = []
    for row in records:
        execution_date = row.get("execution_date")
        normalized.append(
            {
                "metric_id": int(row.get("metric_id") or 0),
                "task_id": str(row.get("task_id", "")),
                "dag_run_id": str(row.get("dag_run_id", "")),
                "execution_date": pd.to_datetime(execution_date, utc=True, errors="coerce")
                if execution_date
                else pd.NaT,
                "dataset_size": str(row.get("dataset_size", "")),
                "rows_clients": int(row.get("rows_clients") or 0),
                "rows_transactions": int(row.get("rows_transactions") or 0),
                "rows_clickstream": int(row.get("rows_clickstream") or 0),
                "rows_offers": int(row.get("rows_offers") or 0),
                "rows_client_offers": int(row.get("rows_client_offers") or 0),
                "extract_time_sec": float(row.get("extract_time_sec") or 0.0),
                "transform_time_sec": float(row.get("transform_time_sec") or 0.0),
                "load_time_sec": float(row.get("load_time_sec") or 0.0),
                "total_execution_time_sec": float(row.get("total_execution_time_sec") or 0.0),
                "parallel_tasks_count": int(row.get("parallel_tasks_count") or 1),
                "task_load": float(row.get("task_load") or 0.0),
                "avg_task_load": float(row.get("avg_task_load") or 0.0),
                "cpu_utilization": float(row.get("cpu_utilization") or 0.0),
                "ram_utilization": float(row.get("ram_utilization") or 0.0),
                "planned_cpu_utilization": float(row.get("planned_cpu_utilization") or 0.0),
                "planned_ram_utilization": float(row.get("planned_ram_utilization") or 0.0),
                "measured_cpu_utilization": _optional_float(row.get("measured_cpu_utilization")),
                "measured_ram_utilization": _optional_float(row.get("measured_ram_utilization")),
                "resource_signal_source": str(row.get("resource_signal_source", "")),
                "speedup": float(row.get("speedup") or 0.0),
                "efficiency": float(row.get("efficiency") or 0.0),
                "amdahl_speedup": float(row.get("amdahl_speedup") or 0.0),
                "critical_path_time_sec": float(row.get("critical_path_time_sec") or 0.0),
                "etl_time_sec": float(row.get("etl_time_sec") or 0.0),
                "load_balance_coeff": float(row.get("load_balance_coeff") or 0.0),
                "predicted_time_old_sec": float(row.get("predicted_time_old_sec") or 0.0),
                "predicted_time_new_sec": float(row.get("predicted_time_new_sec") or 0.0),
                "spark_app_name": str(row.get("spark_app_name", "")),
                "spark_shuffle_partitions": int(row.get("spark_shuffle_partitions") or 0),
                "status": str(row.get("status", "success")),
            }
        )

    df = pd.DataFrame(normalized)
    return coerce_dataframe_to_glue_schema(TABLE_NAME, df)


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def write_local_parquet(df: pd.DataFrame, output_dir: Path) -> Path:
    """Write part-000.parquet under data/parquet/etl_execution_metrics/."""
    table_dir = output_dir / TABLE_NAME
    table_dir.mkdir(parents=True, exist_ok=True)
    out_path = table_dir / "part-000.parquet"
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, out_path, compression="snappy")
    normalize_parquet_timestamps(out_path)
    normalize_parquet_glue_types(TABLE_NAME, out_path)
    return out_path


def sync_metrics_to_glue(
    *,
    upload: bool,
    limit_files: int | None,
    dag_run_id: str | None,
) -> dict[str, Any]:
    storage = AdaptiveStorage.from_env()
    raw_records = load_task_metrics_records(
        storage,
        limit_files=limit_files,
        dag_run_id=dag_run_id,
    )
    records = dedupe_latest_per_task(raw_records)
    if not records:
        raise RuntimeError(
            "No task metrics found in S3 JSONL. "
            "Run the DAG first so metrics/bank_data/etl_metrics_*.jsonl is populated."
        )

    df = records_to_dataframe(records)
    out_path = write_local_parquet(df, PARQUET_DIR)
    s3_uri = None
    if upload:
        import boto3
        from config import AWS_REGION

        client = boto3.client("s3", region_name=AWS_REGION)
        s3_uri = upload_table_parquet(TABLE_NAME, client)

    run_ids = sorted({str(r.get("dag_run_id", "")) for r in records})
    return {
        "rows": len(records),
        "raw_rows": len(raw_records),
        "dag_runs": len(run_ids),
        "local_parquet": str(out_path),
        "s3_uri": s3_uri,
        "sample_run_ids": run_ids[-5:],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync S3 JSONL task metrics into Glue raw table Parquet")
    parser.add_argument(
        "--upload",
        action="store_true",
        help="Upload part-000.parquet to s3://<bucket>/raw/bank_data/etl_execution_metrics/",
    )
    parser.add_argument(
        "--limit-files",
        type=int,
        default=None,
        help="Only read the N most recent etl_metrics_*.jsonl files (default: all)",
    )
    parser.add_argument(
        "--dag-run-id",
        default=None,
        help="Filter to a single Airflow dag_run_id",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    summary = sync_metrics_to_glue(
        upload=args.upload,
        limit_files=args.limit_files,
        dag_run_id=args.dag_run_id,
    )
    logging.info("Synced %s rows (%s raw) across %s DAG runs", summary["rows"], summary["raw_rows"], summary["dag_runs"])
    logging.info("Local Parquet: %s", summary["local_parquet"])
    if summary["s3_uri"]:
        logging.info("Uploaded: %s", summary["s3_uri"])
    logging.info("Recent run_ids: %s", ", ".join(summary["sample_run_ids"]))
    logging.info(
        "Next: run q18_etl_metrics_aggregation in Airflow (or spark-submit) to fill "
        "adaptive_etl_bank.etl_metrics_aggregation"
    )


if __name__ == "__main__":
    main()
