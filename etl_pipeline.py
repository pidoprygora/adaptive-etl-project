"""ETL-пайплайн: зчитування raw із S3, трансформація, завантаження processed і метрики."""

from __future__ import annotations

import argparse
import io
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import boto3
import pandas as pd

from config import AppConfig, METRICS_FILE, OUTPUT_DIR, build_s3_key

RAW_DATASET_FILES = {
    "clients": "clients.csv",
    "offers": "offers.csv",
    "client_offers": "client_offers.csv",
    "mailing_events": "mailing_events.csv",
}

PROCESSED_FILES = {
    "mailing_base": "mailing_base.csv",
    "offers_by_channel": "offers_by_channel.csv",
    "offers_by_product": "offers_by_product.csv",
    "clients_by_segment": "clients_by_segment.csv",
    "offer_conversion": "offer_conversion.csv",
    "delivery_errors": "delivery_errors.csv",
}


def _get_s3_client(config: AppConfig):
    return boto3.client("s3", region_name=config.aws_region)


def _read_csv_from_s3(s3_client, bucket: str, key: str) -> pd.DataFrame:
    response = s3_client.get_object(Bucket=bucket, Key=key)
    content = response["Body"].read().decode("utf-8")
    return pd.read_csv(io.StringIO(content))


def extract_data(config: AppConfig | None = None) -> dict[str, pd.DataFrame]:
    """Зчитує всі CSV із S3 raw-префікса."""
    config = config or AppConfig.from_env()
    s3_client = _get_s3_client(config)
    datasets: dict[str, pd.DataFrame] = {}

    for dataset_name, filename in RAW_DATASET_FILES.items():
        key = build_s3_key(config.raw_prefix, filename)
        datasets[dataset_name] = _read_csv_from_s3(s3_client, config.s3_bucket, key)

    return datasets


def transform_data(datasets: dict[str, pd.DataFrame] | None = None) -> dict[str, pd.DataFrame]:
    """Виконує фільтрацію, джойни та розрахунок бізнес-метрик."""
    if datasets is None:
        datasets = extract_data()

    clients = datasets["clients"].copy()
    offers = datasets["offers"].copy()
    client_offers = datasets["client_offers"].copy()
    mailing_events = datasets["mailing_events"].copy()

    active_clients = clients[clients["is_active"].astype(str).str.lower() == "true"].copy()
    active_offers = offers[offers["is_active"].astype(str).str.lower() == "true"].copy()

    mailing_base = client_offers.merge(active_clients, on="client_id", how="inner")
    mailing_base = mailing_base.merge(active_offers, on="offer_id", how="inner")
    mailing_base = mailing_base[~mailing_base["offer_status"].isin(["expired", "rejected"])].copy()

    offers_by_channel = (
        mailing_base.groupby("channel", as_index=False)
        .agg(offers_count=("client_offer_id", "count"))
        .sort_values("offers_count", ascending=False)
    )

    offers_by_product = (
        mailing_base.groupby("product_type", as_index=False)
        .agg(offers_count=("client_offer_id", "count"))
        .sort_values("offers_count", ascending=False)
    )

    clients_by_segment = (
        mailing_base.groupby("client_segment", as_index=False)
        .agg(clients_count=("client_id", "nunique"))
        .sort_values("clients_count", ascending=False)
    )

    sent_count = int((mailing_base["offer_status"] == "sent").sum())
    accepted_count = int((mailing_base["offer_status"] == "accepted").sum())
    conversion_value = round(accepted_count / sent_count, 6) if sent_count else 0.0
    offer_conversion = pd.DataFrame(
        [
            {
                "accepted": accepted_count,
                "sent": sent_count,
                "offer_conversion": conversion_value,
            }
        ]
    )

    failed_events = mailing_events[mailing_events["delivery_status"].astype(str).str.lower() == "failed"].copy()
    failed_events["error_code"] = failed_events["error_code"].fillna("").astype(str).str.strip()
    failed_events = failed_events[failed_events["error_code"] != ""]

    delivery_errors = (
        failed_events.groupby("error_code", as_index=False)
        .agg(errors_count=("event_id", "count"))
        .sort_values("errors_count", ascending=False)
    )

    return {
        "mailing_base": mailing_base,
        "offers_by_channel": offers_by_channel,
        "offers_by_product": offers_by_product,
        "clients_by_segment": clients_by_segment,
        "offer_conversion": offer_conversion,
        "delivery_errors": delivery_errors,
    }


def load_data(transformed: dict[str, pd.DataFrame] | None = None, config: AppConfig | None = None) -> dict[str, str]:
    """Завантажує processed-результати в S3 та дублює локально в output/."""
    config = config or AppConfig.from_env()
    if transformed is None:
        transformed = transform_data()

    s3_client = _get_s3_client(config)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    uploaded: dict[str, str] = {}

    for dataset_name, filename in PROCESSED_FILES.items():
        frame = transformed[dataset_name]
        local_path = OUTPUT_DIR / filename
        frame.to_csv(local_path, index=False)

        csv_buffer = io.StringIO()
        frame.to_csv(csv_buffer, index=False)
        key = build_s3_key(config.processed_prefix, filename)
        s3_client.put_object(Bucket=config.s3_bucket, Key=key, Body=csv_buffer.getvalue().encode("utf-8"))
        uploaded[dataset_name] = f"s3://{config.s3_bucket}/{key}"

    return uploaded


def validate_data(config: AppConfig | None = None) -> bool:
    """Перевіряє наявність очікуваних об'єктів у S3 processed-префіксі."""
    config = config or AppConfig.from_env()
    s3_client = _get_s3_client(config)

    for filename in PROCESSED_FILES.values():
        key = build_s3_key(config.processed_prefix, filename)
        s3_client.head_object(Bucket=config.s3_bucket, Key=key)

    return True


def save_metrics(metrics_record: dict[str, Any], config: AppConfig | None = None) -> None:
    """Апендить metrics.csv локально та завантажує в S3 metrics-префікс."""
    config = config or AppConfig.from_env()
    metrics_df = pd.DataFrame([metrics_record])

    if METRICS_FILE.exists():
        existing = pd.read_csv(METRICS_FILE)
        all_metrics = pd.concat([existing, metrics_df], ignore_index=True)
    else:
        all_metrics = metrics_df

    all_metrics.to_csv(METRICS_FILE, index=False)

    s3_client = _get_s3_client(config)
    key = build_s3_key(config.metrics_prefix, "metrics.csv")
    with METRICS_FILE.open("rb") as metrics_handle:
        s3_client.upload_fileobj(metrics_handle, config.s3_bucket, key)


def run_pipeline(dag_run_id: str | None = None, execution_date: str | None = None) -> dict[str, Any]:
    """Повний ETL-цикл для локального запуску або Airflow orchestration."""
    config = AppConfig.from_env()
    run_id = dag_run_id or f"manual_{uuid.uuid4().hex[:10]}"
    run_execution_date = execution_date or datetime.now(timezone.utc).isoformat()

    total_start = time.perf_counter()
    extract_time_sec = 0.0
    transform_time_sec = 0.0
    load_time_sec = 0.0
    status = "success"
    extracted_rows = {
        "rows_clients": 0,
        "rows_offers": 0,
        "rows_client_offers": 0,
        "rows_events": 0,
    }

    try:
        extract_start = time.perf_counter()
        datasets = extract_data(config=config)
        extract_time_sec = round(time.perf_counter() - extract_start, 3)

        extracted_rows = {
            "rows_clients": len(datasets["clients"]),
            "rows_offers": len(datasets["offers"]),
            "rows_client_offers": len(datasets["client_offers"]),
            "rows_events": len(datasets["mailing_events"]),
        }

        transform_start = time.perf_counter()
        transformed = transform_data(datasets=datasets)
        transform_time_sec = round(time.perf_counter() - transform_start, 3)

        load_start = time.perf_counter()
        load_data(transformed=transformed, config=config)
        load_time_sec = round(time.perf_counter() - load_start, 3)

        validate_data(config=config)
    except Exception:
        status = "failed"
        raise
    finally:
        total_time_sec = round(time.perf_counter() - total_start, 3)
        metrics_record = {
            "dag_run_id": run_id,
            "execution_date": run_execution_date,
            **extracted_rows,
            "extract_time_sec": extract_time_sec,
            "transform_time_sec": transform_time_sec,
            "load_time_sec": load_time_sec,
            "total_execution_time_sec": total_time_sec,
            "status": status,
        }
        save_metrics(metrics_record, config=config)

    return metrics_record


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Запуск S3-based ETL pipeline")
    parser.add_argument("--dag-run-id", default=None)
    parser.add_argument("--execution-date", default=None)
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    metrics = run_pipeline(dag_run_id=args.dag_run_id, execution_date=args.execution_date)
    print("ETL run completed.")
    for key, value in metrics.items():
        print(f"- {key}: {value}")
