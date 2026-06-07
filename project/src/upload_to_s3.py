"""Завантаження згенерованих Parquet-файлів у Amazon S3."""

from __future__ import annotations

import logging
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from boto3.exceptions import S3UploadFailedError
from botocore.exceptions import BotoCoreError, ClientError

from config import AWS_REGION, PARQUET_DIR, S3_BUCKET, S3_PREFIX
from glue_parquet_types import normalize_parquet_glue_types
from s3_location_bootstrap import ensure_processed_table_prefixes, ensure_raw_table_prefixes


def ensure_bucket_exists(s3_client: boto3.client) -> None:
    """Перевіряє, що S3-бакет існує та доступний поточному профілю."""
    try:
        s3_client.head_bucket(Bucket=S3_BUCKET)
    except ClientError as error:
        error_code = error.response.get("Error", {}).get("Code", "")
        if error_code in {"404", "NoSuchBucket"}:
            raise RuntimeError(
                "S3 bucket does not exist: "
                f"'{S3_BUCKET}' in region '{AWS_REGION}'. "
                "Create it first, for example: "
                f"'aws s3api create-bucket --bucket {S3_BUCKET} "
                f"--region {AWS_REGION} "
                f"--create-bucket-configuration LocationConstraint={AWS_REGION}'."
            ) from error
        raise RuntimeError(
            f"Cannot access bucket '{S3_BUCKET}'. Check AWS credentials and permissions."
        ) from error


def normalize_parquet_timestamps(parquet_path: Path) -> bool:
    """Rewrite ns Parquet timestamps to us so Spark 3.5 can read them."""
    table = pq.read_table(parquet_path)
    new_columns: list[pa.Array] = []
    new_fields: list[pa.Field] = []
    changed = False
    for field in table.schema:
        column = table.column(field.name)
        if pa.types.is_timestamp(field.type) and field.type.unit == "ns":
            changed = True
            new_type = pa.timestamp("us", tz=field.type.tz)
            new_columns.append(pc.cast(column, new_type))
            new_fields.append(pa.field(field.name, new_type))
        else:
            new_columns.append(column)
            new_fields.append(field)
    if not changed:
        return False
    normalized = pa.Table.from_arrays(new_columns, schema=pa.schema(new_fields))
    pq.write_table(normalized, parquet_path, compression="snappy")
    logging.info("Normalized timestamp columns in %s", parquet_path)
    return True


def upload_table_parquet(table_name: str, s3_client: boto3.client) -> None:
    """Завантажує Parquet-файл конкретної таблиці до S3."""
    local_file: Path = PARQUET_DIR / table_name / "part-000.parquet"
    if not local_file.exists():
        raise FileNotFoundError(f"Parquet file not found for table '{table_name}': {local_file}")

    normalize_parquet_timestamps(local_file)
    if normalize_parquet_glue_types(table_name, local_file):
        logging.info("Normalized Glue integer types in %s", local_file)
    s3_key = f"{S3_PREFIX}/{table_name}/part-000.parquet"
    s3_client.upload_file(str(local_file), S3_BUCKET, s3_key)
    logging.info("Завантажено %s -> s3://%s/%s", local_file, S3_BUCKET, s3_key)


def upload_all_tables_to_s3(table_names: list[str]) -> None:
    """Завантажує всі вказані таблиці в S3."""
    s3_client = boto3.client("s3", region_name=AWS_REGION)
    ensure_bucket_exists(s3_client)
    ensure_raw_table_prefixes()
    ensure_processed_table_prefixes()
    for table_name in table_names:
        try:
            upload_table_parquet(table_name, s3_client)
        except (FileNotFoundError, BotoCoreError, ClientError, S3UploadFailedError) as error:
            logging.error("Помилка завантаження таблиці %s: %s", table_name, error)
            raise


def list_local_parquet_tables() -> list[str]:
    """Return table names that have a local part-000.parquet file."""
    return sorted(
        path.name
        for path in PARQUET_DIR.iterdir()
        if path.is_dir() and (path / "part-000.parquet").is_file()
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    upload_all_tables_to_s3(list_local_parquet_tables())

