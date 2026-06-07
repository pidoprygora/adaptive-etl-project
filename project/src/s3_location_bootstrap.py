"""Create empty S3 prefixes for Glue external table LOCATION paths (Spark INSERT)."""

from __future__ import annotations

import argparse
import logging
import re
from pathlib import Path
from urllib.parse import urlparse

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from config import AWS_REGION, PROJECT_ROOT, S3_BUCKET, SQL_DIR

LOCATION_RE = re.compile(
    r"LOCATION\s+'(s3://[^']+)'",
    re.IGNORECASE,
)
CREATE_TABLE_RE = re.compile(
    r"CREATE\s+EXTERNAL\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([\w.]+)",
    re.IGNORECASE,
)


def extract_locations_from_sql(sql_path: Path) -> dict[str, str]:
    """Map fully-qualified table name -> s3:// URI from CREATE TABLE DDL."""
    text = sql_path.read_text(encoding="utf-8")
    locations: dict[str, str] = {}
    current_table: str | None = None
    for line in text.splitlines():
        create_match = CREATE_TABLE_RE.search(line)
        if create_match:
            current_table = create_match.group(1).strip()
            continue
        location_match = LOCATION_RE.search(line)
        if location_match and current_table:
            locations[current_table] = location_match.group(1).strip()
            current_table = None
    return locations


def _normalize_prefix_key(key: str) -> str:
    key = key.lstrip("/")
    if not key.endswith("/"):
        key = f"{key}/"
    return key


def ensure_s3_prefix(s3_uri: str, s3_client: boto3.client | None = None) -> bool:
    """Create an empty folder marker for an s3://bucket/prefix/ URI. Returns True if created."""
    parsed = urlparse(s3_uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(f"Invalid S3 URI: {s3_uri}")
    bucket = parsed.netloc
    key = _normalize_prefix_key(parsed.path)
    client = s3_client or boto3.client("s3", region_name=AWS_REGION)
    try:
        client.head_object(Bucket=bucket, Key=key)
        return False
    except ClientError as error:
        code = error.response.get("Error", {}).get("Code", "")
        if code not in {"404", "NoSuchKey", "NotFound"}:
            raise
    client.put_object(Bucket=bucket, Key=key, Body=b"")
    return True


def ensure_locations(
    locations: dict[str, str],
    *,
    bucket_filter: str | None = S3_BUCKET,
) -> tuple[int, int]:
    """Ensure all prefixes exist. Returns (created_count, skipped_count)."""
    s3_client = boto3.client("s3", region_name=AWS_REGION)
    created = 0
    skipped = 0
    seen_prefixes: set[str] = set()
    for table_name, s3_uri in sorted(locations.items()):
        if bucket_filter and not s3_uri.startswith(f"s3://{bucket_filter}/"):
            logging.warning("Skip %s: bucket mismatch (%s)", table_name, s3_uri)
            continue
        if s3_uri in seen_prefixes:
            continue
        seen_prefixes.add(s3_uri)
        if ensure_s3_prefix(s3_uri, s3_client):
            logging.info("Created s3 prefix for %s -> %s", table_name, s3_uri)
            created += 1
        else:
            skipped += 1
    return created, skipped


def ensure_raw_table_prefixes() -> tuple[int, int]:
    """All raw/bank_data Glue table locations."""
    sql_path = SQL_DIR / "create_athena_tables.sql"
    return ensure_locations(extract_locations_from_sql(sql_path))


def ensure_processed_table_prefixes() -> tuple[int, int]:
    """All processed/bank_data target table locations (q01-q20 outputs)."""
    locations: dict[str, str] = {}
    for name in ("create_athena_target_tables.sql", "complex_etl_queries_targets_ddl.sql"):
        path = SQL_DIR / name
        if path.is_file():
            locations.update(extract_locations_from_sql(path))
    return ensure_locations(locations)


def ensure_insert_target_prefix(sql_text: str) -> str | None:
    """Ensure S3 prefix for INSERT INTO target table. Returns table name if ensured."""
    match = re.search(
        r"INSERT\s+INTO\s+([`\"]?)([\w.]+)\1",
        sql_text,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    table_name = match.group(2)
    locations: dict[str, str] = {}
    for name in ("create_athena_target_tables.sql", "complex_etl_queries_targets_ddl.sql"):
        path = SQL_DIR / name
        if path.is_file():
            locations.update(extract_locations_from_sql(path))
    s3_uri = locations.get(table_name)
    if not s3_uri:
        logging.warning("No LOCATION in DDL for INSERT target table %s", table_name)
        return table_name
    ensure_s3_prefix(s3_uri)
    return table_name


def main() -> None:
    parser = argparse.ArgumentParser(description="Create S3 prefixes for Glue table LOCATIONs")
    parser.add_argument("--raw", action="store_true", help="Bootstrap raw/bank_data prefixes")
    parser.add_argument(
        "--processed",
        action="store_true",
        help="Bootstrap processed/bank_data target prefixes",
    )
    parser.add_argument("--all", action="store_true", help="Bootstrap raw + processed")
    args = parser.parse_args()
    if not (args.raw or args.processed or args.all):
        args.all = True

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    total_created = 0
    total_skipped = 0
    try:
        if args.all or args.raw:
            created, skipped = ensure_raw_table_prefixes()
            logging.info("Raw prefixes: created=%s skipped=%s", created, skipped)
            total_created += created
            total_skipped += skipped
        if args.all or args.processed:
            created, skipped = ensure_processed_table_prefixes()
            logging.info("Processed prefixes: created=%s skipped=%s", created, skipped)
            total_created += created
            total_skipped += skipped
    except (BotoCoreError, ClientError) as error:
        raise SystemExit(f"S3 bootstrap failed: {error}") from error
    logging.info("Done: created=%s skipped=%s", total_created, total_skipped)


if __name__ == "__main__":
    main()
