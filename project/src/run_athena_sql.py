"""Послідовне виконання SQL-скрипта в AWS Athena (по 1 statement за раз)."""

from __future__ import annotations

import argparse
import time
from pathlib import Path

import boto3

from config import AWS_REGION, DATABASE_NAME, PROJECT_ROOT, S3_BUCKET


def split_sql_statements(sql_text: str) -> list[str]:
    """Розбиває SQL на окремі statement-и з урахуванням лапок і коментарів."""
    statements: list[str] = []
    buffer: list[str] = []

    in_single_quote = False
    in_double_quote = False
    in_line_comment = False
    in_block_comment = False

    idx = 0
    while idx < len(sql_text):
        char = sql_text[idx]
        next_char = sql_text[idx + 1] if idx + 1 < len(sql_text) else ""

        if in_line_comment:
            if char == "\n":
                in_line_comment = False
            idx += 1
            continue

        if in_block_comment:
            if char == "*" and next_char == "/":
                in_block_comment = False
                idx += 2
                continue
            idx += 1
            continue

        if not in_single_quote and not in_double_quote:
            if char == "-" and next_char == "-":
                in_line_comment = True
                idx += 2
                continue
            if char == "/" and next_char == "*":
                in_block_comment = True
                idx += 2
                continue

        if char == "'" and not in_double_quote:
            in_single_quote = not in_single_quote
            buffer.append(char)
            idx += 1
            continue

        if char == '"' and not in_single_quote:
            in_double_quote = not in_double_quote
            buffer.append(char)
            idx += 1
            continue

        if char == ";" and not in_single_quote and not in_double_quote:
            statement = "".join(buffer).strip()
            if statement:
                statements.append(statement)
            buffer = []
            idx += 1
            continue

        buffer.append(char)
        idx += 1

    tail = "".join(buffer).strip()
    if tail:
        statements.append(tail)

    return statements


def wait_for_query(
    athena_client: boto3.client,
    query_execution_id: str,
    poll_interval_sec: float,
) -> tuple[str, str]:
    """Очікує завершення запиту та повертає (state, reason)."""
    while True:
        response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        status_info = response["QueryExecution"]["Status"]
        state = status_info["State"]
        reason = status_info.get("StateChangeReason", "")

        if state in {"SUCCEEDED", "FAILED", "CANCELLED"}:
            return state, reason

        time.sleep(poll_interval_sec)


def run_statements(
    sql_file: Path,
    output_s3: str,
    database: str,
    poll_interval_sec: float,
    continue_on_error: bool,
) -> None:
    """Запускає всі SQL statement-и у файлі послідовно через Athena API."""
    sql_text = sql_file.read_text(encoding="utf-8")
    statements = split_sql_statements(sql_text)
    if not statements:
        raise ValueError(f"No SQL statements found in file: {sql_file}")

    athena_client = boto3.client("athena", region_name=AWS_REGION)
    print(f"Found {len(statements)} SQL statements in {sql_file}")

    for idx, statement in enumerate(statements, start=1):
        print(f"\n[{idx}/{len(statements)}] Executing:")
        print(statement[:200].replace("\n", " ") + ("..." if len(statement) > 200 else ""))

        request = {
            "QueryString": statement,
            "ResultConfiguration": {"OutputLocation": output_s3},
        }
        if database:
            request["QueryExecutionContext"] = {"Database": database}

        start_response = athena_client.start_query_execution(**request)
        query_id = start_response["QueryExecutionId"]
        print(f"QueryExecutionId: {query_id}")

        state, reason = wait_for_query(athena_client, query_id, poll_interval_sec)
        if state == "SUCCEEDED":
            print("Status: SUCCEEDED")
            continue

        print(f"Status: {state}")
        if reason:
            print(f"Reason: {reason}")

        if not continue_on_error:
            raise RuntimeError(f"Athena query failed at statement {idx}/{len(statements)}.")


def run_sql_files(
    sql_files: list[Path],
    output_s3: str,
    database: str,
    poll_interval_sec: float,
    continue_on_error: bool,
) -> None:
    """Запускає кілька SQL-файлів послідовно."""
    total_files = len(sql_files)
    for file_idx, sql_file in enumerate(sql_files, start=1):
        print(f"\n=== SQL file {file_idx}/{total_files}: {sql_file} ===")
        run_statements(
            sql_file=sql_file,
            output_s3=output_s3,
            database=database,
            poll_interval_sec=poll_interval_sec,
            continue_on_error=continue_on_error,
        )


def build_parser() -> argparse.ArgumentParser:
    """Створює CLI-парсер параметрів запуску."""
    parser = argparse.ArgumentParser(
        description="Run SQL file in Athena statement-by-statement.",
    )
    parser.add_argument(
        "--sql-file",
        type=Path,
        default=PROJECT_ROOT / "sql" / "create_athena_tables_via_alter.sql",
        help="Path to SQL file with one or many statements.",
    )
    parser.add_argument(
        "--sql-files",
        type=Path,
        nargs="+",
        help="Run several SQL files in sequence.",
    )
    parser.add_argument(
        "--preset",
        choices=["complex_etl"],
        help="Predefined SQL execution set. complex_etl: DDL targets + INSERT queries.",
    )
    parser.add_argument(
        "--database",
        default=DATABASE_NAME,
        help="Athena database context (optional, can be empty string).",
    )
    parser.add_argument(
        "--output-s3",
        default=f"s3://{S3_BUCKET}/athena-query-results/",
        help="S3 path for Athena query results.",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=2.0,
        help="Polling interval in seconds.",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue with next statement if current one fails.",
    )
    return parser


def main() -> None:
    """Точка входу CLI."""
    args = build_parser().parse_args()

    preset_files: list[Path] = []
    if args.preset == "complex_etl":
        preset_files = [
            PROJECT_ROOT / "sql" / "complex_etl_queries_targets_ddl.sql",
            PROJECT_ROOT / "sql" / "complex_etl_queries.sql",
        ]

    selected_files = args.sql_files or preset_files or [args.sql_file]
    missing_files = [sql_file for sql_file in selected_files if not sql_file.exists()]
    if missing_files:
        missing_text = ", ".join(str(path) for path in missing_files)
        raise FileNotFoundError(f"SQL file(s) not found: {missing_text}")

    if not str(args.output_s3).startswith("s3://"):
        raise ValueError("--output-s3 must start with s3://")

    run_sql_files(
        sql_files=selected_files,
        output_s3=args.output_s3.rstrip("/") + "/",
        database=args.database.strip(),
        poll_interval_sec=args.poll_interval,
        continue_on_error=args.continue_on_error,
    )

    print("\nDone.")


if __name__ == "__main__":
    main()
