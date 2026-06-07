"""Fail-fast data quality checks for key production ETL stages."""

from __future__ import annotations

import argparse
from dataclasses import dataclass

from common import RuntimeOptions, build_spark, parse_runtime_options


@dataclass(frozen=True)
class Check:
    name: str
    query: str
    expect_zero: bool


def parse_args() -> tuple[str, RuntimeOptions]:
    """Parse --check-group plus the same runtime flags as ETL q01-q20 tasks."""
    check_parser = argparse.ArgumentParser(add_help=False)
    check_parser.add_argument(
        "--check-group",
        required=True,
        choices=["post_audience", "post_mailing", "post_metrics"],
    )
    check_args, remaining = check_parser.parse_known_args()
    options = parse_runtime_options(remaining)
    return check_args.check_group, options


def checks_for_group(group: str) -> list[Check]:
    if group == "post_audience":
        return [
            Check("q01_row_count_positive", "SELECT COUNT(*) = 0 FROM adaptive_etl_bank.credit_campaign_target_audience", True),
            Check("q06_row_count_positive", "SELECT COUNT(*) = 0 FROM adaptive_etl_bank.client_profile_scoring", True),
            Check("q13_top1000_not_empty", "SELECT COUNT(*) = 0 FROM adaptive_etl_bank.high_value_clients_top1000", True),
            Check(
                "q06_null_client_or_score",
                "SELECT COUNT(*) FROM adaptive_etl_bank.client_profile_scoring WHERE client_id IS NULL OR final_score IS NULL",
                True,
            ),
            Check(
                "q08_null_campaign_or_kpi",
                "SELECT COUNT(*) FROM adaptive_etl_bank.campaign_performance_dashboard WHERE campaign_id IS NULL OR total_client_offers IS NULL",
                True,
            ),
            Check(
                "q06_duplicate_client_id",
                "SELECT COUNT(*) FROM (SELECT client_id, COUNT(*) AS c FROM adaptive_etl_bank.client_profile_scoring GROUP BY client_id HAVING COUNT(*) > 1) t",
                True,
            ),
            Check(
                "q13_duplicate_client_id",
                "SELECT COUNT(*) FROM (SELECT client_id, COUNT(*) AS c FROM adaptive_etl_bank.high_value_clients_top1000 GROUP BY client_id HAVING COUNT(*) > 1) t",
                True,
            ),
            Check(
                "q13_row_limit_1000",
                "SELECT CASE WHEN COUNT(*) > 1000 THEN 1 ELSE 0 END FROM adaptive_etl_bank.high_value_clients_top1000",
                True,
            ),
        ]
    if group == "post_mailing":
        return [
            Check("q16_row_count_positive", "SELECT COUNT(*) = 0 FROM adaptive_etl_bank.mailing_base", True),
            Check("q17_row_count_positive", "SELECT COUNT(*) = 0 FROM adaptive_etl_bank.mailing_schedule_optimization", True),
            Check(
                "q16_null_required_columns",
                "SELECT COUNT(*) FROM adaptive_etl_bank.mailing_base WHERE client_id IS NULL OR campaign_id IS NULL OR offer_id IS NULL OR channel IS NULL",
                True,
            ),
            Check(
                "q17_null_schedule",
                "SELECT COUNT(*) FROM adaptive_etl_bank.mailing_schedule_optimization WHERE schedule_hour IS NULL OR schedule_batch IS NULL",
                True,
            ),
            Check(
                "q16_duplicate_client_campaign_offer",
                "SELECT COUNT(*) FROM (SELECT client_id, campaign_id, offer_id, COUNT(*) AS c FROM adaptive_etl_bank.mailing_base GROUP BY client_id, campaign_id, offer_id HAVING COUNT(*) > 1) t",
                True,
            ),
            Check(
                "q20_invalid_readiness_status",
                "SELECT COUNT(*) FROM adaptive_etl_bank.campaign_readiness_check WHERE readiness_status NOT IN ('blocked', 'warning', 'ready') OR readiness_status IS NULL",
                True,
            ),
        ]
    return [
        Check("q18_row_count_positive", "SELECT COUNT(*) = 0 FROM adaptive_etl_bank.etl_metrics_aggregation", True),
        Check("q19_row_count_positive", "SELECT COUNT(*) = 0 FROM adaptive_etl_bank.adaptive_parallelism_recommendation", True),
        Check(
            "q18_null_dataset_or_total_time",
            "SELECT COUNT(*) FROM adaptive_etl_bank.etl_metrics_aggregation WHERE dataset_size IS NULL OR avg_total_time_sec IS NULL",
            True,
        ),
        Check(
            "q19_invalid_parallelism_range",
            "SELECT COUNT(*) FROM adaptive_etl_bank.adaptive_parallelism_recommendation WHERE recommended_parallel_tasks IS NULL OR recommended_parallel_tasks < 1 OR recommended_parallel_tasks > 8",
            True,
        ),
        Check(
            "q18_duplicate_dataset_size",
            "SELECT COUNT(*) FROM (SELECT dataset_size, COUNT(*) AS c FROM adaptive_etl_bank.etl_metrics_aggregation GROUP BY dataset_size HAVING COUNT(*) > 1) t",
            True,
        ),
        Check(
            "q19_duplicate_dataset_size",
            "SELECT COUNT(*) FROM (SELECT dataset_size, COUNT(*) AS c FROM adaptive_etl_bank.adaptive_parallelism_recommendation GROUP BY dataset_size HAVING COUNT(*) > 1) t",
            True,
        ),
    ]


def run_checks(group: str, options: RuntimeOptions) -> None:
    spark = build_spark(app_name=f"dq_{group}", runtime_options=options)
    try:
        for check in checks_for_group(group):
            result = spark.sql(check.query).collect()[0][0]
            value = int(result) if isinstance(result, bool) else float(result)
            failed = (value != 0) if check.expect_zero else (value <= 0)
            if failed:
                raise RuntimeError(
                    f"Data quality check failed ({group}): {check.name}; observed={value}; query={check.query}"
                )
    finally:
        spark.stop()


if __name__ == "__main__":
    group_name, runtime = parse_args()
    run_checks(group_name, runtime)
