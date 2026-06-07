"""Adaptive PySpark ETL example with runtime parallelism parameters."""

from __future__ import annotations

from common import parse_runtime_options, run_sql

SQL_QUERY = """
INSERT INTO adaptive_etl_bank.campaign_performance_dashboard
WITH campaign_stats AS (
    SELECT
        c.campaign_id,
        c.campaign_name,
        c.status AS campaign_status,
        COUNT(co.client_offer_id) AS total_client_offers,
        COUNT_IF(co.offer_status = 'sent') AS sent_count,
        COUNT_IF(me.event_type = 'delivered') AS delivered_count,
        COUNT_IF(me.event_type = 'opened') AS opened_count,
        COUNT_IF(me.event_type = 'clicked') AS clicked_count,
        COUNT_IF(co.offer_status = 'accepted') AS accepted_count,
        COUNT_IF(me.event_type = 'failed') AS failed_count
    FROM adaptive_etl_bank.campaigns c
    LEFT JOIN adaptive_etl_bank.client_offers co
        ON c.campaign_id = co.campaign_id
    LEFT JOIN adaptive_etl_bank.mailing_events me
        ON co.client_offer_id = me.client_offer_id
    GROUP BY c.campaign_id, c.campaign_name, c.status
)
SELECT
    cs.campaign_id,
    cs.campaign_name,
    cs.campaign_status,
    cs.total_client_offers,
    cs.sent_count,
    cs.delivered_count,
    cs.opened_count,
    cs.clicked_count,
    cs.accepted_count,
    cs.failed_count,
    CASE WHEN cs.sent_count = 0 THEN 0.0 ELSE CAST(cs.delivered_count AS DOUBLE) / cs.sent_count END AS delivery_rate,
    CASE WHEN cs.delivered_count = 0 THEN 0.0 ELSE CAST(cs.opened_count AS DOUBLE) / cs.delivered_count END AS open_rate,
    CASE WHEN cs.opened_count = 0 THEN 0.0 ELSE CAST(cs.clicked_count AS DOUBLE) / cs.opened_count END AS click_rate,
    CASE WHEN cs.sent_count = 0 THEN 0.0 ELSE CAST(cs.accepted_count AS DOUBLE) / cs.sent_count END AS conversion_rate
FROM campaign_stats cs
"""


if __name__ == "__main__":
    runtime_options = parse_runtime_options()
    run_sql(
        sql_text=SQL_QUERY,
        app_name="etl_q21_adaptive_etl_example",
        runtime_options=runtime_options,
        persist_metrics=True,
    )
