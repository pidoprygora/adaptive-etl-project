from common import parse_runtime_options, run_sql


SQL_QUERY = """
INSERT INTO adaptive_etl_bank.delivery_failure_analysis
WITH event_base AS (
    SELECT
        co.channel,
        COALESCE(me.error_code, 'NO_ERROR_CODE') AS error_code,
        me.event_type,
        COALESCE(ds.is_success, false) AS is_success
    FROM adaptive_etl_bank.mailing_events me
    JOIN adaptive_etl_bank.client_offers co
        ON me.client_offer_id = co.client_offer_id
    LEFT JOIN adaptive_etl_bank.delivery_statuses ds
        ON me.delivery_status_id = ds.delivery_status_id
),
grouped_failures AS (
    SELECT
        eb.channel,
        eb.error_code,
        COUNT(1) AS total_events,
        COUNT_IF(eb.event_type = 'failed' OR eb.is_success = false) AS failed_events
    FROM event_base eb
    GROUP BY eb.channel, eb.error_code
    HAVING COUNT(1) >= 10
)
SELECT
    gf.channel,
    gf.error_code,
    gf.total_events,
    gf.failed_events,
    CASE WHEN gf.total_events > 0 THEN (CAST(gf.failed_events AS DOUBLE) / gf.total_events) * 100.0 ELSE 0.0 END AS failure_rate
FROM grouped_failures gf
WHERE CASE WHEN gf.total_events > 0 THEN (CAST(gf.failed_events AS DOUBLE) / gf.total_events) * 100.0 ELSE 0.0 END > 10.0
ORDER BY failure_rate DESC, total_events DESC
"""


if __name__ == "__main__":
    run_sql(
        sql_text=SQL_QUERY,
        app_name="etl_q10_delivery_failure_analysis",
        runtime_options=parse_runtime_options(),
    )
