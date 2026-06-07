from common import parse_runtime_options, run_sql


SQL_QUERY = """
INSERT INTO adaptive_etl_bank.campaign_performance_dashboard
WITH campaign_offers_base AS (
    SELECT
        ca.campaign_id,
        ca.campaign_name,
        ca.status AS campaign_status,
        co.client_offer_id,
        co.client_id,
        co.offer_status
    FROM adaptive_etl_bank.campaigns ca
    LEFT JOIN adaptive_etl_bank.client_offers co
        ON ca.campaign_id = co.campaign_id
),
event_flags AS (
    SELECT
        me.client_offer_id,
        MAX(CASE WHEN me.event_type = 'sent' THEN 1 ELSE 0 END) AS has_sent,
        MAX(CASE WHEN me.event_type = 'delivered' THEN 1 ELSE 0 END) AS has_delivered,
        MAX(CASE WHEN me.event_type = 'opened' THEN 1 ELSE 0 END) AS has_opened,
        MAX(CASE WHEN me.event_type = 'clicked' THEN 1 ELSE 0 END) AS has_clicked,
        MAX(CASE WHEN ds.is_success = false OR me.event_type = 'failed' THEN 1 ELSE 0 END) AS has_failed
    FROM adaptive_etl_bank.mailing_events me
    LEFT JOIN adaptive_etl_bank.delivery_statuses ds
        ON me.delivery_status_id = ds.delivery_status_id
    GROUP BY me.client_offer_id
),
campaign_kpi AS (
    SELECT
        cob.campaign_id,
        cob.campaign_name,
        cob.campaign_status,
        COUNT(cob.client_offer_id) AS total_client_offers,
        COALESCE(SUM(ef.has_sent), 0) AS sent_count,
        COALESCE(SUM(ef.has_delivered), 0) AS delivered_count,
        COALESCE(SUM(ef.has_opened), 0) AS opened_count,
        COALESCE(SUM(ef.has_clicked), 0) AS clicked_count,
        COUNT_IF(cob.offer_status = 'accepted') AS accepted_count,
        COALESCE(SUM(ef.has_failed), 0) AS failed_count
    FROM campaign_offers_base cob
    LEFT JOIN event_flags ef
        ON cob.client_offer_id = ef.client_offer_id
    GROUP BY cob.campaign_id, cob.campaign_name, cob.campaign_status
)
SELECT
    ck.campaign_id,
    ck.campaign_name,
    ck.campaign_status,
    ck.total_client_offers,
    ck.sent_count,
    ck.delivered_count,
    ck.opened_count,
    ck.clicked_count,
    ck.accepted_count,
    ck.failed_count,
    CASE WHEN ck.sent_count > 0 THEN CAST(ck.delivered_count AS DOUBLE) / ck.sent_count ELSE 0.0 END AS delivery_rate,
    CASE WHEN ck.delivered_count > 0 THEN CAST(ck.opened_count AS DOUBLE) / ck.delivered_count ELSE 0.0 END AS open_rate,
    CASE WHEN ck.opened_count > 0 THEN CAST(ck.clicked_count AS DOUBLE) / ck.opened_count ELSE 0.0 END AS click_rate,
    CASE WHEN ck.sent_count > 0 THEN CAST(ck.accepted_count AS DOUBLE) / ck.sent_count ELSE 0.0 END AS conversion_rate
FROM campaign_kpi ck
"""


if __name__ == "__main__":
    run_sql(
        sql_text=SQL_QUERY,
        app_name="etl_q08_campaign_performance_dashboard",
        runtime_options=parse_runtime_options(),
    )
