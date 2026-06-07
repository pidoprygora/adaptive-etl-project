from common import parse_runtime_options, run_sql


SQL_QUERY = """
INSERT INTO adaptive_etl_bank.product_conversion_analysis
WITH offers_with_type AS (
    SELECT
        co.client_offer_id,
        co.client_id,
        co.offer_status,
        co.offer_id,
        o.offer_name,
        p.product_type
    FROM adaptive_etl_bank.client_offers co
    JOIN adaptive_etl_bank.offers o
        ON co.offer_id = o.offer_id
    JOIN adaptive_etl_bank.products p
        ON o.product_id = p.product_id
),
event_summary AS (
    SELECT
        me.client_offer_id,
        MAX(CASE WHEN me.event_type = 'opened' THEN 1 ELSE 0 END) AS has_opened
    FROM adaptive_etl_bank.mailing_events me
    GROUP BY me.client_offer_id
),
product_aggregates AS (
    SELECT
        owt.product_type,
        COUNT(owt.client_offer_id) AS total_offers,
        COALESCE(SUM(es.has_opened), 0) AS opened_offers,
        COUNT_IF(owt.offer_status = 'accepted') AS accepted_offers,
        COUNT_IF(owt.offer_status = 'rejected') AS rejected_offers
    FROM offers_with_type owt
    LEFT JOIN event_summary es
        ON owt.client_offer_id = es.client_offer_id
    GROUP BY owt.product_type
    HAVING COUNT(owt.client_offer_id) > 0
)
SELECT
    pa.product_type,
    pa.total_offers,
    pa.opened_offers,
    pa.accepted_offers,
    pa.rejected_offers,
    CASE WHEN pa.total_offers > 0 THEN CAST(pa.accepted_offers AS DOUBLE) / pa.total_offers ELSE 0.0 END AS conversion_rate
FROM product_aggregates pa
ORDER BY conversion_rate DESC, total_offers DESC
"""


if __name__ == "__main__":
    run_sql(
        sql_text=SQL_QUERY,
        app_name="etl_q09_product_conversion_analysis",
        runtime_options=parse_runtime_options(),
    )
