from common import parse_runtime_options, run_sql


SQL_QUERY = """
INSERT INTO adaptive_etl_bank.app_behavior_offer_recommendation
WITH screen_views_30d AS (
    SELECT
        ac.client_id,
        ac.screen_name,
        COUNT(ac.app_event_id) AS views_count
    FROM adaptive_etl_bank.app_clickstream ac
    WHERE DATE(ac.event_time) >= DATE_ADD('day', -30, CURRENT_DATE)
    GROUP BY ac.client_id, ac.screen_name
),
top_screen AS (
    SELECT client_id, screen_name, views_count
    FROM (
        SELECT
            sv.client_id,
            sv.screen_name,
            sv.views_count,
            ROW_NUMBER() OVER (
                PARTITION BY sv.client_id
                ORDER BY sv.views_count DESC, sv.screen_name ASC
            ) AS rn
        FROM screen_views_30d sv
    ) ranked_screens
    WHERE rn = 1
),
recommended_type AS (
    SELECT
        ts.client_id,
        ts.screen_name AS top_screen_name,
        ts.views_count AS top_screen_views,
        CASE
            WHEN ts.screen_name = 'loans_screen' THEN 'cash_loan'
            WHEN ts.screen_name = 'cards_screen' THEN 'credit_card'
            WHEN ts.screen_name = 'deposits_screen' THEN 'deposit'
            WHEN ts.screen_name = 'insurance_screen' THEN 'insurance'
            ELSE 'credit_card'
        END AS recommended_product_type
    FROM top_screen ts
),
ranked_offers AS (
    SELECT
        p.product_type,
        o.offer_id,
        o.offer_name,
        ROW_NUMBER() OVER (
            PARTITION BY p.product_type
            ORDER BY o.limit_amount DESC, o.interest_rate ASC, o.offer_id ASC
        ) AS rn
    FROM adaptive_etl_bank.offers o
    JOIN adaptive_etl_bank.products p
        ON o.product_id = p.product_id
    WHERE o.is_active = true
)
SELECT
    rt.client_id,
    rt.top_screen_name,
    rt.top_screen_views,
    rt.recommended_product_type,
    ro.offer_id,
    ro.offer_name
FROM recommended_type rt
LEFT JOIN ranked_offers ro
    ON rt.recommended_product_type = ro.product_type
   AND ro.rn = 1
"""


if __name__ == "__main__":
    run_sql(
        sql_text=SQL_QUERY,
        app_name="etl_q11_app_behavior_offer_recommendation",
        runtime_options=parse_runtime_options(),
    )
