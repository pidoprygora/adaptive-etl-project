from common import parse_runtime_options, run_sql


SQL_QUERY = """
INSERT INTO adaptive_etl_bank.high_value_clients_top1000
WITH turnover_180d AS (
    SELECT
        cp.client_id,
        COALESCE(SUM(t.amount), 0.0) AS total_turnover_180d
    FROM adaptive_etl_bank.client_products cp
    LEFT JOIN adaptive_etl_bank.transactions t
        ON cp.client_product_id = t.client_product_id
       AND t.status = 'successful'
       AND DATE(t.transaction_date) >= DATE_ADD('day', -180, CURRENT_DATE)
    GROUP BY cp.client_id
),
balance_and_products AS (
    SELECT
        cp.client_id,
        AVG(CASE WHEN cp.status = 'active' THEN COALESCE(cp.balance, 0.0) END) AS avg_balance,
        COUNT_IF(cp.status = 'active') AS active_products_count
    FROM adaptive_etl_bank.client_products cp
    GROUP BY cp.client_id
),
accepted_offers AS (
    SELECT
        co.client_id,
        COUNT_IF(co.offer_status = 'accepted') AS accepted_offers_count
    FROM adaptive_etl_bank.client_offers co
    GROUP BY co.client_id
),
app_activity AS (
    SELECT
        ac.client_id,
        COUNT(ac.app_event_id) AS app_events_90d
    FROM adaptive_etl_bank.app_clickstream ac
    WHERE DATE(ac.event_time) >= DATE_ADD('day', -90, CURRENT_DATE)
    GROUP BY ac.client_id
),
scored_clients AS (
    SELECT
        c.client_id,
        c.full_name,
        COALESCE(t.total_turnover_180d, 0.0) AS total_turnover_180d,
        COALESCE(bp.avg_balance, 0.0) AS avg_balance,
        COALESCE(bp.active_products_count, 0) AS active_products_count,
        COALESCE(ao.accepted_offers_count, 0) AS accepted_offers_count,
        COALESCE(aa.app_events_90d, 0) AS app_events_90d,
        (
            COALESCE(t.total_turnover_180d, 0.0) / 100000.0
            + COALESCE(bp.avg_balance, 0.0) / 50000.0
            + COALESCE(bp.active_products_count, 0) * 0.5
            + COALESCE(ao.accepted_offers_count, 0) * 0.7
            + COALESCE(aa.app_events_90d, 0) / 20.0
        ) AS value_score
    FROM adaptive_etl_bank.clients c
    LEFT JOIN turnover_180d t
        ON c.client_id = t.client_id
    LEFT JOIN balance_and_products bp
        ON c.client_id = bp.client_id
    LEFT JOIN accepted_offers ao
        ON c.client_id = ao.client_id
    LEFT JOIN app_activity aa
        ON c.client_id = aa.client_id
    WHERE c.is_active = true
),
ranked_clients AS (
    SELECT
        sc.client_id,
        sc.full_name,
        sc.total_turnover_180d,
        sc.avg_balance,
        sc.active_products_count,
        sc.accepted_offers_count,
        sc.app_events_90d,
        sc.value_score,
        ROW_NUMBER() OVER (
            ORDER BY sc.value_score DESC, sc.total_turnover_180d DESC, sc.client_id ASC
        ) AS rn
    FROM scored_clients sc
)
SELECT
    rc.client_id,
    rc.full_name,
    rc.total_turnover_180d,
    rc.avg_balance,
    rc.active_products_count,
    rc.accepted_offers_count,
    rc.app_events_90d,
    rc.value_score
FROM ranked_clients rc
WHERE rc.rn <= 1000
"""


if __name__ == "__main__":
    run_sql(
        sql_text=SQL_QUERY,
        app_name="etl_q13_high_value_clients_top1000",
        runtime_options=parse_runtime_options(),
    )
