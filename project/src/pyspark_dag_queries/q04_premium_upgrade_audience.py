from common import parse_runtime_options, run_sql


SQL_QUERY = """
INSERT INTO adaptive_etl_bank.premium_upgrade_audience
WITH latest_segments AS (
    SELECT client_id, segment_name
    FROM (
        SELECT
            cs.client_id,
            cs.segment_name,
            ROW_NUMBER() OVER (PARTITION BY cs.client_id ORDER BY cs.updated_at DESC) AS rn
        FROM adaptive_etl_bank.client_segments cs
    ) ranked_segments
    WHERE rn = 1
      AND segment_name IN ('mass', 'salary')
),
turnover_180d AS (
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
active_products AS (
    SELECT
        cp.client_id,
        COUNT_IF(cp.status = 'active') AS active_products_count,
        AVG(CASE WHEN cp.status = 'active' THEN COALESCE(cp.balance, 0.0) END) AS avg_balance
    FROM adaptive_etl_bank.client_products cp
    GROUP BY cp.client_id
),
app_activity AS (
    SELECT
        ac.client_id,
        COUNT(ac.app_event_id) AS app_events_90d
    FROM adaptive_etl_bank.app_clickstream ac
    WHERE DATE(ac.event_time) >= DATE_ADD('day', -90, CURRENT_DATE)
    GROUP BY ac.client_id
),
scored AS (
    SELECT
        ls.client_id,
        ls.segment_name,
        COALESCE(t.total_turnover_180d, 0.0) AS total_turnover_180d,
        COALESCE(ap.active_products_count, 0) AS active_products_count,
        COALESCE(ap.avg_balance, 0.0) AS avg_balance,
        COALESCE(aa.app_events_90d, 0) AS app_events_90d,
        CASE
            WHEN COALESCE(t.total_turnover_180d, 0.0) >= 400000 THEN 3
            WHEN COALESCE(t.total_turnover_180d, 0.0) >= 150000 THEN 2
            ELSE 1
        END
        + CASE
            WHEN COALESCE(ap.active_products_count, 0) >= 4 THEN 2
            WHEN COALESCE(ap.active_products_count, 0) >= 2 THEN 1
            ELSE 0
        END
        + CASE
            WHEN COALESCE(ap.avg_balance, 0.0) >= 120000 THEN 2
            WHEN COALESCE(ap.avg_balance, 0.0) >= 50000 THEN 1
            ELSE 0
        END
        + CASE
            WHEN COALESCE(aa.app_events_90d, 0) >= 40 THEN 2
            WHEN COALESCE(aa.app_events_90d, 0) >= 15 THEN 1
            ELSE 0
        END AS premium_score
    FROM latest_segments ls
    LEFT JOIN turnover_180d t
        ON ls.client_id = t.client_id
    LEFT JOIN active_products ap
        ON ls.client_id = ap.client_id
    LEFT JOIN app_activity aa
        ON ls.client_id = aa.client_id
)
SELECT
    s.client_id,
    s.segment_name,
    s.total_turnover_180d,
    s.active_products_count,
    s.avg_balance,
    s.app_events_90d,
    s.premium_score
FROM scored s
WHERE s.premium_score > 2
"""


if __name__ == "__main__":
    run_sql(
        sql_text=SQL_QUERY,
        app_name="etl_q04_premium_upgrade_audience",
        runtime_options=parse_runtime_options(),
    )
