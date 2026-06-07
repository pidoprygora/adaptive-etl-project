from common import parse_runtime_options, run_sql


SQL_QUERY = """
INSERT INTO adaptive_etl_bank.deposit_campaign_target_audience
WITH active_clients AS (
    SELECT c.client_id, c.full_name
    FROM adaptive_etl_bank.clients c
    WHERE c.is_active = true
),
latest_segments AS (
    SELECT client_id, segment_name
    FROM (
        SELECT
            cs.client_id,
            cs.segment_name,
            ROW_NUMBER() OVER (PARTITION BY cs.client_id ORDER BY cs.updated_at DESC) AS rn
        FROM adaptive_etl_bank.client_segments cs
    ) ranked_segments
    WHERE rn = 1
),
average_balance AS (
    SELECT
        cp.client_id,
        AVG(COALESCE(cp.balance, 0.0)) AS avg_balance
    FROM adaptive_etl_bank.client_products cp
    WHERE cp.status = 'active'
    GROUP BY cp.client_id
    HAVING AVG(COALESCE(cp.balance, 0.0)) >= 50000
),
existing_deposit_clients AS (
    SELECT DISTINCT cp.client_id
    FROM adaptive_etl_bank.client_products cp
    JOIN adaptive_etl_bank.products p
        ON cp.product_id = p.product_id
    WHERE cp.status = 'active'
      AND p.product_type = 'deposit'
),
salary_transactions_180d AS (
    SELECT
        cp.client_id,
        COUNT_IF(t.transaction_type = 'salary' AND t.status = 'successful') AS salary_transactions_count
    FROM adaptive_etl_bank.client_products cp
    LEFT JOIN adaptive_etl_bank.transactions t
        ON cp.client_product_id = t.client_product_id
       AND DATE(t.transaction_date) >= DATE_ADD('day', -180, CURRENT_DATE)
    GROUP BY cp.client_id
),
deposit_screen_activity AS (
    SELECT
        ac.client_id,
        COUNT_IF(ac.screen_name = 'deposits_screen') AS deposits_screen_views
    FROM adaptive_etl_bank.app_clickstream ac
    WHERE DATE(ac.event_time) >= DATE_ADD('day', -90, CURRENT_DATE)
    GROUP BY ac.client_id
),
deposit_offer AS (
    SELECT offer_id, channel_hint
    FROM (
        SELECT
            o.offer_id,
            CASE
                WHEN o.min_income >= 50000 THEN 'manager_call'
                ELSE 'email'
            END AS channel_hint,
            ROW_NUMBER() OVER (ORDER BY o.interest_rate DESC, o.offer_id ASC) AS rn
        FROM adaptive_etl_bank.offers o
        JOIN adaptive_etl_bank.products p
            ON o.product_id = p.product_id
        WHERE o.is_active = true
          AND p.product_type = 'deposit'
    ) ranked_offer
    WHERE rn = 1
),
scored_clients AS (
    SELECT
        ac.client_id,
        ac.full_name,
        ls.segment_name,
        COALESCE(ab.avg_balance, 0.0) AS avg_balance,
        COALESCE(st.salary_transactions_count, 0) AS salary_transactions_count,
        COALESCE(dsa.deposits_screen_views, 0) AS deposits_screen_views,
        CASE
            WHEN COALESCE(ab.avg_balance, 0.0) >= 200000 THEN 3
            WHEN COALESCE(ab.avg_balance, 0.0) >= 100000 THEN 2
            ELSE 1
        END
        + CASE
            WHEN COALESCE(st.salary_transactions_count, 0) >= 4 THEN 2
            WHEN COALESCE(st.salary_transactions_count, 0) >= 2 THEN 1
            ELSE 0
        END
        + CASE
            WHEN COALESCE(dsa.deposits_screen_views, 0) >= 5 THEN 2
            WHEN COALESCE(dsa.deposits_screen_views, 0) >= 2 THEN 1
            ELSE 0
        END AS deposit_score
    FROM active_clients ac
    JOIN average_balance ab
        ON ac.client_id = ab.client_id
    LEFT JOIN latest_segments ls
        ON ac.client_id = ls.client_id
    LEFT JOIN salary_transactions_180d st
        ON ac.client_id = st.client_id
    LEFT JOIN deposit_screen_activity dsa
        ON ac.client_id = dsa.client_id
    LEFT JOIN existing_deposit_clients ed
        ON ac.client_id = ed.client_id
    WHERE ed.client_id IS NULL
)
SELECT
    sc.client_id,
    sc.full_name,
    COALESCE(sc.segment_name, 'unclassified') AS segment_name,
    sc.avg_balance,
    sc.salary_transactions_count,
    sc.deposit_score,
    d.offer_id,
    d.channel_hint AS channel
FROM scored_clients sc
JOIN deposit_offer d
    ON 1 = 1
"""


if __name__ == "__main__":
    run_sql(
        sql_text=SQL_QUERY,
        app_name="etl_q02_deposit_campaign_target_audience",
        runtime_options=parse_runtime_options(),
    )
