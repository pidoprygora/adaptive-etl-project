from common import parse_runtime_options, run_sql


SQL_QUERY = """
INSERT INTO adaptive_etl_bank.credit_campaign_target_audience
WITH active_clients AS (
    SELECT c.client_id, c.person_code, c.full_name
    FROM adaptive_etl_bank.clients c
    WHERE c.is_active = true
),
latest_contacts AS (
    SELECT client_id, phone, email, preferred_channel, is_verified
    FROM (
        SELECT
            cc.client_id,
            cc.phone,
            cc.email,
            cc.preferred_channel,
            cc.is_verified,
            ROW_NUMBER() OVER (
                PARTITION BY cc.client_id
                ORDER BY cc.updated_at DESC
            ) AS rn
        FROM adaptive_etl_bank.client_contacts cc
    ) ranked_contacts
    WHERE rn = 1
),
latest_segments AS (
    SELECT client_id, segment_name
    FROM (
        SELECT
            cs.client_id,
            cs.segment_name,
            ROW_NUMBER() OVER (
                PARTITION BY cs.client_id
                ORDER BY cs.updated_at DESC
            ) AS rn
        FROM adaptive_etl_bank.client_segments cs
    ) ranked_segments
    WHERE rn = 1
),
excluded_credit_clients AS (
    SELECT DISTINCT cp.client_id
    FROM adaptive_etl_bank.client_products cp
    JOIN adaptive_etl_bank.products p
        ON cp.product_id = p.product_id
    WHERE cp.status = 'active'
      AND p.product_type IN ('credit_card', 'cash_loan')
),
turnover_90d AS (
    SELECT
        cp.client_id,
        COALESCE(SUM(t.amount), 0.0) AS total_amount_90d
    FROM adaptive_etl_bank.client_products cp
    JOIN adaptive_etl_bank.transactions t
        ON cp.client_product_id = t.client_product_id
    WHERE t.status = 'successful'
      AND DATE(t.transaction_date) >= DATE_ADD('day', -90, CURRENT_DATE)
    GROUP BY cp.client_id
),
screen_activity AS (
    SELECT
        ac.client_id,
        COUNT_IF(ac.screen_name = 'loans_screen') AS loans_screen_views,
        COUNT_IF(ac.screen_name = 'cards_screen') AS cards_screen_views
    FROM adaptive_etl_bank.app_clickstream ac
    WHERE DATE(ac.event_time) >= DATE_ADD('day', -90, CURRENT_DATE)
    GROUP BY ac.client_id
),
credit_offers AS (
    SELECT
        o.offer_id,
        o.offer_name,
        p.product_type,
        ROW_NUMBER() OVER (
            PARTITION BY p.product_type
            ORDER BY o.limit_amount DESC, o.interest_rate ASC, o.offer_id ASC
        ) AS rn
    FROM adaptive_etl_bank.offers o
    JOIN adaptive_etl_bank.products p
        ON o.product_id = p.product_id
    WHERE o.is_active = true
      AND p.product_type IN ('credit_card', 'cash_loan')
),
best_credit_offer AS (
    SELECT offer_id, offer_name, product_type
    FROM credit_offers
    WHERE rn = 1
),
audience_scored AS (
    SELECT
        ac.client_id,
        ac.person_code,
        ac.full_name,
        lc.phone,
        lc.email,
        ls.segment_name,
        COALESCE(t.total_amount_90d, 0.0) AS total_amount_90d,
        COALESCE(sa.loans_screen_views, 0) AS loans_screen_views,
        COALESCE(sa.cards_screen_views, 0) AS cards_screen_views,
        CASE
            WHEN COALESCE(t.total_amount_90d, 0.0) >= 200000 THEN 3
            WHEN COALESCE(t.total_amount_90d, 0.0) >= 80000 THEN 2
            ELSE 1
        END
        + CASE
            WHEN COALESCE(sa.loans_screen_views, 0) + COALESCE(sa.cards_screen_views, 0) >= 10 THEN 2
            WHEN COALESCE(sa.loans_screen_views, 0) + COALESCE(sa.cards_screen_views, 0) >= 3 THEN 1
            ELSE 0
        END
        + CASE
            WHEN ls.segment_name IN ('vip', 'premium') THEN 2
            WHEN ls.segment_name = 'salary' THEN 1
            ELSE 0
        END AS credit_score
    FROM active_clients ac
    LEFT JOIN latest_contacts lc
        ON ac.client_id = lc.client_id
    LEFT JOIN latest_segments ls
        ON ac.client_id = ls.client_id
    LEFT JOIN turnover_90d t
        ON ac.client_id = t.client_id
    LEFT JOIN screen_activity sa
        ON ac.client_id = sa.client_id
    LEFT JOIN excluded_credit_clients ec
        ON ac.client_id = ec.client_id
    WHERE ec.client_id IS NULL
)
SELECT
    s.client_id,
    s.person_code,
    s.full_name,
    s.phone,
    s.email,
    COALESCE(s.segment_name, 'unclassified') AS segment_name,
    bco.offer_id,
    bco.offer_name,
    COALESCE(
        CASE
            WHEN s.phone IS NOT NULL THEN 'sms'
            WHEN s.email IS NOT NULL THEN 'email'
            ELSE 'push'
        END,
        'email'
    ) AS channel,
    s.credit_score,
    CASE
        WHEN s.credit_score >= 6 THEN 1
        WHEN s.credit_score >= 4 THEN 2
        ELSE 3
    END AS priority
FROM audience_scored s
JOIN best_credit_offer bco
    ON 1 = 1
"""


if __name__ == "__main__":
    run_sql(
        sql_text=SQL_QUERY,
        app_name="etl_q01_credit_campaign_target_audience",
        runtime_options=parse_runtime_options(),
    )
