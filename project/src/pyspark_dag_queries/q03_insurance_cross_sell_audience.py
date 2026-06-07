from common import parse_runtime_options, run_sql


SQL_QUERY = """
INSERT INTO adaptive_etl_bank.insurance_cross_sell_audience
WITH active_clients AS (
    SELECT c.client_id, c.full_name
    FROM adaptive_etl_bank.clients c
    WHERE c.is_active = true
),
clients_with_credit_products AS (
    SELECT DISTINCT cp.client_id
    FROM adaptive_etl_bank.client_products cp
    JOIN adaptive_etl_bank.products p
        ON cp.product_id = p.product_id
    WHERE cp.status = 'active'
      AND p.product_type IN ('credit_card', 'mortgage', 'cash_loan')
),
clients_with_insurance AS (
    SELECT DISTINCT cp.client_id
    FROM adaptive_etl_bank.client_products cp
    JOIN adaptive_etl_bank.products p
        ON cp.product_id = p.product_id
    WHERE cp.status = 'active'
      AND p.product_type = 'insurance'
),
insurance_screen_activity AS (
    SELECT
        ac.client_id,
        COUNT_IF(ac.screen_name = 'insurance_screen') AS insurance_screen_views
    FROM adaptive_etl_bank.app_clickstream ac
    WHERE DATE(ac.event_time) >= DATE_ADD('day', -90, CURRENT_DATE)
    GROUP BY ac.client_id
),
category_spending AS (
    SELECT
        cp.client_id,
        COALESCE(SUM(CASE WHEN t.merchant_category = 'travel' THEN t.amount ELSE 0.0 END), 0.0) AS travel_amount,
        COALESCE(SUM(CASE WHEN t.merchant_category = 'pharmacy' THEN t.amount ELSE 0.0 END), 0.0) AS pharmacy_amount,
        COALESCE(SUM(CASE WHEN t.merchant_category = 'fuel' THEN t.amount ELSE 0.0 END), 0.0) AS fuel_amount
    FROM adaptive_etl_bank.client_products cp
    LEFT JOIN adaptive_etl_bank.transactions t
        ON cp.client_product_id = t.client_product_id
       AND t.status = 'successful'
       AND DATE(t.transaction_date) >= DATE_ADD('day', -90, CURRENT_DATE)
    GROUP BY cp.client_id
),
insurance_offer AS (
    SELECT offer_id, offer_name
    FROM (
        SELECT
            o.offer_id,
            o.offer_name,
            ROW_NUMBER() OVER (ORDER BY o.limit_amount DESC, o.offer_id ASC) AS rn
        FROM adaptive_etl_bank.offers o
        JOIN adaptive_etl_bank.products p
            ON o.product_id = p.product_id
        WHERE o.is_active = true
          AND p.product_type = 'insurance'
    ) ranked_offer
    WHERE rn = 1
)
SELECT
    ac.client_id,
    ac.full_name,
    io.offer_id,
    io.offer_name,
    COALESCE(isa.insurance_screen_views, 0) AS insurance_screen_views,
    COALESCE(cs.travel_amount, 0.0) + COALESCE(cs.pharmacy_amount, 0.0) + COALESCE(cs.fuel_amount, 0.0) AS lifestyle_spending_amount,
    CASE
        WHEN COALESCE(isa.insurance_screen_views, 0) >= 5 THEN 3
        WHEN COALESCE(isa.insurance_screen_views, 0) >= 2 THEN 2
        ELSE 1
    END
    + CASE
        WHEN COALESCE(cs.travel_amount, 0.0) + COALESCE(cs.pharmacy_amount, 0.0) + COALESCE(cs.fuel_amount, 0.0) >= 40000 THEN 3
        WHEN COALESCE(cs.travel_amount, 0.0) + COALESCE(cs.pharmacy_amount, 0.0) + COALESCE(cs.fuel_amount, 0.0) >= 15000 THEN 2
        ELSE 1
    END AS insurance_score
FROM active_clients ac
JOIN clients_with_credit_products ccp
    ON ac.client_id = ccp.client_id
LEFT JOIN clients_with_insurance cwi
    ON ac.client_id = cwi.client_id
LEFT JOIN insurance_screen_activity isa
    ON ac.client_id = isa.client_id
LEFT JOIN category_spending cs
    ON ac.client_id = cs.client_id
JOIN insurance_offer io
    ON 1 = 1
WHERE cwi.client_id IS NULL
"""


if __name__ == "__main__":
    run_sql(
        sql_text=SQL_QUERY,
        app_name="etl_q03_insurance_cross_sell_audience",
        runtime_options=parse_runtime_options(),
    )
