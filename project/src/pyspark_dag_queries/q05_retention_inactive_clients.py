from common import parse_runtime_options, run_sql


SQL_QUERY = """
INSERT INTO adaptive_etl_bank.retention_inactive_clients
WITH active_clients AS (
    SELECT c.client_id, c.full_name
    FROM adaptive_etl_bank.clients c
    WHERE c.is_active = true
),
last_transaction AS (
    SELECT
        cp.client_id,
        MAX(DATE(t.transaction_date)) AS last_transaction_date
    FROM adaptive_etl_bank.client_products cp
    LEFT JOIN adaptive_etl_bank.transactions t
        ON cp.client_product_id = t.client_product_id
    GROUP BY cp.client_id
),
last_app_event AS (
    SELECT
        ac.client_id,
        MAX(DATE(ac.event_time)) AS last_app_event_date
    FROM adaptive_etl_bank.app_clickstream ac
    GROUP BY ac.client_id
),
inactive_clients AS (
    SELECT
        c.client_id,
        c.full_name,
        lt.last_transaction_date,
        la.last_app_event_date,
        GREATEST(
            COALESCE(lt.last_transaction_date, DATE_ADD('day', -9999, CURRENT_DATE)),
            COALESCE(la.last_app_event_date, DATE_ADD('day', -9999, CURRENT_DATE))
        ) AS last_activity_date
    FROM active_clients c
    LEFT JOIN last_transaction lt
        ON c.client_id = lt.client_id
    LEFT JOIN last_app_event la
        ON c.client_id = la.client_id
    WHERE GREATEST(
              COALESCE(lt.last_transaction_date, DATE_ADD('day', -9999, CURRENT_DATE)),
              COALESCE(la.last_app_event_date, DATE_ADD('day', -9999, CURRENT_DATE))
          ) < DATE_ADD('day', -60, CURRENT_DATE)
),
last_active_product AS (
    SELECT client_id, product_name, product_type
    FROM (
        SELECT
            cp.client_id,
            p.product_name,
            p.product_type,
            ROW_NUMBER() OVER (
                PARTITION BY cp.client_id
                ORDER BY COALESCE(cp.last_activity_date, cp.open_date) DESC
            ) AS rn
        FROM adaptive_etl_bank.client_products cp
        JOIN adaptive_etl_bank.products p
            ON cp.product_id = p.product_id
        WHERE cp.status = 'active'
    ) ranked_products
    WHERE rn = 1
),
retention_offer AS (
    SELECT offer_id, offer_name
    FROM (
        SELECT
            o.offer_id,
            o.offer_name,
            ROW_NUMBER() OVER (ORDER BY o.offer_id ASC) AS rn
        FROM adaptive_etl_bank.offers o
        WHERE o.is_active = true
          AND LOWER(o.offer_name) LIKE '%retention%'
    ) ranked_retention
    WHERE rn = 1
)
SELECT
    ic.client_id,
    ic.full_name,
    ic.last_transaction_date,
    ic.last_app_event_date,
    ic.last_activity_date,
    lap.product_name AS last_active_product_name,
    lap.product_type AS last_active_product_type,
    ro.offer_id,
    COALESCE(ro.offer_name, 'retention_campaign_default') AS offer_name
FROM inactive_clients ic
LEFT JOIN last_active_product lap
    ON ic.client_id = lap.client_id
LEFT JOIN retention_offer ro
    ON 1 = 1
"""


if __name__ == "__main__":
    run_sql(
        sql_text=SQL_QUERY,
        app_name="etl_q05_retention_inactive_clients",
        runtime_options=parse_runtime_options(),
    )
