from common import parse_runtime_options, run_sql


SQL_QUERY = """
INSERT INTO adaptive_etl_bank.expired_offer_cleanup_candidate
WITH expired_candidates AS (
    SELECT
        co.client_offer_id,
        co.client_id,
        co.offer_id,
        co.campaign_id,
        co.offer_status,
        co.valid_until
    FROM adaptive_etl_bank.client_offers co
    WHERE co.valid_until < CURRENT_DATE
),
filtered_candidates AS (
    SELECT
        ec.client_offer_id,
        ec.client_id,
        ec.offer_id,
        ec.campaign_id,
        ec.valid_until
    FROM expired_candidates ec
    WHERE ec.offer_status NOT IN ('accepted', 'rejected', 'expired')
)
SELECT
    fc.client_offer_id,
    fc.client_id,
    fc.offer_id,
    fc.campaign_id,
    fc.valid_until,
    'expired' AS new_offer_status
FROM filtered_candidates fc
"""


if __name__ == "__main__":
    run_sql(
        sql_text=SQL_QUERY,
        app_name="etl_q15_expired_offer_cleanup_candidate",
        runtime_options=parse_runtime_options(),
    )
