from common import parse_runtime_options, run_sql


SQL_QUERY = """
INSERT INTO adaptive_etl_bank.campaign_readiness_check
WITH active_campaigns AS (
    SELECT
        ca.campaign_id,
        ca.campaign_name
    FROM adaptive_etl_bank.campaigns ca
    WHERE ca.status = 'active'
      AND CURRENT_DATE BETWEEN ca.start_date AND ca.end_date
),
offers_count AS (
    SELECT
        co.campaign_id,
        COUNT(co.offer_id) AS offers_count
    FROM adaptive_etl_bank.campaign_offers co
    GROUP BY co.campaign_id
),
mailing_clients AS (
    SELECT
        mb.campaign_id,
        COUNT(DISTINCT mb.client_id) AS mailing_clients_count
    FROM adaptive_etl_bank.mailing_base mb
    GROUP BY mb.campaign_id
),
valid_contacts AS (
    SELECT
        mb.campaign_id,
        COUNT(DISTINCT mb.client_id) AS valid_contacts_count
    FROM adaptive_etl_bank.mailing_base mb
    JOIN adaptive_etl_bank.client_contacts cc
        ON mb.client_id = cc.client_id
    WHERE cc.is_verified = true
      AND (
          cc.email IS NOT NULL
          OR cc.phone IS NOT NULL
          OR cc.push_token IS NOT NULL
      )
    GROUP BY mb.campaign_id
),
duplicates AS (
    SELECT
        co.campaign_id,
        COALESCE(SUM(CASE WHEN duplicate_count > 1 THEN duplicate_count - 1 ELSE 0 END), 0) AS duplicate_count
    FROM (
        SELECT
            co_inner.campaign_id,
            co_inner.client_id,
            co_inner.offer_id,
            COUNT(1) AS duplicate_count
        FROM adaptive_etl_bank.client_offers co_inner
        GROUP BY co_inner.campaign_id, co_inner.client_id, co_inner.offer_id
    ) co
    GROUP BY co.campaign_id
),
expired_offers AS (
    SELECT
        co.campaign_id,
        COUNT_IF(co.valid_until < CURRENT_DATE AND co.offer_status NOT IN ('accepted', 'rejected')) AS expired_offer_count
    FROM adaptive_etl_bank.client_offers co
    GROUP BY co.campaign_id
)
SELECT
    ac.campaign_id,
    ac.campaign_name,
    COALESCE(oc.offers_count, 0) AS offers_count,
    COALESCE(mc.mailing_clients_count, 0) AS mailing_clients_count,
    COALESCE(vc.valid_contacts_count, 0) AS valid_contacts_count,
    COALESCE(d.duplicate_count, 0) AS duplicate_count,
    COALESCE(eo.expired_offer_count, 0) AS expired_offer_count,
    CASE
        WHEN COALESCE(oc.offers_count, 0) = 0
             OR COALESCE(mc.mailing_clients_count, 0) = 0
             OR COALESCE(vc.valid_contacts_count, 0) = 0 THEN 'blocked'
        WHEN COALESCE(d.duplicate_count, 0) > 0
             OR COALESCE(eo.expired_offer_count, 0) > 0 THEN 'warning'
        ELSE 'ready'
    END AS readiness_status
FROM active_campaigns ac
LEFT JOIN offers_count oc
    ON ac.campaign_id = oc.campaign_id
LEFT JOIN mailing_clients mc
    ON ac.campaign_id = mc.campaign_id
LEFT JOIN valid_contacts vc
    ON ac.campaign_id = vc.campaign_id
LEFT JOIN duplicates d
    ON ac.campaign_id = d.campaign_id
LEFT JOIN expired_offers eo
    ON ac.campaign_id = eo.campaign_id
"""


if __name__ == "__main__":
    run_sql(
        sql_text=SQL_QUERY,
        app_name="etl_q20_campaign_readiness_check",
        runtime_options=parse_runtime_options(),
    )
