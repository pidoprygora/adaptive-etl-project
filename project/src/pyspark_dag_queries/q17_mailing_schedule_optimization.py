from common import parse_runtime_options, run_sql


SQL_QUERY = """
INSERT INTO adaptive_etl_bank.mailing_schedule_optimization
WITH planned_base AS (
    SELECT
        mb.client_id,
        mb.campaign_id,
        mb.offer_id,
        mb.channel,
        mb.priority,
        mb.planned_send_date
    FROM adaptive_etl_bank.mailing_base mb
    WHERE mb.mailing_status = 'planned'
),
scheduled AS (
    SELECT
        pb.client_id,
        pb.campaign_id,
        pb.offer_id,
        pb.channel,
        pb.priority,
        pb.planned_send_date,
        CASE
            WHEN pb.priority = 1 THEN '09:00'
            WHEN pb.priority = 2 THEN '11:00'
            WHEN pb.priority = 3 THEN '14:00'
            ELSE '16:00'
        END AS schedule_hour,
        CASE
            WHEN pb.priority = 1 THEN CONCAT(CAST(pb.planned_send_date AS VARCHAR), ' 09:00')
            WHEN pb.priority = 2 THEN CONCAT(CAST(pb.planned_send_date AS VARCHAR), ' 11:00')
            WHEN pb.priority = 3 THEN CONCAT(CAST(pb.planned_send_date AS VARCHAR), ' 14:00')
            ELSE CONCAT(CAST(pb.planned_send_date AS VARCHAR), ' 16:00')
        END AS schedule_batch
    FROM planned_base pb
)
SELECT
    s.client_id,
    s.campaign_id,
    s.offer_id,
    s.channel,
    s.priority,
    s.planned_send_date,
    s.schedule_hour,
    s.schedule_batch
FROM scheduled s
"""


if __name__ == "__main__":
    run_sql(
        sql_text=SQL_QUERY,
        app_name="etl_q17_mailing_schedule_optimization",
        runtime_options=parse_runtime_options(),
    )
