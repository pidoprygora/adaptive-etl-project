from common import parse_runtime_options, run_sql


SQL_QUERY = """
INSERT INTO adaptive_etl_bank.adaptive_parallelism_recommendation
WITH metrics_base AS (
    SELECT
        ema.dataset_size,
        ema.avg_task_load AS task_integral_load,
        ema.avg_total_time_sec,
        ema.avg_parallel_tasks,
        ema.avg_cpu_utilization,
        ema.avg_ram_utilization,
        ema.avg_load_balance_coeff
    FROM adaptive_etl_bank.etl_metrics_aggregation ema
),
dag_load AS (
    SELECT AVG(mb.task_integral_load) AS dag_avg_load
    FROM metrics_base mb
),
recommended AS (
    SELECT
        mb.dataset_size,
        mb.task_integral_load,
        dl.dag_avg_load,
        mb.avg_total_time_sec,
        mb.avg_parallel_tasks,
        mb.avg_cpu_utilization,
        mb.avg_ram_utilization,
        mb.avg_load_balance_coeff,
        LEAST(
            8,
            GREATEST(
                1,
                CAST(CEIL(mb.task_integral_load / NULLIF(dl.dag_avg_load, 0.0)) AS INTEGER)
            )
        ) AS p_i,
        CASE
            WHEN mb.avg_cpu_utilization > 0.8 OR mb.avg_ram_utilization > 0.8
                THEN GREATEST(
                    1,
                    LEAST(8, CAST(CEIL(mb.task_integral_load / NULLIF(dl.dag_avg_load, 0.0)) AS INTEGER)) - 1
                )
            WHEN mb.avg_cpu_utilization < 0.5 AND mb.avg_ram_utilization < 0.5
                THEN LEAST(
                    8,
                    GREATEST(1, CAST(CEIL(mb.task_integral_load / NULLIF(dl.dag_avg_load, 0.0)) AS INTEGER)) + 1
                )
            ELSE LEAST(
                8,
                GREATEST(
                    1,
                    CAST(CEIL(mb.task_integral_load / NULLIF(dl.dag_avg_load, 0.0)) AS INTEGER)
                )
            )
        END AS recommended_parallel_tasks
    FROM metrics_base mb
    CROSS JOIN dag_load dl
)
SELECT
    r.dataset_size,
    r.task_integral_load,
    r.dag_avg_load,
    r.avg_total_time_sec,
    r.avg_parallel_tasks,
    r.avg_cpu_utilization,
    r.avg_ram_utilization,
    r.avg_load_balance_coeff AS load_balance_coeff,
    r.recommended_parallel_tasks
FROM recommended r
"""


if __name__ == "__main__":
    run_sql(
        sql_text=SQL_QUERY,
        app_name="etl_q19_adaptive_parallelism_recommendation",
        runtime_options=parse_runtime_options(),
    )
