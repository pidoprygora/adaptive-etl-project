from common import parse_runtime_options, run_sql


SQL_QUERY = """
INSERT INTO adaptive_etl_bank.etl_metrics_aggregation
WITH successful_runs AS (
    SELECT
        em.dataset_size,
        em.extract_time_sec,
        em.transform_time_sec,
        em.load_time_sec,
        em.total_execution_time_sec,
        em.parallel_tasks_count,
        COALESCE(em.measured_cpu_utilization, em.cpu_utilization) AS effective_cpu_utilization,
        COALESCE(em.measured_ram_utilization, em.ram_utilization) AS effective_ram_utilization,
        em.speedup,
        em.efficiency,
        em.amdahl_speedup,
        em.load_balance_coeff,
        em.task_load,
        em.critical_path_time_sec,
        em.etl_time_sec,
        em.predicted_time_new_sec
    FROM adaptive_etl_bank.etl_execution_metrics em
    WHERE em.status = 'success'
),
aggregated AS (
    SELECT
        sr.dataset_size,
        AVG(sr.extract_time_sec) AS avg_extract_time_sec,
        MIN(sr.extract_time_sec) AS min_extract_time_sec,
        MAX(sr.extract_time_sec) AS max_extract_time_sec,
        AVG(sr.transform_time_sec) AS avg_transform_time_sec,
        MIN(sr.transform_time_sec) AS min_transform_time_sec,
        MAX(sr.transform_time_sec) AS max_transform_time_sec,
        AVG(sr.load_time_sec) AS avg_load_time_sec,
        MIN(sr.load_time_sec) AS min_load_time_sec,
        MAX(sr.load_time_sec) AS max_load_time_sec,
        AVG(sr.total_execution_time_sec) AS avg_total_time_sec,
        MIN(sr.total_execution_time_sec) AS min_total_time_sec,
        MAX(sr.total_execution_time_sec) AS max_total_time_sec,
        AVG(CAST(sr.parallel_tasks_count AS DOUBLE)) AS avg_parallel_tasks,
        AVG(sr.effective_cpu_utilization) AS avg_cpu_utilization,
        AVG(sr.effective_ram_utilization) AS avg_ram_utilization,
        AVG(sr.speedup) AS avg_speedup,
        AVG(sr.efficiency) AS avg_efficiency,
        AVG(sr.amdahl_speedup) AS avg_amdahl_speedup,
        AVG(sr.load_balance_coeff) AS avg_load_balance_coeff,
        AVG(sr.task_load) AS avg_task_load,
        AVG(sr.critical_path_time_sec) AS avg_critical_path_time_sec,
        AVG(sr.etl_time_sec) AS avg_etl_time_sec,
        AVG(sr.predicted_time_new_sec) AS avg_predicted_time_new_sec
    FROM successful_runs sr
    GROUP BY sr.dataset_size
)
SELECT
    a.dataset_size,
    a.avg_extract_time_sec,
    a.min_extract_time_sec,
    a.max_extract_time_sec,
    a.avg_transform_time_sec,
    a.min_transform_time_sec,
    a.max_transform_time_sec,
    a.avg_load_time_sec,
    a.min_load_time_sec,
    a.max_load_time_sec,
    a.avg_total_time_sec,
    a.min_total_time_sec,
    a.max_total_time_sec,
    a.avg_parallel_tasks,
    a.avg_cpu_utilization,
    a.avg_ram_utilization,
    a.avg_speedup,
    a.avg_efficiency,
    a.avg_amdahl_speedup,
    a.avg_load_balance_coeff,
    a.avg_task_load,
    a.avg_critical_path_time_sec,
    a.avg_etl_time_sec,
    a.avg_predicted_time_new_sec
FROM aggregated a
"""


if __name__ == "__main__":
    run_sql(
        sql_text=SQL_QUERY,
        app_name="etl_q18_etl_metrics_aggregation",
        runtime_options=parse_runtime_options(),
    )
