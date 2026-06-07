# Adaptive ETL Algorithm Implementation

This document describes how the adaptive parallelization algorithm is implemented for Airflow DAG and PySpark ETL tasks.

## Runtime Architecture

1. **Data Source** -> raw domain tables.
2. **Airflow DAG** (`dags/adaptive_etl_q01_q20_production_dag.py`) starts run orchestration.
3. **Adaptive Scheduler** (`src/adaptive_scheduler/scheduler.py`) computes load, parallelism, and execution priority.
4. **PySpark ETL Tasks** (`src/pyspark_dag_queries/q01...q20`) receive runtime parameters from XCom.
5. **Metrics Storage** (`src/adaptive_scheduler/storage.py`) persists task-level and run-level metrics to S3.
6. **Execution Log** stores detailed per-run reports in `logs/`.
7. **Time Prediction Module** (`src/adaptive_scheduler/time_prediction.py`) predicts and smooths task duration.
8. **CloudWatch Agent** (on EC2 host) publishes CPU/RAM host metrics used by scheduler.
9. **Result Storage** writes transformed data into processed tables/layers.

## Formula Coverage

Implemented formulas and locations:

1. `L_i = alpha*D_i + beta*C_i + gamma*T_i`  
   `AdaptiveScheduler.calculate_task_load()`

2. `L_avg = (1 / n) * sum(L_i)`  
   `AdaptiveScheduler.calculate_average_load()`

3. `P_i = min(P_max, ceil(L_i / L_avg))`  
   `AdaptiveScheduler.recommended_parallelism()`

4. `U_cpu = CPU_used / CPU_total`  
   `resource_monitor.cpu_utilization()`

5. `U_ram = RAM_used / RAM_total`  
   `resource_monitor.ram_utilization()`

6. `T_pred = (1 / n) * sum(T_i)`  
   `time_prediction.average_prediction()`

7. `S = T_seq / T_par`  
   `AdaptiveScheduler.speedup()`

8. `E = S / P`  
   `AdaptiveScheduler.efficiency()`

9. `S_Amdahl = 1 / ((1 - q) + q / P)`  
   `AdaptiveScheduler.amdahl_speedup()`

10. `T_DAG = max(sum(T_i) for each DAG path)`  
    `dag_analysis.calculate_critical_path_time()`

11. `T_ETL = T_extract + T_transform + T_load` or branch form  
    `dag_analysis.calculate_etl_time()`

12. `B = L_max / L_avg`  
    `AdaptiveScheduler.load_balance_coefficient()`

13. Parallelism adaptation rule (`P_new`) by CPU/RAM thresholds  
    `resource_monitor.adapt_parallelism()`

14. `T_pred_new = lambda*T_actual + (1-lambda)*T_pred_old`  
    `time_prediction.update_prediction_with_smoothing()`  
    Used in `pyspark_dag_queries/common.py` when persisting run metrics.

15. Runtime resource signal strategy  
    - online planning: CloudWatch host snapshot (`cpu_usage_active`, `mem_used_percent`) with short lookback window;  
    - fallback: local host snapshot from `resource_monitor.collect_local_resource_snapshot()`;  
    - task history fallback: measured CPU/RAM history from `AdaptiveStorage.read_task_resource_history()`.

## Adaptive Execution Plan

In addition to executor allocation, scheduler now computes:

- **execution_order**: adaptive topological order.
- **execution_waves**: readiness layers (parallel waves).
- **priority_score/rank/wave** per task.

Priority is based on normalized load and predicted time, with extra sensitivity when load imbalance coefficient `B` is high.

## Airflow -> PySpark Runtime Contract

For each task, Airflow sends:

- base runtime params: task id, dataset size, executors, shuffle partitions, predicted time, run id.
- scheduler metrics: `L_i`, `L_avg`, `B`, `S`, `E`, `S_Amdahl`.
- execution metadata: priority score/rank and wave.
- run-level timing signals: `T_DAG`, `T_ETL`.

PySpark `common.py` persists these values in task metrics JSONL and updates smoothed prediction.
It now also stores resource fields separately:
- `planned_cpu_utilization`, `planned_ram_utilization` (from planning/XCom)
- `measured_cpu_utilization`, `measured_ram_utilization` (CloudWatch over task runtime when available)
- `cpu_utilization`, `ram_utilization` remain effective values for backward compatibility.

## S3 Structure

Expected layout:

```text
raw/
processed/
metrics/
logs/
```

- `metrics/etl_metrics_YYYY-MM-DD.jsonl`: task-level metrics per job.
- `metrics/etl_run_metrics_YYYY-MM-DD.jsonl`: run-level summary metrics.
- `logs/<run_id>_<timestamp>.json`: detailed run execution report.
- run-level report includes `resource_signal.source` (`cloudwatch`, `history_fallback`, `local_fallback`).

## Local Storage (inside project)

The same artifacts are also mirrored locally:

- default root: `.adaptive_runtime/`
- configurable via `ADAPTIVE_LOCAL_STORAGE_DIR`
- local layout mirrors S3 areas:
  - `.adaptive_runtime/logs/<prefix>/...`
  - `.adaptive_runtime/metrics/<prefix>/...`

If S3 is temporarily unavailable, storage methods return `file://...` URI of the local artifact.

## Why Load Distribution Is Correct

- Tasks get an integrated load estimate (`L_i`) combining volume, transformation complexity, and predicted time.
- Executor count is relative to DAG average load (`L_avg`), so heavier tasks scale up.
- CPU/RAM feedback applies bounded adaptation (`P_new`) to prevent cluster overuse.
- Critical-path (`T_DAG`) and ETL-time (`T_ETL`) metrics validate runtime behavior for each run.
- Smoothing update (`T_pred_new`) makes future decisions more accurate based on observed runtime.
