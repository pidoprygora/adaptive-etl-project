# PySpark jobs for DAG

Кожен файл `qXX_*.py` запускає рівно один ETL-запит з `sql/complex_etl_queries.sql`.

## Структура

- `common.py` — спільний раннер:
  - приймає SQL як рядок із конкретного `qXX` файла;
  - робить мінімальну сумісність Athena -> Spark SQL;
  - підтримує adaptive runtime-параметри (`executors`, `shuffle_partitions`, `pred_time`);
  - запускає `spark.sql(...)`;
  - збирає execution metrics/log і пише їх у S3 (`metrics/`, `logs/`).
- `q01_...py` ... `q20_...py` — незалежні PySpark-джоби для DAG.
- `q21_adaptive_etl_example.py` — приклад adaptive ETL job, який приймає параметри від Airflow Scheduler.

## Приклад запуску


```bash
spark-submit src/pyspark_dag_queries/q01_credit_campaign_target_audience.py
```

### Приклад adaptive запуску

```bash
spark-submit src/pyspark_dag_queries/q21_adaptive_etl_example.py \
  --task-id q21_adaptive_etl_example \
  --dataset-size medium \
  --executors 4 \
  --shuffle-partitions 16 \
  --pred-time 120
```

## Підключення в Airflow (SparkSubmitOperator)

```python
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

q21_task = SparkSubmitOperator(
    task_id="run_q21_adaptive_etl_task",
    application="/opt/airflow/project/src/pyspark_dag_queries/q21_adaptive_etl_example.py",
    conn_id="spark_default",
    conf={
        "spark.executor.instances": "{{ ti.xcom_pull(task_ids='compute_adaptive_plan', key='spark_executor_instances') }}",
        "spark.sql.shuffle.partitions": "{{ ti.xcom_pull(task_ids='compute_adaptive_plan', key='spark_shuffle_partitions') }}",
    },
    application_args=[
        "--task-id", "q21_adaptive_etl_example",
        "--dataset-size", "{{ ti.xcom_pull(task_ids='load_task_profiles', key='dataset_size') }}",
        "--executors", "{{ ti.xcom_pull(task_ids='compute_adaptive_plan', key='spark_executor_instances') }}",
        "--shuffle-partitions", "{{ ti.xcom_pull(task_ids='compute_adaptive_plan', key='spark_shuffle_partitions') }}",
        "--pred-time", "{{ ti.xcom_pull(task_ids='compute_adaptive_plan', key='q21_decision')['predicted_time_sec'] }}",
    ],
)
```

## Production DAG для q01..q20

- Файл DAG: `project/dags/adaptive_etl_q01_q20_production_dag.py`.
- DAG ID: `adaptive_etl_bank_q01_q20_production`.
- Всі `q01..q15` стартують паралельно (до 4 одночасних Spark-задач: `DAG_MAX_ACTIVE_TASKS`, за замовчуванням `4`).
- `q16_mailing_base` залежить від `q06_client_profile_scoring`.
- Від `q16` залежать `q17_mailing_schedule_optimization` і `q20_campaign_readiness_check`.
- `q18_etl_metrics_aggregation` запускається після завершення всіх `q01..q17` і `q20`.
- Фінальний крок: `q19_adaptive_parallelism_recommendation` після `q18`.

### Data Quality + fail-fast

- Скрипт перевірок: `src/pyspark_dag_queries/data_quality_checks.py`.
- Гейти в DAG:
  - `dq_after_audience_stage` (`post_audience`) — після `q01..q15`;
  - `dq_after_mailing_stage` (`post_mailing`) — після `q16 -> q17/q20`;
  - `dq_after_metrics_stage` (`post_metrics`) — після `q18 -> q19`.
- Перевірки включають:
  - row-count sanity checks (таблиці не порожні там, де очікується);
  - null checks для ключових колонок;
  - duplicate checks для ключів у цільових таблицях.
- Політика fail-fast: будь-який DQ-check кидає `RuntimeError`, і DAG зупиняється на відповідному гейті.

## Integration smoke-run (small dataset)

- Скрипт: `src/pyspark_dag_queries/smoke_run_q01_q20_small.py`
- Виконує послідовний інтеграційний прогін `q01 -> ... -> q20` на `small` dataset.
- Використовує єдиний runtime-інтерфейс параметрів для всіх `qXX` та один спільний `dag_run_id`.
- Fail-fast: при падінні будь-якого кроку скрипт завершується з помилкою.

```bash
python src/pyspark_dag_queries/smoke_run_q01_q20_small.py
```

Опційно з кастомним `spark-submit` або параметрами:

```bash
python src/pyspark_dag_queries/smoke_run_q01_q20_small.py \
  --spark-submit spark-submit \
  --executors 1 \
  --shuffle-partitions 4 \
  --pred-time 0
```

## Зворотний цикл метрик

- Метрики виконання ETL задач зберігаються у `s3://<bucket>/metrics/bank_data/`.
- Логи запусків зберігаються у `s3://<bucket>/logs/bank_data/`.
- `Adaptive Scheduler` читає історію запусків, оновлює `T_pred`, перераховує `L_i` і `P_i`, після чого передає нові параметри в наступний запуск.

### Збір JSONL → Glue / Athena

Після запусків DAG синхронізуйте per-task метрики в таблицю `adaptive_etl_bank.etl_execution_metrics`:

```bash
cd project/src
python sync_metrics_jsonl_to_glue.py --upload
```

Потім у Airflow запустіть задачу `q18_etl_metrics_aggregation` — вона заповнить `adaptive_etl_bank.etl_metrics_aggregation`.
