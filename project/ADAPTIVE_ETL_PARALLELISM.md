# Adaptive ETL Parallelism (Airflow + PySpark)

## Компоненти системи

- **Data Source**: сирі таблиці з доменними даними.
- **Amazon S3**: шари `raw/`, `processed/`, `metrics/`, `logs/`.
- **Airflow DAG**: оркестрація ETL і адаптивного планування.
- **Adaptive Scheduler**: обчислює складність задач і рівень паралелізму.
- **PySpark ETL Tasks**: виконують SQL-перетворення з runtime-параметрами.
- **Metrics Storage**: зберігання історії метрик у S3.
- **Execution Log**: журнали конкретних запусків.
- **Time Prediction Module**: прогноз та оновлення `T_pred`.
- **Result Storage**: цільові `processed` таблиці.

## Формули, які використані в реалізації

1. `L_i = alpha*D_i + beta*C_i + gamma*T_i`, де `alpha=0.4`, `beta=0.3`, `gamma=0.3`.
2. `L_avg = (1 / n) * sum(L_i)`.
3. `P_i = min(P_max, ceil(L_i / L_avg))`.
4. `U_cpu = CPU_used / CPU_total`.
5. `U_ram = RAM_used / RAM_total`.
6. `T_pred = (1 / n) * sum(T_i)`.
7. `S = T_seq / T_par`.
8. `E = S / P`.
9. `S_Amdahl = 1 / ((1 - q) + q / P)`.
10. `T_DAG = max(sum(T_i) for each DAG path)`.
11. `T_ETL = T_extract + T_transform + T_load` або `T_ETL = max(T_branch_1..k) + T_load`.
12. `B = L_max / L_avg`.
13. Адаптація `P_new`:
    - якщо `U_cpu > 0.8` або `U_ram > 0.8` -> `P_new = max(1, P_i - 1)`;
    - якщо `U_cpu < 0.5` і `U_ram < 0.5` -> `P_new = min(P_max, P_i + 1)`.
14. `T_pred_new = lambda*T_actual + (1 - lambda)*T_pred_old`, де `lambda=0.3`.

## S3 структура

```text
raw/
processed/
metrics/
logs/
```

## Чому алгоритм забезпечує коректне балансування

- Кожна ETL-задача отримує інтегральну оцінку навантаження `L_i`, яка враховує обсяг даних, складність трансформації та прогноз часу.
- Розподіл паралелізму задається відносно середнього навантаження DAG (`L_avg`), тому більш важкі задачі отримують більше executors.
- Коефіцієнт `B = L_max / L_avg` сигналізує про дисбаланс між задачами: чим вищий `B`, тим більше сенсу в адаптивній корекції.
- Контроль `U_cpu` і `U_ram` запобігає перевантаженню кластера та динамічно зменшує/збільшує `P_new`.
- Після кожного запуску `T_pred` оновлюється через експоненційне згладжування, тому планувальник поступово адаптується до реальної поведінки ETL.
- Оцінки `S`, `E` та `S_Amdahl` дають кількісну перевірку, чи дійсно новий рівень паралелізації ефективний.
