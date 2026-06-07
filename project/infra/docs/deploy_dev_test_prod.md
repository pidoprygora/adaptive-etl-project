# Deployment Runbook: dev -> test -> prod

Це базовий playbook, як безпечно просувати зміни Airflow/DAG/infra між середовищами.

## 0) Принципи промоушену

- Одна й та сама ревізія коду проходить `dev -> test -> prod` (без ручних правок між етапами).
- Конфіг відрізняється лише через env-specific змінні (`terraform.tfvars` і Secrets Manager values).
- Після кожного деплою є обов'язкові quality gates: health, DAG parse, smoke DAG run, логи/метрики/алерти.

## 1) Підготовка конфігів середовищ

У `infra/terraform` підготуй три файли:

- `terraform.dev.tfvars`
- `terraform.test.tfvars`
- `terraform.prod.tfvars`

Мінімально вони мають різнитись такими полями:

- `project_name`
- `cluster_name`
- `allowed_ssh_cidr`
- `airflow_config_secret_name`
- (за потреби) `s3_bucket_name_override`
- `airflow_alert_webhook_url`
- `airflow_alert_email_to`

Рекомендований патерн імен:

- `project_name = "adaptive-etl-dev|test|prod"`
- `cluster_name = "airflow-dev|test|prod"`
- `airflow_config_secret_name = "adaptive-etl/airflow/dev|test|prod/config"`

## 2) Етап dev

### 2.1 Infra apply

```bash
cd infra/terraform
terraform init
terraform workspace select dev || terraform workspace new dev
terraform plan -var-file=terraform.dev.tfvars -out=dev.tfplan
terraform apply dev.tfplan
```

### 2.2 Airflow deploy

```bash
cd ../..
AIRFLOW_CONFIG_SECRET_ID=<dev_secret_name_or_arn> \
NAMESPACE=airflow \
RELEASE_NAME=airflow \
bash infra/scripts/post_bootstrap_deploy_airflow.sh
```

### 2.3 Quality gates (dev)

- `kubectl get pods -n airflow` -> всі `Running/Completed`.
- `kubectl logs -n airflow deploy/airflow-scheduler | rg "remote_logging|statsd"` -> без помилок.
- Запустити 1 тестовий DAG run у UI.
- Перевірити S3 логи: `aws s3 ls s3://<dev_bucket>/logs/ --recursive | head`.
- Перевірити метрики: `curl localhost:9102/metrics` (через `port-forward`).
- Протестувати алерт: форснути retry/fail у тестовому task.

Після проходження gate створюється Git tag (наприклад `release-YYYYMMDD-dev-ok`).

## 3) Етап test

### 3.1 Promote тієї ж ревізії

- Переконатися, що в test іде той самий commit/tag, що пройшов dev.
- Заборонити "quick fixes" напряму в test.

### 3.2 Infra + deploy

```bash
cd infra/terraform
terraform workspace select test || terraform workspace new test
terraform plan -var-file=terraform.test.tfvars -out=test.tfplan
terraform apply test.tfplan

cd ../..
AIRFLOW_CONFIG_SECRET_ID=<test_secret_name_or_arn> \
NAMESPACE=airflow \
RELEASE_NAME=airflow \
bash infra/scripts/post_bootstrap_deploy_airflow.sh
```

### 3.3 Quality gates (test)

- Повний DAG run для production-like набору даних.
- Валідація Data Quality задач (`dq_after_*`) без false-positive.
- Перевірка, що alert-канали не дублюються (1 retry/fail = 1 подія).
- Перевірка продуктивності: час DAG run у допустимому діапазоні.

Після проходження gate створюється релізний tag для prod (наприклад `release-YYYYMMDD-rc1`).

## 4) Етап prod

### 4.1 Pre-flight checklist

- Підтверджений commit/tag із test.
- Погоджене change window.
- Оновлений rollback owner + контакт on-call.

### 4.2 Infra + deploy

```bash
cd infra/terraform
terraform workspace select prod || terraform workspace new prod
terraform plan -var-file=terraform.prod.tfvars -out=prod.tfplan
terraform apply prod.tfplan

cd ../..
AIRFLOW_CONFIG_SECRET_ID=<prod_secret_name_or_arn> \
NAMESPACE=airflow \
RELEASE_NAME=airflow \
bash infra/scripts/post_bootstrap_deploy_airflow.sh
```

### 4.3 Post-deploy verification (prod)

- Перевірка Airflow web/scheduler health.
- Smoke run критичного DAG.
- Підтвердження запису логів у `s3://<prod_bucket>/logs/`.
- Підтвердження метрик у Prometheus/Grafana.
- Підтвердження alert delivery у бойовий канал.

## 5) Rollback

Якщо є інцидент після деплою:

1. Зупинити нові DAG runs (pause критичні DAG-и).
2. Повернути попередню стабільну ревізію DAG/конфігу (попередній git tag).
3. Повторно виконати `post_bootstrap_deploy_airflow.sh` з тим самим `AIRFLOW_CONFIG_SECRET_ID`.
4. За потреби відкотити інфру через попередній `tfplan`/commit Terraform.
5. Перевірити health, логи, метрики, алерти.

## 6) Мінімальна CI/CD логіка (рекомендовано)

- `push` у feature branch -> тільки `dev`.
- `merge` у main + tag `*-rc*` -> `test`.
- manual approval + prod tag -> `prod`.

Обов'язкові кроки пайплайна:

- lint + unit tests;
- `airflow dags list` / DAG parse test;
- Terraform `fmt/validate/plan`;
- deploy;
- post-deploy smoke tests.
