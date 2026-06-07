# Test stand infra (EC2 + Docker + k3d + Airflow)

## Що робить ця інфраструктура

- Підіймає EC2 інстанс в AWS.
- Створює мережевий шар: VPC, public subnet, IGW, route table.
- Через `user_data` ставить Docker, k3d, kubectl, helm.
- Створює k3d кластер (`1 server + 3 workers`).
- Створює S3 bucket для `dags/`, `logs/`, `plugins/` з private + versioning.
- Налаштовує IAM роль EC2 з мінімально необхідними S3 доступами.
- Створює AWS Secrets Manager secret з runtime-конфігом Airflow.
- Готує namespace `airflow` і Helm repo.

## 1) Підготовка Terraform

```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars
```

В `terraform.tfvars` обов'язково задай:
- `aws_region`;
- `allowed_ssh_cidr` (зазвичай твій публічний IP `/32`);
- `ssh_public_key` (вміст `.pub` ключа).
- за потреби `s3_bucket_name_override`, якщо хочеш фіксовану назву bucket.
- опційно `cloudwatch_namespace` і `cloudwatch_metrics_interval_sec` для host-level метрик.

## 2) Створення стенду

```bash
terraform init
terraform plan
terraform apply
```

Після `apply` отримаєш `instance_public_ip` та `ssh_command`.
Також отримаєш `airflow_s3_bucket_name` для підключення remote logging.

## 3) Перевірка bootstrap на EC2

```bash
ssh -i ~/.ssh/<your_key> ubuntu@<EC2_PUBLIC_IP>
docker --version
k3d version
kubectl get nodes
```

Очікування: 4 ноди (`1 control-plane/server + 3 workers/agents`).

Перевірка CloudWatch Agent на хості:

```bash
sudo systemctl status amazon-cloudwatch-agent --no-pager
sudo /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -a status
```

## 4) Деплой Airflow у k3d

Перед деплоєм: не треба хардкодити bucket/region у values.
Передай конфіг через env або Secrets Manager.

Далі:

```bash
# Рекомендовано: через Secrets Manager, створений Terraform
AIRFLOW_CONFIG_SECRET_ID=<airflow_config_secret_name_or_arn> \
  bash infra/scripts/post_bootstrap_deploy_airflow.sh

# Або напряму через env vars
AWS_REGION=<aws_region> \
AIRFLOW_REMOTE_BASE_LOG_URI=s3://<bucket>/logs \
AIRFLOW_FERNET_KEY=<fernet_key> \
AIRFLOW_WEBSERVER_SECRET_KEY=<webserver_secret_key> \
AIRFLOW_ALERT_WEBHOOK_URL=<https://hooks.slack.com/...> \
AIRFLOW_ALERT_EMAIL_TO=<ops@example.com> \
  bash infra/scripts/post_bootstrap_deploy_airflow.sh

kubectl get pods -n airflow
```

Перевірка логів у S3:

```bash
aws s3 ls s3://<bucket_from_terraform_output>/logs/ --recursive | head
```

Перевірка метрик Airflow (StatsD exporter):

```bash
kubectl get pod,svc -n airflow | rg statsd
kubectl port-forward -n airflow svc/airflow-statsd 9102:9102
curl -s localhost:9102/metrics | rg "airflow|custom_dagrun|custom_task"
```

Перевірка host CPU/RAM метрик у CloudWatch:

```bash
TOKEN="$(curl -fsSL -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)"
INSTANCE_ID="$(curl -fsSL -H "X-aws-ec2-metadata-token: ${TOKEN}" http://169.254.169.254/latest/meta-data/instance-id)"

aws cloudwatch list-metrics \
  --namespace "AdaptiveETL/Host" \
  --metric-name cpu_usage_active \
  --dimensions Name=InstanceId,Value="${INSTANCE_ID}"

aws cloudwatch get-metric-statistics \
  --namespace "AdaptiveETL/Host" \
  --metric-name mem_used_percent \
  --dimensions Name=InstanceId,Value="${INSTANCE_ID}" \
  --statistics Average \
  --start-time "$(date -u -d '15 minutes ago' +%Y-%m-%dT%H:%M:%SZ)" \
  --end-time "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --period 60
```

Алерти на `fail/retry`:
- Task-level `on_failure_callback` + `on_retry_callback` відправляють webhook/email (якщо задані);
- DAG-level callbacks публікують метрики завершення DAG runs (`success/failed`, `duration_ms`).

## 5) Доступ через SSH tunnel

Детальна інструкція:

- [docs/ssh_tunnel_kube_access.md](docs/ssh_tunnel_kube_access.md)
- [docs/iac_architecture.md](docs/iac_architecture.md)
- [docs/deploy_dev_test_prod.md](docs/deploy_dev_test_prod.md)
