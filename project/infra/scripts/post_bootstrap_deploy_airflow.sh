#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-airflow}"
RELEASE_NAME="${RELEASE_NAME:-airflow}"
VALUES_FILE="${VALUES_FILE:-./infra/k3d/airflow-values.yaml}"
HELM_REPO_NAME="apache-airflow"
HELM_REPO_URL="https://airflow.apache.org"
HELM_CHART="${HELM_REPO_NAME}/airflow"
AIRFLOW_CONFIG_SECRET_ID="${AIRFLOW_CONFIG_SECRET_ID:-}"
AIRFLOW_LOG_BUCKET="${AIRFLOW_LOG_BUCKET:-}"
AIRFLOW_REMOTE_BASE_LOG_URI="${AIRFLOW_REMOTE_BASE_LOG_URI:-}"
AWS_REGION="${AWS_REGION:-}"
AIRFLOW_FERNET_KEY="${AIRFLOW_FERNET_KEY:-}"
AIRFLOW_WEBSERVER_SECRET_KEY="${AIRFLOW_WEBSERVER_SECRET_KEY:-}"
AIRFLOW_STATSD_HOST="${AIRFLOW_STATSD_HOST:-${RELEASE_NAME}-statsd}"
AIRFLOW_ALERT_WEBHOOK_URL="${AIRFLOW_ALERT_WEBHOOK_URL:-}"
AIRFLOW_ALERT_EMAIL_TO="${AIRFLOW_ALERT_EMAIL_TO:-}"
AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-}"
AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-}"
AWS_SESSION_TOKEN="${AWS_SESSION_TOKEN:-}"

if ! command -v kubectl >/dev/null 2>&1; then
  echo "kubectl is not installed or not in PATH" >&2
  exit 1
fi

if ! command -v helm >/dev/null 2>&1; then
  echo "helm is not installed or not in PATH" >&2
  exit 1
fi

if [[ ! -f "${VALUES_FILE}" ]]; then
  echo "Values file not found: ${VALUES_FILE}" >&2
  exit 1
fi

if [[ -n "${AIRFLOW_CONFIG_SECRET_ID}" ]]; then
  if ! command -v aws >/dev/null 2>&1; then
    echo "aws cli is required when AIRFLOW_CONFIG_SECRET_ID is set" >&2
    exit 1
  fi

  secret_json="$(aws secretsmanager get-secret-value --secret-id "${AIRFLOW_CONFIG_SECRET_ID}" --query SecretString --output text)"
  export secret_json

  secret_values="$(python3 - <<'PY'
import json
import os

payload = json.loads(os.environ["secret_json"])
keys = [
    "airflow_log_bucket",
    "aws_region",
    "airflow_remote_base_log_uri",
    "airflow_fernet_key",
    "airflow_webserver_secret_key",
    "airflow_statsd_host",
    "airflow_alert_webhook_url",
    "airflow_alert_email_to",
    "aws_access_key_id",
    "aws_secret_access_key",
    "aws_session_token",
]
for key in keys:
    value = payload.get(key, "")
    value = str(value).replace("\n", "")
    print(f"{key}={value}")
PY
)"

  while IFS="=" read -r key value; do
    case "${key}" in
      airflow_log_bucket) [[ -z "${AIRFLOW_LOG_BUCKET}" ]] && AIRFLOW_LOG_BUCKET="${value}" ;;
      aws_region) [[ -z "${AWS_REGION}" ]] && AWS_REGION="${value}" ;;
      airflow_remote_base_log_uri) [[ -z "${AIRFLOW_REMOTE_BASE_LOG_URI}" ]] && AIRFLOW_REMOTE_BASE_LOG_URI="${value}" ;;
      airflow_fernet_key) [[ -z "${AIRFLOW_FERNET_KEY}" ]] && AIRFLOW_FERNET_KEY="${value}" ;;
      airflow_webserver_secret_key) [[ -z "${AIRFLOW_WEBSERVER_SECRET_KEY}" ]] && AIRFLOW_WEBSERVER_SECRET_KEY="${value}" ;;
      airflow_statsd_host) [[ -z "${AIRFLOW_STATSD_HOST}" ]] && AIRFLOW_STATSD_HOST="${value}" ;;
      airflow_alert_webhook_url) [[ -z "${AIRFLOW_ALERT_WEBHOOK_URL}" ]] && AIRFLOW_ALERT_WEBHOOK_URL="${value}" ;;
      airflow_alert_email_to) [[ -z "${AIRFLOW_ALERT_EMAIL_TO}" ]] && AIRFLOW_ALERT_EMAIL_TO="${value}" ;;
      aws_access_key_id) [[ -z "${AWS_ACCESS_KEY_ID}" ]] && AWS_ACCESS_KEY_ID="${value}" ;;
      aws_secret_access_key) [[ -z "${AWS_SECRET_ACCESS_KEY}" ]] && AWS_SECRET_ACCESS_KEY="${value}" ;;
      aws_session_token) [[ -z "${AWS_SESSION_TOKEN}" ]] && AWS_SESSION_TOKEN="${value}" ;;
    esac
  done <<< "${secret_values}"
fi

if [[ -z "${AIRFLOW_REMOTE_BASE_LOG_URI}" && -n "${AIRFLOW_LOG_BUCKET}" ]]; then
  AIRFLOW_REMOTE_BASE_LOG_URI="s3://${AIRFLOW_LOG_BUCKET}/logs"
fi

if [[ -z "${AWS_REGION}" || -z "${AIRFLOW_REMOTE_BASE_LOG_URI}" || -z "${AIRFLOW_FERNET_KEY}" || -z "${AIRFLOW_WEBSERVER_SECRET_KEY}" ]]; then
  cat >&2 <<'EOF'
Missing runtime config.
Set vars directly or provide AIRFLOW_CONFIG_SECRET_ID with required keys:
  - aws_region
  - airflow_remote_base_log_uri (or airflow_log_bucket)
  - airflow_fernet_key
  - airflow_webserver_secret_key
EOF
  exit 1
fi

rendered_values_file="$(mktemp)"
export VALUES_FILE rendered_values_file AWS_REGION AIRFLOW_REMOTE_BASE_LOG_URI AIRFLOW_FERNET_KEY AIRFLOW_WEBSERVER_SECRET_KEY
export AIRFLOW_STATSD_HOST AIRFLOW_ALERT_WEBHOOK_URL AIRFLOW_ALERT_EMAIL_TO
export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
python3 - <<'PY'
import os

src = os.environ["VALUES_FILE"]
dst = os.environ["rendered_values_file"]
replacements = {
    "__AWS_REGION__": os.environ["AWS_REGION"],
    "__AIRFLOW_REMOTE_BASE_LOG_URI__": os.environ["AIRFLOW_REMOTE_BASE_LOG_URI"],
    "__AIRFLOW_FERNET_KEY__": os.environ["AIRFLOW_FERNET_KEY"],
    "__AIRFLOW_WEBSERVER_SECRET_KEY__": os.environ["AIRFLOW_WEBSERVER_SECRET_KEY"],
    "__AIRFLOW_STATSD_HOST__": os.environ["AIRFLOW_STATSD_HOST"],
    "__AIRFLOW_ALERT_WEBHOOK_URL__": os.environ["AIRFLOW_ALERT_WEBHOOK_URL"],
    "__AIRFLOW_ALERT_EMAIL_TO__": os.environ["AIRFLOW_ALERT_EMAIL_TO"],
    "__AWS_ACCESS_KEY_ID__": os.environ["AWS_ACCESS_KEY_ID"],
    "__AWS_SECRET_ACCESS_KEY__": os.environ["AWS_SECRET_ACCESS_KEY"],
    "__AWS_SESSION_TOKEN__": os.environ["AWS_SESSION_TOKEN"],
}

with open(src, "r", encoding="utf-8") as f:
    content = f.read()

def drop_optional_aws_env(content_text: str, env_name: str) -> str:
    block = f'  - name: {env_name}\n    value: "__{env_name}__"\n'
    return content_text.replace(block, "")

if not os.environ.get("AWS_ACCESS_KEY_ID"):
    content = drop_optional_aws_env(content, "AWS_ACCESS_KEY_ID")
if not os.environ.get("AWS_SECRET_ACCESS_KEY"):
    content = drop_optional_aws_env(content, "AWS_SECRET_ACCESS_KEY")
if not os.environ.get("AWS_SESSION_TOKEN"):
    content = drop_optional_aws_env(content, "AWS_SESSION_TOKEN")

for old, new in replacements.items():
    content = content.replace(old, new)

with open(dst, "w", encoding="utf-8") as f:
    f.write(content)
PY

kubectl cluster-info >/dev/null
kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

helm repo add "${HELM_REPO_NAME}" "${HELM_REPO_URL}" >/dev/null 2>&1 || true
helm repo update

helm upgrade --install "${RELEASE_NAME}" "${HELM_CHART}" \
  --namespace "${NAMESPACE}" \
  --values "${rendered_values_file}" \
  --wait \
  --timeout 15m

rm -f "${rendered_values_file}"

echo "Airflow deployed. Verify with:"
echo "  kubectl get pods -n ${NAMESPACE}"
echo "  kubectl logs -n ${NAMESPACE} deploy/${RELEASE_NAME}-scheduler | rg \"remote_logging\""
