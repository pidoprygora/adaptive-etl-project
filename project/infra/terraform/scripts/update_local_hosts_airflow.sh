#!/usr/bin/env bash
# Update local /etc/hosts for Airflow endpoint.
# Usage:
#   ./scripts/update_local_hosts_airflow.sh
#   ./scripts/update_local_hosts_airflow.sh airflow.lab
set -euo pipefail

HOST_ALIAS="${1:-airflow.lab}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TF_DIR="${SCRIPT_DIR}/.."

if ! command -v terraform >/dev/null 2>&1; then
  echo "ERROR: terraform is not installed or not in PATH." >&2
  exit 1
fi

INSTANCE_IP="$(terraform -chdir="${TF_DIR}" output -raw instance_public_ip 2>/dev/null || true)"
if [[ -z "${INSTANCE_IP}" ]]; then
  echo "ERROR: cannot read instance_public_ip from Terraform state." >&2
  echo "Run terraform apply first (or check state path)." >&2
  exit 1
fi

TMP_HOSTS="$(mktemp)"
cleanup() {
  rm -f "${TMP_HOSTS}"
}
trap cleanup EXIT

awk -v host_alias="${HOST_ALIAS}" '
  BEGIN { OFS = " " }
  /^[[:space:]]*#/ || /^[[:space:]]*$/ { print; next }
  {
    line = $0
    gsub(/^[[:space:]]+|[[:space:]]+$/, "", line)
    split(line, parts, /[[:space:]]+/)
    keep = 1
    for (i = 2; i <= length(parts); i++) {
      if (parts[i] == host_alias) {
        keep = 0
        break
      }
    }
    if (keep) {
      print $0
    }
  }
' /etc/hosts > "${TMP_HOSTS}"

echo "${INSTANCE_IP} ${HOST_ALIAS}" >> "${TMP_HOSTS}"

sudo cp "${TMP_HOSTS}" /etc/hosts
echo "Updated /etc/hosts:"
echo "  ${INSTANCE_IP} ${HOST_ALIAS}"
echo "Try: http://${HOST_ALIAS}:8080"
