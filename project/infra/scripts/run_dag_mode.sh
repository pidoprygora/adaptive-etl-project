#!/usr/bin/env bash
set -euo pipefail

# --- config (adjust if needed) ---
TF_DIR="/Users/pidoprygora/me/диплом/project/infra/terraform"
SSH_USER="ec2-user"
NAMESPACE="airflow"
SCHEDULER_POD="airflow-scheduler-0"
DAG_ID="adaptive_etl_bank_q01_q20_production"

usage() {
  echo "Usage:"
  echo "  $0 adaptive|sequential|parallel"
  echo "  $0 all"
  exit 1
}

run_mode() {
  local mode="$1"

  echo "==> Trigger DAG in mode: ${mode}"
  cd "$TF_DIR"

  local ip
  ip="$(terraform output -raw instance_public_ip)"

  ssh "${SSH_USER}@${ip}" \
    "kubectl -n ${NAMESPACE} exec ${SCHEDULER_POD} -- \
      airflow dags trigger ${DAG_ID} --conf '{\"scheduler_mode\":\"${mode}\"}'"

  echo "==> Done: ${mode}"
}

main() {
  [[ $# -eq 1 ]] || usage

  case "$1" in
    adaptive|sequential|parallel)
      run_mode "$1"
      ;;
    all)
      run_mode adaptive
      run_mode sequential
      run_mode parallel
      ;;
    *)
      usage
      ;;
  esac
}

main "$@"
