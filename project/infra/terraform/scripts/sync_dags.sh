#!/usr/bin/env bash
# Sync local DAGs and required src modules to EC2 via rsync over SSH.
# Usage:
#   ./scripts/sync_dags.sh            # one-shot sync
#   ./scripts/sync_dags.sh --watch    # auto-sync on file change (requires fswatch)
#   ./scripts/sync_dags.sh --full-src # sync whole src tree
#
# Environment variables:
#   SSH_KEY_PATH   path to SSH private key (default: ~/.ssh/id_rsa)
#   DAGS_DIR       local DAGs directory    (default: ../../dags relative to this script)
#   SRC_DIR        local src directory     (default: ../../src relative to this script)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TF_DIR="${SCRIPT_DIR}/.."

EC2_IP="$(terraform -chdir="${TF_DIR}" output -raw instance_public_ip 2>/dev/null)"
if [[ -z "${EC2_IP}" ]]; then
  echo "ERROR: could not read instance_public_ip from Terraform state." >&2
  exit 1
fi

KEY="${SSH_KEY_PATH:-${HOME}/.ssh/id_rsa}"
DAG_DIR="${DAGS_DIR:-${SCRIPT_DIR}/../../../dags}"
SRC_DIR="${SRC_DIR:-${SCRIPT_DIR}/../../../src}"
REMOTE_DAG_DIR="/opt/airflow/dags/"
REMOTE_SRC_DIR="/opt/airflow/project/src/"
SYNC_MODE="granular"
WATCH_MODE="false"

for arg in "$@"; do
  case "${arg}" in
    --watch) WATCH_MODE="true" ;;
    --full-src) SYNC_MODE="full" ;;
    *)
      echo "ERROR: unknown argument '${arg}'." >&2
      echo "Allowed args: --watch, --full-src" >&2
      exit 1
      ;;
  esac
done

prepare_remote_dirs() {
  ssh -i "${KEY}" -o StrictHostKeyChecking=no -o ConnectTimeout=10 \
    "ec2-user@${EC2_IP}" \
    "sudo mkdir -p /opt/airflow/dags /opt/airflow/project/src"
}

sync_full_src() {
  rsync -avz --delete \
    --exclude '__pycache__' \
    --exclude '*.pyc' \
    --rsync-path="sudo rsync" \
    -e "ssh -i ${KEY} -o StrictHostKeyChecking=no -o ConnectTimeout=10" \
    "${SRC_DIR}/" "ec2-user@${EC2_IP}:${REMOTE_SRC_DIR}"
}

sync_granular_src() {
  # Keep the remote src tree clean while syncing only runtime-required modules.
  ssh -i "${KEY}" -o StrictHostKeyChecking=no -o ConnectTimeout=10 \
    "ec2-user@${EC2_IP}" \
    "sudo rm -rf /opt/airflow/project/src/adaptive_scheduler /opt/airflow/project/src/pyspark_dag_queries && \
     sudo rm -f /opt/airflow/project/src/config.py /opt/airflow/project/src/s3_location_bootstrap.py && \
     sudo mkdir -p /opt/airflow/project/src/adaptive_scheduler /opt/airflow/project/src/pyspark_dag_queries"

  rsync -avz --delete \
    --exclude '__pycache__' \
    --exclude '*.pyc' \
    --rsync-path="sudo rsync" \
    -e "ssh -i ${KEY} -o StrictHostKeyChecking=no -o ConnectTimeout=10" \
    "${SRC_DIR}/adaptive_scheduler/" "ec2-user@${EC2_IP}:${REMOTE_SRC_DIR}adaptive_scheduler/"

  rsync -avz --delete \
    --exclude '__pycache__' \
    --exclude '*.pyc' \
    --rsync-path="sudo rsync" \
    -e "ssh -i ${KEY} -o StrictHostKeyChecking=no -o ConnectTimeout=10" \
    "${SRC_DIR}/pyspark_dag_queries/" "ec2-user@${EC2_IP}:${REMOTE_SRC_DIR}pyspark_dag_queries/"

  for module_file in config.py s3_location_bootstrap.py; do
    if [[ -f "${SRC_DIR}/${module_file}" ]]; then
      rsync -avz \
        --rsync-path="sudo rsync" \
        -e "ssh -i ${KEY} -o StrictHostKeyChecking=no -o ConnectTimeout=10" \
        "${SRC_DIR}/${module_file}" "ec2-user@${EC2_IP}:${REMOTE_SRC_DIR}${module_file}"
    fi
  done
}

sync_once() {
  prepare_remote_dirs
  rsync -avz --delete \
    --rsync-path="sudo rsync" \
    -e "ssh -i ${KEY} -o StrictHostKeyChecking=no -o ConnectTimeout=10" \
    "${DAG_DIR}/" "ec2-user@${EC2_IP}:${REMOTE_DAG_DIR}"
  if [[ "${SYNC_MODE}" == "full" ]]; then
    sync_full_src
  else
    sync_granular_src
  fi
  echo "Synced (${SYNC_MODE}) at $(date)"
}

if [[ "${WATCH_MODE}" == "true" ]]; then
  if ! command -v fswatch >/dev/null 2>&1; then
    echo "ERROR: fswatch is not installed. Install it with: brew install fswatch" >&2
    exit 1
  fi
  if [[ "${SYNC_MODE}" == "full" ]]; then
    echo "Watching ${DAG_DIR} and ${SRC_DIR} for changes..."
    WATCH_PATHS=("${DAG_DIR}" "${SRC_DIR}")
  else
    echo "Watching ${DAG_DIR}, ${SRC_DIR}/adaptive_scheduler, ${SRC_DIR}/pyspark_dag_queries, top-level src modules ..."
    WATCH_PATHS=("${DAG_DIR}" "${SRC_DIR}/adaptive_scheduler" "${SRC_DIR}/pyspark_dag_queries")
    for module_file in config.py s3_location_bootstrap.py; do
      if [[ -f "${SRC_DIR}/${module_file}" ]]; then
        WATCH_PATHS+=("${SRC_DIR}/${module_file}")
      fi
    done
  fi
  sync_once
  fswatch -o "${WATCH_PATHS[@]}" | while read -r _; do sync_once; done
else
  sync_once
fi
