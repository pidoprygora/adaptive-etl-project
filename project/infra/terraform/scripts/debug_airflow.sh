#!/usr/bin/env bash
# Collect an end-to-end Airflow diagnostics report on EC2/k3d host.
# Usage:
#   ./scripts/debug_airflow.sh
#   ./scripts/debug_airflow.sh airflow /tmp/custom-airflow-debug.log
set -euo pipefail

NAMESPACE="${1:-airflow}"
OUT="${2:-/tmp/airflow-debug-$(date +%Y%m%d-%H%M%S).log}"

need_cmd(
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "ERROR: required command '$1' is not available." >&2
    exit 1
  fi
}

need_cmd kubectl
need_cmd helm

run_section() {
  local title="$1"
  shift
  {
    echo "===== ${title} ====="
    "$@"
    echo
  } >>"$OUT" 2>&1 || {
    {
      echo "===== ${title} (FAILED) ====="
      echo "Command: $*"
      echo
    } >>"$OUT"
  }
}

get_first_pod() {
  local selector="$1"
  kubectl get pods -n "$NAMESPACE" -l "$selector" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true
}

SCHED_POD="$(get_first_pod component=scheduler)"
WEBSERVER_POD="$(get_first_pod component=webserver)"
MIGRATION_JOB="$(kubectl get jobs -n "$NAMESPACE" -o name 2>/dev/null | rg 'airflow-run-airflow-migrations' | tail -n 1 || true)"

{
  echo "# Airflow Debug Report"
  echo "generated_at_utc: $(date -u +'%Y-%m-%dT%H:%M:%SZ')"
  echo "namespace: ${NAMESPACE}"
  echo "scheduler_pod: ${SCHED_POD:-N/A}"
  echo "webserver_pod: ${WEBSERVER_POD:-N/A}"
  echo "migration_job: ${MIGRATION_JOB:-N/A}"
  echo
} >"$OUT"

run_section "K8S VERSION" kubectl version
run_section "NODES" kubectl get nodes -o wide
run_section "AIRFLOW PODS" kubectl get pods -n "$NAMESPACE" -o wide
run_section "AIRFLOW SERVICES" kubectl get svc -n "$NAMESPACE" -o wide
run_section "AIRFLOW PVC" kubectl get pvc -n "$NAMESPACE"
run_section "AIRFLOW EVENTS (LATEST 120)" kubectl get events -n "$NAMESPACE" --sort-by=.lastTimestamp
run_section "HELM RELEASE LIST" helm list -n "$NAMESPACE"
run_section "HELM VALUES" helm get values airflow -n "$NAMESPACE"
run_section "HELM MANIFEST (FIRST 400 LINES)" sh -lc "helm get manifest airflow -n \"$NAMESPACE\" | sed -n '1,400p'"

if [[ -n "${SCHED_POD}" ]]; then
  run_section "SCHEDULER POD DESCRIBE" kubectl describe pod "$SCHED_POD" -n "$NAMESPACE"
  run_section "SCHEDULER MOUNTS" sh -lc "kubectl get pod \"$SCHED_POD\" -n \"$NAMESPACE\" -o yaml | rg 'mountPath:|hostPath:|name: dags-hostpath|name: project-src|name: spark-jars|name: spark-conf|name: java-home|name: pyspark-packages'"
  run_section "SCHEDULER DAGS DIR" kubectl exec -n "$NAMESPACE" "$SCHED_POD" -c scheduler -- ls -la /opt/airflow/dags
  run_section "SCHEDULER AIRFLOW CFG dags_folder" kubectl exec -n "$NAMESPACE" "$SCHED_POD" -c scheduler -- airflow config get-value core dags_folder
  run_section "SCHEDULER AIRFLOW DAGS LIST" kubectl exec -n "$NAMESPACE" "$SCHED_POD" -c scheduler -- airflow dags list
  run_section "SCHEDULER AIRFLOW IMPORT ERRORS" kubectl exec -n "$NAMESPACE" "$SCHED_POD" -c scheduler -- airflow dags list-import-errors
  run_section "SCHEDULER PYTHON IMPORT CHECK" kubectl exec -n "$NAMESPACE" "$SCHED_POD" -c scheduler -- python -c "import importlib;mods=['airflow.providers.apache.spark.operators.spark_submit','pyspark'];[print(f'OK: {m}') if importlib.import_module(m) else None for m in mods]"
  run_section "SCHEDULER LOGS (TAIL 300)" kubectl logs -n "$NAMESPACE" "$SCHED_POD" -c scheduler --tail=300
fi

if [[ -n "${WEBSERVER_POD}" ]]; then
  run_section "WEBSERVER POD DESCRIBE" kubectl describe pod "$WEBSERVER_POD" -n "$NAMESPACE"
  run_section "WEBSERVER DAGS DIR" kubectl exec -n "$NAMESPACE" "$WEBSERVER_POD" -c webserver -- ls -la /opt/airflow/dags
  run_section "WEBSERVER AIRFLOW DAGS LIST" kubectl exec -n "$NAMESPACE" "$WEBSERVER_POD" -c webserver -- airflow dags list
  run_section "WEBSERVER LOGS (TAIL 300)" kubectl logs -n "$NAMESPACE" "$WEBSERVER_POD" -c webserver --tail=300
fi

if [[ -n "${MIGRATION_JOB}" ]]; then
  run_section "MIGRATION JOB DESCRIBE" kubectl describe -n "$NAMESPACE" "$MIGRATION_JOB"
  run_section "MIGRATION JOB LOGS (TAIL 300)" kubectl logs -n "$NAMESPACE" "$MIGRATION_JOB" --tail=300
fi

run_section "HOST /opt/airflow/dags" sh -lc "ls -la /opt/airflow/dags && stat /opt/airflow/dags"
run_section "HOST /opt/spark-jars" sh -lc "ls -la /opt/spark-jars | sed -n '1,50p'"
run_section "HOST /opt/spark-conf" sh -lc "ls -la /opt/spark-conf && sed -n '1,120p' /opt/spark-conf/spark-defaults.conf"
run_section "HOST /opt/pyspark-packages (TOP 50)" sh -lc "ls -la /opt/pyspark-packages | sed -n '1,50p'"
run_section "HOST DOCKER CONTAINERS" docker ps --format 'table {{.Names}}\t{{.Image}}\t{{.Status}}'
run_section "HOST K3D CLUSTERS" k3d cluster list

echo "DEBUG_FILE=${OUT}"
echo "LINES=$(wc -l < "$OUT")"
echo "Done."
