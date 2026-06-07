#!/usr/bin/env bash
# Run all DDL statements from complex_etl_queries_targets_ddl.sql in Athena.
# Athena supports only one statement per API call, so this script splits and
# executes each statement individually.
#
# Usage: ./scripts/setup_athena.sh
# Requires: aws CLI configured, terraform apply already done.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TF_DIR="${SCRIPT_DIR}/.."
SQL_FILE="${SCRIPT_DIR}/../../../sql/complex_etl_queries_targets_ddl.sql"

if [[ ! -f "${SQL_FILE}" ]]; then
  echo "ERROR: SQL file not found: ${SQL_FILE}" >&2
  exit 1
fi

AWS_REGION="$(terraform -chdir="${TF_DIR}" output -raw aws_region 2>/dev/null || echo "eu-north-1")"
OUTPUT_LOCATION="$(terraform -chdir="${TF_DIR}" output -raw athena_output_location)"

echo "Region:          ${AWS_REGION}"
echo "Athena output:   ${OUTPUT_LOCATION}"
echo ""

# Split file into individual statements (split on ';' boundaries, skip blanks/comments)
STATEMENTS=()
current=""
while IFS= read -r line; do
  # Skip pure comment lines and blanks for accumulation, but keep them in statement
  if [[ "${line}" =~ ^[[:space:]]*$ ]] && [[ -z "${current}" ]]; then
    continue
  fi
  current+="${line}"$'\n'
  if [[ "${line}" == *";" ]]; then
    trimmed="$(echo "${current}" | sed '/^[[:space:]]*--/d' | sed '/^[[:space:]]*$/d')"
    if [[ -n "${trimmed}" ]]; then
      STATEMENTS+=("${current}")
    fi
    current=""
  fi
done < "${SQL_FILE}"

echo "Found ${#STATEMENTS[@]} SQL statements to execute."
echo ""

ok=0
fail=0
for stmt in "${STATEMENTS[@]}"; do
  preview="$(echo "${stmt}" | grep -v '^[[:space:]]*--' | head -1 | cut -c1-80)"
  echo -n "→ ${preview} ... "

  EXEC_ID="$(aws athena start-query-execution \
    --query-string "${stmt}" \
    --result-configuration "OutputLocation=${OUTPUT_LOCATION}" \
    --region "${AWS_REGION}" \
    --query 'QueryExecutionId' \
    --output text 2>&1)"

  if [[ "${EXEC_ID}" == *"Error"* ]] || [[ -z "${EXEC_ID}" ]]; then
    echo "FAILED to start"
    ((fail++)) || true
    continue
  fi

  # Poll until done
  for _ in $(seq 1 30); do
    sleep 2
    STATUS="$(aws athena get-query-execution \
      --query-execution-id "${EXEC_ID}" \
      --region "${AWS_REGION}" \
      --query 'QueryExecution.Status.State' \
      --output text)"
    if [[ "${STATUS}" == "SUCCEEDED" ]]; then
      echo "OK"
      ((ok++)) || true
      break
    elif [[ "${STATUS}" == "FAILED" ]] || [[ "${STATUS}" == "CANCELLED" ]]; then
      REASON="$(aws athena get-query-execution \
        --query-execution-id "${EXEC_ID}" \
        --region "${AWS_REGION}" \
        --query 'QueryExecution.Status.StateChangeReason' \
        --output text 2>/dev/null || echo "unknown")"
      echo "FAILED: ${REASON}"
      ((fail++)) || true
      break
    fi
  done
done

echo ""
echo "Results: ${ok} succeeded, ${fail} failed."
if [[ "${fail}" -gt 0 ]]; then
  echo "Check Athena console for details. Some statements may already exist (IF NOT EXISTS)."
fi

echo ""
echo "Creating S3 prefixes for Glue table LOCATION paths..."
BOOTSTRAP_SCRIPT="${SCRIPT_DIR}/../../../src/s3_location_bootstrap.py"
if [[ ! -f "${BOOTSTRAP_SCRIPT}" ]]; then
  echo "WARN: ${BOOTSTRAP_SCRIPT} not found; skip S3 prefix bootstrap." >&2
else
  python3 "${BOOTSTRAP_SCRIPT}" --all
fi
