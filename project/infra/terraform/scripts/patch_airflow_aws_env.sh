#!/usr/bin/env bash
# Patch AWS/Spark env vars for an existing Airflow Helm release.
#
# Usage (run on EC2 instance with kubectl/helm configured):
#   export AWS_ACCESS_KEY_ID=...
#   export AWS_SECRET_ACCESS_KEY=...
#   ./scripts/patch_airflow_aws_env.sh
set -euo pipefail

NAMESPACE="${NAMESPACE:-airflow}"
RELEASE_NAME="${RELEASE_NAME:-airflow}"
AWS_REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-eu-north-1}}"
SPARK_GLUE_CLIENT_JAR="${SPARK_GLUE_CLIENT_JAR:-/opt/spark-jars/aws-glue-datacatalog-spark-client.jar}"
GLUE_RELEASE_TAG="${GLUE_RELEASE_TAG:-v3.5.x}"
GLUE_RELEASE_BASE="https://github.com/sdaberdaku/aws-glue-data-catalog-spark-client/releases/download/${GLUE_RELEASE_TAG}"

AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-}"
AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-}"
AWS_SESSION_TOKEN="${AWS_SESSION_TOKEN:-}"

if [[ -z "${AWS_ACCESS_KEY_ID}" || -z "${AWS_SECRET_ACCESS_KEY}" ]]; then
  echo "ERROR: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are required." >&2
  exit 1
fi

if ! command -v helm >/dev/null 2>&1; then
  echo "ERROR: helm is not installed." >&2
  exit 1
fi
if ! command -v kubectl >/dev/null 2>&1; then
  echo "ERROR: kubectl is not installed." >&2
  exit 1
fi

echo "Updating Helm repo cache..."
helm repo add apache-airflow https://airflow.apache.org >/dev/null 2>&1 || true
helm repo update >/dev/null

chart_line="$(helm status "${RELEASE_NAME}" -n "${NAMESPACE}" 2>/dev/null | awk '/^CHART:/{print $2}')"
if [[ -z "${chart_line}" ]]; then
  helm_list_json="$(helm list -n "${NAMESPACE}" -o json 2>/dev/null || true)"
  chart_line="$(python3 - "${RELEASE_NAME}" "${helm_list_json}" <<'PY'
import json
import sys

release = sys.argv[1]
raw = (sys.argv[2] or "").strip()
if not raw:
    sys.exit(0)
try:
    rows = json.loads(raw)
except json.JSONDecodeError:
    sys.exit(0)
for row in rows:
    if row.get("name") == release:
        print(row.get("chart", ""))
        break
PY
)"
fi
if [[ -z "${chart_line}" ]]; then
  chart_line="$(helm list -n "${NAMESPACE}" 2>/dev/null | awk -v rel="${RELEASE_NAME}" '$1==rel{print $9; exit}')"
fi
if [[ -z "${chart_line}" ]]; then
  echo "ERROR: Cannot detect CHART version from helm status/list." >&2
  exit 1
fi
chart_version="${chart_line#airflow-}"
chart_url="https://archive.apache.org/dist/airflow/helm-chart/${chart_version}/airflow-${chart_version}.tgz"

echo "Release: ${RELEASE_NAME}"
echo "Namespace: ${NAMESPACE}"
echo "Chart version: ${chart_version}"
echo "Glue metastore JAR: ${SPARK_GLUE_CLIENT_JAR}"

SPARK_WAREHOUSE="${SPARK_WAREHOUSE:-}"
if [[ -z "${SPARK_WAREHOUSE}" && -f /opt/spark-conf/spark-defaults.conf ]]; then
  SPARK_WAREHOUSE="$(
    awk -F= '/^spark\.sql\.warehouse\.dir=/{print $2; exit}' /opt/spark-conf/spark-defaults.conf
  )"
fi
hive_site_tmp="$(mktemp)"
cat > "${hive_site_tmp}" <<EOF
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>hive.metastore.client.factory.class</name>
    <value>com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory</value>
  </property>
  <property>
    <name>aws.region</name>
    <value>${AWS_REGION}</value>
  </property>
  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>${SPARK_WAREHOUSE}</value>
  </property>
</configuration>
EOF
# /opt/spark-conf is created by root during bootstrap; ec2-user needs sudo to update it.
sudo mkdir -p /opt/spark-conf
sudo install -m 0644 -o root -g root "${hive_site_tmp}" /opt/spark-conf/hive-site.xml
rm -f "${hive_site_tmp}"
echo "Wrote /opt/spark-conf/hive-site.xml (region=${AWS_REGION})"

install_jvm_truststore() {
  local source_path=""
  for candidate in \
    "${JAVA_SSL_TRUSTSTORE:-}" \
    /etc/pki/java/cacerts \
    /usr/lib/jvm/java-17-amazon-corretto.x86_64/lib/security/cacerts \
    /usr/lib/jvm/java-17-amazon-corretto/lib/security/cacerts; do
    if [[ -n "${candidate}" && -f "${candidate}" ]]; then
      source_path="${candidate}"
      break
    fi
  done
  if [[ -z "${source_path}" ]]; then
    echo "ERROR: Cannot find JVM cacerts on EC2 host." >&2
    return 1
  fi
  sudo install -m 0644 -o root -g root "${source_path}" /opt/spark-conf/cacerts
  echo "Installed JVM truststore for pods: /opt/spark-conf/cacerts (from ${source_path})"
}

install_jvm_truststore

download_glue_release_jar() {
  local jar_name="$1"
  local min_bytes="$2"
  local url="${GLUE_RELEASE_BASE}/${jar_name}"
  local tmp_file="${PATCH_DL_DIR}/${jar_name}.partial"

  echo "  -> ${jar_name}"
  if ! curl -fSL --retry 5 --retry-delay 2 --connect-timeout 30 -o "${tmp_file}" "${url}"; then
    echo "ERROR: curl failed for ${url}" >&2
    rm -f "${tmp_file}"
    return 1
  fi

  local size
  size="$(wc -c < "${tmp_file}" | tr -d ' ')"
  if [[ "${size}" -lt "${min_bytes}" ]]; then
    echo "ERROR: ${jar_name} is only ${size} bytes (expected >= ${min_bytes})." >&2
    echo "First bytes:" >&2
    head -c 200 "${tmp_file}" >&2 || true
    echo >&2
    rm -f "${tmp_file}"
    return 1
  fi

  sudo install -m 0644 -o root -g root "${tmp_file}" "/opt/spark-jars/${jar_name}"
  rm -f "${tmp_file}"
}

echo "Downloading patched Hive2 + Glue Spark client (${GLUE_RELEASE_TAG})..."
sudo mkdir -p /opt/spark-jars
PATCH_DL_DIR="$(mktemp -d "${HOME}/patch-spark-jars.XXXXXX")"
trap 'rm -rf "${PATCH_DL_DIR}"' EXIT

download_glue_release_jar "hive-common-2.3.9.jar" 100000
download_glue_release_jar "hive-exec-2.3.9-core.jar" 5000000
download_glue_release_jar "aws-glue-datacatalog-spark-client.jar" 50000
SPARK_GLUE_CLIENT_JAR="/opt/spark-jars/aws-glue-datacatalog-spark-client.jar"

PYSPARK_JARS_DIR="/opt/pyspark-packages/pyspark/jars"
if [[ -d "${PYSPARK_JARS_DIR}" ]]; then
  echo "Overlaying patched Hive + Glue jars into ${PYSPARK_JARS_DIR}..."
  sudo cp -f /opt/spark-jars/hive-common-2.3.9.jar "${PYSPARK_JARS_DIR}/"
  sudo cp -f /opt/spark-jars/hive-exec-2.3.9-core.jar "${PYSPARK_JARS_DIR}/"
  sudo cp -f /opt/spark-jars/aws-glue-datacatalog-spark-client.jar "${PYSPARK_JARS_DIR}/"
  for jar in /opt/spark-jars/hadoop-aws-*.jar /opt/spark-jars/aws-java-sdk-bundle-*.jar; do
    if [[ -f "${jar}" ]]; then
      sudo cp -f "${jar}" "${PYSPARK_JARS_DIR}/"
    fi
  done
  sudo rm -f "${PYSPARK_JARS_DIR}/aws-glue-datacatalog-hive3-client.jar"
fi

build_spark_classpath() {
  local cp="" jar
  for pattern in \
    aws-glue-datacatalog-spark-client.jar \
    hadoop-aws-*.jar \
    aws-java-sdk-bundle-*.jar; do
    for jar in /opt/spark-jars/${pattern}; do
      if [[ -f "${jar}" ]]; then
        cp="${cp:+$cp:}${jar}"
      fi
    done
  done
  if [[ -z "${cp}" ]]; then
    cp="${SPARK_GLUE_CLIENT_JAR}"
  fi
  printf '%s' "${cp}"
}

SPARK_CP="$(build_spark_classpath)"
JAVA_TRUSTSTORE_PATH="/opt/spark-conf/cacerts"
if [[ ! -f "${JAVA_TRUSTSTORE_PATH}" ]]; then
  echo "ERROR: ${JAVA_TRUSTSTORE_PATH} is missing after install_jvm_truststore." >&2
  exit 1
fi
SPARK_JAVA_OPTS="-Dhive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
if [[ -n "${JAVA_TRUSTSTORE_PATH}" ]]; then
  SPARK_JAVA_OPTS="${SPARK_JAVA_OPTS} -Djavax.net.ssl.trustStore=${JAVA_TRUSTSTORE_PATH} -Djavax.net.ssl.trustStorePassword=changeit -Djavax.net.ssl.trustStoreType=JKS"
  echo "Using JVM truststore: ${JAVA_TRUSTSTORE_PATH}"
else
  echo "WARN: JVM truststore not found; Glue HTTPS may fail." >&2
fi

if [[ -f /opt/spark-conf/spark-defaults.conf ]]; then
  spark_defaults_tmp="$(mktemp)"
  sudo cat /opt/spark-conf/spark-defaults.conf > "${spark_defaults_tmp}"
  grep -v '^spark\.driver\.extraClassPath=' "${spark_defaults_tmp}" \
    | grep -v '^spark\.executor\.extraClassPath=' \
    | grep -v '^spark\.hive\.imetastoreclient\.factory\.class=' \
    | grep -v '^spark\.driver\.userClassPathFirst=' \
    | grep -v '^spark\.executor\.userClassPathFirst=' \
    | grep -v '^spark\.driver\.extraJavaOptions=' \
    | grep -v '^spark\.executor\.extraJavaOptions=' \
    | grep -v '^spark\.hadoop\.fs\.s3a\.aws\.credentials\.provider=' \
    | grep -v '^spark\.hadoop\.fs\.s3\.aws\.credentials\.provider=' \
    | grep -v '^spark\.hadoop\.fs\.s3\.impl=' \
    | grep -v '^spark\.hadoop\.fs\.s3\.endpoint=' > "${spark_defaults_tmp}.new"
  {
    cat "${spark_defaults_tmp}.new"
    echo "spark.driver.userClassPathFirst=false"
    echo "spark.executor.userClassPathFirst=false"
    echo "spark.driver.extraJavaOptions=${SPARK_JAVA_OPTS}"
    echo "spark.executor.extraJavaOptions=${SPARK_JAVA_OPTS}"
    echo "spark.driver.extraClassPath=${SPARK_CP}"
    echo "spark.executor.extraClassPath=${SPARK_CP}"
    echo "spark.hive.imetastoreclient.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
    echo "spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
    echo "spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem"
    echo "spark.hadoop.fs.s3.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
    echo "spark.hadoop.fs.s3.endpoint=s3.${AWS_REGION}.amazonaws.com"
  } > "${spark_defaults_tmp}"
  sudo install -m 0644 -o root -g root "${spark_defaults_tmp}" /opt/spark-conf/spark-defaults.conf
  rm -f "${spark_defaults_tmp}" "${spark_defaults_tmp}.new"
  echo "Updated spark-defaults.conf (S3A classpath only; patched Hive in pyspark/jars)."
else
  echo "WARN: /opt/spark-conf/spark-defaults.conf not found; skipping classpath patch." >&2
fi

patch_file="$(mktemp)"
cat > "${patch_file}" <<EOF
env:
  - name: PYTHONPATH
    value: /opt/pyspark-packages
  - name: JAVA_HOME
    value: /usr/lib/jvm/java-17-amazon-corretto.x86_64
  - name: JAVA_SSL_TRUSTSTORE
    value: /opt/spark-conf/cacerts
  - name: SPARK_CONF_DIR
    value: /opt/spark-conf
  - name: SPARK_GLUE_CLIENT_JAR
    value: ${SPARK_GLUE_CLIENT_JAR}
  - name: AIRFLOW_CONN_AWS_DEFAULT
    value: aws://?region_name=${AWS_REGION}
  - name: AWS_DEFAULT_REGION
    value: ${AWS_REGION}
  - name: AWS_REGION
    value: ${AWS_REGION}
  - name: PYSPARK_PYTHON
    value: python3
  - name: AWS_EC2_METADATA_DISABLED
    value: "false"
  - name: AWS_METADATA_SERVICE_TIMEOUT
    value: "5"
  - name: AWS_METADATA_SERVICE_NUM_ATTEMPTS
    value: "3"
  - name: AWS_ACCESS_KEY_ID
    value: "${AWS_ACCESS_KEY_ID}"
  - name: AWS_SECRET_ACCESS_KEY
    value: "${AWS_SECRET_ACCESS_KEY}"
  - name: DATASET_SIZE
    value: small
  - name: DAG_MAX_ACTIVE_TASKS
    value: "4"
EOF

if [[ -n "${AWS_SESSION_TOKEN}" ]]; then
  cat >> "${patch_file}" <<EOF
  - name: AWS_SESSION_TOKEN
    value: "${AWS_SESSION_TOKEN}"
EOF
fi

echo "Deleting immutable jobs before upgrade..."
kubectl delete job -n "${NAMESPACE}" \
  "${RELEASE_NAME}-create-user" \
  "${RELEASE_NAME}-run-airflow-migrations" \
  --ignore-not-found=true >/dev/null || true

echo "Running helm upgrade..."
helm upgrade --install "${RELEASE_NAME}" "${chart_url}" \
  -n "${NAMESPACE}" \
  --reuse-values \
  -f "${patch_file}" \
  --timeout 10m \
  --wait

rm -f "${patch_file}"

echo "Done. Validate with:"
echo "  kubectl -n ${NAMESPACE} exec ${RELEASE_NAME}-scheduler-0 -c scheduler -- env | grep '^AWS_'"
echo "  kubectl -n ${NAMESPACE} exec ${RELEASE_NAME}-scheduler-0 -c scheduler -- python -c \"import boto3; print(boto3.client('sts').get_caller_identity())\""
