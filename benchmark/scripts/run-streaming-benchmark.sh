#!/usr/bin/env bash
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

# Runs one experiment of the Lance streaming-write scalability benchmark.
# Usage: ./run-streaming-benchmark.sh <experiment> [extra args...]
#   experiment ∈ {E1, E2, E3, E4}
# Extra args are forwarded to StreamingScalabilityBenchmark, e.g.:
#   ./run-streaming-benchmark.sh E1 --batches 200 --rows-per-second 5000

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCHMARK_DIR="${SCRIPT_DIR}/.."

SPARK_VERSION="${SPARK_VERSION:-3.5}"
SCALA_VERSION="${SCALA_VERSION:-2.12}"

EXPERIMENT="${1:-}"
if [ -z "${EXPERIMENT}" ]; then
  echo "Usage: $0 <E1|E2|E3|E4> [extra args...]" >&2
  exit 1
fi
shift

# Use a sibling directory rather than benchmark/data and benchmark/results — the TPC-DS docker
# harness owns those paths (often as root after a docker run) and would block mkdir here.
DATA_DIR="${DATA_DIR:-${BENCHMARK_DIR}/streaming-data}"
RESULTS_DIR="${RESULTS_DIR:-${BENCHMARK_DIR}/streaming-results}"
SPARK_MASTER="${SPARK_MASTER:-local[2]}"

echo "=== Lance Streaming Scalability Benchmark ==="
echo "Experiment:      ${EXPERIMENT}"
echo "Spark master:    ${SPARK_MASTER}"
echo "Spark version:   ${SPARK_VERSION}"
echo "Scala version:   ${SCALA_VERSION}"
echo "Data dir:        ${DATA_DIR}"
echo "Results dir:     ${RESULTS_DIR}"
echo ""

BENCHMARK_JAR="${BENCHMARK_DIR}/target/lance-spark-benchmark.jar"
if [ ! -f "${BENCHMARK_JAR}" ]; then
  echo "--- Building benchmark jar ---"
  cd "${BENCHMARK_DIR}"
  ../mvnw package -DskipTests -q \
    -Dspark.compat.version="${SPARK_VERSION}" \
    -Dscala.compat.version="${SCALA_VERSION}"
  cd "${SCRIPT_DIR}"
fi

# Pin the exact bundle version from the root pom — a stale jar from a prior build (e.g. an old
# 0.3.x left over in target/) would lack the streaming-write code from this branch and produce
# "Data source lance does not support streamed writing".
LANCE_SPARK_VERSION=$(grep -oP '<lance-spark\.version>\K[^<]+' "${BENCHMARK_DIR}/../pom.xml" | head -1)
BUNDLE_JAR="${BENCHMARK_DIR}/../lance-spark-bundle-${SPARK_VERSION}_${SCALA_VERSION}/target/lance-spark-bundle-${SPARK_VERSION}_${SCALA_VERSION}-${LANCE_SPARK_VERSION}.jar"
if [ ! -f "${BUNDLE_JAR}" ]; then
  echo "--- Building lance-spark bundle ${LANCE_SPARK_VERSION} ---"
  cd "${BENCHMARK_DIR}/.."
  make bundle SPARK_VERSION="${SPARK_VERSION}" SCALA_VERSION="${SCALA_VERSION}"
  cd "${SCRIPT_DIR}"
fi
if [ ! -f "${BUNDLE_JAR}" ]; then
  echo "ERROR: expected bundle ${BUNDLE_JAR} not found after build" >&2
  exit 1
fi

mkdir -p "${DATA_DIR}" "${RESULTS_DIR}"

SPARK_SUBMIT="spark-submit"
if [ -n "${SPARK_HOME:-}" ]; then
  SPARK_SUBMIT="${SPARK_HOME}/bin/spark-submit"
fi

${SPARK_SUBMIT} \
  --class org.lance.spark.benchmark.streaming.StreamingScalabilityBenchmark \
  --master "${SPARK_MASTER}" \
  --driver-memory "${DRIVER_MEMORY:-4g}" \
  --executor-memory "${EXECUTOR_MEMORY:-4g}" \
  --jars "${BUNDLE_JAR}" \
  --conf spark.sql.extensions=org.lance.spark.LanceSparkSessionExtension \
  --conf spark.sql.streaming.stopTimeout=10s \
  --conf spark.driver.extraJavaOptions="-XX:+IgnoreUnrecognizedVMOptions --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true" \
  "${BENCHMARK_JAR}" \
  --experiment "${EXPERIMENT}" \
  --data-dir "${DATA_DIR}" \
  --results-dir "${RESULTS_DIR}" \
  "$@"

echo ""
echo "=== Done. CSV in ${RESULTS_DIR}/ ==="
