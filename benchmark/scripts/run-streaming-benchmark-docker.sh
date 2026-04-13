#!/usr/bin/env bash
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

# Runs the Lance streaming-write scalability benchmark inside the spark-lance Docker container,
# for users who don't have a local Spark install. Mirrors run-streaming-benchmark.sh but routes
# spark-submit through `docker exec` and pushes the locally-built lance-spark bundle into the
# container so the streaming-sink code under test (this branch) is actually loaded — the baked-in
# Maven Central bundle in /opt/spark/jars/ is from a release that may pre-date this PR.
#
# Usage:
#   ./run-streaming-benchmark-docker.sh <E1|E2|E3|E4> [extra args...]
#
# Output CSVs land in benchmark/results/streaming/ on the host (mounted into the container as
# /home/lance/results/streaming). Note: that directory is owned by root on the host because the
# container writes as root via a bind mount; read it normally, but `sudo rm -rf` if you want to
# clean up between runs.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCHMARK_DIR="${SCRIPT_DIR}/.."
REPO_ROOT="${BENCHMARK_DIR}/.."

CONTAINER="${CONTAINER:-spark-lance}"
SPARK_VERSION="${SPARK_VERSION:-3.5}"
SCALA_VERSION="${SCALA_VERSION:-2.12}"

EXPERIMENT="${1:-}"
if [ -z "${EXPERIMENT}" ]; then
  echo "Usage: $0 <E1|E2|E3|E4> [extra args...]" >&2
  exit 1
fi
shift

echo "=== Lance Streaming Scalability Benchmark (Docker) ==="
echo "Container:       ${CONTAINER}"
echo "Experiment:      ${EXPERIMENT}"
echo "Spark version:   ${SPARK_VERSION}"
echo "Scala version:   ${SCALA_VERSION}"
echo ""

# 1. Make sure the locally-built lance-spark bundle exists at the version this branch is on. A
#    stale jar from a prior build (e.g. 0.3.0-beta.3 lying around in target/) would lack the
#    streaming-write code introduced in 0.4.0-beta.1 and produce
#    "Data source lance does not support streamed writing". Pin the exact version from the root pom.
LANCE_SPARK_VERSION=$(grep -oP '<lance-spark\.version>\K[^<]+' "${REPO_ROOT}/pom.xml" | head -1)
BUNDLE_NAME="lance-spark-bundle-${SPARK_VERSION}_${SCALA_VERSION}-${LANCE_SPARK_VERSION}.jar"
BUNDLE_DIR="${REPO_ROOT}/lance-spark-bundle-${SPARK_VERSION}_${SCALA_VERSION}/target"
BUNDLE_JAR="${BUNDLE_DIR}/${BUNDLE_NAME}"
if [ ! -f "${BUNDLE_JAR}" ]; then
  echo "--- Building lance-spark bundle ${LANCE_SPARK_VERSION} (this can take a few minutes) ---"
  (cd "${REPO_ROOT}" && make bundle SPARK_VERSION="${SPARK_VERSION}" SCALA_VERSION="${SCALA_VERSION}")
fi
if [ ! -f "${BUNDLE_JAR}" ]; then
  echo "ERROR: expected bundle ${BUNDLE_JAR} not found after build" >&2
  exit 1
fi
echo "Local bundle: ${BUNDLE_JAR}"

# 2. Make sure the benchmark fat jar exists.
BENCHMARK_JAR="${BENCHMARK_DIR}/target/lance-spark-benchmark.jar"
if [ ! -f "${BENCHMARK_JAR}" ]; then
  echo "--- Building benchmark jar ---"
  (cd "${BENCHMARK_DIR}" && ../mvnw package -DskipTests -q \
    -Dspark.compat.version="${SPARK_VERSION}" \
    -Dscala.compat.version="${SCALA_VERSION}")
fi
echo "Benchmark jar: ${BENCHMARK_JAR}"

# 3. Make sure the container is running.
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
  echo "--- Starting docker container ---"
  (cd "${REPO_ROOT}" && make docker-up)
  # Brief wait for entrypoint to settle. Keeps us under the cache TTL.
  sleep 3
fi

# 4. Push our local bundle into the container, replacing whatever Lance bundle was baked into the
#    image. Done idempotently — running this script repeatedly is fine.
LOCAL_BUNDLE_BASENAME="$(basename "${BUNDLE_JAR}")"
echo "--- Replacing baked-in lance-spark bundle in container ---"
docker exec "${CONTAINER}" bash -c "rm -f /opt/spark/jars/lance-spark-bundle*.jar"
docker cp "${BUNDLE_JAR}" "${CONTAINER}:/opt/spark/jars/${LOCAL_BUNDLE_BASENAME}"

# 5. Make sure result/data dirs exist inside the container.
docker exec "${CONTAINER}" bash -c \
  "mkdir -p /home/lance/data/streaming /home/lance/results/streaming"

# 6. Run the benchmark.
echo "--- Running benchmark ---"
docker exec "${CONTAINER}" spark-submit \
  --class org.lance.spark.benchmark.streaming.StreamingScalabilityBenchmark \
  --master "local[*]" \
  --driver-memory "${DRIVER_MEMORY:-4g}" \
  --conf spark.sql.extensions=org.lance.spark.LanceSparkSessionExtension \
  --conf spark.sql.streaming.stopTimeout=10s \
  --conf spark.driver.extraJavaOptions="-XX:+IgnoreUnrecognizedVMOptions --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true" \
  /home/lance/benchmark/lance-spark-benchmark.jar \
  --experiment "${EXPERIMENT}" \
  --data-dir /home/lance/data/streaming \
  --results-dir /home/lance/results/streaming \
  "$@"

echo ""
echo "=== Done ==="
echo "CSV(s) on host: ${REPO_ROOT}/benchmark/results/streaming/"
echo "Files are owned by root (container UID); use sudo to delete."
