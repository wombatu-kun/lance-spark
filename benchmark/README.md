# TPC-DS Benchmark for Lance Spark

Runs the [TPC-DS](http://www.tpc.org/tpcds/) query suite against Lance and Parquet formats using Apache Spark, comparing query performance, correctness, and resource usage.

`parquet` here refers to Spark's built-in Parquet reader, used as a performance baseline.

## Architecture

Data generation uses the [Apache Kyuubi TPC-DS connector](https://kyuubi.readthedocs.io/en/master/connector/spark/tpcds.html), a pure-Spark data source that generates TPC-DS data in parallel across executors. No external `dsdgen` binary, CSV files, or intermediate formats are needed — data is written directly into the target format (Lance, Parquet, etc.) and can target any Spark-supported storage (local, S3, GCS, HDFS).

The pipeline is split into two independent Spark jobs:

1. **`TpcdsDataGenerator`** — Reads from the Kyuubi TPC-DS catalog and writes all 24 tables directly into each target format.
2. **`TpcdsBenchmarkRunner`** — Registers the generated tables and runs the 99 TPC-DS queries, comparing formats.

### TODO
- **Delta / Iceberg support** — not yet included because they require additional catalog/metastore tooling outside lance-spark's scope.

## Quick Start

```bash
# Build the jars first
make bundle SPARK_VERSION=3.5 SCALA_VERSION=2.12
make benchmark-build

# End-to-end: generate data + run queries
make benchmark SF=10 FORMATS=parquet,lance ITERATIONS=3
```

Or run the two phases separately:

```bash
# Step 1: Generate TPC-DS data (parallel Spark job)
./benchmark/scripts/generate-data.sh [SCALE_FACTOR] [FORMATS] [SPARK_MASTER]

# Step 2: Run benchmark queries against generated data
./benchmark/scripts/run-benchmark.sh [FORMATS] [SPARK_MASTER] [ITERATIONS]
```

### Examples

```bash
# Generate SF=10 data in both formats
./benchmark/scripts/generate-data.sh 10 parquet,lance local[*]

# Run queries with profiling
EXPLAIN=true METRICS=true ./benchmark/scripts/run-benchmark.sh parquet,lance local[*] 3

# Run a subset of queries
QUERIES=q3,q14a,q55 ./benchmark/scripts/run-benchmark.sh lance local[*] 1

# Override Spark/Scala versions
SPARK_VERSION=3.5 SCALA_VERSION=2.12 ./benchmark/scripts/generate-data.sh 10
```

Set `SPARK_HOME` if `spark-submit` is not on your `PATH`.

### Using the Docker environment

If you don't have Spark installed locally, use the existing `spark-lance` Docker container. The benchmark jar, data, and results directories are volume-mounted automatically.

```bash
# Build jars on the host
make bundle SPARK_VERSION=3.5 SCALA_VERSION=2.12
make benchmark-build

# Start the Spark container
make docker-up

# Generate data (inside the container)
docker exec spark-lance spark-submit \
  --class org.lance.spark.benchmark.TpcdsDataGenerator \
  --master local[*] \
  /home/lance/benchmark/lance-spark-benchmark-0.3.0-beta.1.jar \
  --data-dir /home/lance/data --scale-factor 1 --formats parquet,lance

# Run benchmark queries (inside the container)
docker exec spark-lance spark-submit \
  --class org.lance.spark.benchmark.TpcdsBenchmarkRunner \
  --master local[*] \
  /home/lance/benchmark/lance-spark-benchmark-0.3.0-beta.1.jar \
  --data-dir /home/lance/data --results-dir /home/lance/results \
  --formats parquet,lance --iterations 3

# Results appear on the host at benchmark/results/
```

Or shell in and run interactively:

```bash
make docker-shell
# Now inside the container, spark-submit as usual
```

### Profiling Features

**`EXPLAIN=true`** prints the Spark query plan before executing each query (first iteration only), useful for verifying filter/projection pushdown.

**`METRICS=true`** registers a `SparkListener` that captures per-task stats:
```
  [OK] q3 iter=1 time=822ms rows=89
       Metrics: tasks=12 cpu=680ms gc=15ms read=45MB shuffle_r=2MB shuffle_w=2MB
```

## Running on an External Cluster

To benchmark against a standalone or YARN cluster with data in an object store (S3, GCS, HDFS):

### 1. Build the jars

```bash
make bundle SPARK_VERSION=3.5 SCALA_VERSION=2.12
make benchmark-build
```

### 2. Generate TPC-DS data directly to object store

Data generation is a Spark job — it writes directly to any Spark-supported storage:

```bash
spark-submit \
  --class org.lance.spark.benchmark.TpcdsDataGenerator \
  --master local[*] \
  --driver-memory 8g \
  --jars path/to/lance-spark-bundle-3.5_2.12-*.jar \
  --conf spark.sql.extensions=org.lance.spark.LanceSparkSessionExtension \
  --conf spark.hadoop.fs.s3a.access.key=YOUR_KEY \
  --conf spark.hadoop.fs.s3a.secret.key=YOUR_SECRET \
  benchmark/target/lance-spark-benchmark-*.jar \
  --data-dir s3a://my-bucket/tpcds/sf10 \
  --scale-factor 10 \
  --formats parquet,lance
```

Or use the script with env vars:

```bash
DATA_DIR=s3a://my-bucket/tpcds/sf10 \
./benchmark/scripts/generate-data.sh 10 parquet,lance local[*]
```

### 3. Run benchmark queries on the cluster

```bash
spark-submit \
  --class org.lance.spark.benchmark.TpcdsBenchmarkRunner \
  --master spark://cluster-master:7077 \
  --deploy-mode client \
  --driver-memory 8g \
  --executor-memory 16g \
  --executor-cores 4 \
  --num-executors 8 \
  --jars path/to/lance-spark-bundle-3.5_2.12-*.jar \
  --conf spark.sql.extensions=org.lance.spark.LanceSparkSessionExtension \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.hadoop.fs.s3a.access.key=YOUR_KEY \
  --conf spark.hadoop.fs.s3a.secret.key=YOUR_SECRET \
  benchmark/target/lance-spark-benchmark-*.jar \
  --data-dir s3a://my-bucket/tpcds/sf10 \
  --results-dir s3a://my-bucket/tpcds/sf10/results \
  --formats parquet,lance \
  --iterations 3 \
  --explain \
  --metrics
```

For YARN:
```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  ...
```

For GCS, use `--conf spark.hadoop.google.cloud.auth.service.account.json.keyfile=/path/to/key.json` and `gs://` paths.

## Output

Results are written to a timestamped CSV file (e.g. `tpcds_20260318_062221.csv`) and a summary table is printed to stdout:

```
Query     parquet(ms)    lance(ms)      Ratio   Status
-------------------------------------------------------
q1               2505         1529       1.64x     PASS
q3                796          756       1.05x     PASS
q4               6426        14229       0.45x     PASS
...
Geometric mean ratio (parquet/lance): 0.87x
Queries passed: 75, partial/failed: 30
Row count validation: all matching
```

When `--metrics` is enabled, extra columns appear (CPU time, bytes read, shuffle).

## Project Structure

```
benchmark/
├── scripts/
│   ├── generate-data.sh                 # TPC-DS data generation
│   ├── run-benchmark.sh                 # TPC-DS query runner
│   ├── run-streaming-benchmark.sh       # Streaming-write scalability runner (host Spark)
│   └── run-streaming-benchmark-docker.sh # Same, routed through the spark-lance container
├── src/main/java/org/lance/spark/benchmark/
│   ├── TpcdsDataGenerator.java   # Spark job: Kyuubi TPC-DS → Lance/Parquet
│   ├── TpcdsBenchmarkRunner.java # Main entry point for query benchmarking
│   ├── TpcdsDataLoader.java      # Register pre-generated tables as temp views
│   ├── TpcdsQueryRunner.java     # Query execution loop
│   ├── BenchmarkResult.java      # Per-query result record
│   ├── BenchmarkReporter.java    # CSV + summary output
│   ├── QueryMetrics.java         # Per-query task-level metrics
│   ├── QueryMetricsListener.java # SparkListener for metrics collection
│   └── streaming/                # Streaming-write scalability benchmark
│       ├── StreamingScalabilityBenchmark.java  # Driver (E1/E2/E3/E4)
│       ├── MicroBatchTimingListener.java       # Per-batch CSV sample capture
│       └── BatchSample.java                    # CSV row schema
├── src/main/resources/tpcds-queries/
│   └── q1.sql ... q99.sql        # 103 TPC-DS query files
├── data/                         # TPC-DS data (gitignored)
├── results/                      # TPC-DS CSVs (gitignored)
├── streaming-data/               # Streaming-bench Lance datasets (gitignored)
├── streaming-results/            # Streaming-bench CSVs (gitignored)
└── pom.xml
```

# Streaming-Write Scalability Benchmark

Quantifies the per-microbatch overhead of the Lance Structured Streaming sink to validate the
scalability claims in
[`docs/src/operations/streaming/streaming-writes.md`](../docs/src/operations/streaming/streaming-writes.md).
The reviewer concerns from PR #399 — *manifest grows linearly with fragment count*, *commit speed
degrades as manifest grows*, *recovery scan walks N versions*, *two manifest updates per batch* —
each map to one experiment below.

## Quick Start

If you have Spark installed locally:

```bash
./benchmark/scripts/run-streaming-benchmark.sh E1 --batches 200 --rows-per-second 5000
```

If you don't, route through the existing Docker container:

```bash
make docker-up   # one-time
./benchmark/scripts/run-streaming-benchmark-docker.sh E1 --batches 200 --rows-per-second 5000
```

The Docker wrapper builds the lance-spark bundle locally if needed (it will not use the Maven
Central jar baked into the image — that release pre-dates the streaming sink), pushes the local
bundle into `/opt/spark/jars/`, and runs `spark-submit` inside the container. Output CSVs land in
`benchmark/streaming-results/` (host) or `benchmark/results/streaming/` (when run via the Docker
mount, owned by root from the container; use `sudo rm -rf` to clean up).

## Experiments

| ID | Question answered | Driver flags |
|----|-------------------|--------------|
| **E1** | How does commit latency / manifest size grow with fragment count, with no compaction? | `--batches 5000` |
| **E2** | How often must `OPTIMIZE` run to keep commit latency bounded? | `--batches 1000 --optimize-cadences 50,200,1000` |
| **E3** | How long does the bounded-recovery scan take vs. version history depth? | `--version-targets 10,100,500,1000,5000 --recovery-lookbacks 100,500,1000` |
| **E4** | Smallest sustainable `processingTime` trigger interval at a given input rate? | `--trigger-intervals '1 second,5 seconds,30 seconds' --window-seconds 600` |

All experiments share the same CSV schema, one row per Spark micro-batch (or per measurement
point for E3):

```
experiment,run_label,batch_id,batch_ts_ms,input_rows,batch_duration_ms,
fragment_count,lance_version,manifest_bytes,optimized_this_batch
```

Field semantics:

- `batch_duration_ms` — Spark's reported batch wall time (includes source read + sink commit).
- `fragment_count` — total fragments in the dataset *after* the commit (read via Lance API).
- `lance_version` — dataset version after the commit. Increments by **2 per batch** (Txn1 +
  Txn2) for the streaming sink — confirms the two-transaction protocol.
- `manifest_bytes` — size of the latest manifest file on disk. Lance encodes the version in the
  filename as `MAX_U64 - version`; only populated for `file://` / local paths.
- `optimized_this_batch` — `true` for the first sample after a periodic `OPTIMIZE` call (E2).

## Common Driver Flags

| Flag | Default | Applies to | Description |
|------|---------|------------|-------------|
| `--experiment` | required | all | One of `E1`, `E2`, `E3`, `E4`. |
| `--data-dir` | required | all | Where Lance datasets are written. |
| `--results-dir` | required | all | Where the CSV is written. |
| `--batches` | `1000` | E1, E2 | Number of micro-batches to run. |
| `--rows-per-second` | `1000` | E1, E2, E4 | Spark `rate` source rate. |
| `--optimize-cadences` | `50,200,1000` | E2 | Comma-separated list of "OPTIMIZE every K batches" cadences; one separate run per K. |
| `--version-targets` | `10,100,500,1000,5000` | E3 | Dataset version depths to measure. |
| `--recovery-lookbacks` | `100,500,1000` | E3 | Maximum-lookback values to time. |
| `--recovery-trials` | `5` | E3 | Repeat count per (version × lookback) point. |
| `--trigger-intervals` | `1 second,5 seconds,30 seconds,60 seconds` | E4 | One run per trigger. |
| `--window-seconds` | `300` | E4 | Wall-clock window per run. |

## Environment Variables (scripts only)

| Var | Default | Meaning |
|-----|---------|---------|
| `SPARK_VERSION` | `3.5` | Selects which Spark/Scala bundle to use. |
| `SCALA_VERSION` | `2.12` | Same. |
| `SPARK_MASTER` | `local[2]` (host) | Forwarded to `spark-submit`. |
| `DRIVER_MEMORY` | `4g` | `spark-submit --driver-memory`. |
| `DATA_DIR` | `benchmark/streaming-data` (host) | Override Lance dataset location. |
| `RESULTS_DIR` | `benchmark/streaming-results` (host) | Override CSV output dir. |
| `CONTAINER` | `spark-lance` | Docker container name (Docker wrapper only). |

## Reading the Results

Quickest sanity check — sample every Nth batch from a CSV:

```bash
awk -F, 'NR>1 && $5>0 {print $3"\t"$6"\t"$7"\t"$9}' \
  benchmark/streaming-results/streaming_E1_*.csv \
  | awk 'NR==1 || NR%20==0 {printf "batch=%s dur=%sms frag=%s manifest=%s\n", $1,$2,$3,$4}'
```

What to look for:

- **`lance_version` increments by exactly 2 per non-empty batch.** Confirms Txn1 (`Append`) +
  Txn2 (`UpdateConfig`) — the two-transaction commit protocol.
- **`manifest_bytes` grows linearly with `fragment_count`.** A roughly constant
  `manifest_bytes / fragment_count` ratio (~80 bytes/fragment in observed runs) means each commit
  serializes the entire fragment list — exactly what makes compaction necessary.
- **`batch_duration_ms` trend over time.** Flat curve = fragment count is below the I/O-cost
  knee. Upward curve = manifest serialization is dominating. E2's overlay shows whether
  `OPTIMIZE` keeps the curve flat.

## Reproducing the Numbers in the Docs

The placeholder ranges in `docs/src/operations/streaming/streaming-writes.md` ("recommended
trigger ≥ ~1 s on local FS", etc.) come from this benchmark. To regenerate them end-to-end:

```bash
# E1 — long no-compaction run; gives the unmitigated growth curve
./benchmark/scripts/run-streaming-benchmark-docker.sh E1 --batches 5000 --rows-per-second 5000

# E2 — same workload, OPTIMIZE every 50/200/1000 batches
./benchmark/scripts/run-streaming-benchmark-docker.sh E2 --batches 2000 --rows-per-second 5000 \
  --optimize-cadences 50,200,1000

# E3 — recovery-scan cost vs. version history
./benchmark/scripts/run-streaming-benchmark-docker.sh E3 \
  --version-targets 10,100,500,1000,5000 --recovery-lookbacks 100,500,1000

# E4 — sustained throughput vs. trigger interval
./benchmark/scripts/run-streaming-benchmark-docker.sh E4 \
  --trigger-intervals '1 second,5 seconds,30 seconds,60 seconds' --window-seconds 600
```

A long E1 (5000 batches at the rates above) takes ~15–20 minutes on a laptop and produces ~60k
fragments — the regime where the manifest-size effect becomes visible.
