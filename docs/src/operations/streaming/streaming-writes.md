# Streaming Writes

Write Spark Structured Streaming queries to Lance tables with exactly-once guarantees.

The Lance connector implements the Spark DataSource V2 `StreamingWrite` interface, so any
Spark Structured Streaming query can use `.writeStream.format("lance")` as its sink. Each
micro-batch is committed atomically via a Lance transaction, and the sink is idempotent on
Spark's epoch identifier so that failed queries can be safely restarted without producing
duplicates.

## Quick Start

The canonical example: read from a rate source and write to a Lance table, one commit per
trigger interval.

=== "Python"
    ```python
    from pyspark.sql.streaming import Trigger

    # Pre-create the target table (streaming writes do NOT auto-create tables)
    spark.range(0).selectExpr(
        "CAST(id AS LONG) AS id",
        "CAST(id AS STRING) AS name"
    ).write.format("lance").save("/tmp/lance-streaming-demo/sink.lance")

    # Start the streaming query
    query = (spark.readStream.format("rate")
        .option("rowsPerSecond", 10)
        .load()
        .selectExpr("CAST(value AS LONG) AS id", "CAST(value AS STRING) AS name")
        .writeStream.format("lance")
        .option("path", "/tmp/lance-streaming-demo/sink.lance")
        .option("checkpointLocation", "/tmp/lance-streaming-demo/ckpt")
        .option("streamingQueryId", "demo-sink")
        .outputMode("append")
        .trigger(processingTime="5 seconds")
        .start())
    ```

=== "Scala"
    ```scala
    import org.apache.spark.sql.streaming.Trigger

    // Pre-create the target table
    spark.range(0).selectExpr(
      "CAST(id AS LONG) AS id",
      "CAST(id AS STRING) AS name"
    ).write.format("lance").save("/tmp/lance-streaming-demo/sink.lance")

    // Start the streaming query
    val query = spark.readStream.format("rate")
      .option("rowsPerSecond", 10)
      .load()
      .selectExpr("CAST(value AS LONG) AS id", "CAST(value AS STRING) AS name")
      .writeStream.format("lance")
      .option("path", "/tmp/lance-streaming-demo/sink.lance")
      .option("checkpointLocation", "/tmp/lance-streaming-demo/ckpt")
      .option("streamingQueryId", "demo-sink")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()
    ```

=== "Java"
    ```java
    import org.apache.spark.sql.streaming.Trigger;

    // Pre-create the target table
    spark.range(0)
        .selectExpr("CAST(id AS LONG) AS id", "CAST(id AS STRING) AS name")
        .write().format("lance")
        .save("/tmp/lance-streaming-demo/sink.lance");

    // Start the streaming query
    StreamingQuery query = spark.readStream().format("rate")
        .option("rowsPerSecond", 10)
        .load()
        .selectExpr("CAST(value AS LONG) AS id", "CAST(value AS STRING) AS name")
        .writeStream().format("lance")
        .option("path", "/tmp/lance-streaming-demo/sink.lance")
        .option("checkpointLocation", "/tmp/lance-streaming-demo/ckpt")
        .option("streamingQueryId", "demo-sink")
        .outputMode("append")
        .trigger(Trigger.ProcessingTime("5 seconds"))
        .start();
    ```

## Output Modes

Spark Structured Streaming defines three output modes. The Lance sink supports all three:

| Mode | Supported | Behavior |
|------|-----------|----------|
| **Append** | Yes | Each micro-batch's new rows are appended via a Lance `Append` operation. Best for ingestion pipelines. |
| **Complete** | Yes | Each micro-batch replaces the entire table via a Lance `Overwrite` operation. Used for streaming aggregation queries that emit their full state each trigger. |
| **Update** | Yes (with append-on-update semantics — see below) | Each micro-batch's emitted delta rows are appended. No in-place upserts. |

### Update output mode semantics

The Lance sink accepts `.outputMode("update")` by implementing Spark's internal
`SupportsStreamingUpdateAsAppend` marker interface. This tells Spark's query analyzer to route
Update-mode rows through the same append path as Append mode.

In practice this means:

- **Non-aggregation queries in Update mode** are byte-equivalent to Append mode. Use whichever
  you prefer — there is no semantic difference for Lance.
- **Aggregation queries in Update mode produce accumulating rows across epochs.** If a
  streaming `groupBy(key).count()` emits `("hello", 1)` on epoch 0 and `("hello", 2)` on epoch
  1, the Lance table will contain **both** rows, not a single row with `count=2`. If you want
  the "current full state" instead, use **Complete mode**.
- **Native MERGE-based upsert semantics** — where Update mode would perform row-level UPDATE
  against existing rows — are **not implemented in this release**. They are a planned future
  enhancement.

If you need true upsert semantics against existing data today, use `foreachBatch` with
explicit DML:

=== "Python"
    ```python
    def upsert_batch(batch_df, batch_id):
        batch_df.createOrReplaceTempView("incoming")
        spark.sql("""
            MERGE INTO lance.`/path/to/target` AS t
            USING incoming AS s
            ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

    stream_df.writeStream.foreachBatch(upsert_batch).start()
    ```

## Exactly-once semantics

The Lance streaming sink guarantees exactly-once commits under the following conditions:

1. The `streamingQueryId` option is set to a globally-unique identifier for the streaming
   query (required — see the [Options](#write-options) table below).
2. Spark restarts the query within a reasonable window after any crash (by default, within
   100 unrelated commits to the same Lance table — tunable via `maxRecoveryLookback`).

Under the hood, each micro-batch commit is implemented as a **two-transaction protocol**:

1. **Txn1 — Append/Overwrite.** The sink builds a Lance `Append` (or `Overwrite` for
   Complete mode) operation with the micro-batch's fragments and attaches two transaction
   properties: `streaming.queryId` and `streaming.epochId`. This commit is atomic via Lance's
   MVCC.
2. **Txn2 — UpdateConfig.** A second Lance transaction bumps a per-query epoch watermark
   stored in the dataset's config map under the key
   `spark.lance.streaming.lastEpoch.<streamingQueryId>`. This watermark is checked at the
   start of every subsequent commit to short-circuit replayed epochs.

**Recovery.** If the process crashes between Txn1 and Txn2, the next commit attempt observes
that the watermark is below the current epoch and performs a bounded lookback scan: it walks
backwards through the last `maxRecoveryLookback` Lance versions, reading each transaction's
properties to find a match on `(streamingQueryId, epochId)`. If found, Txn1 is skipped and
only Txn2 is re-executed, preserving exactly-once semantics.

**Bounded at-least-once fallback.** If *more than* `maxRecoveryLookback` unrelated commits
land between the original crash and the retry — which is extremely unlikely in typical
workloads — the recovery scan cannot find the prior Txn1, and the commit degrades to
**at-least-once** (the data is re-appended, producing duplicates). For tables with very high
concurrent write volume from multiple writers, raise `maxRecoveryLookback` accordingly.

## Write Options

| Option | Type | Required | Description |
|--------|------|----------|-------------|
| `path` | String | Yes | The Lance dataset URI to write to. |
| `checkpointLocation` | String | Yes | Spark checkpoint directory (standard streaming option). |
| `streamingQueryId` | String | Yes | Globally-unique identifier for this streaming query. Used as the per-query idempotency key for the epoch watermark and recovery scan. MUST be unique across all streaming queries writing to the same Lance table. |
| `maxRecoveryLookback` | Int | No (default: `100`) | Maximum number of historical Lance versions to walk during the crash-between-Txn1-and-Txn2 recovery scan. Values ≥ 1. Larger values provide stronger exactly-once guarantees in the presence of high-concurrency writers at the cost of slower crash recovery. Typical streaming workloads work well with the default. |

All [standard Lance write options](../../config.md) (storage credentials, namespace
configuration, etc.) also apply.

## Triggers

The Lance streaming sink works with all standard Spark triggers:

- `Trigger.ProcessingTime("N seconds")` — processes available data every N seconds. Best for
  continuous ingestion.
- `Trigger.AvailableNow()` — processes all currently-available source data in one or more
  micro-batches and then terminates. Best for scheduled batch-like runs over a streaming
  source.
- `Trigger.Once()` — deprecated but still functional; processes all available data in a
  single micro-batch and terminates. Prefer `Trigger.AvailableNow()`.

**Continuous mode** (`Trigger.Continuous("1 second")`) is **not supported** — Lance's
commit-based MVCC is not compatible with Spark's per-record low-latency continuous streaming
model. Attempting to use it will throw `UnsupportedOperationException`.

## Limitations

- **Streaming source (`readStream.format("lance")`) is not yet supported.** This is tracked
  as a follow-up feature. To stream data OUT of a Lance table, use batch reads with time
  travel (`VERSION AS OF` or `TIMESTAMP AS OF`).
- **Target tables must exist before starting the streaming query.** Streaming writes do not
  auto-create tables. Pre-create with a batch write or DDL statement.
- **Schema must match exactly** between the streaming DataFrame and the target Lance table.
  Schema evolution mid-stream is not supported.
- **Native MERGE-based upserts in Update mode are not implemented.** See the
  [Update output mode semantics](#update-output-mode-semantics) section above.
- **The `streamingQueryId` option is required** and must be globally unique. Running two
  streaming queries with the same `streamingQueryId` against the same Lance table is a
  misconfiguration and will produce version conflicts.

## Scalability and Operational Tuning

Each micro-batch performs **two Lance manifest updates** — Txn1 (`Append` of the batch's
fragments) and Txn2 (`UpdateConfig` to bump the per-query epoch watermark). The dataset version
therefore advances by exactly **2 per micro-batch**, and the manifest grows linearly with the
number of fragments. Each commit re-serializes the entire manifest, so commit latency grows
linearly with fragment count.

> **Periodic `OPTIMIZE` is required, not optional.** Without compaction, a sustained streaming
> workload will eventually have per-commit latency exceed its trigger interval and start
> building unbounded backpressure. Numbers below.

The numbers come from the streaming-scalability benchmark in
[`benchmark/`](https://github.com/lance-format/lance-spark/tree/main/benchmark) — see
`benchmark/streaming-results/` for the raw CSVs and `benchmark/scripts/run-streaming-benchmark.sh`
to reproduce them. The reference workload is **5 000 rows per micro-batch on a `local[12]`
driver writing to local FS**; absolute milliseconds will differ on your hardware, but the
*shapes* of the curves transfer.

### Manifest and latency growth without `OPTIMIZE`

In the reference run, the manifest grew at ~**83 bytes per fragment**. With Spark producing one
fragment per task (12 on `local[12]`), each batch added ~**1 KB to the manifest** and ~**0.1 ms
to subsequent commit latency**. Measured per-commit latency, bucketed by fragment count over a
~2 000-batch run with no compaction:

| Fragment count | Manifest size | p50 commit | p90 commit | p99 commit |
|---------------:|--------------:|-----------:|-----------:|-----------:|
| 0 – 5 000      | ~0 – 0.4 MB   | 243 ms     | 387 ms     | 466 ms     |
| 5 000 – 10 000 | 0.4 – 0.8 MB  | 692 ms     | 936 ms     | 1.12 s     |
| 10 000 – 15 000 | 0.8 – 1.2 MB | **1.19 s** | 1.40 s     | 1.83 s     |
| 15 000 – 20 000 | 1.2 – 1.7 MB | 1.80 s     | 2.15 s     | 2.53 s     |
| 20 000 – 25 000 | 1.7 – 2.1 MB | 2.29 s     | 2.51 s     | 2.89 s     |

The relationship is essentially linear: **p50 commit (ms) ≈ 0.1 × `fragment_count`** on the
reference setup. Extrapolated:

- 100 000 fragments → ~10 s per commit
- 1 000 000 fragments → ~100 s per commit

For a `processingTime("1 second")` trigger, **commit latency exceeds the trigger interval at
~10 000 fragments**, which is reached after ~840 batches at default parallelism — i.e. minutes
to a few hours of sustained streaming, depending on input rate.

### `OPTIMIZE` keeps it bounded

E2 in the benchmark runs the same workload but invokes `OPTIMIZE` every K batches, collapsing
the accumulated small fragments into one. Steady-state behaviour:

| `OPTIMIZE` cadence | Steady-state `fragment_count` | Steady-state `manifest_bytes` | p50 commit | p90 commit |
|--------------------|------------------------------:|------------------------------:|-----------:|-----------:|
| **Never** (E1)     | grows without bound           | grows without bound           | drifts up  | drifts up  |
| Every **50** batches  | ~600 (1 compacted + 600 new) | ~50 KB                       | **157 ms** | **191 ms** |
| Every **200** batches | cycles 600 ↔ 2 400          | cycles 50 KB ↔ 200 KB        | 190 ms     | 250 ms     |

With every-50, p50 commit latency stays roughly **8× lower** than the same workload without
compaction at 5k+ fragments, and stops drifting upward.

A pragmatic default is **`OPTIMIZE` every 50–200 micro-batches**: every-50 holds latency
flattest, every-200 still bounds it but lets the manifest swing 4× between compactions. Tune
based on how tight your batch-time SLO is.

### Recommended trigger intervals

Pick `processingTime` based on **data-freshness latency target**, not throughput. At a fixed
input rate, achieved throughput is identical across trigger intervals on the reference setup —
what changes is fragment-accumulation rate (and therefore how often `OPTIMIZE` must run).

Measured at 1 000 rows/s sustained for 2 minutes:

| Trigger | Achieved rate | p90 commit | Final `fragment_count` | Margin under trigger |
|---------|--------------:|-----------:|-----------------------:|---------------------:|
| 1 s     | 1 008 rows/s  | 197 ms     | 1 428                  | ~80% headroom        |
| 5 s     | 1 009 rows/s  | 94 ms      | 288                    | ~98% headroom        |
| 15 s    | 1 010 rows/s  | 80 ms      | 96                     | ~99% headroom        |

So `processingTime ≥ ~1 s` is safe on local FS at this rate — even at 1 400+ fragments, p90
commit (197 ms) sits at ~20% of the 1 s budget. Tighter triggers (e.g. `processingTime("200ms")`)
would put commit time at the same order as the trigger interval and start building
back-pressure.

For object storage (S3, GCS, Azure Blob) commit latency is dominated by put / list round-trips
(typically 50–200 ms each, ×2 for the two transactions). Plan for `processingTime ≥ ~5
seconds` there; this range is not yet measured in-tree and your own benchmark on representative
storage is recommended before committing to a value.

`Trigger.Continuous("…")` is **not supported** — use the smallest acceptable `processingTime`
instead.

### How to schedule `OPTIMIZE`

Co-locate it with the streaming query via `foreachBatch`, or run it out-of-band on a cron with
the same Spark job configuration:

```sql
-- Run alongside the streaming query (separate Spark session)
OPTIMIZE lance.`/path/to/streaming/table`;

-- With explicit options
OPTIMIZE lance.`/path/to/table` WITH (target_rows_per_fragment = 1000000);
```

For very high commit rates (sub-second triggers, sustained), compact more aggressively (every
20–50 batches). For low rates (≥ 30 s triggers), compaction can be much less frequent (hourly
or daily).

### Tuning `maxRecoveryLookback`

The recovery scan walks back through up to `maxRecoveryLookback` Lance versions on the first
commit after a restart, reading each version's transaction properties to find a matching
`(streamingQueryId, epochId)`. Actual cost is `0.5 ms × min(maxRecoveryLookback,
current_version)` on the reference setup — i.e. the lookback is a *cap*, not a floor; a small
table never pays for unused headroom.

Measured (E3 in the benchmark, local FS, 5 trials per point):

| `current_version` | `maxRecoveryLookback` | versions actually visited | avg restart cost |
|------------------:|----------------------:|--------------------------:|-----------------:|
| 100               | 100 / 500 / 1 000     | 100                       | ~18 ms           |
| 1 000             | 100 (default)         | 100                       | 75 ms            |
| 1 000             | 1 000                 | 1 000                     | 507 ms           |
| 10 000 (extrapolated) | 10 000 (max)      | 10 000                    | ~5 s             |

The default of `100` is sized for single-writer streaming pipelines: restart cost stays under
~75 ms regardless of how many historical versions the table has. Raise it (up to the hard cap
of `10 000`) only if multiple writers commit concurrently to the same table at a rate where
more than 100 unrelated commits could land between a Txn1 and the next Txn2 retry — otherwise
the larger lookback is overhead paid for nothing.

Recovery cost is paid **once per query restart**, not per commit, so even the worst-case
~5 s scan is acceptable for typical ETL pipelines that restart at most a few times a day.
