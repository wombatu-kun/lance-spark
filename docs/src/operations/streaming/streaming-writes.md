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
