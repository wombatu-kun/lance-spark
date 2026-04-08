/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lance.spark.streaming;

import org.lance.spark.LanceDataSource;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.LanceSparkWriteOptions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * Shared fixtures for the streaming sink tests.
 *
 * <p>The tests need to drive a Spark Structured Streaming query from known input data and then
 * assert on the resulting Lance table state. The canonical Spark streaming source for tests is
 * Scala's {@code MemoryStream[T]}, but it's awkward to use from Java due to implicit encoder
 * resolution. This utility instead uses Spark's file-streaming source: we write JSON files into a
 * scratch directory and let Spark's {@code readStream().json(dir)} pick them up one file per
 * micro-batch. Deterministic, Java-friendly, and requires no Scala shims.
 *
 * <p>Two main execution patterns are supported:
 *
 * <ul>
 *   <li><b>AvailableNow mode</b> — stage all input files up front, start the query with {@link
 *       Trigger#AvailableNow()}, and {@code awaitTermination()}. Best for tests that don't care
 *       about per-epoch behavior.
 *   <li><b>ProcessingTime mode</b> — start the query with a short {@code ProcessingTime} trigger,
 *       then push files into the source dir one at a time while calling {@link
 *       #awaitEpochsProcessed(StreamingQuery, long, long)} between writes. Best for tests that need
 *       to verify per-micro-batch behavior (exactly-once replay, recovery, etc.).
 * </ul>
 */
public final class StreamingTestUtils {

  private static final Logger LOG = LoggerFactory.getLogger(StreamingTestUtils.class);

  /** Trigger polling interval used by streaming tests that want fast, tight feedback. */
  public static final String FAST_TRIGGER = "200 milliseconds";

  /** Default timeout for {@link #awaitEpochsProcessed}. 30s is generous for local tests. */
  public static final long DEFAULT_EPOCH_TIMEOUT_MS = 30_000L;

  private StreamingTestUtils() {}

  // =======================================================================
  // Source directory + file helpers
  // =======================================================================

  /** Creates an empty source directory under the given temp dir. */
  public static Path createSourceDir(Path tempDir, String name) throws IOException {
    Path src = tempDir.resolve(name);
    Files.createDirectories(src);
    return src;
  }

  /** Creates an empty checkpoint directory under the given temp dir. */
  public static Path createCheckpointDir(Path tempDir, String name) throws IOException {
    Path ck = tempDir.resolve("ckpt-" + name);
    Files.createDirectories(ck);
    return ck;
  }

  /**
   * Writes a JSON-lines file into the source directory. Each line becomes one row. Uses a
   * monotonically-increasing filename so file-source ordering is deterministic.
   */
  public static void writeJsonBatch(Path sourceDir, String fileName, List<String> jsonLines)
      throws IOException {
    Path file = sourceDir.resolve(fileName + ".json");
    String content = String.join("\n", jsonLines) + "\n";
    Files.writeString(file, content, StandardCharsets.UTF_8);
  }

  /** Convenience overload — writes a single JSON row. */
  public static void writeJsonRow(Path sourceDir, String fileName, String jsonRow)
      throws IOException {
    writeJsonBatch(sourceDir, fileName, List.of(jsonRow));
  }

  // =======================================================================
  // Streaming query construction
  // =======================================================================

  /**
   * Reads a streaming DataFrame from a JSON file source rooted at {@code sourceDir}. Uses the
   * supplied schema instead of schema inference so missing fields produce nulls rather than
   * silently expanding the schema mid-stream.
   */
  public static Dataset<Row> readJsonStream(SparkSession spark, Path sourceDir, StructType schema) {
    return spark.readStream().schema(schema).json(sourceDir.toString());
  }

  /**
   * Starts a streaming query that writes the supplied DataFrame to a Lance table. Caller picks the
   * output mode; caller is responsible for calling {@code query.stop()} when done.
   */
  public static StreamingQuery startLanceSink(
      Dataset<Row> streamingDf,
      String lanceDatasetUri,
      String queryId,
      Path checkpointDir,
      String outputMode,
      Trigger trigger,
      Map<String, String> extraOptions) {
    DataStreamWriter<Row> writer =
        streamingDf
            .writeStream()
            .format(LanceDataSource.name)
            .option(LanceSparkReadOptions.CONFIG_DATASET_URI, lanceDatasetUri)
            .option("checkpointLocation", checkpointDir.toString())
            .option(LanceSparkWriteOptions.CONFIG_STREAMING_QUERY_ID, queryId)
            .outputMode(outputMode)
            .trigger(trigger);
    if (extraOptions != null) {
      for (Map.Entry<String, String> entry : extraOptions.entrySet()) {
        writer = writer.option(entry.getKey(), entry.getValue());
      }
    }
    try {
      return writer.start();
    } catch (java.util.concurrent.TimeoutException e) {
      throw new IllegalStateException(
          "Timed out starting Lance streaming sink for queryId=" + queryId, e);
    }
  }

  /** Short-form helper for the common case: Append output mode with a fast processing trigger. */
  public static StreamingQuery startAppendSink(
      Dataset<Row> streamingDf, String lanceDatasetUri, String queryId, Path checkpointDir) {
    return startLanceSink(
        streamingDf,
        lanceDatasetUri,
        queryId,
        checkpointDir,
        "append",
        Trigger.ProcessingTime(FAST_TRIGGER),
        null);
  }

  /** Short-form helper for AvailableNow mode — process everything staged and terminate. */
  public static StreamingQuery startAvailableNowSink(
      Dataset<Row> streamingDf,
      String lanceDatasetUri,
      String queryId,
      Path checkpointDir,
      String outputMode) {
    return startLanceSink(
        streamingDf,
        lanceDatasetUri,
        queryId,
        checkpointDir,
        outputMode,
        Trigger.AvailableNow(),
        null);
  }

  // =======================================================================
  // Awaiting / completion
  // =======================================================================

  /**
   * Blocks until the streaming query has processed at least {@code targetBatchId} micro-batches, or
   * the timeout elapses. Polls {@link StreamingQuery#recentProgress()} so the caller observes
   * forward progress without tight busy-looping.
   *
   * @throws IllegalStateException if the timeout is reached or the query encounters an exception.
   */
  public static void awaitEpochsProcessed(StreamingQuery query, long targetBatchId, long timeoutMs)
      throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < deadline) {
      if (query.exception().isDefined()) {
        throw new IllegalStateException(
            "Streaming query failed while awaiting epoch " + targetBatchId,
            query.exception().get());
      }
      long latestBatchId = -1L;
      for (org.apache.spark.sql.streaming.StreamingQueryProgress p : query.recentProgress()) {
        if (p != null) {
          latestBatchId = Math.max(latestBatchId, p.batchId());
        }
      }
      if (latestBatchId >= targetBatchId) {
        return;
      }
      Thread.sleep(50);
    }
    throw new IllegalStateException(
        "Timed out waiting for streaming query to reach batchId "
            + targetBatchId
            + " within "
            + timeoutMs
            + "ms. Latest recentProgress="
            + java.util.Arrays.toString(query.recentProgress()));
  }

  /**
   * Waits for the query to naturally terminate (intended for AvailableNow / Once triggers). On
   * timeout, stops the query forcefully and throws.
   */
  public static void awaitTermination(StreamingQuery query, long timeoutMs)
      throws InterruptedException {
    boolean terminated;
    try {
      terminated = query.awaitTermination(timeoutMs);
    } catch (org.apache.spark.sql.streaming.StreamingQueryException e) {
      throw new IllegalStateException("Streaming query terminated with an exception", e);
    }
    if (!terminated) {
      stopQuietly(query);
      throw new IllegalStateException(
          "Streaming query did not terminate within " + timeoutMs + "ms");
    }
    if (query.exception().isDefined()) {
      throw new IllegalStateException(
          "Streaming query terminated with an exception", query.exception().get());
    }
  }

  /**
   * Stops the query, swallowing the checked {@link java.util.concurrent.TimeoutException} that
   * {@link StreamingQuery#stop()} can raise in Spark 3.5+. Test code doesn't care about the stop
   * timing out — we want a best-effort shutdown.
   */
  public static void stopQuietly(StreamingQuery query) {
    if (query == null) {
      return;
    }
    try {
      query.stop();
    } catch (java.util.concurrent.TimeoutException e) {
      LOG.warn("Timed out while stopping streaming query; continuing", e);
    }
  }

  // =======================================================================
  // Lance-side assertion helpers
  // =======================================================================

  /** Reads the Lance table via Spark batch and returns the row count. */
  public static long countRows(SparkSession spark, String lanceDatasetUri) {
    return spark
        .read()
        .format(LanceDataSource.name)
        .option(LanceSparkReadOptions.CONFIG_DATASET_URI, lanceDatasetUri)
        .load()
        .count();
  }

  /** Returns the current Lance dataset version. */
  public static long getLatestVersion(String lanceDatasetUri) {
    try (org.lance.Dataset ds =
        org.lance.Dataset.open().allocator(LanceRuntime.allocator()).uri(lanceDatasetUri).build()) {
      return ds.version();
    }
  }

  /** Returns the value of a key in the Lance dataset config map, or {@code null} if absent. */
  public static String getConfigValue(String lanceDatasetUri, String key) {
    try (org.lance.Dataset ds =
        org.lance.Dataset.open().allocator(LanceRuntime.allocator()).uri(lanceDatasetUri).build()) {
      return ds.getConfig().get(key);
    }
  }

  /**
   * Returns the current per-query epoch watermark persisted in the Lance dataset config map, or
   * {@link StreamingCommitProtocol#NO_EPOCH} if the key is absent or unparseable. Used by
   * exactly-once tests to verify the two-transaction commit protocol is wiring the watermark
   * correctly.
   */
  public static long getPersistedEpochWatermark(String lanceDatasetUri, String queryId) {
    String raw =
        getConfigValue(lanceDatasetUri, StreamingCommitProtocol.WATERMARK_KEY_PREFIX + queryId);
    return StreamingCommitProtocol.parseEpochWatermark(raw);
  }

  /** Returns the number of rows currently in the Lance dataset via the native API (not Spark). */
  public static long countRowsNative(String lanceDatasetUri) {
    try (org.lance.Dataset ds =
        org.lance.Dataset.open().allocator(LanceRuntime.allocator()).uri(lanceDatasetUri).build()) {
      return ds.countRows();
    }
  }

  /**
   * Reads a streaming DataFrame from a rate source. Useful for tests that want to verify throughput
   * behavior without caring about exact row values.
   */
  public static Dataset<Row> readRateStream(SparkSession spark, long rowsPerSecond) {
    return spark
        .readStream()
        .format("rate")
        .option("rowsPerSecond", String.valueOf(rowsPerSecond))
        .load();
  }

  /**
   * Manually bumps the streaming epoch watermark for the given queryId in the dataset config map,
   * bypassing Spark entirely. Used by exactly-once tests to simulate state left over from a prior
   * streaming query run — e.g., "pretend epoch 999 is already committed, then start a new query and
   * verify it skips every epoch ≤ 999".
   */
  public static void setEpochWatermark(String lanceDatasetUri, String queryId, long epochId) {
    try (org.lance.Dataset ds =
        org.lance.Dataset.open().allocator(LanceRuntime.allocator()).uri(lanceDatasetUri).build()) {
      java.util.Map<String, String> updates = new java.util.HashMap<>();
      updates.put(StreamingCommitProtocol.WATERMARK_KEY_PREFIX + queryId, String.valueOf(epochId));
      org.lance.operation.UpdateConfig updateConfig =
          org.lance.operation.UpdateConfig.builder().upsertValues(updates).build();
      try (org.lance.Transaction txn =
              new org.lance.Transaction.Builder()
                  .readVersion(ds.version())
                  .operation(updateConfig)
                  .build();
          org.lance.Dataset committed = new org.lance.CommitBuilder(ds).execute(txn)) {
        // auto-close
      }
    }
  }
}
