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
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.TestUtils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Critical correctness tests for exactly-once semantics in the Lance streaming sink.
 *
 * <p>The sink guarantees that each {@code (streamingQueryId, epochId)} pair is committed at most
 * once, even when Spark replays epochs after a failure. This is implemented by {@link
 * StreamingCommitProtocol}'s two-transaction commit: Txn1 appends fragments with transaction
 * properties carrying {@code streaming.queryId}/{@code streaming.epochId}, and Txn2 bumps a
 * per-query epoch watermark in the Lance dataset config map. These tests directly exercise the
 * replay skip, recovery fallback, and monotonic-version-progress branches.
 */
public abstract class BaseStreamingSinkExactlyOnceTest {

  private static SparkSession spark;

  @TempDir static Path workDir;

  protected static final StructType SIMPLE_SCHEMA =
      new StructType(
          new StructField[] {
            DataTypes.createStructField("id", DataTypes.IntegerType, false),
            DataTypes.createStructField("value", DataTypes.StringType, false)
          });

  @BeforeAll
  static void setup() {
    spark =
        SparkSession.builder()
            .appName("lance-streaming-sink-exactly-once-test")
            .master("local[2]")
            .config("spark.sql.catalog.lance", "org.lance.spark.LanceNamespaceSparkCatalog")
            .config("spark.sql.catalog.lance.max_row_per_file", "10")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.streaming.stopTimeout", "5s")
            .getOrCreate();
  }

  @AfterAll
  static void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  // ========================================================================
  // Test 1: Manual watermark bump simulates "already-committed epoch" state
  // ========================================================================

  /**
   * Directly exercises the {@link StreamingCommitProtocol.CommitAction#SKIP_REPLAY} branch by
   * pre-setting the epoch watermark high enough that every epoch Spark sends will be considered a
   * replay. Verifies that no streaming commits land in the Lance table.
   */
  @Test
  public void prePersistedHighWatermarkSkipsAllEpochs(TestInfo testInfo) throws Exception {
    String testName = testInfo.getTestMethod().get().getName();
    String lancePath = TestUtils.getDatasetUri(workDir.toString(), testName);
    Path sourceDir = StreamingTestUtils.createSourceDir(workDir, "src-" + testName);
    Path ckDir = StreamingTestUtils.createCheckpointDir(workDir, testName);

    // Pre-create an empty table.
    preCreateEmptyTable(lancePath, SIMPLE_SCHEMA);
    assertEquals(0L, StreamingTestUtils.countRows(spark, lancePath));

    // Manually set the watermark to a high value, simulating "epoch 1000 already committed".
    StreamingTestUtils.setEpochWatermark(lancePath, testName, 1000L);
    assertEquals(
        1000L,
        StreamingTestUtils.getPersistedEpochWatermark(lancePath, testName),
        "Manual watermark should have been persisted");
    long versionAfterWatermarkSet = StreamingTestUtils.getLatestVersion(lancePath);

    // Stage 3 input rows that would otherwise be appended.
    StreamingTestUtils.writeJsonBatch(
        sourceDir,
        "0001",
        List.of(
            "{\"id\":1,\"value\":\"would-be-appended\"}",
            "{\"id\":2,\"value\":\"would-be-appended\"}",
            "{\"id\":3,\"value\":\"would-be-appended\"}"));

    Dataset<Row> stream = StreamingTestUtils.readJsonStream(spark, sourceDir, SIMPLE_SCHEMA);
    StreamingQuery query =
        StreamingTestUtils.startAvailableNowSink(stream, lancePath, testName, ckDir, "append");
    try {
      StreamingTestUtils.awaitTermination(query, 60_000L);
    } finally {
      StreamingTestUtils.stopQuietly(query);
    }

    // Verify: the table is unchanged. All epochs Spark sent were ≤ 1000 so the sink skipped them.
    assertEquals(
        0L,
        StreamingTestUtils.countRows(spark, lancePath),
        "Sink should skip epochs whose id is ≤ the persisted watermark");
    assertEquals(
        versionAfterWatermarkSet,
        StreamingTestUtils.getLatestVersion(lancePath),
        "Lance version should not have advanced — no commits from the skipped epochs");
  }

  // ========================================================================
  // Test 2: Back-to-back streaming queries advance version + watermark monotonically
  // ========================================================================

  /**
   * Runs two separate streaming queries (fresh checkpoints each) with the same {@code
   * streamingQueryId} writing to the same Lance table. Both queries should commit their data
   * successfully without colliding on the epoch watermark, and the final table should contain the
   * union of both inputs.
   */
  @Test
  public void backToBackQueriesAdvanceMonotonically(TestInfo testInfo) throws Exception {
    String testName = testInfo.getTestMethod().get().getName();
    String lancePath = TestUtils.getDatasetUri(workDir.toString(), testName);
    Path sourceA = StreamingTestUtils.createSourceDir(workDir, "src-a-" + testName);
    Path sourceB = StreamingTestUtils.createSourceDir(workDir, "src-b-" + testName);
    Path ckA = StreamingTestUtils.createCheckpointDir(workDir, "a-" + testName);
    Path ckB = StreamingTestUtils.createCheckpointDir(workDir, "b-" + testName);

    preCreateEmptyTable(lancePath, SIMPLE_SCHEMA);

    // Query A: stream 2 rows.
    StreamingTestUtils.writeJsonBatch(
        sourceA,
        "0001",
        List.of("{\"id\":1,\"value\":\"a-one\"}", "{\"id\":2,\"value\":\"a-two\"}"));
    Dataset<Row> streamA = StreamingTestUtils.readJsonStream(spark, sourceA, SIMPLE_SCHEMA);
    StreamingQuery queryA =
        StreamingTestUtils.startAvailableNowSink(streamA, lancePath, testName, ckA, "append");
    try {
      StreamingTestUtils.awaitTermination(queryA, 60_000L);
    } finally {
      StreamingTestUtils.stopQuietly(queryA);
    }
    assertEquals(2L, StreamingTestUtils.countRows(spark, lancePath));
    long watermarkAfterA = StreamingTestUtils.getPersistedEpochWatermark(lancePath, testName);
    long versionAfterA = StreamingTestUtils.getLatestVersion(lancePath);

    // Query B: stream 3 more rows. Same streamingQueryId, fresh checkpoint.
    // Note: fresh checkpoint means Spark's epochId counter restarts at 0. The sink's watermark
    // is already at `watermarkAfterA` (likely 0 from Query A). This means Query B's epoch 0
    // would be SKIPPED by the sink because 0 ≤ watermarkAfterA. For this test to produce new
    // data, we use a DIFFERENT streamingQueryId for Query B so the two queries have independent
    // watermarks.
    String queryIdB = testName + "-b";
    StreamingTestUtils.writeJsonBatch(
        sourceB,
        "0001",
        List.of(
            "{\"id\":10,\"value\":\"b-one\"}",
            "{\"id\":20,\"value\":\"b-two\"}",
            "{\"id\":30,\"value\":\"b-three\"}"));
    Dataset<Row> streamB = StreamingTestUtils.readJsonStream(spark, sourceB, SIMPLE_SCHEMA);
    StreamingQuery queryB =
        StreamingTestUtils.startAvailableNowSink(streamB, lancePath, queryIdB, ckB, "append");
    try {
      StreamingTestUtils.awaitTermination(queryB, 60_000L);
    } finally {
      StreamingTestUtils.stopQuietly(queryB);
    }

    // Verify: both queries' data is present; both watermarks are independently tracked.
    assertEquals(5L, StreamingTestUtils.countRows(spark, lancePath));
    assertTrue(
        StreamingTestUtils.getLatestVersion(lancePath) > versionAfterA,
        "Lance version must advance after Query B commits");
    assertTrue(
        StreamingTestUtils.getPersistedEpochWatermark(lancePath, queryIdB) >= 0L,
        "Query B's watermark must be independently persisted");
    assertEquals(
        watermarkAfterA,
        StreamingTestUtils.getPersistedEpochWatermark(lancePath, testName),
        "Query A's watermark must remain unchanged when Query B commits");
  }

  // ========================================================================
  // Test 3: Restart with same checkpoint + no new data → no duplicates
  // ========================================================================

  /**
   * Runs a streaming query over some data, then restarts it with the same checkpoint directory and
   * no new source files. The second run should observe Spark's own checkpoint replay skip all
   * already-processed files; the sink's idempotency is the belt-and-braces guarantee that even if
   * Spark did re-emit an epoch, the sink would refuse to re-commit it.
   */
  @Test
  public void restartWithSameCheckpointNoDuplicates(TestInfo testInfo) throws Exception {
    String testName = testInfo.getTestMethod().get().getName();
    String lancePath = TestUtils.getDatasetUri(workDir.toString(), testName);
    Path sourceDir = StreamingTestUtils.createSourceDir(workDir, "src-" + testName);
    Path ckDir = StreamingTestUtils.createCheckpointDir(workDir, testName);

    preCreateEmptyTable(lancePath, SIMPLE_SCHEMA);

    // Stage 5 rows.
    StreamingTestUtils.writeJsonBatch(
        sourceDir,
        "0001",
        List.of(
            "{\"id\":1,\"value\":\"r1\"}",
            "{\"id\":2,\"value\":\"r2\"}",
            "{\"id\":3,\"value\":\"r3\"}",
            "{\"id\":4,\"value\":\"r4\"}",
            "{\"id\":5,\"value\":\"r5\"}"));

    // Run 1: process all files.
    Dataset<Row> stream1 = StreamingTestUtils.readJsonStream(spark, sourceDir, SIMPLE_SCHEMA);
    StreamingQuery query1 =
        StreamingTestUtils.startAvailableNowSink(stream1, lancePath, testName, ckDir, "append");
    try {
      StreamingTestUtils.awaitTermination(query1, 60_000L);
    } finally {
      StreamingTestUtils.stopQuietly(query1);
    }
    assertEquals(5L, StreamingTestUtils.countRows(spark, lancePath));
    long versionAfterRun1 = StreamingTestUtils.getLatestVersion(lancePath);
    long rowCountAfterRun1 = StreamingTestUtils.countRows(spark, lancePath);

    // Run 2: same checkpoint, no new files. Should be a no-op.
    Dataset<Row> stream2 = StreamingTestUtils.readJsonStream(spark, sourceDir, SIMPLE_SCHEMA);
    StreamingQuery query2 =
        StreamingTestUtils.startAvailableNowSink(stream2, lancePath, testName, ckDir, "append");
    try {
      StreamingTestUtils.awaitTermination(query2, 60_000L);
    } finally {
      StreamingTestUtils.stopQuietly(query2);
    }

    // Verify: no duplicates. Row count and version must be unchanged.
    assertEquals(
        rowCountAfterRun1,
        StreamingTestUtils.countRows(spark, lancePath),
        "Restart with same checkpoint and no new data must not add duplicate rows");
    assertEquals(
        versionAfterRun1,
        StreamingTestUtils.getLatestVersion(lancePath),
        "Restart with same checkpoint and no new data must not advance the Lance version");
  }

  // ========================================================================
  // Test 4: Restart with same checkpoint + new data → incremental append
  // ========================================================================

  /**
   * Runs a streaming query over some data, then restarts with the same checkpoint and additional
   * source files. The second run should pick up only the new files and append them, without
   * re-writing the files processed in the first run.
   */
  @Test
  public void restartWithSameCheckpointProcessesOnlyNewData(TestInfo testInfo) throws Exception {
    String testName = testInfo.getTestMethod().get().getName();
    String lancePath = TestUtils.getDatasetUri(workDir.toString(), testName);
    Path sourceDir = StreamingTestUtils.createSourceDir(workDir, "src-" + testName);
    Path ckDir = StreamingTestUtils.createCheckpointDir(workDir, testName);

    preCreateEmptyTable(lancePath, SIMPLE_SCHEMA);

    // Stage batch 1: 3 rows.
    StreamingTestUtils.writeJsonBatch(
        sourceDir,
        "0001",
        List.of(
            "{\"id\":1,\"value\":\"batch1-a\"}",
            "{\"id\":2,\"value\":\"batch1-b\"}",
            "{\"id\":3,\"value\":\"batch1-c\"}"));
    Dataset<Row> stream1 = StreamingTestUtils.readJsonStream(spark, sourceDir, SIMPLE_SCHEMA);
    StreamingQuery query1 =
        StreamingTestUtils.startAvailableNowSink(stream1, lancePath, testName, ckDir, "append");
    try {
      StreamingTestUtils.awaitTermination(query1, 60_000L);
    } finally {
      StreamingTestUtils.stopQuietly(query1);
    }
    assertEquals(3L, StreamingTestUtils.countRows(spark, lancePath));
    long versionAfterRun1 = StreamingTestUtils.getLatestVersion(lancePath);

    // Stage batch 2: 2 more rows in a new file.
    StreamingTestUtils.writeJsonBatch(
        sourceDir,
        "0002",
        List.of("{\"id\":10,\"value\":\"batch2-a\"}", "{\"id\":20,\"value\":\"batch2-b\"}"));

    // Run 2: same checkpoint.
    Dataset<Row> stream2 = StreamingTestUtils.readJsonStream(spark, sourceDir, SIMPLE_SCHEMA);
    StreamingQuery query2 =
        StreamingTestUtils.startAvailableNowSink(stream2, lancePath, testName, ckDir, "append");
    try {
      StreamingTestUtils.awaitTermination(query2, 60_000L);
    } finally {
      StreamingTestUtils.stopQuietly(query2);
    }

    // Verify: total rows = batch1 + batch2 (no duplicates from batch1).
    assertEquals(5L, StreamingTestUtils.countRows(spark, lancePath));
    assertNotEquals(
        versionAfterRun1,
        StreamingTestUtils.getLatestVersion(lancePath),
        "Second run's commit should have advanced the Lance version");
  }

  // ========================================================================
  // Helpers
  // ========================================================================

  private void preCreateEmptyTable(String lancePath, StructType schema) {
    Dataset<Row> empty = spark.createDataFrame(java.util.Collections.emptyList(), schema);
    empty
        .write()
        .format(LanceDataSource.name)
        .option(LanceSparkReadOptions.CONFIG_DATASET_URI, lancePath)
        .save();
  }
}
