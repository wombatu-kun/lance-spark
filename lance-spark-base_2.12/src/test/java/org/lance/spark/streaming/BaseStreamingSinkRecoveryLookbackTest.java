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
import org.lance.spark.LanceSparkWriteOptions;
import org.lance.spark.TestUtils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the bounded at-least-once fallback behavior of {@link StreamingCommitProtocol} when the
 * recovery scan window is smaller than the gap between a crashed-after-Txn1 state and the retry.
 *
 * <p>The sink's exactly-once guarantee degrades to at-least-once IFF more than {@code
 * maxRecoveryLookback} unrelated commits land between the original crash (between Txn1 and Txn2)
 * and the subsequent retry. This test explicitly constructs that scenario:
 *
 * <ol>
 *   <li>Streaming query #1 successfully commits epoch 0 (both Txn1 and Txn2).
 *   <li>A batch of unrelated commits is performed, pushing the epoch-0 transaction out of the scan
 *       lookback window.
 *   <li>The epoch watermark is manually reset to {@link StreamingCommitProtocol#NO_EPOCH},
 *       simulating the state "Txn2 did not complete".
 *   <li>Streaming query #2 runs with the SAME {@code streamingQueryId} and re-presents the same
 *       epoch 0 with the same data.
 *   <li>The sink's recovery scan walks back only {@code maxRecoveryLookback} versions and does NOT
 *       find the prior Txn1 — so it re-appends the data, producing a documented bounded duplicate.
 * </ol>
 *
 * <p>This test is intentionally sensitive to any change in the protocol's recovery behavior. If a
 * future refactor moves the Txn1-lookup to a different mechanism (e.g., a dedicated index or
 * extended transaction properties storage), this test will have to be updated explicitly — which is
 * the right signal that the protocol's documented semantics are changing.
 */
public abstract class BaseStreamingSinkRecoveryLookbackTest {

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
            .appName("lance-streaming-sink-recovery-lookback-test")
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

  @Test
  public void lookbackExhaustedProducesBoundedDuplicates(TestInfo testInfo) throws Exception {
    String testName = testInfo.getTestMethod().get().getName();
    String queryId = testName;
    String lancePath = TestUtils.getDatasetUri(workDir.toString(), testName);
    Path sourceDir = StreamingTestUtils.createSourceDir(workDir, "src-" + testName);
    Path ckDir1 = StreamingTestUtils.createCheckpointDir(workDir, testName + "-1");
    Path ckDir2 = StreamingTestUtils.createCheckpointDir(workDir, testName + "-2");

    // -------------------------------------------------------------------
    // Step 1: Pre-create empty target table.
    // -------------------------------------------------------------------
    preCreateEmptyTable(lancePath);

    // -------------------------------------------------------------------
    // Step 2: Streaming query #1 runs and fully commits 2 rows.
    // -------------------------------------------------------------------
    StreamingTestUtils.writeJsonBatch(
        sourceDir,
        "0001",
        Arrays.asList("{\"id\":1,\"value\":\"s1-row-a\"}", "{\"id\":2,\"value\":\"s1-row-b\"}"));
    Dataset<Row> stream1 = StreamingTestUtils.readJsonStream(spark, sourceDir, SIMPLE_SCHEMA);
    StreamingQuery query1 =
        StreamingTestUtils.startLanceSink(
            stream1,
            lancePath,
            queryId,
            ckDir1,
            "append",
            org.apache.spark.sql.streaming.Trigger.AvailableNow(),
            // Use a very small lookback so the test doesn't need dozens of filler commits.
            java.util.Map.of(LanceSparkWriteOptions.CONFIG_MAX_RECOVERY_LOOKBACK, "3"));
    try {
      StreamingTestUtils.awaitTermination(query1, 60_000L);
    } finally {
      StreamingTestUtils.stopQuietly(query1);
    }

    assertEquals(2L, StreamingTestUtils.countRows(spark, lancePath));
    long watermarkAfterQuery1 = StreamingTestUtils.getPersistedEpochWatermark(lancePath, queryId);
    assertTrue(
        watermarkAfterQuery1 >= 0L,
        "Query #1's Txn2 should have persisted a watermark, got " + watermarkAfterQuery1);

    // -------------------------------------------------------------------
    // Step 3: Perform a burst of unrelated batch commits (no streaming props).
    // Each commit bumps the dataset version, burying the streaming transaction from step 2
    // deeper in history. We deliberately do MORE than maxRecoveryLookback commits so the scan
    // cannot reach it.
    // -------------------------------------------------------------------
    int unrelatedCommits = 6; // > maxRecoveryLookback (3)
    for (int i = 0; i < unrelatedCommits; i++) {
      List<Row> unrelated = Arrays.asList(RowFactory.create(1000 + i, "unrelated-" + i));
      spark
          .createDataFrame(unrelated, SIMPLE_SCHEMA)
          .write()
          .format(LanceDataSource.name)
          .option(LanceSparkReadOptions.CONFIG_DATASET_URI, lancePath)
          .mode(SaveMode.Append)
          .save();
    }
    assertEquals(
        2L + unrelatedCommits,
        StreamingTestUtils.countRows(spark, lancePath),
        "Unrelated batch writes should have produced distinct rows");

    // -------------------------------------------------------------------
    // Step 4: Reset the epoch watermark to NO_EPOCH to simulate "Txn2 never completed".
    // After this, the sink state is indistinguishable from a real crashed-between-Txn1-and-Txn2
    // scenario except that the Txn1 is already way out of reach of the recovery scan.
    // -------------------------------------------------------------------
    StreamingTestUtils.setEpochWatermark(lancePath, queryId, StreamingCommitProtocol.NO_EPOCH);
    assertEquals(
        StreamingCommitProtocol.NO_EPOCH,
        StreamingTestUtils.getPersistedEpochWatermark(lancePath, queryId),
        "Watermark reset should have taken effect");
    long versionAfterReset = StreamingTestUtils.getLatestVersion(lancePath);

    // -------------------------------------------------------------------
    // Step 5: Streaming query #2 re-presents the same epoch 0 with the same data. It uses the
    // SAME streamingQueryId and a fresh checkpoint (so Spark's own dedup doesn't intervene).
    //
    // Expected behavior:
    //   - Watermark is NO_EPOCH, so the fast-path skip does NOT fire.
    //   - Recovery scan walks back only 3 versions. The prior Txn1 is 7+ versions back.
    //   - Scan returns false → protocol runs Txn1 AGAIN (re-append) → duplicate rows.
    //   - Txn2 bumps the watermark.
    //
    // Final row count = 2 (query #1) + 6 (unrelated) + 2 (query #2 retry) = 10.
    // -------------------------------------------------------------------
    Dataset<Row> stream2 = StreamingTestUtils.readJsonStream(spark, sourceDir, SIMPLE_SCHEMA);
    StreamingQuery query2 =
        StreamingTestUtils.startLanceSink(
            stream2,
            lancePath,
            queryId,
            ckDir2,
            "append",
            org.apache.spark.sql.streaming.Trigger.AvailableNow(),
            java.util.Map.of(LanceSparkWriteOptions.CONFIG_MAX_RECOVERY_LOOKBACK, "3"));
    try {
      StreamingTestUtils.awaitTermination(query2, 60_000L);
    } finally {
      StreamingTestUtils.stopQuietly(query2);
    }

    long finalRowCount = StreamingTestUtils.countRows(spark, lancePath);
    assertEquals(
        10L,
        finalRowCount,
        "Row count after retry must equal 10 (2 original + 6 unrelated + 2 retry duplicates) — "
            + "this pins the documented bounded at-least-once behavior. If this assertion changes, "
            + "the recovery semantics of StreamingCommitProtocol are changing and the docs must "
            + "be updated in lockstep.");

    // Sanity check: the 2 retry rows should be byte-identical duplicates of the 2 original rows.
    List<Row> rowsWithS1A =
        spark
            .read()
            .format(LanceDataSource.name)
            .option(LanceSparkReadOptions.CONFIG_DATASET_URI, lancePath)
            .load()
            .filter("value = 's1-row-a'")
            .collectAsList();
    assertEquals(
        2,
        rowsWithS1A.size(),
        "The 's1-row-a' marker should appear exactly twice — once from query #1, once from the "
            + "bounded-at-least-once retry");

    assertTrue(
        StreamingTestUtils.getLatestVersion(lancePath) > versionAfterReset,
        "Retry must have advanced the Lance version with a new Txn1 commit");
    assertTrue(
        StreamingTestUtils.getPersistedEpochWatermark(lancePath, queryId) >= 0L,
        "Retry's Txn2 must have re-established the watermark");
  }

  @Test
  public void lookbackWithinWindowSelfHealsAndAvoidsDuplicates(TestInfo testInfo) throws Exception {
    String testName = testInfo.getTestMethod().get().getName();
    String queryId = testName;
    String lancePath = TestUtils.getDatasetUri(workDir.toString(), testName);
    Path sourceDir = StreamingTestUtils.createSourceDir(workDir, "src-" + testName);
    Path ckDir1 = StreamingTestUtils.createCheckpointDir(workDir, testName + "-1");
    Path ckDir2 = StreamingTestUtils.createCheckpointDir(workDir, testName + "-2");

    // Contrast with the exhausted-lookback test: here we do FEWER unrelated commits than the
    // lookback window, so the recovery scan successfully finds the prior Txn1 and skips to
    // Txn2. The final row count matches the "exactly-once" expectation: 2 + N, no duplicates.

    preCreateEmptyTable(lancePath);

    // Query #1 commits 2 rows.
    StreamingTestUtils.writeJsonBatch(
        sourceDir,
        "0001",
        Arrays.asList("{\"id\":1,\"value\":\"commit-a\"}", "{\"id\":2,\"value\":\"commit-b\"}"));
    Dataset<Row> stream1 = StreamingTestUtils.readJsonStream(spark, sourceDir, SIMPLE_SCHEMA);
    StreamingQuery query1 =
        StreamingTestUtils.startLanceSink(
            stream1,
            lancePath,
            queryId,
            ckDir1,
            "append",
            org.apache.spark.sql.streaming.Trigger.AvailableNow(),
            java.util.Map.of(LanceSparkWriteOptions.CONFIG_MAX_RECOVERY_LOOKBACK, "20"));
    try {
      StreamingTestUtils.awaitTermination(query1, 60_000L);
    } finally {
      StreamingTestUtils.stopQuietly(query1);
    }
    assertEquals(2L, StreamingTestUtils.countRows(spark, lancePath));

    // Only 2 unrelated commits (well under the 20-version lookback).
    List<Row> unrelated = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      unrelated.add(RowFactory.create(100 + i, "ok-" + i));
    }
    spark
        .createDataFrame(unrelated, SIMPLE_SCHEMA)
        .write()
        .format(LanceDataSource.name)
        .option(LanceSparkReadOptions.CONFIG_DATASET_URI, lancePath)
        .mode(SaveMode.Append)
        .save();
    assertEquals(4L, StreamingTestUtils.countRows(spark, lancePath));

    // Reset watermark to simulate crashed-after-Txn1.
    StreamingTestUtils.setEpochWatermark(lancePath, queryId, StreamingCommitProtocol.NO_EPOCH);

    // Query #2 retries; the recovery scan finds the prior Txn1 within the lookback window.
    Dataset<Row> stream2 = StreamingTestUtils.readJsonStream(spark, sourceDir, SIMPLE_SCHEMA);
    StreamingQuery query2 =
        StreamingTestUtils.startLanceSink(
            stream2,
            lancePath,
            queryId,
            ckDir2,
            "append",
            org.apache.spark.sql.streaming.Trigger.AvailableNow(),
            java.util.Map.of(LanceSparkWriteOptions.CONFIG_MAX_RECOVERY_LOOKBACK, "20"));
    try {
      StreamingTestUtils.awaitTermination(query2, 60_000L);
    } finally {
      StreamingTestUtils.stopQuietly(query2);
    }

    // Final row count must be 4 — the 2 original streaming rows plus 2 unrelated, with NO
    // duplicate from the retry because the recovery scan found the prior Txn1 and skipped it.
    assertEquals(
        4L,
        StreamingTestUtils.countRows(spark, lancePath),
        "Recovery scan within lookback window should have found prior Txn1 and skipped re-append");
  }

  private void preCreateEmptyTable(String lancePath) {
    Dataset<Row> empty = spark.createDataFrame(java.util.Collections.emptyList(), SIMPLE_SCHEMA);
    empty
        .write()
        .format(LanceDataSource.name)
        .option(LanceSparkReadOptions.CONFIG_DATASET_URI, lancePath)
        .save();
  }
}
