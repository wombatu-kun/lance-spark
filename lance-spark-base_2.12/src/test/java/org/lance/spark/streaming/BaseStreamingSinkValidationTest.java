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
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises the streaming sink's user-facing error paths and trigger-mode variations.
 *
 * <p>The test cases are intentionally narrow and assertive — each one targets a single failure or
 * behavior that a user might encounter and verifies the error message is actionable. Covers:
 *
 * <ul>
 *   <li>Streaming write against a non-existent target table (clear error).
 *   <li>Streaming write without {@code streamingQueryId} set (clear error referencing the required
 *       option name).
 *   <li>Streaming query using {@link Trigger#ProcessingTime(String)} instead of {@code
 *       AvailableNow} — validates the alternative trigger path.
 *   <li>Schema mismatch between the streaming DataFrame and the Lance table (rejected with a
 *       schema-related error at commit time).
 * </ul>
 */
public abstract class BaseStreamingSinkValidationTest {

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
            .appName("lance-streaming-sink-validation-test")
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
  // Non-existent target table → clear error
  // ========================================================================

  @Test
  public void nonExistentTargetTableProducesClearError(TestInfo testInfo) throws Exception {
    String testName = testInfo.getTestMethod().get().getName();
    String lancePath = TestUtils.getDatasetUri(workDir.toString(), testName + "-missing");
    Path sourceDir = StreamingTestUtils.createSourceDir(workDir, "src-" + testName);
    Path ckDir = StreamingTestUtils.createCheckpointDir(workDir, testName);

    // Deliberately do NOT create the table.
    StreamingTestUtils.writeJsonBatch(sourceDir, "0001", List.of("{\"id\":1,\"value\":\"x\"}"));

    Dataset<Row> stream = StreamingTestUtils.readJsonStream(spark, sourceDir, SIMPLE_SCHEMA);

    // The error is thrown at commit time when the sink tries to open the target dataset. We
    // catch it either at query startup or in awaitTermination.
    Exception ex =
        assertThrows(
            Exception.class,
            () -> {
              StreamingQuery q =
                  StreamingTestUtils.startAvailableNowSink(
                      stream, lancePath, testName, ckDir, "append");
              try {
                StreamingTestUtils.awaitTermination(q, 60_000L);
              } finally {
                StreamingTestUtils.stopQuietly(q);
              }
            });

    // The underlying cause must mention that the target table does not exist.
    String msg = flattenMessage(ex);
    assertTrue(
        msg.contains("does not exist")
            || msg.contains("not found")
            || msg.contains("Failed to open"),
        "Error message should explain that the target table is missing. Got: " + msg);
  }

  // ========================================================================
  // Missing streamingQueryId → clear error
  // ========================================================================

  @Test
  public void missingStreamingQueryIdProducesClearError(TestInfo testInfo) throws Exception {
    String testName = testInfo.getTestMethod().get().getName();
    String lancePath = TestUtils.getDatasetUri(workDir.toString(), testName);
    Path sourceDir = StreamingTestUtils.createSourceDir(workDir, "src-" + testName);
    Path ckDir = StreamingTestUtils.createCheckpointDir(workDir, testName);

    preCreateEmptyTable(lancePath);
    StreamingTestUtils.writeJsonBatch(sourceDir, "0001", List.of("{\"id\":1,\"value\":\"x\"}"));

    // Deliberately omit the streamingQueryId option.
    Dataset<Row> stream = StreamingTestUtils.readJsonStream(spark, sourceDir, SIMPLE_SCHEMA);
    Exception ex =
        assertThrows(
            Exception.class,
            () -> {
              StreamingQuery q =
                  stream
                      .writeStream()
                      .format(LanceDataSource.name)
                      .option(LanceSparkReadOptions.CONFIG_DATASET_URI, lancePath)
                      .option("checkpointLocation", ckDir.toString())
                      .outputMode("append")
                      .trigger(Trigger.AvailableNow())
                      .start();
              try {
                StreamingTestUtils.awaitTermination(q, 60_000L);
              } finally {
                StreamingTestUtils.stopQuietly(q);
              }
            });

    String msg = flattenMessage(ex);
    assertTrue(
        msg.contains(LanceSparkWriteOptions.CONFIG_STREAMING_QUERY_ID),
        "Error message should name the required 'streamingQueryId' option. Got: " + msg);
  }

  // ========================================================================
  // ProcessingTime trigger works (AvailableNow is tested extensively elsewhere)
  // ========================================================================

  @Test
  public void processingTimeTriggerWorks(TestInfo testInfo) throws Exception {
    String testName = testInfo.getTestMethod().get().getName();
    String lancePath = TestUtils.getDatasetUri(workDir.toString(), testName);
    Path sourceDir = StreamingTestUtils.createSourceDir(workDir, "src-" + testName);
    Path ckDir = StreamingTestUtils.createCheckpointDir(workDir, testName);

    preCreateEmptyTable(lancePath);

    // Stage files BEFORE the query starts so the ProcessingTime trigger picks them up on its
    // first poll. We use a short trigger interval and then stop the query once we have some
    // progress.
    StreamingTestUtils.writeJsonBatch(
        sourceDir, "0001", List.of("{\"id\":1,\"value\":\"a\"}", "{\"id\":2,\"value\":\"b\"}"));

    Dataset<Row> stream = StreamingTestUtils.readJsonStream(spark, sourceDir, SIMPLE_SCHEMA);
    StreamingQuery q =
        StreamingTestUtils.startLanceSink(
            stream,
            lancePath,
            testName,
            ckDir,
            "append",
            Trigger.ProcessingTime("100 milliseconds"),
            null);
    try {
      // Wait for the first micro-batch to commit.
      StreamingTestUtils.awaitEpochsProcessed(q, 0L, StreamingTestUtils.DEFAULT_EPOCH_TIMEOUT_MS);
      // Give it a moment to fully commit before stopping.
      Thread.sleep(500);
    } finally {
      StreamingTestUtils.stopQuietly(q);
    }

    assertEquals(
        2L,
        StreamingTestUtils.countRows(spark, lancePath),
        "ProcessingTime trigger should have picked up the staged files on its first poll");
  }

  // ========================================================================
  // Schema mismatch between the streaming DataFrame and the Lance table
  // ========================================================================

  @Test
  public void schemaMismatchRejectedAtCommitTime(TestInfo testInfo) throws Exception {
    String testName = testInfo.getTestMethod().get().getName();
    String lancePath = TestUtils.getDatasetUri(workDir.toString(), testName);
    Path sourceDir = StreamingTestUtils.createSourceDir(workDir, "src-" + testName);
    Path ckDir = StreamingTestUtils.createCheckpointDir(workDir, testName);

    // Pre-create the table with SIMPLE_SCHEMA = (id INT, value STRING).
    preCreateEmptyTable(lancePath);

    // Stage a file whose rows conform to the wrong schema: (id INT, unknown_col FLOAT).
    // The mismatch surfaces at commit time when LanceBatchWrite.doCommit builds an Arrow
    // schema from the wrong StructType and tries to append fragments that are incompatible
    // with the existing dataset schema.
    StructType wrongSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField("unknown_col", DataTypes.FloatType, false)
            });
    StreamingTestUtils.writeJsonBatch(sourceDir, "0001", List.of("{\"id\":1,\"unknown_col\":1.5}"));

    Dataset<Row> stream = StreamingTestUtils.readJsonStream(spark, sourceDir, wrongSchema);
    Exception ex =
        assertThrows(
            Exception.class,
            () -> {
              StreamingQuery q =
                  StreamingTestUtils.startAvailableNowSink(
                      stream, lancePath, testName, ckDir, "append");
              try {
                StreamingTestUtils.awaitTermination(q, 60_000L);
              } finally {
                StreamingTestUtils.stopQuietly(q);
              }
            });

    String msg = flattenMessage(ex);
    assertTrue(
        msg.toLowerCase().contains("schema")
            || msg.toLowerCase().contains("field")
            || msg.toLowerCase().contains("column"),
        "Error should reference a schema or field mismatch. Got: " + msg);
  }

  // ========================================================================
  // Helpers
  // ========================================================================

  private void preCreateEmptyTable(String lancePath) {
    Dataset<Row> empty = spark.createDataFrame(java.util.Collections.emptyList(), SIMPLE_SCHEMA);
    empty
        .write()
        .format(LanceDataSource.name)
        .option(LanceSparkReadOptions.CONFIG_DATASET_URI, lancePath)
        .save();
  }

  /** Walks the cause chain of an exception and returns a single concatenated message. */
  private String flattenMessage(Throwable t) {
    StringBuilder sb = new StringBuilder();
    Throwable cur = t;
    int depth = 0;
    while (cur != null && depth < 10) {
      if (cur.getMessage() != null) {
        sb.append(cur.getMessage()).append(" | ");
      }
      cur = cur.getCause();
      depth++;
    }
    return sb.toString();
  }
}
