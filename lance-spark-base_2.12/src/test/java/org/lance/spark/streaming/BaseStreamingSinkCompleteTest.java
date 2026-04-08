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
import org.apache.spark.sql.RowFactory;
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
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises the {@link LanceStreamingWrite} sink in Complete output mode — used for streaming
 * aggregation queries that emit their full state on every trigger.
 *
 * <p>Spark routes Complete mode to the sink by rewriting the query to call {@link
 * org.apache.spark.sql.connector.write.SupportsTruncate#truncate()} on the write builder, which
 * flips {@code overwrite=true} on {@link org.lance.spark.write.SparkWrite.SparkWriteBuilder}. The
 * streaming commit protocol then builds an {@code Overwrite} operation instead of an {@code
 * Append}, replacing the entire table contents per epoch.
 *
 * <p>The correctness tests here verify: (1) Complete-mode writes overwrite any pre-existing data in
 * the table, and (2) the final table matches the aggregation query's result rather than any
 * accumulation of per-epoch deltas.
 */
public abstract class BaseStreamingSinkCompleteTest {

  private static SparkSession spark;

  @TempDir static Path workDir;

  /** Input schema for the streaming source: a single {@code key} column. */
  protected static final StructType INPUT_SCHEMA =
      new StructType(
          new StructField[] {DataTypes.createStructField("key", DataTypes.StringType, false)});

  /** Aggregation output schema: {@code (key STRING, count BIGINT)}. */
  protected static final StructType AGG_SCHEMA =
      new StructType(
          new StructField[] {
            DataTypes.createStructField("key", DataTypes.StringType, false),
            DataTypes.createStructField("count", DataTypes.LongType, false)
          });

  @BeforeAll
  static void setup() {
    spark =
        SparkSession.builder()
            .appName("lance-streaming-sink-complete-test")
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
  // Scenario: Complete mode wipes pre-existing data
  // ========================================================================

  @Test
  public void completeModeOverwritesPreExistingData(TestInfo testInfo) throws Exception {
    String testName = testInfo.getTestMethod().get().getName();
    String lancePath = TestUtils.getDatasetUri(workDir.toString(), testName);
    Path sourceDir = StreamingTestUtils.createSourceDir(workDir, "src-" + testName);
    Path ckDir = StreamingTestUtils.createCheckpointDir(workDir, testName);

    // Pre-populate the target table with data that must be WIPED by Complete mode commits.
    List<Row> preExisting = Arrays.asList(RowFactory.create("will-be-gone", 999L));
    spark
        .createDataFrame(preExisting, AGG_SCHEMA)
        .write()
        .format(LanceDataSource.name)
        .option(LanceSparkReadOptions.CONFIG_DATASET_URI, lancePath)
        .save();
    assertEquals(1L, StreamingTestUtils.countRows(spark, lancePath));

    // Stream 5 input rows (3×"a", 2×"b") for aggregation.
    StreamingTestUtils.writeJsonBatch(
        sourceDir,
        "0001",
        Arrays.asList(
            "{\"key\":\"a\"}",
            "{\"key\":\"a\"}",
            "{\"key\":\"a\"}",
            "{\"key\":\"b\"}",
            "{\"key\":\"b\"}"));

    Dataset<Row> stream = StreamingTestUtils.readJsonStream(spark, sourceDir, INPUT_SCHEMA);
    Dataset<Row> agg = stream.groupBy("key").count();

    StreamingQuery query =
        StreamingTestUtils.startAvailableNowSink(agg, lancePath, testName, ckDir, "complete");
    try {
      StreamingTestUtils.awaitTermination(query, 60_000L);
    } finally {
      StreamingTestUtils.stopQuietly(query);
    }

    // Verify: pre-existing data is gone, and the table matches the aggregation.
    Dataset<Row> result =
        spark
            .read()
            .format(LanceDataSource.name)
            .option(LanceSparkReadOptions.CONFIG_DATASET_URI, lancePath)
            .load();

    assertEquals(2L, result.count(), "Aggregation should have produced 2 groups: 'a' and 'b'");
    assertEquals(
        0L,
        result.filter(col("key").equalTo("will-be-gone")).count(),
        "Pre-existing row should have been overwritten by Complete mode");
    assertEquals(
        3L,
        result.filter(col("key").equalTo("a")).head().getLong(1),
        "Group 'a' should have count 3");
    assertEquals(
        2L,
        result.filter(col("key").equalTo("b")).head().getLong(1),
        "Group 'b' should have count 2");
  }

  // ========================================================================
  // Scenario: Complete mode advances Lance versions (proof of commits)
  // ========================================================================

  @Test
  public void completeModeAdvancesLanceVersion(TestInfo testInfo) throws Exception {
    String testName = testInfo.getTestMethod().get().getName();
    String lancePath = TestUtils.getDatasetUri(workDir.toString(), testName);
    Path sourceDir = StreamingTestUtils.createSourceDir(workDir, "src-" + testName);
    Path ckDir = StreamingTestUtils.createCheckpointDir(workDir, testName);

    // Start with an empty table.
    spark
        .createDataFrame(java.util.Collections.emptyList(), AGG_SCHEMA)
        .write()
        .format(LanceDataSource.name)
        .option(LanceSparkReadOptions.CONFIG_DATASET_URI, lancePath)
        .save();
    long v0 = StreamingTestUtils.getLatestVersion(lancePath);

    StreamingTestUtils.writeJsonBatch(
        sourceDir, "0001", Arrays.asList("{\"key\":\"x\"}", "{\"key\":\"y\"}"));

    Dataset<Row> agg =
        StreamingTestUtils.readJsonStream(spark, sourceDir, INPUT_SCHEMA).groupBy("key").count();

    StreamingQuery query =
        StreamingTestUtils.startAvailableNowSink(agg, lancePath, testName, ckDir, "complete");
    try {
      StreamingTestUtils.awaitTermination(query, 60_000L);
    } finally {
      StreamingTestUtils.stopQuietly(query);
    }

    long v1 = StreamingTestUtils.getLatestVersion(lancePath);
    assertTrue(
        v1 > v0,
        "Complete-mode commit should have advanced the Lance version. v0=" + v0 + " v1=" + v1);
    assertEquals(2L, StreamingTestUtils.countRows(spark, lancePath));

    // Watermark should be persisted just like Append mode.
    long watermark = StreamingTestUtils.getPersistedEpochWatermark(lancePath, testName);
    assertTrue(watermark >= 0L, "Epoch watermark should have been persisted for Complete mode");
  }
}
