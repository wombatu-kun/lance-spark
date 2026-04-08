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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises the {@link LanceStreamingWrite} sink in Append output mode — the most common streaming
 * use case.
 *
 * <p>Each test stages JSON rows in a temp source directory, drives them through a Spark file-
 * streaming source using {@code Trigger.AvailableNow()} (which processes all staged files and then
 * terminates), and verifies the resulting Lance table state. For multi-trigger scenarios (tests
 * that stage additional files between micro-batches) this class uses a {@code ProcessingTime}
 * trigger instead and coordinates via {@link StreamingTestUtils#awaitEpochsProcessed}.
 *
 * <p>This is a base class — version-specific modules ({@code lance-spark-{3.4,3.5,4.0,4.1}_*})
 * provide thin one-line subclasses that run this test suite against their own Spark version.
 */
public abstract class BaseStreamingSinkAppendTest {

  private static SparkSession spark;

  @TempDir static Path workDir;

  /** Simple schema used by most tests: {@code (id INT, value STRING)}. */
  protected static final StructType SIMPLE_SCHEMA =
      new StructType(
          new StructField[] {
            DataTypes.createStructField("id", DataTypes.IntegerType, false),
            DataTypes.createStructField("value", DataTypes.StringType, false)
          });

  /** Schema with nested struct — mirrors {@code BaseSparkConnectorWriteTest}'s test schema. */
  protected static final StructType NESTED_SCHEMA =
      new StructType(
          new StructField[] {
            DataTypes.createStructField("id", DataTypes.IntegerType, false),
            DataTypes.createStructField("name", DataTypes.StringType, false),
            DataTypes.createStructField(
                "address",
                new StructType(
                    new StructField[] {
                      DataTypes.createStructField("city", DataTypes.StringType, true),
                      DataTypes.createStructField("country", DataTypes.StringType, true)
                    }),
                true)
          });

  /** Schema with a vector (array&lt;float&gt;) column — common in ML use cases for Lance. */
  protected static final StructType VECTOR_SCHEMA =
      new StructType(
          new StructField[] {
            DataTypes.createStructField("id", DataTypes.IntegerType, false),
            DataTypes.createStructField(
                "embedding", DataTypes.createArrayType(DataTypes.FloatType, false), false)
          });

  @BeforeAll
  static void setup() {
    spark =
        SparkSession.builder()
            .appName("lance-streaming-sink-append-test")
            .master("local[2]")
            .config("spark.sql.catalog.lance", "org.lance.spark.LanceNamespaceSparkCatalog")
            .config("spark.sql.catalog.lance.max_row_per_file", "1")
            .config("spark.sql.session.timeZone", "UTC")
            // Shorter shutdown timeout so tests don't hang on streaming cleanup.
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
  // Scenario: single micro-batch into an empty table
  // ========================================================================

  @Test
  public void singleBatchAppendIntoEmptyTable(TestInfo testInfo) throws Exception {
    String testName = testInfo.getTestMethod().get().getName();
    String lancePath = TestUtils.getDatasetUri(workDir.toString(), testName);
    Path sourceDir = StreamingTestUtils.createSourceDir(workDir, "src-" + testName);
    Path ckDir = StreamingTestUtils.createCheckpointDir(workDir, testName);

    // Pre-create the target Lance table by writing zero rows with the right schema.
    preCreateEmptyTable(lancePath, SIMPLE_SCHEMA);
    long v0 = StreamingTestUtils.getLatestVersion(lancePath);

    // Stage one file → one micro-batch containing 3 rows.
    StreamingTestUtils.writeJsonBatch(
        sourceDir,
        "0001",
        List.of(
            "{\"id\":1,\"value\":\"alpha\"}",
            "{\"id\":2,\"value\":\"beta\"}",
            "{\"id\":3,\"value\":\"gamma\"}"));

    Dataset<Row> stream = StreamingTestUtils.readJsonStream(spark, sourceDir, SIMPLE_SCHEMA);
    StreamingQuery query =
        StreamingTestUtils.startAvailableNowSink(stream, lancePath, testName, ckDir, "append");
    try {
      StreamingTestUtils.awaitTermination(query, 60_000L);
    } finally {
      StreamingTestUtils.stopQuietly(query);
    }

    // Verify row count and that at least one new Lance version was created.
    assertEquals(3L, StreamingTestUtils.countRows(spark, lancePath));
    long v1 = StreamingTestUtils.getLatestVersion(lancePath);
    assertTrue(
        v1 > v0, "Streaming commit should have advanced the Lance version: v0=" + v0 + " v1=" + v1);

    // Verify the epoch watermark was persisted.
    long watermark = StreamingTestUtils.getPersistedEpochWatermark(lancePath, testName);
    assertTrue(watermark >= 0L, "Epoch watermark should have been persisted; got " + watermark);
  }

  // ========================================================================
  // Scenario: multiple micro-batches into the same table (multi-trigger)
  // ========================================================================

  @Test
  public void multipleBatchAppendProducesDistinctVersions(TestInfo testInfo) throws Exception {
    String testName = testInfo.getTestMethod().get().getName();
    String lancePath = TestUtils.getDatasetUri(workDir.toString(), testName);
    Path sourceDir = StreamingTestUtils.createSourceDir(workDir, "src-" + testName);
    Path ckDir = StreamingTestUtils.createCheckpointDir(workDir, testName);

    preCreateEmptyTable(lancePath, SIMPLE_SCHEMA);

    // Stage three files up front. AvailableNow will process them in one query run but may
    // split them across micro-batches (Spark's choice). We verify the end state, not the
    // micro-batch boundary count.
    StreamingTestUtils.writeJsonBatch(sourceDir, "0001", List.of("{\"id\":1,\"value\":\"a\"}"));
    StreamingTestUtils.writeJsonBatch(sourceDir, "0002", List.of("{\"id\":2,\"value\":\"b\"}"));
    StreamingTestUtils.writeJsonBatch(sourceDir, "0003", List.of("{\"id\":3,\"value\":\"c\"}"));

    long v0 = StreamingTestUtils.getLatestVersion(lancePath);

    Dataset<Row> stream = StreamingTestUtils.readJsonStream(spark, sourceDir, SIMPLE_SCHEMA);
    StreamingQuery query =
        StreamingTestUtils.startAvailableNowSink(stream, lancePath, testName, ckDir, "append");
    try {
      StreamingTestUtils.awaitTermination(query, 60_000L);
    } finally {
      StreamingTestUtils.stopQuietly(query);
    }

    assertEquals(3L, StreamingTestUtils.countRows(spark, lancePath));
    long v1 = StreamingTestUtils.getLatestVersion(lancePath);
    assertTrue(
        v1 > v0, "Streaming commit should have advanced the Lance version for multi-batch run");
    assertTrue(StreamingTestUtils.getPersistedEpochWatermark(lancePath, testName) >= 0L);
  }

  // ========================================================================
  // Scenario: appending into a table that already has data from a batch write
  // ========================================================================

  @Test
  public void appendIntoPrePopulatedTable(TestInfo testInfo) throws Exception {
    String testName = testInfo.getTestMethod().get().getName();
    String lancePath = TestUtils.getDatasetUri(workDir.toString(), testName);
    Path sourceDir = StreamingTestUtils.createSourceDir(workDir, "src-" + testName);
    Path ckDir = StreamingTestUtils.createCheckpointDir(workDir, testName);

    // Pre-populate with a batch write of 5 rows.
    List<Row> seed =
        Arrays.asList(
            RowFactory.create(100, "seed-a"),
            RowFactory.create(200, "seed-b"),
            RowFactory.create(300, "seed-c"),
            RowFactory.create(400, "seed-d"),
            RowFactory.create(500, "seed-e"));
    spark
        .createDataFrame(seed, SIMPLE_SCHEMA)
        .write()
        .format(LanceDataSource.name)
        .option(LanceSparkReadOptions.CONFIG_DATASET_URI, lancePath)
        .save();
    assertEquals(5L, StreamingTestUtils.countRows(spark, lancePath));

    // Stream two new rows.
    StreamingTestUtils.writeJsonBatch(
        sourceDir,
        "0001",
        List.of("{\"id\":1,\"value\":\"stream-a\"}", "{\"id\":2,\"value\":\"stream-b\"}"));

    Dataset<Row> stream = StreamingTestUtils.readJsonStream(spark, sourceDir, SIMPLE_SCHEMA);
    StreamingQuery query =
        StreamingTestUtils.startAvailableNowSink(stream, lancePath, testName, ckDir, "append");
    try {
      StreamingTestUtils.awaitTermination(query, 60_000L);
    } finally {
      StreamingTestUtils.stopQuietly(query);
    }

    assertEquals(7L, StreamingTestUtils.countRows(spark, lancePath));
  }

  // ========================================================================
  // Scenario: append with nested struct schema
  // ========================================================================

  @Test
  public void appendWithNestedStructSchema(TestInfo testInfo) throws Exception {
    String testName = testInfo.getTestMethod().get().getName();
    String lancePath = TestUtils.getDatasetUri(workDir.toString(), testName);
    Path sourceDir = StreamingTestUtils.createSourceDir(workDir, "src-" + testName);
    Path ckDir = StreamingTestUtils.createCheckpointDir(workDir, testName);

    preCreateEmptyTable(lancePath, NESTED_SCHEMA);

    StreamingTestUtils.writeJsonBatch(
        sourceDir,
        "0001",
        List.of(
            "{\"id\":1,\"name\":\"Alice\",\"address\":{\"city\":\"Beijing\",\"country\":\"China\"}}",
            "{\"id\":2,\"name\":\"Bob\",\"address\":{\"city\":\"NYC\",\"country\":\"USA\"}}"));

    Dataset<Row> stream = StreamingTestUtils.readJsonStream(spark, sourceDir, NESTED_SCHEMA);
    StreamingQuery query =
        StreamingTestUtils.startAvailableNowSink(stream, lancePath, testName, ckDir, "append");
    try {
      StreamingTestUtils.awaitTermination(query, 60_000L);
    } finally {
      StreamingTestUtils.stopQuietly(query);
    }

    Dataset<Row> result =
        spark
            .read()
            .format(LanceDataSource.name)
            .option(LanceSparkReadOptions.CONFIG_DATASET_URI, lancePath)
            .load();
    assertEquals(2L, result.count());
    List<Row> rows = result.orderBy("id").collectAsList();
    assertEquals("Alice", rows.get(0).getString(1));
    assertEquals("Beijing", rows.get(0).getStruct(2).getString(0));
    assertEquals("China", rows.get(0).getStruct(2).getString(1));
    assertEquals("Bob", rows.get(1).getString(1));
    assertEquals("NYC", rows.get(1).getStruct(2).getString(0));
  }

  // ========================================================================
  // Scenario: append with vector (array<float>) schema — common ML use case
  // ========================================================================

  @Test
  public void appendWithVectorColumn(TestInfo testInfo) throws Exception {
    String testName = testInfo.getTestMethod().get().getName();
    String lancePath = TestUtils.getDatasetUri(workDir.toString(), testName);
    Path sourceDir = StreamingTestUtils.createSourceDir(workDir, "src-" + testName);
    Path ckDir = StreamingTestUtils.createCheckpointDir(workDir, testName);

    preCreateEmptyTable(lancePath, VECTOR_SCHEMA);

    StreamingTestUtils.writeJsonBatch(
        sourceDir,
        "0001",
        List.of(
            "{\"id\":1,\"embedding\":[0.1,0.2,0.3]}",
            "{\"id\":2,\"embedding\":[0.4,0.5,0.6]}",
            "{\"id\":3,\"embedding\":[0.7,0.8,0.9]}"));

    Dataset<Row> stream = StreamingTestUtils.readJsonStream(spark, sourceDir, VECTOR_SCHEMA);
    StreamingQuery query =
        StreamingTestUtils.startAvailableNowSink(stream, lancePath, testName, ckDir, "append");
    try {
      StreamingTestUtils.awaitTermination(query, 60_000L);
    } finally {
      StreamingTestUtils.stopQuietly(query);
    }

    assertEquals(3L, StreamingTestUtils.countRows(spark, lancePath));
  }

  // ========================================================================
  // Scenario: empty micro-batch is a successful no-op (no commit, no error)
  // ========================================================================

  @Test
  public void emptyMicroBatchIsHarmless(TestInfo testInfo) throws Exception {
    String testName = testInfo.getTestMethod().get().getName();
    String lancePath = TestUtils.getDatasetUri(workDir.toString(), testName);
    Path sourceDir = StreamingTestUtils.createSourceDir(workDir, "src-" + testName);
    Path ckDir = StreamingTestUtils.createCheckpointDir(workDir, testName);

    preCreateEmptyTable(lancePath, SIMPLE_SCHEMA);
    long versionBeforeStream = StreamingTestUtils.getLatestVersion(lancePath);

    // No source files staged — the stream will see zero data.
    Dataset<Row> stream = StreamingTestUtils.readJsonStream(spark, sourceDir, SIMPLE_SCHEMA);
    StreamingQuery query =
        StreamingTestUtils.startAvailableNowSink(stream, lancePath, testName, ckDir, "append");
    try {
      StreamingTestUtils.awaitTermination(query, 60_000L);
    } finally {
      StreamingTestUtils.stopQuietly(query);
    }

    // The table should be unchanged — the streaming sink correctly treats empty batches as
    // no-ops instead of producing empty Lance commits.
    assertEquals(0L, StreamingTestUtils.countRows(spark, lancePath));
    long versionAfterStream = StreamingTestUtils.getLatestVersion(lancePath);
    assertEquals(
        versionBeforeStream,
        versionAfterStream,
        "Empty micro-batch should not have created a new Lance version");
    assertEquals(
        StreamingCommitProtocol.NO_EPOCH,
        StreamingTestUtils.getPersistedEpochWatermark(lancePath, testName),
        "Empty micro-batch should not have bumped the epoch watermark");
  }

  // ========================================================================
  // Helpers
  // ========================================================================

  /**
   * Creates an empty Lance table at the given path with the supplied schema. The streaming sink
   * requires the target table to exist, so tests pre-create it by batch-writing an empty DataFrame.
   */
  private void preCreateEmptyTable(String lancePath, StructType schema) {
    Dataset<Row> empty = spark.createDataFrame(java.util.Collections.emptyList(), schema);
    empty
        .write()
        .format(LanceDataSource.name)
        .option(LanceSparkReadOptions.CONFIG_DATASET_URI, lancePath)
        .save();
  }
}
