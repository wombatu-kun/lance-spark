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
package org.lance.spark.read;

import org.lance.Fragment;
import org.lance.ipc.ColumnOrdering;
import org.lance.ipc.Query;
import org.lance.ipc.ScanOptions;
import org.lance.spark.LanceConstant;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * End-to-end coverage for the {@code lance_vector_search} SQL table-valued function.
 *
 * <p>Most tests run in brute-force mode ({@code use_index=false}) and exercise the TVF mechanics in
 * isolation: argument parsing, positional vs. named call sites, error surface, and {@code WHERE}
 * composition. {@link #testTvfWithIndexAgreesWithBruteForce()} is the one exception — it
 * additionally requires the vector-index DDL (the {@code ivf_*} method values for {@code ALTER
 * TABLE … CREATE INDEX}) and verifies the two features compose end-to-end.
 */
public abstract class BaseLanceVectorSearchTest {

  protected static final int DIM = 16;
  protected static final int ROWS = 256;
  protected static final long SEED = 1234567L;

  protected String catalogName = "lance_vec";
  protected String tableName;
  protected String fullTable;

  protected SparkSession spark;

  @TempDir Path tempDir;

  protected String tableDir;

  @BeforeEach
  public void setup() throws IOException {
    Path rootPath = tempDir.resolve(UUID.randomUUID().toString());
    Files.createDirectories(rootPath);
    String testRoot = rootPath.toString();
    this.spark =
        SparkSession.builder()
            .appName("lance-vector-search-test")
            .master("local[2]")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config(
                "spark.sql.extensions", "org.lance.spark.extensions.LanceSparkSessionExtensions")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + ".root", testRoot)
            .config("spark.sql.catalog." + catalogName + ".single_level_ns", "true")
            .getOrCreate();
    this.tableName = "vec_" + UUID.randomUUID().toString().replace("-", "");
    this.fullTable = catalogName + ".default." + this.tableName;
    this.tableDir =
        FileSystems.getDefault().getPath(testRoot, this.tableName + ".lance").toString();
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (spark != null) {
      spark.close();
      spark = null;
    }
  }

  // ─── Tests ────────────────────────────────────────────────────────────────

  @Test
  public void testTvfBruteForceReturnsPlantedNeighbor() {
    prepareDataset();
    Set<Integer> ids = collectIds(runTvfSql(/* k= */ 10, "l2"));
    Assertions.assertTrue(
        ids.contains(plantedRowId()),
        "Planted neighbour id=" + plantedRowId() + " missing from top-k results " + ids);
  }

  @Test
  public void testTvfPreFilter() {
    prepareDataset();
    Dataset<Row> result =
        spark.sql(
            "SELECT id, category FROM lance_vector_search('"
                + fullTable
                + "', 'emb', "
                + queryVectorLiteral()
                + ", 10, 'l2', 20, 1, 64, false) "
                + "WHERE category = 'odd'");
    List<Row> rows = result.collectAsList();
    Assertions.assertFalse(rows.isEmpty(), "Pre-filter must leave at least one row");
    for (Row r : rows) {
      Assertions.assertEquals("odd", r.getString(1));
    }
  }

  @Test
  public void testTvfRejectsNonPositiveK() {
    prepareDataset();
    Exception ex =
        Assertions.assertThrows(
            Exception.class,
            () ->
                spark
                    .sql(
                        "SELECT * FROM lance_vector_search('"
                            + fullTable
                            + "', 'emb', "
                            + queryVectorLiteral()
                            + ", 0)")
                    .collect());
    String msg = rootCauseMessage(ex);
    Assertions.assertTrue(
        msg.contains("k") && msg.contains("positive"),
        "Expected complaint about non-positive k, got: " + msg);
  }

  @Test
  public void testTvfRejectsUnknownMetric() {
    prepareDataset();
    Exception ex =
        Assertions.assertThrows(
            Exception.class,
            () ->
                spark
                    .sql(
                        "SELECT * FROM lance_vector_search('"
                            + fullTable
                            + "', 'emb', "
                            + queryVectorLiteral()
                            + ", 5, 'manhattan')")
                    .collect());
    String msg = rootCauseMessage(ex);
    Assertions.assertTrue(
        msg.toLowerCase(Locale.ROOT).contains("metric"),
        "Expected complaint about unsupported metric, got: " + msg);
  }

  @Test
  public void testTvfRejectsNonExistentTable() {
    Exception ex =
        Assertions.assertThrows(
            Exception.class,
            () ->
                spark
                    .sql(
                        "SELECT * FROM lance_vector_search('"
                            + catalogName
                            + ".default.does_not_exist_"
                            + UUID.randomUUID().toString().replace('-', '_')
                            + "', 'emb', "
                            + queryVectorLiteral()
                            + ", 5)")
                    .collect());
    Assertions.assertNotNull(ex.getMessage());
  }

  // ─── Tests: TVF + vector index integration ────────────────────────────────

  /**
   * Confirms TVF correctly threads {@code use_index=true} through to the {@code Query.Builder} —
   * i.e. that the `lance_vector_search` SQL TVF (this PR's feature) and the `ALTER TABLE … CREATE
   * INDEX … USING ivf_pq` statement (the vector-index PR's feature) compose end-to-end.
   *
   * <p>This is the only test in the suite that exercises both halves of the vector-search story
   * together; the rest of `BaseLanceVectorSearchTest` runs in brute-force mode so the file stays
   * usable on its own.
   */
  @Test
  public void testTvfWithIndexAgreesWithBruteForce() {
    prepareDataset();
    spark.sql(
        String.format(
            "ALTER TABLE %s CREATE INDEX idx_bf USING ivf_pq (emb) "
                + "WITH (num_partitions=4, num_sub_vectors=4, num_bits=8, metric='l2')",
            fullTable));

    Set<Integer> withIndex = collectIds(runTvfSqlIndexed(/* k= */ 10, "l2"));
    Set<Integer> bruteForce = collectIds(runTvfSql(/* k= */ 5, "l2"));
    Assertions.assertTrue(
        bruteForce.contains(plantedRowId()),
        "brute-force scan must include the planted neighbour, got " + bruteForce);
    Assertions.assertTrue(
        withIndex.contains(plantedRowId()),
        "indexed scan must include the planted neighbour, got " + withIndex);
    // Indexed top-10 should subsume a majority of the brute-force top-5 — IVF/PQ is approximate,
    // so don't demand strict containment.
    long shared = bruteForce.stream().filter(withIndex::contains).count();
    Assertions.assertTrue(
        shared >= (bruteForce.size() + 1) / 2,
        "Expected indexed top-10 to share a majority of the brute-force top-5; "
            + "indexed="
            + withIndex
            + ", bruteForce="
            + bruteForce);
  }

  protected Dataset<Row> runTvfSqlIndexed(int k, String metric) {
    return spark.sql(
        "SELECT id FROM lance_vector_search('"
            + fullTable
            + "', 'emb', "
            + queryVectorLiteral()
            + ", "
            + k
            + ", '"
            + metric
            + "', 20, 1, 64, true)");
  }

  // ─── Tests: _distance virtual column ──────────────────────────────────────

  @Test
  public void testTvfSchemaSurfacesDistanceColumn() {
    prepareDataset();
    StructType schema = tvfSqlAllColumns(10, "l2").schema();
    StructField distanceField = null;
    for (StructField f : schema.fields()) {
      if (LanceConstant.DISTANCE.equals(f.name())) {
        distanceField = f;
        break;
      }
    }
    Assertions.assertNotNull(
        distanceField,
        "Expected '"
            + LanceConstant.DISTANCE
            + "' field in schema, got "
            + Arrays.toString(schema.fieldNames()));
    Assertions.assertEquals(DataTypes.FloatType, distanceField.dataType());
    Assertions.assertFalse(distanceField.nullable(), "_distance should be non-nullable");
  }

  @Test
  public void testSelectDistanceReturnsNonNullFloats() {
    prepareDataset();
    List<Row> rows =
        spark
            .sql(
                "SELECT id, _distance FROM lance_vector_search('"
                    + fullTable
                    + "', 'emb', "
                    + queryVectorLiteral()
                    + ", 10, 'l2', 20, 1, 64, false)")
            .collectAsList();
    Assertions.assertFalse(rows.isEmpty(), "TVF must return rows");
    for (Row r : rows) {
      Assertions.assertFalse(r.isNullAt(1), "_distance must be non-null for row id=" + r.getInt(0));
      float d = r.getFloat(1);
      Assertions.assertTrue(
          Float.isFinite(d) && d >= 0.0f,
          "_distance must be finite and non-negative (L2), got " + d + " for id=" + r.getInt(0));
    }
  }

  @Test
  public void testOrderByDistanceProducesGlobalTopK() {
    prepareDataset();
    int k = 5;
    List<Row> rows =
        spark
            .sql(
                "SELECT id, _distance FROM lance_vector_search('"
                    + fullTable
                    + "', 'emb', "
                    + queryVectorLiteral()
                    + ", "
                    + k
                    + ", 'l2', 20, 1, 64, false) ORDER BY _distance LIMIT "
                    + k)
            .collectAsList();
    Assertions.assertEquals(k, rows.size(), "Expected " + k + " rows after global top-k");
    for (int i = 1; i < rows.size(); i++) {
      float prev = rows.get(i - 1).getFloat(1);
      float cur = rows.get(i).getFloat(1);
      Assertions.assertTrue(
          prev <= cur, "ORDER BY _distance must be ascending; prev=" + prev + " cur=" + cur);
    }
    Assertions.assertEquals(
        plantedRowId(),
        rows.get(0).getInt(0),
        "Closest row by _distance should be the planted neighbour");
  }

  @Test
  public void testWhereDistanceFilters() {
    prepareDataset();
    float threshold = 0.5f;
    List<Row> rows =
        spark
            .sql(
                "SELECT id, _distance FROM lance_vector_search('"
                    + fullTable
                    + "', 'emb', "
                    + queryVectorLiteral()
                    + ", 20, 'l2', 20, 1, 64, false) WHERE _distance < "
                    + threshold)
            .collectAsList();
    Assertions.assertFalse(rows.isEmpty(), "At least the planted neighbour must pass threshold");
    for (Row r : rows) {
      Assertions.assertTrue(
          r.getFloat(1) < threshold,
          "WHERE _distance < " + threshold + " leaked row with d=" + r.getFloat(1));
    }
  }

  @Test
  public void testScalarOnlyProjectionStillWorks() {
    // Scalar-only projection (no _distance) must not regress with the decorator in place.
    prepareDataset();
    Set<Integer> ids = collectIds(runTvfSql(10, "l2"));
    Assertions.assertTrue(
        ids.contains(plantedRowId()),
        "Scalar-only projection must still return planted neighbour, got " + ids);
  }

  @Test
  public void testNonNearestReadDoesNotExposeDistance() {
    // Regression guard: regular reads must not pick up _distance.
    prepareDataset();
    Dataset<Row> df = spark.read().format("lance").load(tableDir);
    Assertions.assertFalse(
        Arrays.asList(df.schema().fieldNames()).contains(LanceConstant.DISTANCE),
        "Non-nearest read must not contain _distance; got "
            + Arrays.toString(df.schema().fieldNames()));
  }

  /**
   * Contract test pinning Lance native's current behaviour: attempting to filter on {@code
   * _distance} at the scanner level raises {@code Column _distance does not exist}. Documents
   * <em>why</em> {@code LanceScanBuilder#pushFilters} refuses to push filters that reference the
   * virtual column. If Lance upstream starts accepting {@code _distance} in SQL WHERE clauses this
   * test will begin to pass without an exception — at which point the guard can be relaxed.
   */
  @Test
  public void testLanceNativeRejectsDistanceInWhereClause() {
    prepareDataset();
    runAndAssertDistanceRejected(
        b -> b.filter(LanceConstant.DISTANCE + " < 0.5"),
        new Query.Builder()
            .setColumn("emb")
            .setKey(queryVector())
            .setK(5)
            .setUseIndex(false)
            .build());
  }

  /**
   * Contract test pinning Lance native's current behaviour: attempting to sort by {@code _distance}
   * at the scanner level raises {@code Column _distance not found}. Same unblock criterion as
   * {@link #testLanceNativeRejectsDistanceInWhereClause}.
   */
  @Test
  public void testLanceNativeRejectsDistanceInColumnOrderings() {
    prepareDataset();
    ColumnOrdering.Builder cob = new ColumnOrdering.Builder();
    cob.setColumnName(LanceConstant.DISTANCE);
    cob.setAscending(true);
    cob.setNullFirst(false);
    ColumnOrdering ordering = cob.build();
    runAndAssertDistanceRejected(
        b -> b.setColumnOrderings(Collections.singletonList(ordering)),
        new Query.Builder()
            .setColumn("emb")
            .setKey(queryVector())
            .setK(5)
            .setUseIndex(false)
            .build());
  }

  /**
   * Builds a nearest-scan over fragment 0 with the caller-supplied extra option (filter, ordering,
   * …), executes it, and asserts that Lance native raises an {@link IllegalArgumentException} whose
   * message mentions {@code _distance}.
   */
  private void runAndAssertDistanceRejected(Consumer<ScanOptions.Builder> extraOption, Query q) {
    org.lance.Dataset ds = org.lance.Dataset.open().uri(tableDir).build();
    try {
      Fragment fragment = ds.getFragments().get(0);
      ScanOptions.Builder b = new ScanOptions.Builder();
      b.columns(Collections.singletonList("id"));
      b.nearest(q);
      b.prefilter(true);
      extraOption.accept(b);
      ScanOptions opts = b.build();
      IllegalArgumentException ex =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () -> fragment.newScan(opts).scanBatches().loadNextBatch());
      Assertions.assertTrue(
          ex.getMessage().contains(LanceConstant.DISTANCE),
          "Expected Lance error to mention _distance, got: " + ex.getMessage());
    } finally {
      ds.close();
    }
  }

  /** Like {@link #runTvfSql} but selects all columns so the schema includes {@code _distance}. */
  private Dataset<Row> tvfSqlAllColumns(int k, String metric) {
    return spark.sql(
        "SELECT * FROM lance_vector_search('"
            + fullTable
            + "', 'emb', "
            + queryVectorLiteral()
            + ", "
            + k
            + ", '"
            + metric
            + "', 20, 1, 64, false)");
  }

  // ─── Helpers ──────────────────────────────────────────────────────────────

  /**
   * Creates a table with a 16-dim vector column plus two scalar columns (id, category), inserts
   * {@link #ROWS} deterministic rows, and "plants" the neighbour closest to {@link #queryVector()}
   * at {@link #plantedRowId()}. Data is split across two inserts to force at least two fragments.
   */
  protected void prepareDataset() {
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT NOT NULL, category STRING, emb ARRAY<FLOAT> NOT NULL) "
                + "USING lance TBLPROPERTIES ('emb.arrow.fixed-size-list.size' = '%d')",
            fullTable, DIM));
    int half = ROWS / 2;
    insertRange(0, half);
    insertRange(half, ROWS);
  }

  private void insertRange(int from, int to) {
    Random rng = new Random(SEED + from);
    StringBuilder sql = new StringBuilder();
    sql.append("INSERT INTO ").append(fullTable).append(" VALUES ");
    boolean first = true;
    for (int i = from; i < to; i++) {
      if (!first) {
        sql.append(", ");
      }
      first = false;
      String cat = (i % 2 == 0) ? "even" : "odd";
      sql.append("(")
          .append(i)
          .append(", '")
          .append(cat)
          .append("', array(")
          .append(vectorLiteral(i, rng))
          .append("))");
    }
    spark.sql(sql.toString());
  }

  private String vectorLiteral(int i, Random rng) {
    float[] query = queryVector();
    StringBuilder sb = new StringBuilder();
    for (int d = 0; d < DIM; d++) {
      if (d > 0) sb.append(", ");
      float v;
      if (i == plantedRowId()) {
        v = query[d] + ((rng.nextFloat() - 0.5f) * 0.001f);
      } else {
        v = rng.nextFloat() * 10.0f - 5.0f;
      }
      sb.append(Float.toString(v)).append("f");
    }
    return sb.toString();
  }

  protected int plantedRowId() {
    return 42;
  }

  protected float[] queryVector() {
    float[] v = new float[DIM];
    for (int i = 0; i < DIM; i++) {
      v[i] = (float) (0.1 * (i + 1));
    }
    return v;
  }

  protected String queryVectorLiteral() {
    float[] v = queryVector();
    StringBuilder sb = new StringBuilder();
    sb.append("array(");
    for (int i = 0; i < v.length; i++) {
      if (i > 0) sb.append(", ");
      sb.append("CAST(").append(v[i]).append(" AS FLOAT)");
    }
    sb.append(")");
    return sb.toString();
  }

  protected Dataset<Row> runTvfSql(int k, String metric) {
    return spark.sql(
        "SELECT id FROM lance_vector_search('"
            + fullTable
            + "', 'emb', "
            + queryVectorLiteral()
            + ", "
            + k
            + ", '"
            + metric
            + "', 20, 1, 64, false)");
  }

  protected Set<Integer> collectIds(Dataset<Row> df) {
    return df.collectAsList().stream().map(r -> r.getInt(0)).collect(Collectors.toSet());
  }

  protected static String rootCauseMessage(Throwable t) {
    Throwable cur = t;
    while (cur.getCause() != null && cur.getCause() != cur) {
      cur = cur.getCause();
    }
    String msg = cur.getMessage();
    return msg == null ? cur.getClass().getName() : msg;
  }
}
