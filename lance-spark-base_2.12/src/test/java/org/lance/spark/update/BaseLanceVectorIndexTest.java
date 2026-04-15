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
package org.lance.spark.update;

import org.lance.Fragment;
import org.lance.index.Index;
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
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * End-to-end coverage for the {@code ivf_*} vector index DDL plus the {@code lance_vector_search}
 * table-valued function.
 *
 * <p>The matrix is deliberately narrow — one representative cell per (index × metric × dtype)
 * combination — because training an IVF/HNSW index is substantially more expensive than a scalar
 * one and the Spark side of each case is identical. What we really care about is that
 *
 * <ul>
 *   <li>each supported index type builds without error;
 *   <li>each supported metric is accepted and returns results in the right order;
 *   <li>each supported vector dtype (float32 / float64 / float16) round-trips through the pushdown;
 *   <li>the TVF's own error surface (bad arg types, invalid k, vector index on scalar column) is
 *       covered.
 * </ul>
 *
 * <p>Tests that exercise features that only exist on Spark 4.0+ (e.g. {@code arrow.float16}) call
 * {@link #assumeArrow18Available()} up front so they are skipped on 3.x without failure.
 */
public abstract class BaseLanceVectorIndexTest {

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
            .appName("lance-vector-index-test")
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

  // ─── Tests: DDL × index type ──────────────────────────────────────────────

  @Test
  public void testIvfFlatIndexL2Float32() {
    prepareFloat32Dataset();
    createVectorIndex("idx_ivf_flat", "ivf_flat", "num_partitions=4, metric='l2'");
    assertIndexExists("idx_ivf_flat");
    assertVectorSearchReturnsPlantedNeighbor();
  }

  @Test
  public void testIvfPqIndexL2Float32() {
    prepareFloat32Dataset();
    createVectorIndex(
        "idx_ivf_pq", "ivf_pq", "num_partitions=4, num_sub_vectors=4, num_bits=8, metric='l2'");
    assertIndexExists("idx_ivf_pq");
    assertVectorSearchReturnsPlantedNeighbor();
  }

  @Test
  public void testIvfHnswPqIndexCosineFloat32() {
    prepareFloat32Dataset();
    createVectorIndex(
        "idx_ivf_hnsw_pq",
        "ivf_hnsw_pq",
        "num_partitions=4, num_sub_vectors=4, num_bits=8, metric='cosine', "
            + "m=8, ef_construction=40");
    assertIndexExists("idx_ivf_hnsw_pq");
    // Re-plant using the 'cosine' metric so the expected neighbour matches.
    runTvfAndAssertPlantedNeighbor("cosine");
  }

  @Test
  public void testIvfHnswSqIndexL2Float32() {
    prepareFloat32Dataset();
    createVectorIndex(
        "idx_ivf_hnsw_sq",
        "ivf_hnsw_sq",
        "num_partitions=4, num_bits=8, metric='l2', m=8, ef_construction=40");
    assertIndexExists("idx_ivf_hnsw_sq");
    assertVectorSearchReturnsPlantedNeighbor();
  }

  // ─── Tests: vector dtype ──────────────────────────────────────────────────

  @Test
  public void testIvfPqOnFloat64Column() {
    prepareDataset(/* useDouble= */ true, /* useFloat16= */ false);
    createVectorIndex(
        "idx_pq_f64", "ivf_pq", "num_partitions=4, num_sub_vectors=4, num_bits=8, metric='l2'");
    assertIndexExists("idx_pq_f64");
    assertVectorSearchReturnsPlantedNeighbor();
  }

  @Test
  public void testIvfPqOnFloat16Column() {
    // float16 via `arrow.float16` metadata requires Arrow 18+ which is bundled with Spark 4.0+.
    assumeArrow18Available();
    prepareDataset(/* useDouble= */ false, /* useFloat16= */ true);
    createVectorIndex(
        "idx_pq_f16", "ivf_pq", "num_partitions=4, num_sub_vectors=4, num_bits=8, metric='l2'");
    assertIndexExists("idx_pq_f16");
    assertVectorSearchReturnsPlantedNeighbor();
  }

  // ─── Tests: TVF behaviour ─────────────────────────────────────────────────

  @Test
  public void testTvfBruteForceAndIndexedAgreeOnTopK() {
    prepareFloat32Dataset();
    createVectorIndex(
        "idx_bf", "ivf_pq", "num_partitions=4, num_sub_vectors=4, num_bits=8, metric='l2'");

    Set<Integer> withIndex = collectIds(runTvfSql(/* useIndex= */ true, /* k= */ 10, "l2"));
    Set<Integer> bruteForce = collectIds(runTvfSql(/* useIndex= */ false, /* k= */ 5, "l2"));
    Assertions.assertTrue(
        bruteForce.contains(plantedRowId()),
        "brute-force scan must include the planted neighbour, got " + bruteForce);
    Assertions.assertTrue(
        withIndex.contains(plantedRowId()),
        "indexed scan must include the planted neighbour, got " + withIndex);
    // Indexed top-10 should subsume a majority of the brute-force top-5 — IVF/PQ is
    // approximate, so don't demand strict containment.
    long shared = bruteForce.stream().filter(withIndex::contains).count();
    Assertions.assertTrue(
        shared >= (bruteForce.size() + 1) / 2,
        "Expected indexed top-10 to share a majority of the brute-force top-5; "
            + "indexed="
            + withIndex
            + ", bruteForce="
            + bruteForce);
  }

  @Test
  public void testTvfPreFilter() {
    prepareFloat32Dataset();
    createVectorIndex(
        "idx_prefilter", "ivf_pq", "num_partitions=4, num_sub_vectors=4, num_bits=8, metric='l2'");

    // Vector search + pre-filter on category column.
    Dataset<Row> result =
        spark.sql(
            "SELECT id, category FROM lance_vector_search('"
                + fullTable
                + "', 'emb', "
                + queryVectorLiteral("FLOAT")
                + ", 10) "
                + "WHERE category = 'odd'");
    List<Row> rows = result.collectAsList();
    Assertions.assertFalse(rows.isEmpty(), "Pre-filter must leave at least one row");
    for (Row r : rows) {
      Assertions.assertEquals("odd", r.getString(1));
    }
  }

  @Test
  public void testCreateVectorIndexOnScalarColumnFails() {
    prepareFloat32Dataset();
    IllegalArgumentException ex =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                spark
                    .sql(
                        String.format(
                            "ALTER TABLE %s CREATE INDEX bad_idx USING ivf_pq (id) "
                                + "WITH (num_partitions=4)",
                            fullTable))
                    .collect());
    Assertions.assertTrue(
        ex.getMessage().toLowerCase(Locale.ROOT).contains("vector"),
        "Error should mention vector requirement, got: " + ex.getMessage());
  }

  @Test
  public void testTvfRejectsNonPositiveK() {
    prepareFloat32Dataset();
    Exception ex =
        Assertions.assertThrows(
            Exception.class,
            () ->
                spark
                    .sql(
                        "SELECT * FROM lance_vector_search('"
                            + fullTable
                            + "', 'emb', "
                            + queryVectorLiteral("FLOAT")
                            + ", 0)")
                    .collect());
    String msg = rootCauseMessage(ex);
    Assertions.assertTrue(
        msg.contains("k") && msg.contains("positive"),
        "Expected complaint about non-positive k, got: " + msg);
  }

  @Test
  public void testTvfRejectsUnknownMetric() {
    prepareFloat32Dataset();
    Exception ex =
        Assertions.assertThrows(
            Exception.class,
            () ->
                spark
                    .sql(
                        "SELECT * FROM lance_vector_search('"
                            + fullTable
                            + "', 'emb', "
                            + queryVectorLiteral("FLOAT")
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
                            + queryVectorLiteral("FLOAT")
                            + ", 5)")
                    .collect());
    Assertions.assertNotNull(ex.getMessage());
  }

  // ─── Tests: _distance column surfacing ────────────────────────────────────

  @Test
  public void testTvfSchemaSurfacesDistanceColumn() {
    prepareFloat32Dataset();
    StructType schema = tvfSql(10, "l2").schema();
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
    prepareFloat32Dataset();
    List<Row> rows =
        spark
            .sql(
                "SELECT id, _distance FROM lance_vector_search('"
                    + fullTable
                    + "', 'emb', "
                    + queryVectorLiteral("FLOAT")
                    + ", 10)")
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
    prepareFloat32Dataset();
    int k = 5;
    List<Row> rows =
        spark
            .sql(
                "SELECT id, _distance FROM lance_vector_search('"
                    + fullTable
                    + "', 'emb', "
                    + queryVectorLiteral("FLOAT")
                    + ", "
                    + k
                    + ") ORDER BY _distance LIMIT "
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
    prepareFloat32Dataset();
    float threshold = 0.5f;
    List<Row> rows =
        spark
            .sql(
                "SELECT id, _distance FROM lance_vector_search('"
                    + fullTable
                    + "', 'emb', "
                    + queryVectorLiteral("FLOAT")
                    + ", 20) WHERE _distance < "
                    + threshold)
            .collectAsList();
    // The planted neighbour sits essentially on top of the query vector, so at least
    // one row must pass the threshold.
    Assertions.assertFalse(rows.isEmpty(), "At least the planted neighbour must pass threshold");
    for (Row r : rows) {
      Assertions.assertTrue(
          r.getFloat(1) < threshold,
          "WHERE _distance < " + threshold + " leaked row with d=" + r.getFloat(1));
    }
  }

  @Test
  public void testScalarOnlyProjectionStillWorks() {
    // Scalar-only projection (no _distance) must not regress after the decorator lands.
    prepareFloat32Dataset();
    Set<Integer> ids =
        collectIds(
            spark.sql(
                "SELECT id FROM lance_vector_search('"
                    + fullTable
                    + "', 'emb', "
                    + queryVectorLiteral("FLOAT")
                    + ", 10)"));
    Assertions.assertTrue(
        ids.contains(plantedRowId()),
        "Scalar-only projection must still return planted neighbour, got " + ids);
  }

  /**
   * Contract test pinning Lance native's current behaviour: attempting to filter on {@code
   * _distance} at the scanner level raises {@code Column _distance does not exist}. This documents
   * <em>why</em> {@link LanceScanBuilder#pushFilters} refuses to push filters that reference the
   * virtual column. If Lance upstream starts accepting {@code _distance} in SQL WHERE clauses this
   * test will begin to pass without an exception — at which point the guard in {@code
   * LanceScanBuilder} can be relaxed.
   */
  @Test
  public void testLanceNativeRejectsDistanceInWhereClause() {
    prepareFloat32Dataset();
    Query q =
        new Query.Builder()
            .setColumn("emb")
            .setKey(queryVector())
            .setK(5)
            .setUseIndex(false)
            .build();
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
   * at the scanner level raises {@code Column _distance not found}. This documents <em>why</em>
   * {@link LanceScanBuilder#pushTopN} refuses to push sort orderings that reference the virtual
   * column. Same unblock criterion as {@link #testLanceNativeRejectsDistanceInWhereClause}.
   */
  @Test
  public void testLanceNativeRejectsDistanceInColumnOrderings() {
    prepareFloat32Dataset();
    ColumnOrdering.Builder cob = new ColumnOrdering.Builder();
    cob.setColumnName(LanceConstant.DISTANCE);
    cob.setAscending(true);
    cob.setNullFirst(false);
    ColumnOrdering ordering = cob.build();
    runAndAssertDistanceRejected(
        b -> b.setColumnOrderings(java.util.Collections.singletonList(ordering)),
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
  private void runAndAssertDistanceRejected(
      java.util.function.Consumer<ScanOptions.Builder> extraOption, Query query) {
    org.lance.Dataset ds = org.lance.Dataset.open().uri(tableDir).build();
    try {
      Fragment fragment = ds.getFragments().get(0);
      ScanOptions.Builder b = new ScanOptions.Builder();
      b.columns(java.util.Collections.singletonList("id"));
      b.nearest(query);
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

  @Test
  public void testNonNearestReadDoesNotExposeDistance() {
    // Regression guard: regular reads must not pick up _distance.
    prepareFloat32Dataset();
    Dataset<Row> df = spark.read().format("lance").load(tableDir);
    Assertions.assertFalse(
        Arrays.asList(df.schema().fieldNames()).contains(LanceConstant.DISTANCE),
        "Non-nearest read must not contain _distance; got "
            + Arrays.toString(df.schema().fieldNames()));
  }

  /**
   * Runs the TVF with {@code (k, metric)} and returns the unprojected result (caller decides which
   * columns to read). Separate from {@link #runTvfSql} which hard-codes {@code SELECT id}.
   */
  private Dataset<Row> tvfSql(int k, String metric) {
    return spark.sql(
        "SELECT * FROM lance_vector_search('"
            + fullTable
            + "', 'emb', "
            + queryVectorLiteral("FLOAT")
            + ", "
            + k
            + ", '"
            + metric
            + "')");
  }

  // ─── Helpers ──────────────────────────────────────────────────────────────

  protected void prepareFloat32Dataset() {
    prepareDataset(/* useDouble= */ false, /* useFloat16= */ false);
  }

  /**
   * Creates a table with a 16-dim vector column plus two scalar columns (id, category), inserts
   * {@link #ROWS} deterministic rows, and "plants" the neighbour closest to {@link #queryVector()}
   * at {@link #plantedRowId()}. Data is split across two inserts to force at least two fragments.
   */
  protected void prepareDataset(boolean useDouble, boolean useFloat16) {
    String elementType = useDouble ? "DOUBLE" : "FLOAT";
    StringBuilder tblProps = new StringBuilder();
    tblProps.append("'emb.arrow.fixed-size-list.size' = '").append(DIM).append("'");
    if (useFloat16) {
      tblProps.append(", 'emb.arrow.float16' = 'true'");
    }
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT NOT NULL, category STRING, emb ARRAY<%s> NOT NULL) "
                + "USING lance TBLPROPERTIES (%s)",
            fullTable, elementType, tblProps));

    int half = ROWS / 2;
    insertRange(0, half, useDouble);
    insertRange(half, ROWS, useDouble);
  }

  private void insertRange(int from, int to, boolean useDouble) {
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
          .append(vectorLiteral(i, rng, useDouble))
          .append("))");
    }
    spark.sql(sql.toString());
  }

  /**
   * Generates a vector for row {@code i}. The row at {@link #plantedRowId()} is made very close to
   * the query vector so that any correctly-working search must surface it.
   */
  private String vectorLiteral(int i, Random rng, boolean useDouble) {
    float[] query = queryVector();
    StringBuilder sb = new StringBuilder();
    for (int d = 0; d < DIM; d++) {
      if (d > 0) sb.append(", ");
      float v;
      if (i == plantedRowId()) {
        // Same direction as the query, tiny perturbation.
        v = query[d] + ((rng.nextFloat() - 0.5f) * 0.001f);
      } else {
        v = rng.nextFloat() * 10.0f - 5.0f;
      }
      if (useDouble) {
        sb.append(Double.toString(v));
      } else {
        sb.append(Float.toString(v)).append("f");
      }
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

  protected String queryVectorLiteral(String castType) {
    float[] v = queryVector();
    StringBuilder sb = new StringBuilder();
    sb.append("array(");
    for (int i = 0; i < v.length; i++) {
      if (i > 0) sb.append(", ");
      sb.append("CAST(").append(v[i]).append(" AS ").append(castType).append(")");
    }
    sb.append(")");
    return sb.toString();
  }

  protected void createVectorIndex(String indexName, String method, String withClause) {
    Dataset<Row> out =
        spark.sql(
            String.format(
                "ALTER TABLE %s CREATE INDEX %s USING %s (emb) WITH (%s)",
                fullTable, indexName, method, withClause));
    Row row = out.collectAsList().get(0);
    Assertions.assertEquals(indexName, row.getString(1));
    Assertions.assertTrue(row.getLong(0) >= 1, "Expected at least one fragment indexed");
  }

  protected void assertIndexExists(String indexName) {
    org.lance.Dataset ds = org.lance.Dataset.open().uri(tableDir).build();
    try {
      List<Index> indexes = ds.getIndexes();
      Set<String> names = indexes.stream().map(Index::name).collect(Collectors.toSet());
      Assertions.assertTrue(
          names.contains(indexName), "Expected index '" + indexName + "' in " + names);
    } finally {
      ds.close();
    }
  }

  protected Dataset<Row> runTvfSql(boolean useIndex, int k, String metric) {
    return spark.sql(
        "SELECT id FROM lance_vector_search('"
            + fullTable
            + "', 'emb', "
            + queryVectorLiteral("FLOAT")
            + ", "
            + k
            + ", '"
            + metric
            + "', 20, 1, 64, "
            + (useIndex ? "true" : "false")
            + ")");
  }

  protected Set<Integer> collectIds(Dataset<Row> df) {
    return df.collectAsList().stream().map(r -> r.getInt(0)).collect(Collectors.toSet());
  }

  protected void assertVectorSearchReturnsPlantedNeighbor() {
    runTvfAndAssertPlantedNeighbor("l2");
  }

  protected void runTvfAndAssertPlantedNeighbor(String metric) {
    Set<Integer> ids = collectIds(runTvfSql(/* useIndex= */ true, /* k= */ 10, metric));
    Assertions.assertTrue(
        ids.contains(plantedRowId()),
        "Planted neighbour id=" + plantedRowId() + " missing from top-k results " + ids);
  }

  /** Returns the root-cause exception message, flattening AnalysisException / RuntimeException. */
  protected static String rootCauseMessage(Throwable t) {
    Throwable cur = t;
    while (cur.getCause() != null && cur.getCause() != cur) {
      cur = cur.getCause();
    }
    String msg = cur.getMessage();
    return msg == null ? cur.getClass().getName() : msg;
  }

  /**
   * Skips the current test if the Arrow version on the classpath is older than 18 (where the
   * canonical half-precision float type landed) — i.e. on Spark 3.x. Subclasses may override to
   * force-enable on newer Spark versions.
   */
  protected void assumeArrow18Available() {
    boolean available;
    try {
      Class.forName("org.apache.arrow.vector.Float2Vector");
      available = true;
    } catch (ClassNotFoundException e) {
      available = false;
    }
    Assumptions.assumeTrue(available, "Arrow 18+ required for float16 vectors (Spark 4.0+)");
  }
}
