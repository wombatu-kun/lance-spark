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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * End-to-end coverage for the {@code lance_vector_search} SQL table-valued function.
 *
 * <p>All tests run in brute-force mode ({@code use_index=false}) so this class has no dependency on
 * the vector-index DDL feature — it exercises the TVF mechanics in isolation: argument parsing,
 * positional vs. named call sites, error surface, and {@code WHERE} composition.
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
