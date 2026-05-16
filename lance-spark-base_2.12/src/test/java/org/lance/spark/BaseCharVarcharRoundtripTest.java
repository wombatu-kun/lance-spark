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
package org.lance.spark;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * E2E roundtrip tests for CHAR and VARCHAR types via SQL DDL through a V2 catalog. Since Spark 3.1+
 * sets {@code spark.sql.legacy.charVarcharAsString=false} by default, CHAR/VARCHAR remain as
 * distinct types ({@code CharType}/{@code VarcharType}) through the entire V2 write path and reach
 * the Lance connector as-is. No special user configuration is required.
 *
 * <p>Note: Lance stores both CHAR and VARCHAR as Arrow Utf8 — the length constraint is not
 * preserved on read. CHAR(n) values are NOT space-padded by Spark before reaching the connector.
 */
public abstract class BaseCharVarcharRoundtripTest {
  private SparkSession spark;
  private static final String catalogName = "lance_ns";

  @TempDir protected Path tempDir;

  @BeforeEach
  void setup() {
    spark =
        SparkSession.builder()
            .appName("lance-char-varchar-roundtrip-test")
            .master("local")
            .config(
                "spark.sql.catalog." + catalogName, "org.lance.spark.LanceNamespaceSparkCatalog")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + ".root", tempDir.toString())
            .getOrCreate();
    // Create default namespace required by LanceNamespaceSparkCatalog's multi-level namespace mode.
    spark.sql("CREATE NAMESPACE IF NOT EXISTS " + catalogName + ".default");
  }

  @AfterEach
  void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  private String qualifiedName(String table) {
    return catalogName + ".default." + table;
  }

  @Test
  public void testCharTypeRoundtrip() {
    String table = "char_test_" + System.currentTimeMillis();
    String qn = qualifiedName(table);

    spark.sql("CREATE TABLE " + qn + " (id INT, v CHAR(10)) USING lance");

    spark.sql("INSERT INTO " + qn + " VALUES (0, 'hello'), (1, ''), (2, NULL)");

    List<Row> out = spark.sql("SELECT * FROM " + qn + " ORDER BY id").collectAsList();

    assertEquals(3, out.size());
    // CHAR is stored as Arrow Utf8 — length constraint is not preserved on read.
    assertEquals("hello", out.get(0).getString(1));
    assertEquals("", out.get(1).getString(1));
    assertTrue(out.get(2).isNullAt(1));

    spark.sql("DROP TABLE IF EXISTS " + qn);
  }

  @Test
  public void testVarcharTypeRoundtrip() {
    String table = "varchar_test_" + System.currentTimeMillis();
    String qn = qualifiedName(table);

    spark.sql("CREATE TABLE " + qn + " (id INT, v VARCHAR(50)) USING lance");

    spark.sql("INSERT INTO " + qn + " VALUES (0, 'world'), (1, ''), (2, NULL)");

    List<Row> out = spark.sql("SELECT * FROM " + qn + " ORDER BY id").collectAsList();

    assertEquals(3, out.size());
    // VARCHAR is stored as Arrow Utf8 — length constraint is not preserved on read.
    assertEquals("world", out.get(0).getString(1));
    assertEquals("", out.get(1).getString(1));
    assertTrue(out.get(2).isNullAt(1));

    spark.sql("DROP TABLE IF EXISTS " + qn);
  }

  @Test
  public void testMixedCharVarcharAndStringColumns() {
    String table = "mixed_str_test_" + System.currentTimeMillis();
    String qn = qualifiedName(table);

    spark.sql("CREATE TABLE " + qn + " (id INT, c CHAR(5), v VARCHAR(20), s STRING) USING lance");

    spark.sql(
        "INSERT INTO "
            + qn
            + " VALUES (0, 'abc', 'hello world', 'plain string'), (1, NULL, NULL, NULL)");

    List<Row> out = spark.sql("SELECT * FROM " + qn + " ORDER BY id").collectAsList();

    assertEquals(2, out.size());
    assertEquals("abc", out.get(0).getString(1));
    assertEquals("hello world", out.get(0).getString(2));
    assertEquals("plain string", out.get(0).getString(3));
    assertTrue(out.get(1).isNullAt(1));
    assertTrue(out.get(1).isNullAt(2));
    assertTrue(out.get(1).isNullAt(3));

    spark.sql("DROP TABLE IF EXISTS " + qn);
  }
}
