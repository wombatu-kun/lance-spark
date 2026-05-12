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
package org.lance.spark.benchmark;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * Spark job that generates TPC-H data using the Kyuubi TPC-H connector
 * and writes each table directly into the target format(s) (Lance, Parquet, etc.).
 *
 * <p>The Kyuubi connector generates data in parallel across Spark executors —
 * no external {@code dbgen} binary or intermediate CSV/dat files are needed.
 *
 * <p>Usage:
 * <pre>
 *   spark-submit --class org.lance.spark.benchmark.TpchDataGenerator \
 *     benchmark.jar \
 *     --data-dir s3a://bucket/tpch/sf10 \
 *     --scale-factor 10 \
 *     --formats parquet,lance
 * </pre>
 */
public class TpchDataGenerator {

  /** The 8 TPC-H tables as named in the Kyuubi catalog. */
  static final List<String> TPCH_TABLES =
      Arrays.asList(
          "customer",
          "lineitem",
          "nation",
          "orders",
          "part",
          "partsupp",
          "region",
          "supplier");

  public static void main(String[] args) throws Exception {
    String dataDir = null;
    int scaleFactor = 1;
    String formatsStr = "parquet,lance";
    boolean useDoubleForDecimal = false;
    String fileFormatVersion = null;

    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
        case "--data-dir":
          dataDir = args[++i];
          break;
        case "--scale-factor":
          scaleFactor = Integer.parseInt(args[++i]);
          break;
        case "--formats":
          formatsStr = args[++i];
          break;
        case "--use-double-for-decimal":
          useDoubleForDecimal = true;
          break;
        case "--file-format-version":
          fileFormatVersion = args[++i];
          break;
        default:
          System.err.println("Unknown argument: " + args[i]);
          printUsage();
          System.exit(1);
      }
    }

    if (dataDir == null) {
      System.err.println("Missing required argument: --data-dir");
      printUsage();
      System.exit(1);
    }

    String[] formats = formatsStr.split(",");

    // Configure Kyuubi TPC-H catalog
    SparkSession.Builder builder =
        SparkSession.builder()
            .appName("TPC-H Data Generator (SF=" + scaleFactor + ")")
            .config(
                "spark.sql.catalog.tpch",
                "org.apache.kyuubi.spark.connector.tpch.TPCHCatalog");

    if (useDoubleForDecimal) {
      builder.config("spark.sql.catalog.tpch.useDoubleForDecimal", "true");
    }

    SparkSession spark = builder.getOrCreate();

    try {
      System.out.println("=== TPC-H Data Generation ===");
      System.out.println("Scale factor:  " + scaleFactor);
      System.out.println("Formats:       " + formatsStr);
      System.out.println("Data dir:      " + dataDir);
      if (fileFormatVersion != null) {
        System.out.println("File format version: " + fileFormatVersion);
      }
      System.out.println();
      System.out.flush();

      String catalogDb = "tpch.sf" + scaleFactor;

      for (String format : formats) {
        format = format.trim();
        System.out.println("--- Generating " + format + " tables ---");
        System.out.flush();

        for (String table : TPCH_TABLES) {
          generateTable(spark, catalogDb, table, format, dataDir, fileFormatVersion);
        }

        System.out.println();
      }

      System.out.println("=== Data generation complete ===");
      System.out.flush();

    } finally {
      spark.stop();
    }
  }

  private static void generateTable(
      SparkSession spark, String catalogDb, String table, String format, String dataDir,
      String fileFormatVersion) {

    boolean isLance = "lance".equalsIgnoreCase(format);
    String tablePath = dataDir + "/" + format + "/" + table;
    if (isLance) {
      tablePath = TpcdsDataGenerator.toLancePath(tablePath) + ".lance";
    }

    // Check if table already exists using Hadoop filesystem API (avoids noisy Spark warnings)
    try {
      Path hadoopPath = new Path(dataDir + "/" + format + "/" + table + (isLance ? ".lance" : ""));
      FileSystem fs = hadoopPath.getFileSystem(
          spark.sparkContext().hadoopConfiguration());
      if (fs.exists(hadoopPath)) {
        System.out.println("  SKIP " + table + " (already exists at " + hadoopPath + ")");
        System.out.flush();
        return;
      }
    } catch (Exception e) {
      // Could not check — proceed with generation
    }

    System.out.print("  GENERATE " + table + "...");
    System.out.flush();
    long start = System.currentTimeMillis();

    // Read from Kyuubi TPC-H catalog — data is generated in parallel
    Dataset<Row> df = spark.read().table(catalogDb + "." + table);

    String writeFormat = isLance ? "lance" : format;
    SaveMode mode = isLance ? SaveMode.ErrorIfExists : SaveMode.Overwrite;

    org.apache.spark.sql.DataFrameWriter<Row> writer = df.write().mode(mode).format(writeFormat);
    if (isLance && fileFormatVersion != null) {
      writer = writer.option("file_format_version", fileFormatVersion);
    }
    writer.save(tablePath);

    long elapsed = System.currentTimeMillis() - start;
    long count = spark.read().format(writeFormat).load(tablePath).count();
    System.out.println(" " + count + " rows (" + elapsed + "ms)");
    System.out.flush();
  }

  private static void printUsage() {
    System.err.println(
        "Usage: TpchDataGenerator"
            + " --data-dir <path>"
            + " [--scale-factor 1]"
            + " [--formats parquet,lance]"
            + " [--use-double-for-decimal]"
            + " [--file-format-version <version>]");
  }
}
