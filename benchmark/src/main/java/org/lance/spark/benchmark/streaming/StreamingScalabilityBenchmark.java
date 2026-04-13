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
package org.lance.spark.benchmark.streaming;

import org.lance.Dataset;
import org.lance.compaction.Compaction;
import org.lance.compaction.CompactionMetrics;
import org.lance.compaction.CompactionOptions;
import org.lance.compaction.CompactionPlan;
import org.lance.compaction.CompactionTask;
import org.lance.compaction.RewriteResult;
import org.lance.spark.LanceDataSource;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkReadOptions;
import org.lance.spark.LanceSparkWriteOptions;

import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryProgress;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * Streaming-write scalability benchmark for the Lance Spark sink.
 *
 * <p>Driver for four experiments that quantify the per-microbatch overhead of the two-transaction
 * commit protocol (Append + UpdateConfig) and the cost of the bounded recovery scan. Each
 * experiment writes one CSV row per micro-batch (or per measurement point for E3) into the supplied
 * results dir; downstream plotting / analysis is left to whoever consumes the CSVs.
 *
 * <h2>Experiments</h2>
 *
 * <ul>
 *   <li><b>E1 — commit latency vs. fragment count.</b> Drive N micro-batches with no compaction and
 *       record per-batch wall time, fragment count, and manifest bytes.
 *   <li><b>E2 — same, with periodic OPTIMIZE.</b> Like E1 but call Lance's compaction every K
 *       batches. Comparing E1 vs E2 quantifies the mitigation.
 *   <li><b>E3 — recovery-scan cost vs. version history.</b> Build datasets at multiple version
 *       depths and time the cost of walking back through them looking for a Txn1 match — the work
 *       the streaming sink does on restart.
 *   <li><b>E4 — sustained throughput vs. trigger interval.</b> Run for a fixed wall-clock window at
 *       different trigger intervals; compare achieved rows/sec.
 * </ul>
 */
public final class StreamingScalabilityBenchmark {

  private static final StructType SCHEMA =
      new StructType()
          .add("id", DataTypes.LongType, false)
          .add("value", DataTypes.StringType, false);

  public static void main(String[] args) throws Exception {
    Args parsed = Args.parse(args);

    SparkSession spark =
        SparkSession.builder()
            .appName("Lance Streaming Scalability Benchmark — " + parsed.experiment)
            .getOrCreate();

    String stamp =
        LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
    Files.createDirectories(Paths.get(parsed.resultsDir));
    Path csvOut =
        Paths.get(parsed.resultsDir, "streaming_" + parsed.experiment + "_" + stamp + ".csv");
    System.out.println("Writing per-batch samples to " + csvOut.toAbsolutePath());

    try (BufferedWriter out =
        Files.newBufferedWriter(csvOut, StandardCharsets.UTF_8)) {
      out.write(BatchSample.csvHeader());
      out.newLine();

      switch (parsed.experiment) {
        case "E1":
          runE1(spark, parsed, out);
          break;
        case "E2":
          runE2(spark, parsed, out);
          break;
        case "E3":
          runE3(spark, parsed, out);
          break;
        case "E4":
          runE4(spark, parsed, out);
          break;
        default:
          throw new IllegalArgumentException(
              "Unknown --experiment '" + parsed.experiment + "', expected E1|E2|E3|E4");
      }
      out.flush();
    } finally {
      spark.stop();
    }

    System.out.println("Done. Results at " + csvOut.toAbsolutePath());
  }

  // =========================================================================
  // E1 — commit latency vs. fragment count, no compaction.
  // =========================================================================

  private static void runE1(SparkSession spark, Args a, BufferedWriter out) throws Exception {
    String lanceUri = freshDataset(spark, a, "e1");
    runRateStream(spark, lanceUri, "E1", "no-optimize", a, /*optimizeEvery=*/ 0, out);
  }

  // =========================================================================
  // E2 — same workload, OPTIMIZE every K batches.
  // =========================================================================

  private static void runE2(SparkSession spark, Args a, BufferedWriter out) throws Exception {
    int[] cadences = parseInts(a.optimizeCadences, new int[] {50, 200, 1000});
    for (int k : cadences) {
      String lanceUri = freshDataset(spark, a, "e2-k" + k);
      runRateStream(spark, lanceUri, "E2", "optimize-every-" + k, a, k, out);
    }
  }

  /**
   * Shared driver for E1/E2: streams from the rate source for {@code a.batches} micro-batches,
   * captures per-batch samples via {@link MicroBatchTimingListener}, optionally calls Lance
   * compaction every {@code optimizeEvery} batches (0 = never), and flushes samples to {@code out}
   * as it goes so a long run is observable in real time.
   */
  private static void runRateStream(
      SparkSession spark,
      String lanceUri,
      String experiment,
      String runLabel,
      Args a,
      int optimizeEvery,
      BufferedWriter out)
      throws Exception {
    System.out.printf(
        "[%s/%s] streaming %d batches into %s (rowsPerSecond=%d, optimizeEvery=%d)%n",
        experiment, runLabel, a.batches, lanceUri, a.rowsPerSecond, optimizeEvery);

    MicroBatchTimingListener listener = new MicroBatchTimingListener(experiment, runLabel, lanceUri);
    spark.streams().addListener(listener);
    int written = 0;

    StreamingQuery query =
        spark
            .readStream()
            .format("rate")
            .option("rowsPerSecond", String.valueOf(a.rowsPerSecond))
            .load()
            .selectExpr("CAST(value AS LONG) AS id", "CAST(value AS STRING) AS value")
            .writeStream()
            .format(LanceDataSource.name)
            .option(LanceSparkReadOptions.CONFIG_DATASET_URI, lanceUri)
            .option("checkpointLocation", checkpointDir(a, experiment + "-" + runLabel).toString())
            .option(LanceSparkWriteOptions.CONFIG_STREAMING_QUERY_ID, "bench-" + UUID.randomUUID())
            .outputMode("append")
            .trigger(Trigger.ProcessingTime("0 seconds"))
            .start();

    try {
      while (true) {
        long latestBatch = latestBatchId(query);
        // Flush any new samples to disk + run optional periodic OPTIMIZE.
        synchronized (listener.samples()) {
          while (written < listener.samples().size()) {
            BatchSample s = listener.samples().get(written++);
            out.write(s.toString());
            out.newLine();
            out.flush();
            if (optimizeEvery > 0 && s.batchId > 0 && s.batchId % optimizeEvery == 0) {
              CompactionMetrics m = compact(lanceUri);
              System.out.printf(
                  "  OPTIMIZE @batch=%d -> -%dfrag +%dfrag%n",
                  s.batchId, m.getFragmentsRemoved(), m.getFragmentsAdded());
              listener.markOptimizedNextBatch();
            }
          }
        }
        if (latestBatch >= a.batches - 1) {
          break;
        }
        Thread.sleep(50);
      }
    } finally {
      try {
        query.stop();
      } catch (Exception ignored) {
        // best-effort
      }
      spark.streams().removeListener(listener);
    }

    // Drain any final samples that arrived between the last poll and stop().
    synchronized (listener.samples()) {
      while (written < listener.samples().size()) {
        out.write(listener.samples().get(written++).toString());
        out.newLine();
      }
    }
    out.flush();
  }

  // =========================================================================
  // E3 — recovery-scan cost vs. version history.
  // =========================================================================

  /**
   * For each target version count, build (or re-use) a dataset with that many versions, then time
   * a worst-case recovery scan: walk back through {@code maxRecoveryLookback} versions,
   * deserializing each transaction's properties — the same work {@link
   * org.lance.spark.streaming.StreamingCommitProtocol} does on restart when the prior Txn1 is
   * absent. Reports one row per (versionTarget, lookback) pair.
   */
  private static void runE3(SparkSession spark, Args a, BufferedWriter out) throws Exception {
    int[] versionTargets = parseInts(a.versionTargets, new int[] {10, 100, 500, 1000, 5000});
    int[] lookbacks = parseInts(a.recoveryLookbacks, new int[] {100, 500, 1000});

    String lanceUri = freshDataset(spark, a, "e3");
    long currentVersion = openLatestVersion(lanceUri);
    int trials = a.recoveryTrials > 0 ? a.recoveryTrials : 5;

    for (int target : versionTargets) {
      while (currentVersion < target) {
        appendOneVersion(spark, lanceUri);
        currentVersion++;
      }
      System.out.printf("[E3] dataset at version %d, measuring recovery scan...%n", currentVersion);
      for (int lookback : lookbacks) {
        for (int trial = 0; trial < trials; trial++) {
          long t0 = System.nanoTime();
          long visited = walkRecoveryScan(lanceUri, currentVersion, lookback);
          long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;

          BatchSample s = new BatchSample();
          s.experiment = "E3";
          s.runLabel = "lookback=" + lookback + ";trial=" + trial;
          s.batchId = visited;
          s.batchTimestampMs = System.currentTimeMillis();
          s.batchDurationMs = elapsedMs;
          s.lanceVersion = currentVersion;
          s.fragmentCount = -1L;
          s.manifestBytes = -1L;
          out.write(s.toString());
          out.newLine();
          out.flush();
        }
      }
    }
  }

  /**
   * Walks backward from {@code currentVersion} for up to {@code lookback} versions, opening each
   * one and reading its transaction properties — mirroring the worst-case (no-match) cost of the
   * recovery scan in StreamingCommitProtocol.
   *
   * @return number of versions actually visited (clamped by {@code currentVersion}).
   */
  private static long walkRecoveryScan(String lanceUri, long currentVersion, int lookback) {
    long lower = Math.max(1L, currentVersion - lookback + 1L);
    long visited = 0L;
    try (Dataset latest =
        Dataset.open().allocator(LanceRuntime.allocator()).uri(lanceUri).build()) {
      for (long v = currentVersion; v >= lower; v--) {
        try (Dataset historical = latest.checkoutVersion(v)) {
          historical.readTransaction(); // discard — we're paying the IO + parse cost
          visited++;
        } catch (Exception ignored) {
          // versions can be missing in pathological cases — skip without breaking the trial
        }
      }
    }
    return visited;
  }

  // =========================================================================
  // E4 — sustained throughput vs. trigger interval.
  // =========================================================================

  private static void runE4(SparkSession spark, Args a, BufferedWriter out) throws Exception {
    String[] intervals = a.triggerIntervals != null
        ? a.triggerIntervals.split(",")
        : new String[] {"1 second", "5 seconds", "30 seconds", "60 seconds"};
    long windowMs = a.windowSeconds > 0 ? a.windowSeconds * 1000L : 600_000L;

    for (String interval : intervals) {
      String runLabel = "trigger=" + interval.trim();
      String lanceUri = freshDataset(spark, a, "e4-" + interval.trim().replace(' ', '_'));
      System.out.printf(
          "[E4/%s] running for %d ms (rowsPerSecond=%d)%n", runLabel, windowMs, a.rowsPerSecond);

      MicroBatchTimingListener listener =
          new MicroBatchTimingListener("E4", runLabel, lanceUri);
      spark.streams().addListener(listener);

      StreamingQuery query =
          spark
              .readStream()
              .format("rate")
              .option("rowsPerSecond", String.valueOf(a.rowsPerSecond))
              .load()
              .selectExpr("CAST(value AS LONG) AS id", "CAST(value AS STRING) AS value")
              .writeStream()
              .format(LanceDataSource.name)
              .option(LanceSparkReadOptions.CONFIG_DATASET_URI, lanceUri)
              .option("checkpointLocation", checkpointDir(a, "e4-" + interval.trim()).toString())
              .option(
                  LanceSparkWriteOptions.CONFIG_STREAMING_QUERY_ID, "bench-" + UUID.randomUUID())
              .outputMode("append")
              .trigger(Trigger.ProcessingTime(interval.trim()))
              .start();

      long deadline = System.currentTimeMillis() + windowMs;
      int written = 0;
      try {
        while (System.currentTimeMillis() < deadline) {
          synchronized (listener.samples()) {
            while (written < listener.samples().size()) {
              out.write(listener.samples().get(written++).toString());
              out.newLine();
            }
          }
          out.flush();
          Thread.sleep(200);
        }
      } finally {
        try {
          query.stop();
        } catch (Exception ignored) {
          // best-effort
        }
        spark.streams().removeListener(listener);
      }
      synchronized (listener.samples()) {
        while (written < listener.samples().size()) {
          out.write(listener.samples().get(written++).toString());
          out.newLine();
        }
      }
      out.flush();
    }
  }

  // =========================================================================
  // Helpers.
  // =========================================================================

  /** Latest batchId observed on the query, or -1 if no batch has completed yet. */
  private static long latestBatchId(StreamingQuery query) {
    long latest = -1L;
    if (query.exception().isDefined()) {
      throw new IllegalStateException("Streaming query failed", query.exception().get());
    }
    for (StreamingQueryProgress p : query.recentProgress()) {
      if (p != null) {
        latest = Math.max(latest, p.batchId());
      }
    }
    return latest;
  }

  private static long openLatestVersion(String lanceUri) {
    try (Dataset ds =
        Dataset.open().allocator(LanceRuntime.allocator()).uri(lanceUri).build()) {
      return ds.version();
    }
  }

  /**
   * Pre-creates an empty Lance dataset at a fresh URI under the data dir and returns the URI.
   * Streaming sinks require the target table to exist before the query starts.
   */
  private static String freshDataset(SparkSession spark, Args a, String suffix) throws IOException {
    Files.createDirectories(Paths.get(a.dataDir));
    String uri =
        Paths.get(a.dataDir, "stream-bench-" + suffix + "-" + UUID.randomUUID() + ".lance")
            .toString();
    // Default save mode (ErrorIfExists). Append would trigger loadTable() to verify existence
    // first, which fails on a path that doesn't exist yet — see #399 review thread.
    spark
        .createDataFrame(java.util.Collections.emptyList(), SCHEMA)
        .write()
        .format(LanceDataSource.name)
        .option(LanceSparkReadOptions.CONFIG_DATASET_URI, uri)
        .save();
    return uri;
  }

  /** Append a tiny batch via the connector to advance the dataset version by one. */
  private static void appendOneVersion(SparkSession spark, String lanceUri) {
    spark
        .range(0, 1)
        .selectExpr("CAST(id AS LONG) AS id", "CAST(id AS STRING) AS value")
        .write()
        .format(LanceDataSource.name)
        .option(LanceSparkReadOptions.CONFIG_DATASET_URI, lanceUri)
        .mode(SaveMode.Append)
        .save();
  }

  private static Path checkpointDir(Args a, String label) throws IOException {
    Path p = Paths.get(a.dataDir, "ckpt-" + label + "-" + UUID.randomUUID());
    Files.createDirectories(p);
    return p;
  }

  /**
   * Single-threaded compaction over the entire dataset. Mirrors what {@code OptimizeExec} does
   * minus the Spark RDD parallelism — adequate for a benchmark on a single machine, and avoids the
   * compaction itself becoming a Spark-job overhead measurement.
   */
  private static CompactionMetrics compact(String lanceUri) {
    CompactionOptions opts = CompactionOptions.builder().build();
    try (Dataset ds =
        Dataset.open().allocator(LanceRuntime.allocator()).uri(lanceUri).build()) {
      CompactionPlan plan = Compaction.planCompaction(ds, opts);
      List<RewriteResult> results = new ArrayList<>();
      for (CompactionTask task : plan.getCompactionTasks()) {
        try (Dataset taskDs =
            Dataset.open().allocator(LanceRuntime.allocator()).uri(lanceUri).build()) {
          results.add(task.execute(taskDs));
        }
      }
      if (results.isEmpty()) {
        return new CompactionMetrics(0L, 0L, 0L, 0L);
      }
      try (Dataset commitDs =
          Dataset.open().allocator(LanceRuntime.allocator()).uri(lanceUri).build()) {
        return Compaction.commitCompaction(commitDs, results, opts);
      }
    }
  }

  private static int[] parseInts(String csv, int[] fallback) {
    if (csv == null || csv.isEmpty()) {
      return fallback;
    }
    String[] parts = csv.split(",");
    int[] out = new int[parts.length];
    for (int i = 0; i < parts.length; i++) {
      out[i] = Integer.parseInt(parts[i].trim());
    }
    return out;
  }

  // =========================================================================
  // Args.
  // =========================================================================

  /** Plain CLI args struct — mirrors {@code TpcdsBenchmarkRunner} for consistency. */
  static final class Args {
    String experiment;
    String dataDir;
    String resultsDir;
    int batches = 1000;
    long rowsPerSecond = 1000L;
    String optimizeCadences;
    String versionTargets;
    String recoveryLookbacks;
    int recoveryTrials = 5;
    String triggerIntervals;
    long windowSeconds = 300L;

    static Args parse(String[] args) {
      Args a = new Args();
      for (int i = 0; i < args.length; i++) {
        switch (args[i]) {
          case "--experiment": a.experiment = args[++i]; break;
          case "--data-dir": a.dataDir = args[++i]; break;
          case "--results-dir": a.resultsDir = args[++i]; break;
          case "--batches": a.batches = Integer.parseInt(args[++i]); break;
          case "--rows-per-second": a.rowsPerSecond = Long.parseLong(args[++i]); break;
          case "--optimize-cadences": a.optimizeCadences = args[++i]; break;
          case "--version-targets": a.versionTargets = args[++i]; break;
          case "--recovery-lookbacks": a.recoveryLookbacks = args[++i]; break;
          case "--recovery-trials": a.recoveryTrials = Integer.parseInt(args[++i]); break;
          case "--trigger-intervals": a.triggerIntervals = args[++i]; break;
          case "--window-seconds": a.windowSeconds = Long.parseLong(args[++i]); break;
          default:
            System.err.println("Unknown argument: " + args[i]);
            printUsage();
            System.exit(1);
        }
      }
      if (a.experiment == null || a.dataDir == null || a.resultsDir == null) {
        printUsage();
        throw new IllegalArgumentException(
            "--experiment, --data-dir, and --results-dir are required");
      }
      if (!Arrays.asList("E1", "E2", "E3", "E4").contains(a.experiment)) {
        throw new IllegalArgumentException("--experiment must be one of E1, E2, E3, E4");
      }
      return a;
    }

    static void printUsage() {
      System.err.println(
          "Usage: StreamingScalabilityBenchmark"
              + " --experiment {E1|E2|E3|E4}"
              + " --data-dir <path>"
              + " --results-dir <path>"
              + " [--batches 1000]"
              + " [--rows-per-second 1000]"
              + " [--optimize-cadences 50,200,1000]   # E2 only"
              + " [--version-targets 10,100,500,1000,5000]   # E3 only"
              + " [--recovery-lookbacks 100,500,1000]   # E3 only"
              + " [--recovery-trials 5]                 # E3 only"
              + " [--trigger-intervals '1 second,5 seconds,30 seconds,60 seconds']  # E4 only"
              + " [--window-seconds 300]                # E4 only");
    }
  }

}
