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
import org.lance.spark.LanceRuntime;

import org.apache.spark.sql.streaming.StreamingQueryListener;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Captures one {@link BatchSample} per completed Spark micro-batch for the streaming-scalability
 * benchmark.
 *
 * <p>Spark's {@code StreamingQueryListener.onQueryProgress} fires once per micro-batch after the
 * sink's {@code commit()} returns — so by the time we observe a progress event, both Lance
 * transactions (Append + UpdateConfig) have already landed and the dataset's latest version
 * reflects the new state. We open the dataset read-only at that point to record the post-commit
 * fragment count and stat the latest manifest file to record its on-disk size.
 *
 * <p>Manifest sizing only works for {@code file://} URIs. For object-store URIs (S3, GCS) we
 * record {@code -1} for {@code manifestBytes} and rely on {@code fragmentCount} as a proxy.
 */
public final class MicroBatchTimingListener extends StreamingQueryListener {

  private final String experiment;
  private final String runLabel;
  private final String lanceUri;
  private final List<BatchSample> samples = new CopyOnWriteArrayList<>();
  /**
   * Set externally by the driver right before it triggers OPTIMIZE so the next captured sample is
   * tagged. Reset after capture.
   */
  private volatile boolean optimizedThisBatchPending;

  public MicroBatchTimingListener(String experiment, String runLabel, String lanceUri) {
    this.experiment = experiment;
    this.runLabel = runLabel;
    this.lanceUri = lanceUri;
  }

  /** Mark the *next* captured batch as having been preceded by an OPTIMIZE call. */
  public void markOptimizedNextBatch() {
    this.optimizedThisBatchPending = true;
  }

  public List<BatchSample> samples() {
    return samples;
  }

  @Override
  public void onQueryStarted(QueryStartedEvent event) {
    // no-op
  }

  @Override
  public void onQueryProgress(QueryProgressEvent event) {
    org.apache.spark.sql.streaming.StreamingQueryProgress p = event.progress();
    BatchSample s = new BatchSample();
    s.experiment = experiment;
    s.runLabel = runLabel;
    s.batchId = p.batchId();
    s.batchTimestampMs = parseProgressTimestamp(p.timestamp());
    s.inputRows = p.numInputRows();
    s.batchDurationMs = p.batchDuration();
    s.optimizedThisBatch = optimizedThisBatchPending;
    optimizedThisBatchPending = false;

    if (s.inputRows > 0) {
      // Empty batches are no-ops in the Lance sink (no commit) — opening the dataset for them
      // would just record the unchanged prior state, which is misleading. Skip them.
      try (Dataset ds =
          Dataset.open().allocator(LanceRuntime.allocator()).uri(lanceUri).build()) {
        s.lanceVersion = ds.version();
        s.fragmentCount = ds.getFragments().size();
      } catch (Exception e) {
        s.lanceVersion = -1L;
        s.fragmentCount = -1L;
      }
      s.manifestBytes = manifestBytesIfLocal(lanceUri, s.lanceVersion);
    } else {
      s.lanceVersion = -1L;
      s.fragmentCount = -1L;
      s.manifestBytes = -1L;
    }

    samples.add(s);
  }

  @Override
  public void onQueryTerminated(QueryTerminatedEvent event) {
    // no-op
  }

  /**
   * Spark's progress timestamp is an ISO-8601 string. Parse it to epoch-millis so the CSV stays
   * numeric. Returns 0 on any parse failure rather than throwing — the driver doesn't care about
   * absolute time, only deltas.
   */
  private static long parseProgressTimestamp(String iso) {
    if (iso == null || iso.isEmpty()) {
      return 0L;
    }
    try {
      return java.time.Instant.parse(iso).toEpochMilli();
    } catch (Exception e) {
      return 0L;
    }
  }

  /**
   * Best-effort manifest size lookup for {@code file://} / bare-path URIs. Lance stores manifests
   * under {@code <dataset>/_versions/<filename>.manifest}, where the filename is the version
   * encoded as {@code MAX_U64 - version} as a decimal string — Lance does this so the listing is
   * sorted with the latest version first. Returns {@code -1} for any non-local URI scheme or if
   * the file isn't readable.
   */
  static long manifestBytesIfLocal(String uri, long version) {
    if (version <= 0L) {
      return -1L;
    }
    try {
      Path datasetPath;
      if (uri.startsWith("file:")) {
        datasetPath = Paths.get(URI.create(uri));
      } else if (uri.startsWith("/")) {
        datasetPath = Paths.get(uri);
      } else {
        // s3://, gs://, etc. — out of scope for filesystem stat.
        return -1L;
      }
      Path manifest = datasetPath.resolve("_versions").resolve(manifestFileName(version));
      return Files.isRegularFile(manifest) ? Files.size(manifest) : -1L;
    } catch (IOException | RuntimeException e) {
      return -1L;
    }
  }

  /**
   * Lance manifest filenames encode the version as the unsigned decimal of {@code (2^64 - 1) -
   * version} (e.g. v1 -> {@code 18446744073709551614.manifest}). In Java the unsigned arithmetic
   * is just {@code -1L - version}; rendering needs {@link Long#toUnsignedString} so the result
   * isn't a negative number.
   */
  static String manifestFileName(long version) {
    return Long.toUnsignedString(-1L - version) + ".manifest";
  }
}
