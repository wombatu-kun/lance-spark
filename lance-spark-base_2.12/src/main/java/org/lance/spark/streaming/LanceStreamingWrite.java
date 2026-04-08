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

import org.lance.Dataset;
import org.lance.FragmentMetadata;
import org.lance.ReadOptions;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkWriteOptions;
import org.lance.spark.utils.Utils;
import org.lance.spark.write.LanceBatchWrite;
import org.lance.spark.write.LanceDataWriter;

import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Spark Structured Streaming sink for Lance tables.
 *
 * <p>This class implements {@link StreamingWrite}, which the Spark streaming engine calls once per
 * micro-batch epoch. The per-task data-write path is reused verbatim from the batch code (via
 * {@link LanceStreamingDataWriterFactory} wrapping {@link LanceDataWriter.WriterFactory}), so the
 * only streaming-specific code lives in {@link #commit(long, WriterCommitMessage[])}, which
 * delegates to {@link StreamingCommitProtocol} for idempotent, exactly-once commits.
 *
 * <h2>Output mode routing</h2>
 *
 * <ul>
 *   <li><b>Append</b> — the default. Fragments are committed via a Lance {@code Append} operation.
 *   <li><b>Complete</b> — Spark rewrites the query to call {@code SparkWriteBuilder.truncate()}
 *       before building, which propagates {@code overwrite=true} into {@link #overwrite}. Fragments
 *       are committed via a Lance {@code Overwrite} operation instead, replacing the entire table
 *       each epoch.
 *   <li><b>Update</b> — supported via the {@code SupportsStreamingUpdateAsAppend} marker interface
 *       attached to {@code SparkWrite}. Spark routes update-mode rows through the same append path
 *       as Append mode. See {@code streaming-writes.md} for the documented append-on-update
 *       semantics and the trade-offs for aggregation queries.
 * </ul>
 *
 * <h2>Exactly-once semantics</h2>
 *
 * <p>Idempotency is enforced by {@link StreamingCommitProtocol}'s two-transaction protocol (Txn1:
 * Append/Overwrite with transaction properties; Txn2: UpdateConfig bumping the per-query epoch
 * watermark). See that class's Javadoc for the full protocol description and the bounded
 * at-least-once fallback.
 */
public class LanceStreamingWrite implements StreamingWrite {

  private static final Logger LOG = LoggerFactory.getLogger(LanceStreamingWrite.class);

  private final StructType schema;
  private final LanceSparkWriteOptions writeOptions;
  private final boolean overwrite;
  private final Map<String, String> initialStorageOptions;
  private final String namespaceImpl;
  private final Map<String, String> namespaceProperties;
  private final List<String> tableId;
  private final boolean managedVersioning;
  private final LanceDataWriter.WriterFactory batchWriterFactory;

  public LanceStreamingWrite(
      StructType schema,
      LanceSparkWriteOptions writeOptions,
      boolean overwrite,
      Map<String, String> initialStorageOptions,
      String namespaceImpl,
      Map<String, String> namespaceProperties,
      List<String> tableId,
      boolean managedVersioning,
      LanceDataWriter.WriterFactory batchWriterFactory) {
    if (writeOptions.getStreamingQueryId() == null
        || writeOptions.getStreamingQueryId().isEmpty()) {
      throw LanceStreamingExceptions.streamingQueryIdRequired();
    }

    // Fail fast if the target table doesn't exist — streaming writes don't auto-create.
    try (Dataset probe = Utils.openDataset(writeOptions)) {
      // OK, table exists; use the probe only to validate.
      LOG.debug(
          "Lance streaming sink bound to dataset at version {} (queryId={})",
          probe.version(),
          writeOptions.getStreamingQueryId());
    } catch (Exception e) {
      throw LanceStreamingExceptions.targetTableDoesNotExist(writeOptions.getDatasetUri());
    }

    this.schema = schema;
    this.writeOptions = writeOptions;
    this.overwrite = overwrite;
    this.initialStorageOptions = initialStorageOptions;
    this.namespaceImpl = namespaceImpl;
    this.namespaceProperties = namespaceProperties;
    this.tableId = tableId;
    this.managedVersioning = managedVersioning;
    this.batchWriterFactory = batchWriterFactory;
  }

  @Override
  public StreamingDataWriterFactory createStreamingWriterFactory(PhysicalWriteInfo info) {
    return new LanceStreamingDataWriterFactory(batchWriterFactory);
  }

  @Override
  public void commit(long epochId, WriterCommitMessage[] messages) {
    List<FragmentMetadata> fragments =
        Arrays.stream(messages)
            .filter(java.util.Objects::nonNull)
            .map(m -> (LanceBatchWrite.TaskCommit) m)
            .map(LanceBatchWrite.TaskCommit::getFragments)
            .flatMap(List::stream)
            .collect(Collectors.toList());

    if (fragments.isEmpty()) {
      // Empty micro-batch — no data to commit. Return successfully without touching the
      // dataset. Spark will mark the epoch as committed in its checkpoint. On replay, we'll
      // re-enter with the same (possibly still empty) epoch and return again. This matches
      // the behavior of other append-only streaming sinks (Delta, Iceberg).
      LOG.info(
          "Empty micro-batch: queryId={} epochId={} — nothing to commit",
          writeOptions.getStreamingQueryId(),
          epochId);
      return;
    }

    boolean isOverwrite = overwrite || writeOptions.isOverwrite();
    StreamingCommitProtocol protocol = buildProtocol();
    protocol.commit(epochId, fragments, isOverwrite);
  }

  @Override
  public void abort(long epochId, WriterCommitMessage[] messages) {
    // No-op. Uncommitted task fragments are never visible in the Lance dataset — they only
    // become part of a version when CommitBuilder.execute() is called in commit(). If we're
    // here, no commit happened, so there's nothing to roll back.
    LOG.info(
        "Aborting streaming epoch (no-op): queryId={} epochId={}",
        writeOptions.getStreamingQueryId(),
        epochId);
  }

  /**
   * Note: {@code @Override} is deliberately omitted because {@code useCommitCoordinator()} was
   * added to the {@code StreamingWrite} interface in Spark 3.5 — earlier Spark versions do not have
   * this method. The annotation would fail compilation on Spark 3.4. At runtime, Spark 3.5+ will
   * still call this method via interface dispatch.
   *
   * <p>We return {@code false} because Lance commits are already atomic via {@link
   * org.lance.CommitBuilder}, matching {@code LanceBatchWrite.useCommitCoordinator()}.
   */
  public boolean useCommitCoordinator() {
    return false;
  }

  /**
   * Builds a {@link StreamingCommitProtocol} with production dataset openers wired to {@link
   * Utils#openDataset} and to a versioned-open path that threads a {@code version} through {@link
   * ReadOptions.Builder#setVersion(int)}. Exposed as package-private for test visibility.
   */
  StreamingCommitProtocol buildProtocol() {
    StreamingCommitProtocol.DatasetOpener opener = () -> Utils.openDataset(writeOptions);
    StreamingCommitProtocol.VersionedDatasetOpener versionedOpener =
        version -> {
          ReadOptions readOptions =
              new ReadOptions.Builder()
                  .setSession(LanceRuntime.session())
                  .setStorageOptions(writeOptions.getStorageOptions())
                  .setVersion((int) version)
                  .build();
          if (writeOptions.hasNamespace()) {
            return Dataset.open()
                .allocator(LanceRuntime.allocator())
                .namespaceClient(writeOptions.getNamespace())
                .tableId(writeOptions.getTableId())
                .readOptions(readOptions)
                .build();
          }
          return Dataset.open()
              .allocator(LanceRuntime.allocator())
              .uri(writeOptions.getDatasetUri())
              .readOptions(readOptions)
              .build();
        };

    return new StreamingCommitProtocol(
        writeOptions.getStreamingQueryId(),
        writeOptions.getMaxRecoveryLookback(),
        writeOptions,
        namespaceImpl,
        namespaceProperties,
        managedVersioning,
        tableId,
        schema,
        opener,
        versionedOpener);
  }

  /**
   * @return whether this streaming write is in overwrite (Complete) output mode.
   */
  public boolean isOverwrite() {
    return overwrite;
  }

  /**
   * @return the initial storage options carried from driver to workers.
   */
  public Map<String, String> getInitialStorageOptions() {
    return initialStorageOptions;
  }
}
