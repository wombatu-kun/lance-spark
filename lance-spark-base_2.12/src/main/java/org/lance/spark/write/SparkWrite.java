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
package org.lance.spark.write;

import org.lance.WriteParams;
import org.lance.spark.LanceSparkWriteOptions;
import org.lance.spark.streaming.LanceStreamingExceptions;
import org.lance.spark.streaming.LanceStreamingWrite;

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.SupportsTruncate;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.internal.connector.SupportsStreamingUpdateAsAppend;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.Map;

/** Spark write builder. */
public class SparkWrite implements Write {
  private final LanceSparkWriteOptions writeOptions;
  private final StructType schema;
  private final boolean overwrite;

  /**
   * Initial storage options fetched from namespace.describeTable() on the driver. These are passed
   * to workers so they can reuse the credentials without calling describeTable again.
   */
  private final Map<String, String> initialStorageOptions;

  /** Namespace configuration for credential refresh on workers. */
  private final String namespaceImpl;

  private final Map<String, String> namespaceProperties;
  private final List<String> tableId;
  private final boolean managedVersioning;
  private final StagedCommit stagedCommit;

  SparkWrite(
      StructType schema,
      LanceSparkWriteOptions writeOptions,
      boolean overwrite,
      Map<String, String> initialStorageOptions,
      String namespaceImpl,
      Map<String, String> namespaceProperties,
      List<String> tableId,
      boolean managedVersioning,
      StagedCommit stagedCommit) {
    this.schema = schema;
    this.writeOptions = writeOptions;
    this.overwrite = overwrite;
    this.initialStorageOptions = initialStorageOptions;
    this.namespaceImpl = namespaceImpl;
    this.namespaceProperties = namespaceProperties;
    this.tableId = tableId;
    this.managedVersioning = managedVersioning;
    this.stagedCommit = stagedCommit;
  }

  @Override
  public BatchWrite toBatch() {
    return new LanceBatchWrite(
        schema,
        writeOptions,
        overwrite,
        initialStorageOptions,
        namespaceImpl,
        namespaceProperties,
        tableId,
        managedVersioning,
        stagedCommit);
  }

  @Override
  public StreamingWrite toStreaming() {
    // Staged commits are CTAS / REPLACE TABLE flows that eagerly materialize a commit object
    // on the driver, to be committed once via Table.commitStagedChanges(). Streaming writes
    // commit once per epoch and are fundamentally incompatible with that model.
    if (stagedCommit != null) {
      throw LanceStreamingExceptions.stagedTableNotSupported();
    }

    // Pre-build the batch writer factory here (same package, can call the protected constructor)
    // and pass it into LanceStreamingWrite so it doesn't need to duplicate per-task wiring.
    LanceDataWriter.WriterFactory batchWriterFactory =
        new LanceDataWriter.WriterFactory(
            schema,
            writeOptions,
            initialStorageOptions,
            namespaceImpl,
            namespaceProperties,
            tableId);

    return new LanceStreamingWrite(
        schema,
        writeOptions,
        overwrite,
        initialStorageOptions,
        namespaceImpl,
        namespaceProperties,
        tableId,
        managedVersioning,
        batchWriterFactory);
  }

  /**
   * Spark write builder.
   *
   * <p>Implements {@link SupportsStreamingUpdateAsAppend} — an internal Spark marker interface that
   * tells the streaming engine's {@code V2Writes} analyzer that this sink accepts {@code Update}
   * output mode by treating the emitted rows as appends. Spark's analyzer checks this interface on
   * the {@code WriteBuilder} (not on the resulting {@code Write}) when resolving a streaming
   * query's output mode. Without this marker, Spark rejects {@code .outputMode("update")} at query
   * analysis time. See the "Update output mode semantics" section in {@code
   * docs/src/operations/streaming/streaming-writes.md} for the documented trade-offs.
   */
  public static class SparkWriteBuilder
      implements SupportsTruncate, WriteBuilder, SupportsStreamingUpdateAsAppend {
    private final LanceSparkWriteOptions writeOptions;
    private final StructType schema;
    private boolean overwrite = false;
    private StagedCommit stagedCommit;

    /**
     * Initial storage options fetched from namespace.describeTable() on the driver. These are
     * passed to workers so they can reuse the credentials without calling describeTable again.
     */
    private final Map<String, String> initialStorageOptions;

    /** Namespace configuration for credential refresh on workers. */
    private final String namespaceImpl;

    private final Map<String, String> namespaceProperties;
    private final List<String> tableId;
    private final boolean managedVersioning;

    public SparkWriteBuilder(
        StructType schema,
        LanceSparkWriteOptions writeOptions,
        Map<String, String> initialStorageOptions,
        String namespaceImpl,
        Map<String, String> namespaceProperties,
        List<String> tableId,
        boolean managedVersioning) {
      this.schema = schema;
      this.writeOptions = writeOptions;
      this.initialStorageOptions = initialStorageOptions;
      this.namespaceImpl = namespaceImpl;
      this.namespaceProperties = namespaceProperties;
      this.tableId = tableId;
      this.managedVersioning = managedVersioning;
    }

    public void setStagedCommit(StagedCommit stagedCommit) {
      this.stagedCommit = stagedCommit;
    }

    @Override
    public Write build() {
      LanceSparkWriteOptions options =
          !overwrite
              ? writeOptions
              : LanceSparkWriteOptions.builder()
                  .storageOptions(writeOptions.getStorageOptions())
                  .namespace(writeOptions.getNamespace())
                  .tableId(writeOptions.getTableId())
                  .batchSize(writeOptions.getBatchSize())
                  .datasetUri(writeOptions.getDatasetUri())
                  .fileFormatVersion(writeOptions.getFileFormatVersion())
                  .maxBytesPerFile(writeOptions.getMaxBytesPerFile())
                  .maxRowsPerFile(writeOptions.getMaxRowsPerFile())
                  .maxRowsPerGroup(writeOptions.getMaxRowsPerGroup())
                  .queueDepth(writeOptions.getQueueDepth())
                  .useQueuedWriteBuffer(writeOptions.isUseQueuedWriteBuffer())
                  .enableStableRowIds(writeOptions.getEnableStableRowIds())
                  .streamingQueryId(writeOptions.getStreamingQueryId())
                  .maxRecoveryLookback(writeOptions.getMaxRecoveryLookback())
                  .writeMode(WriteParams.WriteMode.OVERWRITE)
                  .build();

      return new SparkWrite(
          schema,
          options,
          overwrite,
          initialStorageOptions,
          namespaceImpl,
          namespaceProperties,
          tableId,
          managedVersioning,
          stagedCommit);
    }

    @Override
    public WriteBuilder truncate() {
      this.overwrite = true;
      return this;
    }
  }
}
