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

import org.lance.CommitBuilder;
import org.lance.Dataset;
import org.lance.FragmentMetadata;
import org.lance.Transaction;
import org.lance.namespace.LanceNamespace;
import org.lance.operation.Append;
import org.lance.operation.Operation;
import org.lance.operation.Overwrite;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkWriteOptions;
import org.lance.spark.utils.Utils;

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class LanceBatchWrite implements BatchWrite {
  private static final Logger logger = LoggerFactory.getLogger(LanceBatchWrite.class);

  private final StructType schema;
  private LanceSparkWriteOptions writeOptions;
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

  public LanceBatchWrite(
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
    this.overwrite = overwrite;
    this.initialStorageOptions = initialStorageOptions;
    this.namespaceImpl = namespaceImpl;
    this.namespaceProperties = namespaceProperties;
    this.tableId = tableId;
    this.managedVersioning = managedVersioning;
    this.stagedCommit = stagedCommit;

    // For staged operations, the dataset is managed by StagedCommit.
    // For non-staged operations, pin the dataset version for OCC.
    if (stagedCommit != null) {
      this.writeOptions = writeOptions;
    } else {
      try (Dataset ds = Utils.openDatasetBuilder(writeOptions).build()) {
        this.writeOptions = writeOptions.withVersion(ds.version());
        logger.debug(
            "Resolved dataset version for batch write: {}", this.writeOptions.getVersion());
      }
    }
  }

  @Override
  public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
    return new LanceDataWriter.WriterFactory(
        schema, writeOptions, initialStorageOptions, namespaceImpl, namespaceProperties, tableId);
  }

  @Override
  public boolean useCommitCoordinator() {
    return false;
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {
    List<FragmentMetadata> fragments =
        Arrays.stream(messages)
            .map(m -> (TaskCommit) m)
            .map(TaskCommit::getFragments)
            .flatMap(List::stream)
            .collect(Collectors.toList());

    Schema arrowSchema = LanceArrowUtils.toArrowSchema(schema, "UTC", true);
    boolean isOverwrite = overwrite || writeOptions.isOverwrite();

    // Boxed: null means unset (inherit in lance-core); see LanceSparkWriteOptions.
    final Boolean enableStableRowIds = writeOptions.getEnableStableRowIds();

    if (stagedCommit != null) {
      // For staged tables, update the eagerly-created StagedCommit with fragments and schema.
      // commitStagedChanges() will perform the actual commit.
      stagedCommit.setFragments(fragments);
      stagedCommit.setSchema(arrowSchema);
      if (enableStableRowIds != null) {
        stagedCommit.setEnableStableRowIds(enableStableRowIds);
      }
    } else {
      // For non-staged tables, commit immediately via the shared doCommit helper
      // (reused by the streaming sink for exactly-once commits). The constructor pinned
      // the dataset version on writeOptions, so we re-open at that pinned version here —
      // this matches main's "don't hold Dataset across phases" contract while still
      // letting LanceStreamingWrite reuse the same commit path.
      Objects.requireNonNull(
          writeOptions.getVersion(),
          "version must be set (resolved in LanceBatchWrite constructor)");
      try (Dataset ds = Utils.openDatasetBuilder(writeOptions).build()) {
        doCommit(
            ds,
            fragments,
            arrowSchema,
            isOverwrite,
            writeOptions,
            namespaceImpl,
            namespaceProperties,
            managedVersioning,
            tableId,
            null /* no transaction properties for batch writes */);
      }
    }
  }

  /**
   * Core commit path shared between batch and streaming writes: builds an {@link Append} or {@link
   * Overwrite} operation from the supplied fragments, wraps it in a {@link Transaction} (optionally
   * carrying {@code transactionProperties} for streaming exactly-once tracking), and executes the
   * commit via {@link CommitBuilder}.
   *
   * <p>The caller is responsible for opening and closing the input {@link Dataset}. This method
   * only closes the transient objects it creates ({@link Transaction} and the post-commit {@link
   * Dataset} returned by {@link CommitBuilder#execute}).
   *
   * @param dataset existing dataset to commit to
   * @param fragments fragments produced by worker tasks
   * @param arrowSchema Arrow schema, used only for {@link Overwrite} operations
   * @param isOverwrite {@code true} to build an {@link Overwrite} op, {@code false} for {@link
   *     Append}
   * @param writeOptions write options (storage options, stable row IDs, etc.)
   * @param namespaceImpl namespace implementation class name, may be {@code null}
   * @param namespaceProperties namespace connection properties, may be {@code null}
   * @param managedVersioning whether the namespace manages versioning
   * @param tableId table identifier for namespace-managed versioning
   * @param transactionProperties optional transaction properties to attach; used by the streaming
   *     sink to persist {@code streaming.queryId} and {@code streaming.epochId} for idempotent
   *     replay. May be {@code null} or empty to commit without transaction properties.
   */
  public static void doCommit(
      Dataset dataset,
      List<FragmentMetadata> fragments,
      Schema arrowSchema,
      boolean isOverwrite,
      LanceSparkWriteOptions writeOptions,
      String namespaceImpl,
      Map<String, String> namespaceProperties,
      boolean managedVersioning,
      List<String> tableId,
      Map<String, String> transactionProperties) {
    Operation operation;
    if (isOverwrite) {
      operation = Overwrite.builder().fragments(fragments).schema(arrowSchema).build();
    } else {
      operation = Append.builder().fragments(fragments).build();
    }

    CommitBuilder commitBuilder =
        new CommitBuilder(dataset).writeParams(writeOptions.getStorageOptions());

    // When enableStableRowIds is null (user didn't pass the option), lance-core auto-inherits
    // the flag from the existing manifest. Appending to a table with stable row IDs works
    // without re-specifying the option.
    Boolean enableStableRowIds = writeOptions.getEnableStableRowIds();
    if (enableStableRowIds != null) {
      commitBuilder.useStableRowIds(enableStableRowIds);
    }
    if (managedVersioning) {
      LanceNamespace namespace =
          LanceRuntime.getOrCreateNamespace(namespaceImpl, namespaceProperties);
      commitBuilder.namespaceClient(namespace).tableId(tableId);
    }

    Transaction.Builder txnBuilder =
        new Transaction.Builder().readVersion(dataset.version()).operation(operation);
    if (transactionProperties != null && !transactionProperties.isEmpty()) {
      txnBuilder.transactionProperties(transactionProperties);
    }

    try (Transaction txn = txnBuilder.build();
        Dataset committed = commitBuilder.execute(txn)) {
      // auto-close txn and committed dataset
    }
  }

  @Override
  public void abort(WriterCommitMessage[] messages) {
    // For staged tables, the dataset is managed by StagedCommit (via abortStagedChanges)
    // For non-staged tables, no resources to clean up (dataset opened fresh at commit time)
  }

  @Override
  public String toString() {
    return String.format("LanceBatchWrite(datasetUri=%s)", writeOptions.getDatasetUri());
  }

  public static class TaskCommit implements WriterCommitMessage {
    private final List<FragmentMetadata> fragments;

    public TaskCommit(List<FragmentMetadata> fragments) {
      this.fragments = fragments;
    }

    public List<FragmentMetadata> getFragments() {
      return fragments;
    }
  }
}
