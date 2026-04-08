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

import org.lance.CommitBuilder;
import org.lance.Dataset;
import org.lance.FragmentMetadata;
import org.lance.Transaction;
import org.lance.namespace.LanceNamespace;
import org.lance.operation.UpdateConfig;
import org.lance.spark.LanceRuntime;
import org.lance.spark.LanceSparkWriteOptions;
import org.lance.spark.write.LanceBatchWrite;

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The two-transaction commit protocol that gives the Lance streaming sink exactly-once semantics.
 *
 * <p>Spark's streaming engine calls {@link #commit(long, List, boolean)} once per micro-batch
 * epoch, and guarantees that on restart-after-failure it will replay the same epochId for any
 * uncommitted epoch. The sink must be idempotent on epochId — committing the same epoch twice must
 * not produce duplicates.
 *
 * <p>Lance's {@link Transaction.Builder} only accepts one {@code operation()}, which means we
 * cannot atomically combine an {@link org.lance.operation.Append} (the data write) with an {@link
 * UpdateConfig} (the epoch-watermark bump). This protocol therefore uses two separate Lance
 * transactions and recovers from crashes between them via a bounded-lookback scan of historical
 * transaction properties.
 *
 * <h2>Protocol</h2>
 *
 * On every call to {@link #commit(long, List, boolean)}:
 *
 * <ol>
 *   <li><b>Open + read watermark.</b> Open the dataset at its latest version; read {@code lastEpoch
 *       = dataset.getConfig().get("spark.lance.streaming.lastEpoch." + queryId)}, defaulting to
 *       {@code -1} if absent.
 *   <li><b>Fast-path skip.</b> If {@code epochId <= lastEpoch}, this is a replay of an already-
 *       committed epoch — return immediately without touching the dataset.
 *   <li><b>Recovery scan.</b> Walk backwards through the last {@code maxRecoveryLookback}
 *       historical versions, reading each one's transaction properties via {@link
 *       Dataset#readTransaction()}. If a prior transaction carries {@code streaming.queryId ==
 *       queryId} and {@code streaming.epochId == epochId}, the previous attempt's Txn1 (Append)
 *       succeeded but its Txn2 (UpdateConfig) did not — skip Txn1 and run Txn2 only to self-heal.
 *   <li><b>Txn1 — Append or Overwrite.</b> Build an Append (or Overwrite for Complete output mode)
 *       operation with {@code transactionProperties = {"streaming.queryId": queryId,
 *       "streaming.epochId": epochId}} so that a future recovery scan can identify this
 *       transaction. Commit via {@link CommitBuilder#execute(Transaction)}.
 *   <li><b>Txn2 — UpdateConfig.</b> Issue a second Lance transaction whose operation is an {@link
 *       UpdateConfig} that sets {@code spark.lance.streaming.lastEpoch.<queryId>} to {@code
 *       epochId}. If this fails (crash, transient error), the next call to {@link #commit(long,
 *       List, boolean)} will re-enter step 1, see {@code lastEpoch < epochId}, and the recovery
 *       scan in step 3 will find the Txn1 transaction and re-run Txn2 only.
 * </ol>
 *
 * <h2>Bounded at-least-once fallback</h2>
 *
 * <p>If more than {@code maxRecoveryLookback} unrelated commits land between a crash (between Txn1
 * and Txn2) and the subsequent retry, the recovery scan misses the prior Txn1 and Txn1 is re-run —
 * producing duplicates. This is the documented at-least-once fallback. The default lookback of 100
 * is sufficient for typical streaming workloads; users with very high-churn tables can raise it via
 * {@link LanceSparkWriteOptions#CONFIG_MAX_RECOVERY_LOOKBACK}.
 */
public class StreamingCommitProtocol {

  /** Prefix of the per-query epoch-watermark key in the dataset config map. */
  public static final String WATERMARK_KEY_PREFIX = "spark.lance.streaming.lastEpoch.";

  /** Transaction property key for the streaming query identifier. */
  public static final String TXN_QUERY_ID = "streaming.queryId";

  /** Transaction property key for the streaming epoch identifier. */
  public static final String TXN_EPOCH_ID = "streaming.epochId";

  /** Sentinel value stored as the watermark when no epoch has ever been committed. */
  public static final long NO_EPOCH = -1L;

  private static final Logger LOG = LoggerFactory.getLogger(StreamingCommitProtocol.class);

  /** Opens the Lance dataset at its latest version. Caller must close the returned Dataset. */
  @FunctionalInterface
  public interface DatasetOpener {
    Dataset open() throws Exception;
  }

  /** Opens the Lance dataset at a specific historical version. Caller must close the returned. */
  @FunctionalInterface
  public interface VersionedDatasetOpener {
    Dataset openAtVersion(long version) throws Exception;
  }

  /** The outcome of analyzing whether a commit attempt is fresh, a replay, or a recovery. */
  public enum CommitAction {
    /** Epoch was already committed past Txn2 — skip entirely (fast-path replay). */
    SKIP_REPLAY,
    /** Prior attempt's Txn1 succeeded but Txn2 did not — run Txn2 only to self-heal. */
    RECOVERY_TXN2_ONLY,
    /** Fresh commit — run Txn1 (Append/Overwrite) then Txn2 (UpdateConfig). */
    FULL_COMMIT
  }

  private final String queryId;
  private final int maxRecoveryLookback;
  private final LanceSparkWriteOptions writeOptions;
  private final String namespaceImpl;
  private final Map<String, String> namespaceProperties;
  private final boolean managedVersioning;
  private final List<String> tableId;
  private final StructType schema;
  private final DatasetOpener opener;
  private final VersionedDatasetOpener versionedOpener;

  public StreamingCommitProtocol(
      String queryId,
      int maxRecoveryLookback,
      LanceSparkWriteOptions writeOptions,
      String namespaceImpl,
      Map<String, String> namespaceProperties,
      boolean managedVersioning,
      List<String> tableId,
      StructType schema,
      DatasetOpener opener,
      VersionedDatasetOpener versionedOpener) {
    if (queryId == null || queryId.isEmpty()) {
      throw LanceStreamingExceptions.streamingQueryIdRequired();
    }
    this.queryId = queryId;
    this.maxRecoveryLookback = maxRecoveryLookback;
    this.writeOptions = writeOptions;
    this.namespaceImpl = namespaceImpl;
    this.namespaceProperties = namespaceProperties;
    this.managedVersioning = managedVersioning;
    this.tableId = tableId;
    this.schema = schema;
    this.opener = Objects.requireNonNull(opener, "opener must not be null");
    this.versionedOpener =
        Objects.requireNonNull(versionedOpener, "versionedOpener must not be null");
  }

  /**
   * @return the key under which this query's epoch watermark is stored in the dataset config.
   */
  public String watermarkKey() {
    return WATERMARK_KEY_PREFIX + queryId;
  }

  public String getQueryId() {
    return queryId;
  }

  public int getMaxRecoveryLookback() {
    return maxRecoveryLookback;
  }

  // ========================================================================
  // Pure helpers — extracted for direct unit testing without any Dataset/IO.
  // ========================================================================

  /**
   * Parses a stored watermark string into a long. Returns {@link #NO_EPOCH} if the input is {@code
   * null} or cannot be parsed.
   */
  public static long parseEpochWatermark(String raw) {
    if (raw == null) {
      return NO_EPOCH;
    }
    try {
      return Long.parseLong(raw);
    } catch (NumberFormatException e) {
      LOG.warn("Unparseable epoch watermark '{}', treating as NO_EPOCH", raw);
      return NO_EPOCH;
    }
  }

  /**
   * Decides whether a commit attempt is a replay, a recovery, or a fresh commit, given the current
   * watermark and an already-performed recovery-scan result.
   *
   * @param epochId the epoch we are being asked to commit
   * @param lastEpoch the value of the watermark in the dataset config (or {@link #NO_EPOCH})
   * @param priorTxn1Found whether the recovery scan already found a matching prior Txn1
   */
  public static CommitAction decideAction(long epochId, long lastEpoch, boolean priorTxn1Found) {
    if (epochId <= lastEpoch) {
      return CommitAction.SKIP_REPLAY;
    }
    if (priorTxn1Found) {
      return CommitAction.RECOVERY_TXN2_ONLY;
    }
    return CommitAction.FULL_COMMIT;
  }

  /**
   * Returns {@code true} iff the supplied transaction-properties map is the Txn1 output of a
   * streaming commit matching the given (queryId, epochId) pair.
   */
  public static boolean matchesTransactionProperties(
      Map<String, String> props, String queryId, long epochId) {
    if (props == null) {
      return false;
    }
    return queryId.equals(props.get(TXN_QUERY_ID))
        && String.valueOf(epochId).equals(props.get(TXN_EPOCH_ID));
  }

  /**
   * Computes the lowest (oldest) version that the recovery scan should visit given the current
   * dataset version. Ensures the scan never goes below version 1 and never walks more than {@code
   * maxRecoveryLookback} versions.
   */
  public static long recoveryScanLowerBound(long currentVersion, int maxRecoveryLookback) {
    return Math.max(1L, currentVersion - maxRecoveryLookback + 1L);
  }

  // ========================================================================
  // Orchestrator — uses real Lance primitives, covered by integration tests.
  // ========================================================================

  /**
   * Executes the 5-step commit protocol for the given epoch. Throws a {@link RuntimeException} only
   * on unrecoverable errors (e.g., the dataset cannot be opened at all). Replays and recoveries are
   * handled silently.
   */
  public void commit(long epochId, List<FragmentMetadata> fragments, boolean isOverwrite) {
    // Step 1 + 2 + 3: open, read watermark, scan for prior commit, decide action.
    CommitAction action;
    try (Dataset current = openOrThrow()) {
      long lastEpoch = parseEpochWatermark(current.getConfig().get(watermarkKey()));
      boolean priorTxn1Found = false;
      if (epochId > lastEpoch) {
        priorTxn1Found = scanForPriorTxn1(current, epochId);
      }
      action = decideAction(epochId, lastEpoch, priorTxn1Found);

      // Step 4: Txn1 (only if it's a fresh full commit).
      if (action == CommitAction.FULL_COMMIT) {
        runTxn1(current, fragments, isOverwrite, epochId);
      }
    } catch (Exception e) {
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      }
      throw new RuntimeException(
          "Lance streaming Txn1 failed for queryId=" + queryId + " epochId=" + epochId, e);
    }

    switch (action) {
      case SKIP_REPLAY:
        LOG.info(
            "Streaming commit replay detected: queryId={} epochId={} (already committed)",
            queryId,
            epochId);
        return;
      case RECOVERY_TXN2_ONLY:
        LOG.info(
            "Streaming commit recovery: queryId={} epochId={} — prior Txn1 found, running Txn2",
            queryId,
            epochId);
        // fall through to Txn2
        break;
      case FULL_COMMIT:
        LOG.info(
            "Streaming commit: queryId={} epochId={} isOverwrite={} — Txn1 committed {} fragments",
            queryId,
            epochId,
            isOverwrite,
            fragments.size());
        // fall through to Txn2
        break;
      default:
        throw new IllegalStateException("Unknown commit action: " + action);
    }

    // Step 5: Txn2 — persist the epoch watermark in the dataset config map.
    try {
      runTxn2(epochId);
    } catch (Exception e) {
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      }
      throw new RuntimeException(
          "Lance streaming Txn2 (UpdateConfig) failed for queryId="
              + queryId
              + " epochId="
              + epochId,
          e);
    }
  }

  /**
   * Walks backward from the current dataset's version up to {@link #maxRecoveryLookback} versions,
   * reading each one's {@link Transaction#transactionProperties()} and checking for a match against
   * this query's {@code (queryId, epochId)} pair.
   */
  private boolean scanForPriorTxn1(Dataset current, long epochId) {
    long currentVersion = current.version();
    long lowerBound = recoveryScanLowerBound(currentVersion, maxRecoveryLookback);

    for (long v = currentVersion; v >= lowerBound; v--) {
      try (Dataset historical = versionedOpener.openAtVersion(v)) {
        Optional<Transaction> txnOpt = historical.readTransaction();
        if (!txnOpt.isPresent()) {
          continue;
        }
        try (Transaction txn = txnOpt.get()) {
          Optional<Map<String, String>> propsOpt = txn.transactionProperties();
          if (!propsOpt.isPresent()) {
            continue;
          }
          if (matchesTransactionProperties(propsOpt.get(), queryId, epochId)) {
            LOG.debug(
                "Recovery scan hit at version {} for queryId={} epochId={}", v, queryId, epochId);
            return true;
          }
        }
      } catch (Exception e) {
        // Non-fatal — log and keep walking.
        LOG.warn(
            "Failed to read transaction at version {} during recovery scan for queryId={}: {}",
            v,
            queryId,
            e.getMessage());
      }
    }

    // If the scan covered the full lookback window without a hit AND the dataset has more
    // history than we checked, we've entered the bounded at-least-once fallback region.
    if (currentVersion >= maxRecoveryLookback) {
      LOG.warn(
          "{}",
          LanceStreamingExceptions.recoveryLookbackExhausted(queryId, epochId, maxRecoveryLookback)
              .getMessage());
    }
    return false;
  }

  /** Runs Txn1: delegates to the shared {@link LanceBatchWrite#doCommit} helper. */
  private void runTxn1(
      Dataset current, List<FragmentMetadata> fragments, boolean isOverwrite, long epochId) {
    Schema arrowSchema = LanceArrowUtils.toArrowSchema(schema, "UTC", true);
    Map<String, String> txnProps = new HashMap<>(2);
    txnProps.put(TXN_QUERY_ID, queryId);
    txnProps.put(TXN_EPOCH_ID, String.valueOf(epochId));

    LanceBatchWrite.doCommit(
        current,
        fragments,
        arrowSchema,
        isOverwrite,
        writeOptions,
        namespaceImpl,
        namespaceProperties,
        managedVersioning,
        tableId,
        txnProps);
  }

  /**
   * Runs Txn2: a fresh {@link UpdateConfig} transaction that persists the epoch watermark in the
   * dataset config map. Uses {@code maxRetries(10)} on {@link CommitBuilder} so that concurrent
   * commits from other writers are handled transparently by lance-core's conflict resolution.
   */
  private void runTxn2(long epochId) throws Exception {
    try (Dataset latest = opener.open()) {
      Map<String, String> configUpdates = new HashMap<>(1);
      configUpdates.put(watermarkKey(), String.valueOf(epochId));

      UpdateConfig updateConfig = UpdateConfig.builder().upsertValues(configUpdates).build();

      CommitBuilder commitBuilder =
          new CommitBuilder(latest).writeParams(writeOptions.getStorageOptions()).maxRetries(10);
      if (managedVersioning) {
        LanceNamespace namespace =
            LanceRuntime.getOrCreateNamespace(namespaceImpl, namespaceProperties);
        commitBuilder.namespaceClient(namespace).tableId(tableId);
      }

      try (Transaction txn =
              new Transaction.Builder()
                  .readVersion(latest.version())
                  .operation(updateConfig)
                  .build();
          Dataset committed = commitBuilder.execute(txn)) {
        // auto-close txn and committed dataset
      }
    }
  }

  private Dataset openOrThrow() {
    try {
      return opener.open();
    } catch (Exception e) {
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      }
      throw new RuntimeException(
          "Failed to open Lance dataset for streaming commit (queryId=" + queryId + ")", e);
    }
  }
}
