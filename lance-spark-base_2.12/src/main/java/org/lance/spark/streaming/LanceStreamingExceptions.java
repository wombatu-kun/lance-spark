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

import org.lance.spark.LanceSparkWriteOptions;

/**
 * Centralized factory for clear, actionable error messages thrown by the Lance streaming sink.
 *
 * <p>Keeping all streaming-related error messages in one place makes them consistent, greppable,
 * and easy to update across the codebase. Every method returns an exception (rather than throwing)
 * so that call sites can write {@code throw LanceStreamingExceptions.xxx(...)} — preserving the
 * call site's stack frame and letting tests assert on the message without an intermediate wrap.
 */
public final class LanceStreamingExceptions {

  private LanceStreamingExceptions() {}

  /**
   * Thrown when a streaming write attempts to use a {@code StagedTable} commit path. Staged commits
   * are the CTAS / REPLACE TABLE flow — they eagerly materialize a commit object on the driver that
   * must be committed exactly once via {@code Table.commitStagedChanges()}. Streaming writes use a
   * completely different commit cadence (one commit per epoch) and are not compatible with the
   * staged path.
   */
  public static IllegalStateException stagedTableNotSupported() {
    return new IllegalStateException(
        "Lance streaming sink cannot be used with a staged table (CTAS / REPLACE TABLE). "
            + "Create the target table first with a batch write or DDL, then start the streaming "
            + "query against the existing table.");
  }

  /**
   * Thrown when {@link LanceSparkWriteOptions#getStreamingQueryId()} is missing (null or empty).
   * The {@code streamingQueryId} is the disambiguating key for the per-query epoch watermark and
   * for the bounded-lookback recovery scan, so it is required and must be globally unique per
   * logical streaming query.
   */
  public static IllegalArgumentException streamingQueryIdRequired() {
    return new IllegalArgumentException(
        "Lance streaming sink requires the '"
            + LanceSparkWriteOptions.CONFIG_STREAMING_QUERY_ID
            + "' option to be set to a globally-unique identifier for this streaming query. "
            + "Example: .option(\""
            + LanceSparkWriteOptions.CONFIG_STREAMING_QUERY_ID
            + "\", \"my-sink-query-v1\"). This value is used as the idempotency key for "
            + "exactly-once commits and MUST be unique across all streaming queries writing "
            + "to the same Lance table.");
  }

  /**
   * Thrown when the streaming sink is pointed at a non-existent Lance table. Streaming writes don't
   * auto-create tables in v1 — the user must create the table (schema + empty data) via a batch
   * write or DDL before starting the streaming query.
   */
  public static IllegalStateException targetTableDoesNotExist(String datasetUri) {
    return new IllegalStateException(
        "Lance streaming sink target table does not exist: "
            + datasetUri
            + ". Streaming writes do not auto-create tables. Create the target table first with "
            + "a batch write (e.g. spark.createDataFrame(...).write.format(\"lance\").save(path)) "
            + "or with CREATE TABLE, then start the streaming query.");
  }

  /**
   * Thrown when the recovery scan in the streaming commit protocol walks past its bounded lookback
   * window without finding a matching transaction for the replayed epoch. This indicates that more
   * than {@code maxRecoveryLookback} unrelated commits landed between the original crash and the
   * retry, and the at-least-once fallback is now in effect — the caller should log a warning and
   * fall through to a fresh append.
   *
   * <p>This is not a hard error — the streaming sink SHOULD catch it and degrade to at-least-once
   * semantics. It exists purely for test assertions and diagnostic logging. Production code should
   * never propagate it.
   */
  public static IllegalStateException recoveryLookbackExhausted(
      String queryId, long epochId, int lookback) {
    return new IllegalStateException(
        "Lance streaming recovery lookback exhausted for query '"
            + queryId
            + "' epoch "
            + epochId
            + " (searched "
            + lookback
            + " prior versions). Falling back to at-least-once semantics. "
            + "To reduce the chance of this happening, raise '"
            + LanceSparkWriteOptions.CONFIG_MAX_RECOVERY_LOOKBACK
            + "' for this query.");
  }
}
