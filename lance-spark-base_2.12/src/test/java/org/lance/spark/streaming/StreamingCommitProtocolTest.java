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

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pure unit tests for {@link StreamingCommitProtocol}'s static helper methods and constructor
 * validation.
 *
 * <p>These tests do NOT construct a Spark session or a Lance dataset — they exercise only the
 * deterministic, side-effect-free logic: watermark parsing, commit-action decision-making,
 * transaction-property matching, and recovery-scan lower-bound arithmetic. This test runs in
 * milliseconds on any JVM and gives the tightest possible feedback loop while iterating on the
 * commit protocol.
 *
 * <p>End-to-end behavior (real Lance datasets, recovery from injected failures, kill-and-restart
 * across Spark sessions) is covered by the higher-level {@code BaseStreamingSink*Test} classes that
 * extend {@code BaseSparkConnectorWriteTest}.
 */
class StreamingCommitProtocolTest {

  // =======================================================================
  // parseEpochWatermark — reads the per-query watermark out of the config.
  // =======================================================================

  @Nested
  class ParseEpochWatermark {

    @Test
    void nullInputReturnsNoEpoch() {
      assertEquals(
          StreamingCommitProtocol.NO_EPOCH, StreamingCommitProtocol.parseEpochWatermark(null));
    }

    @Test
    void validDigitsParseCorrectly() {
      assertEquals(0L, StreamingCommitProtocol.parseEpochWatermark("0"));
      assertEquals(42L, StreamingCommitProtocol.parseEpochWatermark("42"));
      assertEquals(9999999999L, StreamingCommitProtocol.parseEpochWatermark("9999999999"));
    }

    @Test
    void negativeValuesAreAccepted() {
      // NO_EPOCH itself is -1, so a legitimate value can be -1. Other negatives are never
      // produced by the sink but we still parse them as literal numbers rather than reject.
      assertEquals(-1L, StreamingCommitProtocol.parseEpochWatermark("-1"));
      assertEquals(-42L, StreamingCommitProtocol.parseEpochWatermark("-42"));
    }

    @Test
    void malformedInputDegradesToNoEpoch() {
      // We want streaming writes to survive a corrupted or human-edited watermark value by
      // falling back to NO_EPOCH and letting the recovery scan take over, rather than crashing
      // the entire streaming query.
      assertEquals(
          StreamingCommitProtocol.NO_EPOCH, StreamingCommitProtocol.parseEpochWatermark(""));
      assertEquals(
          StreamingCommitProtocol.NO_EPOCH, StreamingCommitProtocol.parseEpochWatermark("abc"));
      assertEquals(
          StreamingCommitProtocol.NO_EPOCH, StreamingCommitProtocol.parseEpochWatermark("42abc"));
      assertEquals(
          StreamingCommitProtocol.NO_EPOCH, StreamingCommitProtocol.parseEpochWatermark("3.14"));
    }
  }

  // =======================================================================
  // decideAction — the 3-way decision that drives the commit protocol.
  // =======================================================================

  @Nested
  class DecideAction {

    @Test
    void freshCommitWhenEpochIsNewAndNoPriorTxn1() {
      assertEquals(
          StreamingCommitProtocol.CommitAction.FULL_COMMIT,
          StreamingCommitProtocol.decideAction(
              /* epochId= */ 5, /* lastEpoch= */ 4, /* priorTxn1Found= */ false));
    }

    @Test
    void firstEverCommitFromNoEpochSentinel() {
      assertEquals(
          StreamingCommitProtocol.CommitAction.FULL_COMMIT,
          StreamingCommitProtocol.decideAction(0L, StreamingCommitProtocol.NO_EPOCH, false));
    }

    @Test
    void replayOnExactMatchIsSkipped() {
      // The watermark records the last *committed* epoch, so re-committing the same epoch is a
      // replay. Spark may re-emit commit(N) if it restarts from a checkpoint that already had
      // epoch N marked complete on the sink side but not yet on the Spark side.
      assertEquals(
          StreamingCommitProtocol.CommitAction.SKIP_REPLAY,
          StreamingCommitProtocol.decideAction(/* epochId= */ 5, /* lastEpoch= */ 5, false));
    }

    @Test
    void replayOnStaleEpochIsSkipped() {
      assertEquals(
          StreamingCommitProtocol.CommitAction.SKIP_REPLAY,
          StreamingCommitProtocol.decideAction(/* epochId= */ 3, /* lastEpoch= */ 5, false));
    }

    @Test
    void recoveryWhenPriorTxn1FoundAndEpochIsNew() {
      // Txn1 succeeded on a previous attempt but Txn2 did not run; the recovery scan found the
      // matching transaction, so we must skip Txn1 and run Txn2 only to bump the watermark.
      assertEquals(
          StreamingCommitProtocol.CommitAction.RECOVERY_TXN2_ONLY,
          StreamingCommitProtocol.decideAction(
              /* epochId= */ 5, /* lastEpoch= */ 4, /* priorTxn1Found= */ true));
    }

    @Test
    void replayStillWinsEvenIfRecoveryScanWouldHaveMatched() {
      // Defensive: if the watermark already says the epoch is committed, we don't care what the
      // recovery scan found — the fast-path replay skip is the authoritative answer.
      assertEquals(
          StreamingCommitProtocol.CommitAction.SKIP_REPLAY,
          StreamingCommitProtocol.decideAction(
              /* epochId= */ 5, /* lastEpoch= */ 5, /* priorTxn1Found= */ true));
    }

    @ParameterizedTest(name = "epoch={0} lastEpoch={1} priorFound={2} expected={3}")
    @CsvSource({
      // epoch, lastEpoch, priorFound, expected
      "  10,  -1,  false, FULL_COMMIT",
      "  10,  -1,  true,  RECOVERY_TXN2_ONLY",
      "  10,   9,  false, FULL_COMMIT",
      "  10,   9,  true,  RECOVERY_TXN2_ONLY",
      "  10,  10,  false, SKIP_REPLAY",
      "  10,  10,  true,  SKIP_REPLAY",
      "   9,  10,  false, SKIP_REPLAY",
      "   0,  -1,  false, FULL_COMMIT"
    })
    void exhaustiveDecisionMatrix(
        long epochId, long lastEpoch, boolean priorFound, String expected) {
      assertEquals(
          StreamingCommitProtocol.CommitAction.valueOf(expected),
          StreamingCommitProtocol.decideAction(epochId, lastEpoch, priorFound));
    }
  }

  // =======================================================================
  // matchesTransactionProperties — the key-value check used by recovery.
  // =======================================================================

  @Nested
  class MatchesTransactionProperties {

    @Test
    void matchesWhenBothKeysPresentAndEqual() {
      Map<String, String> props = new HashMap<>();
      props.put(StreamingCommitProtocol.TXN_QUERY_ID, "my-query");
      props.put(StreamingCommitProtocol.TXN_EPOCH_ID, "42");
      assertTrue(StreamingCommitProtocol.matchesTransactionProperties(props, "my-query", 42L));
    }

    @Test
    void doesNotMatchOnWrongQueryId() {
      Map<String, String> props = new HashMap<>();
      props.put(StreamingCommitProtocol.TXN_QUERY_ID, "other-query");
      props.put(StreamingCommitProtocol.TXN_EPOCH_ID, "42");
      assertFalse(StreamingCommitProtocol.matchesTransactionProperties(props, "my-query", 42L));
    }

    @Test
    void doesNotMatchOnWrongEpoch() {
      Map<String, String> props = new HashMap<>();
      props.put(StreamingCommitProtocol.TXN_QUERY_ID, "my-query");
      props.put(StreamingCommitProtocol.TXN_EPOCH_ID, "41");
      assertFalse(StreamingCommitProtocol.matchesTransactionProperties(props, "my-query", 42L));
    }

    @Test
    void doesNotMatchOnMissingKeys() {
      // Props from unrelated Lance operations (batch writes, DDL) that don't carry streaming
      // metadata must never be mistaken for prior streaming commits.
      Map<String, String> props = new HashMap<>();
      props.put("some-other-key", "some-value");
      assertFalse(StreamingCommitProtocol.matchesTransactionProperties(props, "my-query", 42L));
    }

    @Test
    void doesNotMatchOnNullProps() {
      assertFalse(StreamingCommitProtocol.matchesTransactionProperties(null, "my-query", 42L));
    }

    @Test
    void doesNotMatchOnEmptyProps() {
      assertFalse(
          StreamingCommitProtocol.matchesTransactionProperties(new HashMap<>(), "my-query", 42L));
    }

    @Test
    void differentQueriesWithSameEpochDoNotCrossMatch() {
      // Two streaming queries happening to write to the same table must not see each other's
      // transactions as recoveries.
      Map<String, String> propsQueryA = new HashMap<>();
      propsQueryA.put(StreamingCommitProtocol.TXN_QUERY_ID, "query-a");
      propsQueryA.put(StreamingCommitProtocol.TXN_EPOCH_ID, "7");

      assertTrue(StreamingCommitProtocol.matchesTransactionProperties(propsQueryA, "query-a", 7L));
      assertFalse(StreamingCommitProtocol.matchesTransactionProperties(propsQueryA, "query-b", 7L));
    }
  }

  // =======================================================================
  // recoveryScanLowerBound — the bounded-lookback math.
  // =======================================================================

  @Nested
  class RecoveryScanLowerBound {

    @Test
    void lookbackBelowVersionGivesFullWindow() {
      assertEquals(
          91L /* 100 - 10 + 1 */,
          StreamingCommitProtocol.recoveryScanLowerBound(
              /* currentVersion= */ 100L, /* maxRecoveryLookback= */ 10));
    }

    @Test
    void lookbackMatchingVersionExactlyGivesOne() {
      // currentVersion=10, lookback=10: scan from 10 down to 1 (10 versions total).
      assertEquals(1L, StreamingCommitProtocol.recoveryScanLowerBound(10L, 10));
    }

    @Test
    void lookbackLargerThanVersionClampsToOne() {
      // The dataset has only 5 versions but the user configured a lookback of 1000 — we must
      // not descend below version 1.
      assertEquals(1L, StreamingCommitProtocol.recoveryScanLowerBound(5L, 1000));
    }

    @Test
    void singleVersionWithSingleLookbackGivesOne() {
      assertEquals(1L, StreamingCommitProtocol.recoveryScanLowerBound(1L, 1));
    }

    @Test
    void largeValuesDoNotOverflow() {
      // Sanity check with realistic 'tall' version numbers. 1M versions with 100-wide lookback
      // means scanning versions [999_901 .. 1_000_000] inclusive — 100 versions total.
      assertEquals(999_901L, StreamingCommitProtocol.recoveryScanLowerBound(1_000_000L, 100));
    }

    @Test
    void scanWindowSizeEqualsLookbackWhenVersionIsLarge() {
      // Number of versions visited = currentVersion - lowerBound + 1 should equal lookback.
      long currentVersion = 500L;
      int lookback = 50;
      long lowerBound = StreamingCommitProtocol.recoveryScanLowerBound(currentVersion, lookback);
      assertEquals(lookback, currentVersion - lowerBound + 1);
    }
  }

  // =======================================================================
  // Constructor validation — queryId is required and cannot be empty.
  // =======================================================================

  @Nested
  class ConstructorValidation {

    @Test
    void nullQueryIdIsRejected() {
      IllegalArgumentException ex =
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  new StreamingCommitProtocol(
                      /* queryId= */ null,
                      100,
                      null,
                      null,
                      null,
                      false,
                      null,
                      null,
                      () -> {
                        throw new AssertionError("opener must not be invoked from constructor");
                      },
                      v -> {
                        throw new AssertionError(
                            "versionedOpener must not be invoked from constructor");
                      }));
      assertTrue(
          ex.getMessage().contains(LanceSparkWriteOptionsConstants.CONFIG_STREAMING_QUERY_ID),
          "Error message should reference the required option name, got: " + ex.getMessage());
    }

    @Test
    void emptyQueryIdIsRejected() {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              new StreamingCommitProtocol(
                  /* queryId= */ "",
                  100,
                  null,
                  null,
                  null,
                  false,
                  null,
                  null,
                  () -> {
                    throw new AssertionError("opener must not be invoked from constructor");
                  },
                  v -> {
                    throw new AssertionError(
                        "versionedOpener must not be invoked from constructor");
                  }));
    }
  }

  /**
   * Tiny helper to keep the test class decoupled from {@code LanceSparkWriteOptions}'s concrete
   * type while still asserting on the constant value that appears in error messages. Using a string
   * literal directly would silently break if the constant were renamed; this indirection catches
   * that.
   */
  private static final class LanceSparkWriteOptionsConstants {
    static final String CONFIG_STREAMING_QUERY_ID =
        org.lance.spark.LanceSparkWriteOptions.CONFIG_STREAMING_QUERY_ID;
  }
}
