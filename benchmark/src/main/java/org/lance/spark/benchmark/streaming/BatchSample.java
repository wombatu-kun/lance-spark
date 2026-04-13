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

/**
 * One row of a streaming-scalability CSV — captured per Spark micro-batch.
 *
 * <p>Field set is the union of what the four experiments need; experiment-irrelevant fields are
 * left as {@code 0} / {@code -1} so the same CSV schema serves all four runs and downstream
 * plotting code does not have to special-case them.
 */
public final class BatchSample {
  public String experiment;
  public String runLabel;
  public long batchId;
  public long batchTimestampMs;
  public long inputRows;
  public long batchDurationMs;
  public long fragmentCount;
  public long lanceVersion;
  public long manifestBytes;
  public boolean optimizedThisBatch;

  @Override
  public String toString() {
    return String.join(
        ",",
        csvEscape(experiment),
        csvEscape(runLabel),
        Long.toString(batchId),
        Long.toString(batchTimestampMs),
        Long.toString(inputRows),
        Long.toString(batchDurationMs),
        Long.toString(fragmentCount),
        Long.toString(lanceVersion),
        Long.toString(manifestBytes),
        Boolean.toString(optimizedThisBatch));
  }

  public static String csvHeader() {
    return "experiment,run_label,batch_id,batch_ts_ms,input_rows,batch_duration_ms,"
        + "fragment_count,lance_version,manifest_bytes,optimized_this_batch";
  }

  private static String csvEscape(String s) {
    if (s == null) {
      return "";
    }
    if (s.indexOf(',') < 0 && s.indexOf('"') < 0 && s.indexOf('\n') < 0) {
      return s;
    }
    return "\"" + s.replace("\"", "\"\"") + "\"";
  }
}
