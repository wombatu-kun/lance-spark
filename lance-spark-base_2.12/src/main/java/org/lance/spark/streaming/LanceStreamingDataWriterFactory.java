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

import org.lance.spark.write.LanceDataWriter;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;

/**
 * Thin adapter that lets Spark's streaming engine reuse the batch {@link
 * LanceDataWriter.WriterFactory} for per-task data writing.
 *
 * <p>The per-task writer is identical for batch and streaming: workers receive rows, accumulate
 * them into Lance fragments, and emit a {@link org.lance.spark.write.LanceBatchWrite.TaskCommit}
 * message. The {@code epochId} parameter is deliberately ignored — fragments are epoch-agnostic and
 * exactly-once idempotency is enforced at the driver-side commit step by {@link
 * StreamingCommitProtocol}, not at task time.
 */
public class LanceStreamingDataWriterFactory implements StreamingDataWriterFactory {

  private static final long serialVersionUID = 1L;

  private final LanceDataWriter.WriterFactory delegate;

  public LanceStreamingDataWriterFactory(LanceDataWriter.WriterFactory delegate) {
    this.delegate = delegate;
  }

  @Override
  public DataWriter<InternalRow> createWriter(int partitionId, long taskId, long epochId) {
    // epochId is intentionally discarded — exactly-once is enforced by
    // StreamingCommitProtocol at the driver, not by per-task writers.
    return delegate.createWriter(partitionId, taskId);
  }
}
