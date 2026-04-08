"""
Integration tests for the Lance Structured Streaming sink.

These tests run inside the Docker container against a real Spark environment
and a real Lance backend. They exercise the full streaming write path end to
end: Spark streaming engine → LanceStreamingWrite → two-transaction commit →
persistent epoch watermark.

The ``spark`` fixture (defined in conftest.py) is parameterized over storage
backends (local filesystem, Azurite, MinIO) so every test runs on each.

Key scenarios covered:
  * Basic rate source → Lance sink end-to-end.
  * Crash-and-restart with the same checkpoint produces no duplicates.
  * Two concurrent streaming queries with distinct streamingQueryIds each
    commit successfully without watermark collisions.
  * Manual epoch-watermark bump causes the sink to skip replayed epochs.

The tests intentionally use short trigger intervals and small batch sizes so
they run in under a minute per backend. If you add a slow test here, gate it
behind ``@pytest.mark.slow``.
"""

import json
import os
import shutil
import tempfile
import time

import pytest
from pyspark.sql.streaming import StreamingQueryException


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _simple_schema(spark):
    """A minimal test schema: (id LONG, value STRING)."""
    from pyspark.sql.types import LongType, StringType, StructField, StructType

    return StructType(
        [
            StructField("id", LongType(), nullable=False),
            StructField("value", StringType(), nullable=False),
        ]
    )


def _write_json_batch(source_dir, file_name, rows):
    """Stage a single JSON-lines file into the file-streaming source directory."""
    os.makedirs(source_dir, exist_ok=True)
    path = os.path.join(source_dir, f"{file_name}.json")
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")


def _pre_create_empty_table(spark, path):
    """Streaming writes require the target table to exist — pre-create with an empty DataFrame."""
    empty = spark.createDataFrame([], schema=_simple_schema(spark))
    empty.write.format("lance").save(path)


def _count_rows(spark, path):
    return spark.read.format("lance").load(path).count()


def _lance_version(path):
    """Read the current Lance dataset version via the Python binding, if available."""
    try:
        import lance  # Lance Python binding

        ds = lance.dataset(path)
        return ds.version
    except ImportError:
        # Fall back to reading via Spark metadata column if lance-python is unavailable.
        return None


def _set_epoch_watermark(spark, path, query_id, epoch_id):
    """Manually set the Lance streaming epoch watermark for ``query_id`` to ``epoch_id``.

    This mirrors ``StreamingTestUtils.setEpochWatermark`` from the Java test suite.
    It uses the PySpark py4j JVM gateway to call the Lance Java API directly, so
    no lance Python binding is required in the test environment.

    The watermark is stored in the dataset's config map under the key
    ``spark.lance.streaming.lastEpoch.<query_id>``.  Setting it to a high value
    causes the Lance streaming sink to skip every subsequent epoch whose id is
    ≤ that value (the SKIP_REPLAY fast-path in StreamingCommitProtocol).
    """
    jvm = spark._jvm
    watermark_key = f"spark.lance.streaming.lastEpoch.{query_id}"

    LanceRuntime = jvm.org.lance.spark.LanceRuntime
    ds = (
        jvm.org.lance.Dataset
        .open()
        .allocator(LanceRuntime.allocator())
        .uri(path)
        .build()
    )
    config_map = jvm.java.util.HashMap()
    config_map.put(watermark_key, str(epoch_id))
    update_config = (
        jvm.org.lance.operation.UpdateConfig
        .builder()
        .upsertValues(config_map)
        .build()
    )
    # Transaction.Builder is a static nested class; py4j resolves it as
    # org.lance.Transaction$Builder via the JVM classloader.
    txn = (
        jvm.org.lance.Transaction.Builder()
        .readVersion(ds.version())
        .operation(update_config)
        .build()
    )
    committed = jvm.org.lance.CommitBuilder(ds).execute(txn)
    txn.close()
    committed.close()
    ds.close()


# ---------------------------------------------------------------------------
# Basic rate source → Lance sink
# ---------------------------------------------------------------------------


class TestStreamingSinkBasic:
    """End-to-end rate source → Lance sink smoke test."""

    def test_rate_source_to_lance_available_now(self, spark, tmp_path):
        """A rate source with Trigger.AvailableNow should drain into a Lance table."""
        path = str(tmp_path / "rate_sink.lance")
        checkpoint = str(tmp_path / "ckpt-rate")

        # Pre-create the target table with a matching schema (id LONG, value STRING).
        _pre_create_empty_table(spark, path)

        # The rate source emits (timestamp, value) — we rename/cast to match the target.
        stream_df = (
            spark.readStream.format("rate")
            .option("rowsPerSecond", 50)
            .option("numPartitions", 1)
            .load()
            .selectExpr("CAST(value AS LONG) AS id", "CAST(value AS STRING) AS value")
        )

        query = (
            stream_df.writeStream.format("lance")
            .option("path", path)
            .option("checkpointLocation", checkpoint)
            .option("streamingQueryId", "rate-sink-basic")
            .outputMode("append")
            .trigger(availableNow=True)
            .start()
        )
        try:
            query.awaitTermination(timeout=90)
        finally:
            query.stop()

        # AvailableNow will process whatever is available at the moment it runs.
        # We assert strictly > 0 rows rather than a specific count to avoid flakiness.
        assert _count_rows(spark, path) > 0


# ---------------------------------------------------------------------------
# Crash-and-restart idempotency
# ---------------------------------------------------------------------------


class TestStreamingSinkRestart:
    """Kill-and-restart with the same checkpoint must not duplicate data."""

    def test_restart_with_same_checkpoint_is_idempotent(self, spark, tmp_path):
        path = str(tmp_path / "restart_sink.lance")
        source_dir = str(tmp_path / "source")
        checkpoint = str(tmp_path / "ckpt-restart")

        _pre_create_empty_table(spark, path)

        # Stage a batch of rows.
        _write_json_batch(
            source_dir,
            "0001",
            [
                {"id": 1, "value": "a"},
                {"id": 2, "value": "b"},
                {"id": 3, "value": "c"},
            ],
        )

        stream1 = spark.readStream.schema(_simple_schema(spark)).json(source_dir)
        q1 = (
            stream1.writeStream.format("lance")
            .option("path", path)
            .option("checkpointLocation", checkpoint)
            .option("streamingQueryId", "restart-idempotent")
            .outputMode("append")
            .trigger(availableNow=True)
            .start()
        )
        try:
            q1.awaitTermination(timeout=60)
        finally:
            q1.stop()

        count_after_run1 = _count_rows(spark, path)
        assert count_after_run1 == 3

        # Re-run with the SAME checkpoint and the SAME source data.
        stream2 = spark.readStream.schema(_simple_schema(spark)).json(source_dir)
        q2 = (
            stream2.writeStream.format("lance")
            .option("path", path)
            .option("checkpointLocation", checkpoint)
            .option("streamingQueryId", "restart-idempotent")
            .outputMode("append")
            .trigger(availableNow=True)
            .start()
        )
        try:
            q2.awaitTermination(timeout=60)
        finally:
            q2.stop()

        # No duplicates: same row count, no new rows.
        count_after_run2 = _count_rows(spark, path)
        assert count_after_run2 == count_after_run1 == 3

    def test_restart_picks_up_new_data_only(self, spark, tmp_path):
        path = str(tmp_path / "restart_incr.lance")
        source_dir = str(tmp_path / "source")
        checkpoint = str(tmp_path / "ckpt-incr")

        _pre_create_empty_table(spark, path)

        _write_json_batch(source_dir, "0001", [{"id": 1, "value": "first"}])

        stream1 = spark.readStream.schema(_simple_schema(spark)).json(source_dir)
        q1 = (
            stream1.writeStream.format("lance")
            .option("path", path)
            .option("checkpointLocation", checkpoint)
            .option("streamingQueryId", "restart-incremental")
            .outputMode("append")
            .trigger(availableNow=True)
            .start()
        )
        try:
            q1.awaitTermination(timeout=60)
        finally:
            q1.stop()
        assert _count_rows(spark, path) == 1

        # Stage an additional file and restart with the same checkpoint.
        _write_json_batch(source_dir, "0002", [{"id": 2, "value": "second"}])

        stream2 = spark.readStream.schema(_simple_schema(spark)).json(source_dir)
        q2 = (
            stream2.writeStream.format("lance")
            .option("path", path)
            .option("checkpointLocation", checkpoint)
            .option("streamingQueryId", "restart-incremental")
            .outputMode("append")
            .trigger(availableNow=True)
            .start()
        )
        try:
            q2.awaitTermination(timeout=60)
        finally:
            q2.stop()

        # Total rows = 1 (from run 1) + 1 (new file in run 2), no duplicates.
        assert _count_rows(spark, path) == 2


# ---------------------------------------------------------------------------
# Concurrent queries with distinct streamingQueryIds
# ---------------------------------------------------------------------------


class TestStreamingSinkConcurrency:
    """Multiple streaming queries writing to the same Lance table in parallel."""

    def test_two_queries_distinct_query_ids_commit_independently(self, spark, tmp_path):
        path = str(tmp_path / "concurrent_sink.lance")
        source_a = str(tmp_path / "source-a")
        source_b = str(tmp_path / "source-b")
        ck_a = str(tmp_path / "ckpt-a")
        ck_b = str(tmp_path / "ckpt-b")

        _pre_create_empty_table(spark, path)

        _write_json_batch(source_a, "0001", [{"id": i, "value": f"a-{i}"} for i in range(5)])
        _write_json_batch(source_b, "0001", [{"id": i + 100, "value": f"b-{i}"} for i in range(3)])

        stream_a = spark.readStream.schema(_simple_schema(spark)).json(source_a)
        q_a = (
            stream_a.writeStream.format("lance")
            .option("path", path)
            .option("checkpointLocation", ck_a)
            .option("streamingQueryId", "concurrent-query-a")
            .outputMode("append")
            .trigger(availableNow=True)
            .start()
        )

        stream_b = spark.readStream.schema(_simple_schema(spark)).json(source_b)
        q_b = (
            stream_b.writeStream.format("lance")
            .option("path", path)
            .option("checkpointLocation", ck_b)
            .option("streamingQueryId", "concurrent-query-b")
            .outputMode("append")
            .trigger(availableNow=True)
            .start()
        )

        try:
            q_a.awaitTermination(timeout=60)
            q_b.awaitTermination(timeout=60)
        finally:
            q_a.stop()
            q_b.stop()

        # Both queries' data must land in the table. Lance's MVCC serializes the two commits
        # into distinct versions, but the final row count equals the sum.
        assert _count_rows(spark, path) == 5 + 3


# ---------------------------------------------------------------------------
# Non-existent table → clear error
# ---------------------------------------------------------------------------


class TestStreamingSinkErrors:
    """User-facing error paths."""

    def test_streaming_write_to_nonexistent_table_errors_clearly(self, spark, tmp_path):
        path = str(tmp_path / "does-not-exist.lance")
        source_dir = str(tmp_path / "source")
        checkpoint = str(tmp_path / "ckpt-err")

        _write_json_batch(source_dir, "0001", [{"id": 1, "value": "x"}])

        stream = spark.readStream.schema(_simple_schema(spark)).json(source_dir)

        with pytest.raises((StreamingQueryException, Exception)) as exc_info:
            q = (
                stream.writeStream.format("lance")
                .option("path", path)
                .option("checkpointLocation", checkpoint)
                .option("streamingQueryId", "error-test")
                .outputMode("append")
                .trigger(availableNow=True)
                .start()
            )
            try:
                q.awaitTermination(timeout=30)
            finally:
                q.stop()

        # The underlying message should mention the missing table.
        msg = str(exc_info.value).lower()
        assert "does not exist" in msg or "not found" in msg or "failed to open" in msg

    def test_streaming_write_missing_query_id_errors_clearly(self, spark, tmp_path):
        path = str(tmp_path / "missing_qid.lance")
        source_dir = str(tmp_path / "source")
        checkpoint = str(tmp_path / "ckpt-qid")

        _pre_create_empty_table(spark, path)
        _write_json_batch(source_dir, "0001", [{"id": 1, "value": "x"}])

        stream = spark.readStream.schema(_simple_schema(spark)).json(source_dir)

        with pytest.raises((StreamingQueryException, Exception)) as exc_info:
            q = (
                stream.writeStream.format("lance")
                .option("path", path)
                .option("checkpointLocation", checkpoint)
                # deliberately omit streamingQueryId
                .outputMode("append")
                .trigger(availableNow=True)
                .start()
            )
            try:
                q.awaitTermination(timeout=30)
            finally:
                q.stop()

        assert "streamingqueryid" in str(exc_info.value).lower()


# ---------------------------------------------------------------------------
# Manual epoch-watermark bump → sink skips replayed epochs
# ---------------------------------------------------------------------------


class TestStreamingSinkWatermark:
    """Manual epoch-watermark bump causes the sink to skip replayed epochs.

    This exercises the SKIP_REPLAY fast-path of StreamingCommitProtocol:
    when the persisted watermark for a query is already >= the incoming epochId,
    the sink returns immediately without touching the dataset.
    """

    def test_manual_watermark_bump_skips_replayed_epochs(self, spark, tmp_path):
        """Pre-setting the epoch watermark high causes the sink to skip all commits.

        Steps:
          1. Pre-create an empty Lance table.
          2. Manually set the epoch watermark for ``query_id`` to 1000 — far above
             any epoch Spark will generate for a freshly-started query.
          3. Stage 3 rows that *would* be written if the sink didn't skip them.
          4. Run a streaming query with the same ``query_id``.
          5. Assert the table is still empty: all epochs (0, 1, …) are ≤ 1000
             so every commit attempt hits the SKIP_REPLAY branch.
        """
        path = str(tmp_path / "watermark_skip.lance")
        source_dir = str(tmp_path / "source")
        checkpoint = str(tmp_path / "ckpt-wm")
        query_id = "manual-wm-test"

        _pre_create_empty_table(spark, path)

        # Bump the watermark to 1000 — any epoch Spark sends will be ≤ this.
        _set_epoch_watermark(spark, path, query_id, 1000)

        # Stage 3 rows that would be appended if the sink committed them.
        _write_json_batch(
            source_dir,
            "0001",
            [
                {"id": 1, "value": "would-be-skipped"},
                {"id": 2, "value": "would-be-skipped"},
                {"id": 3, "value": "would-be-skipped"},
            ],
        )

        stream = spark.readStream.schema(_simple_schema(spark)).json(source_dir)
        q = (
            stream.writeStream.format("lance")
            .option("path", path)
            .option("checkpointLocation", checkpoint)
            .option("streamingQueryId", query_id)
            .outputMode("append")
            .trigger(availableNow=True)
            .start()
        )
        try:
            q.awaitTermination(timeout=60)
        finally:
            q.stop()

        # The table must remain empty. Every epoch Spark sent was ≤ 1000 so the
        # sink's SKIP_REPLAY fast-path fired and no data was committed.
        row_count = _count_rows(spark, path)
        assert row_count == 0, (
            f"Sink should have skipped all epochs (watermark=1000 ≥ any epoch "
            f"a fresh query generates), but found {row_count} rows."
        )
