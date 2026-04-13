# Streaming-write scalability benchmark — reference results

These four CSVs are the reference data referenced by
[`docs/src/operations/streaming/streaming-writes.md`](../../docs/src/operations/streaming/streaming-writes.md)
and PR #399. They were captured on a single machine with `local[12]` Spark master, writing to
local FS, lance-spark `0.4.0-beta.1` from this branch.

| File | Experiment | What it shows |
|------|-----------|---------------|
| `streaming_E1_20260413_094028.csv` | E1 — no compaction, 1 999 batches, 5 000 rows/s | Manifest grows linearly (~83 B/fragment); commit latency reaches 2.3 s p50 at ~21 000 fragments. The "smoking gun" for why `OPTIMIZE` is required, not optional. |
| `streaming_E2_20260413_091542.csv` | E2 — same workload, periodic `OPTIMIZE` | Two runs (every-50, every-200) showing how compaction holds the manifest in a bounded range and keeps p90 commit at ~191 ms / ~250 ms respectively. |
| `streaming_E3_20260413_103456.csv` | E3 — recovery scan vs. version history depth | Restart cost = `~0.5 ms × min(maxRecoveryLookback, current_version)`. Default `maxRecoveryLookback=100` keeps restart under ~75 ms regardless of dataset history depth. |
| `streaming_E4_20260413_103909.csv` | E4 — sustained throughput at multiple trigger intervals | At 1 000 rows/s, all of `1 s` / `5 s` / `15 s` triggers achieved input rate without back-pressure. Fragment-accumulation rate (and therefore `OPTIMIZE` cadence) scales inversely with trigger interval — pick by data-freshness target, not throughput.|

## CSV schema

```
experiment,run_label,batch_id,batch_ts_ms,input_rows,batch_duration_ms,
fragment_count,lance_version,manifest_bytes,optimized_this_batch
```

See [`benchmark/README.md`](../README.md#streaming-write-scalability-benchmark) for field
semantics and how to reproduce these runs.

## Reproducing

```bash
make docker-up
./benchmark/scripts/run-streaming-benchmark-docker.sh E1 --batches 2000 --rows-per-second 5000
./benchmark/scripts/run-streaming-benchmark-docker.sh E2 --batches 500 --rows-per-second 5000 \
  --optimize-cadences 50,200
./benchmark/scripts/run-streaming-benchmark-docker.sh E3 \
  --version-targets 10,100,500,1000 --recovery-lookbacks 100,500,1000
./benchmark/scripts/run-streaming-benchmark-docker.sh E4 \
  --trigger-intervals '1 second,5 seconds,15 seconds' --window-seconds 120
```
