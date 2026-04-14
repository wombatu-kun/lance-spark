# CREATE VECTOR INDEX

Creates an Approximate-Nearest-Neighbour (ANN) index on a Lance **vector column** to accelerate
similarity search.

!!! warning "Spark Extension Required"
    This feature requires the Lance Spark SQL extension to be enabled. See
    [Spark SQL Extensions](../../config.md#spark-sql-extensions) for configuration details.

!!! tip "Also see"
    - [CREATE INDEX](create-index.md) — scalar `btree` / `fts` indexes.
    - [lance_vector_search](../dql/vector-search.md) — the kNN query that uses these indexes.

## Overview

A vector index lets Lance answer *"find the `k` rows whose `embedding` is closest to this query
vector"* without scanning every row. Four index families are supported, each balancing recall,
latency, and storage differently.

Index creation reuses the existing `ALTER TABLE ... CREATE INDEX` syntax — the only thing that
changes is the `USING` method and the arguments under `WITH (...)`.

## Basic Usage

=== "SQL"
    ```sql
    ALTER TABLE lance.db.items CREATE INDEX emb_idx USING ivf_pq (embedding)
      WITH (num_partitions = 256, num_sub_vectors = 16, metric = 'cosine');
    ```

The column must be a **vector column** (see [CREATE TABLE → Vector Columns](create-table.md)): an
`ARRAY<FLOAT>` or `ARRAY<DOUBLE>` with the `arrow.fixed-size-list.size` property set to the
vector dimension. Asking for a vector index on a scalar column fails fast with a clear error.

## Index Methods

| Method          | Storage vs. recall                        | Typical use case                              |
|-----------------|-------------------------------------------|-----------------------------------------------|
| `ivf_flat`      | Full precision. Largest index, best recall. | Small/medium corpora, recall-first workloads. |
| `ivf_pq`        | PQ-compressed. Smallest index, good recall. | Large corpora where storage is the constraint. |
| `ivf_hnsw_pq`   | HNSW graph + PQ codes. Fast + compressed. | Latency-sensitive search on large corpora.    |
| `ivf_hnsw_sq`   | HNSW graph + scalar quantisation. Fast, medium size. | Balanced latency/size on medium corpora. |

## Distance Metrics

| Metric    | Alias                       | Notes                                                         |
|-----------|-----------------------------|---------------------------------------------------------------|
| `l2`      | `euclidean`                 | Default. Euclidean distance.                                  |
| `cosine`  | —                           | Cosine distance (implemented as L2 on normalised vectors).    |
| `dot`     | `inner_product`, `ip`       | Negative inner product — larger magnitude ⇒ *closer*.         |
| `hamming` | —                           | Hamming distance over binary-encoded vectors.                 |

The metric is stored in the index — queries that don't specify a metric will use the index-stored
default, but they can override it in the TVF call if needed.

## Options

All options go under the `WITH (...)` clause. Every knob is optional; unspecified values fall
back to the Lance defaults, which are chosen to work well for most workloads.

### Shared IVF options

| Option           | Type    | Default     | Meaning                                    |
|------------------|---------|-------------|--------------------------------------------|
| `metric`         | String  | `l2`        | See metric table above.                    |
| `num_partitions` | Integer | `256`       | IVF centroid count (k-means training).     |
| `sample_rate`    | Integer | `256`       | Training sample ratio.                     |
| `max_iterations` | Integer | `50`        | Max k-means / PQ training iterations.      |

### Additional `ivf_pq`, `ivf_hnsw_pq` options

| Option            | Type    | Default         | Meaning                                   |
|-------------------|---------|-----------------|-------------------------------------------|
| `num_sub_vectors` | Integer | `dim / 16`      | Number of PQ subquantisers. Must divide `dim` evenly. |
| `num_bits`        | Integer | `8`             | Bits per PQ code (4 or 8).                |

### Additional `ivf_hnsw_sq` options

| Option            | Type    | Default | Meaning                                    |
|-------------------|---------|---------|--------------------------------------------|
| `num_bits`        | Integer | `8`     | Bits of scalar quantisation.               |

### Additional HNSW options (`ivf_hnsw_*`)

| Option            | Type    | Default | Meaning                                    |
|-------------------|---------|---------|--------------------------------------------|
| `m`               | Integer | `20`    | HNSW graph degree.                         |
| `ef_construction` | Integer | `300`   | Build-time candidate list size.            |

## Supported Vector Data Types

| Spark type               | Metadata                                         | Spark support                  |
|--------------------------|--------------------------------------------------|--------------------------------|
| `ARRAY<FLOAT>`           | `'<col>.arrow.fixed-size-list.size' = '<dim>'`   | All (3.4, 3.5, 4.0, 4.1).      |
| `ARRAY<DOUBLE>`          | `'<col>.arrow.fixed-size-list.size' = '<dim>'`   | All (3.4, 3.5, 4.0, 4.1).      |
| `ARRAY<FLOAT>` (float16) | add `'<col>.arrow.float16' = 'true'`             | Spark 4.0+ only (Arrow 18+).   |

## Examples

### IVF-PQ with cosine similarity
```sql
ALTER TABLE lance.db.items CREATE INDEX emb_idx USING ivf_pq (embedding)
  WITH (num_partitions = 256, num_sub_vectors = 16, metric = 'cosine');
```

### IVF-Flat on a small corpus (highest recall, largest footprint)
```sql
ALTER TABLE lance.db.small CREATE INDEX emb_idx USING ivf_flat (embedding)
  WITH (num_partitions = 32, metric = 'l2');
```

### IVF-HNSW-PQ for latency-sensitive search
```sql
ALTER TABLE lance.db.items CREATE INDEX emb_idx USING ivf_hnsw_pq (embedding)
  WITH (num_partitions = 256, num_sub_vectors = 16, m = 32, ef_construction = 200);
```

### IVF-HNSW-SQ with scalar quantisation
```sql
ALTER TABLE lance.db.items CREATE INDEX emb_idx USING ivf_hnsw_sq (embedding)
  WITH (num_partitions = 128, num_bits = 8, m = 16);
```

### Rebuild (replace) an existing index

Running `CREATE INDEX` with the same name **replaces** the previous index atomically. There is no
separate "rebuild" statement.

## Output

| Column              | Type   | Description                                |
|---------------------|--------|--------------------------------------------|
| `fragments_indexed` | Long   | Number of fragments that were indexed.     |
| `index_name`        | String | Name of the created index.                 |

## How It Works

1. **Driver-side validation** — the column is verified to be a vector column (correct dtype +
   `arrow.fixed-size-list.size` metadata). Misconfigured columns fail fast.
2. **Distributed training + encoding** — one Spark task per fragment opens the Lance dataset,
   builds the requested IVF / PQ / HNSW / SQ parameters locally, and creates a per-fragment
   index shard.
3. **Metadata merging** — per-fragment shards are merged into a single global index via
   `Dataset.mergeIndexMetadata`.
4. **Transactional commit** — the new index is committed in a single Lance transaction; existing
   readers continue to see the previous version until they refresh.

## Notes and Limitations

- **Column arity**: vector indexes take exactly **one vector column**. You cannot create a
  composite vector index across two columns.
- **Dimension**: the `arrow.fixed-size-list.size` metadata must be set on the column. Without it
  the DDL refuses to build the index.
- **`num_sub_vectors`** must divide the vector dimension. A dimension of 128 is compatible with
  `num_sub_vectors ∈ {1, 2, 4, 8, 16, 32, 64, 128}`.
- **Float16**: requires Spark 4.0+ (Arrow 18+). On earlier Spark versions, use `ARRAY<FLOAT>`
  without the `arrow.float16` property.
- **Interaction with OPTIMIZE/VACUUM**: both commands preserve vector indexes. Running
  `OPTIMIZE` after large writes is recommended so the ANN search sees a consolidated layout.
