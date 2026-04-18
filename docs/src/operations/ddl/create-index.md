# CREATE INDEX

Creates a scalar or vector index on a Lance table to accelerate queries.

!!! warning "Spark Extension Required"
    This feature requires the Lance Spark SQL extension to be enabled. See [Spark SQL Extensions](../../config.md#spark-sql-extensions) for configuration details.

## Overview

The `CREATE INDEX` command builds an index on one or more columns of a Lance table. Indexing can improve the performance of queries that filter on the indexed columns or run nearest-neighbour search on vector columns. Scalar index building is performed in a distributed manner, with one task per fragment in parallel; vector index training is performed in a single driver-side step (see [How It Works](#how-it-works) below).

## Basic Usage

The command uses the `ALTER TABLE` syntax to add an index. The same statement covers both scalar and vector indexes — only the `USING` method changes.

=== "SQL — scalar"
    ```sql
    ALTER TABLE lance.db.users CREATE INDEX user_id_idx USING btree (id);
    ```

=== "SQL — vector"
    ```sql
    ALTER TABLE lance.db.items CREATE INDEX emb_idx USING ivf_pq (embedding)
      WITH (num_partitions = 256, num_sub_vectors = 16, metric = 'cosine');
    ```

## Index Methods

The following index methods are supported:

| Method          | Kind   | Description                                                                  |
|-----------------|--------|------------------------------------------------------------------------------|
| `btree`         | scalar | B-tree index for efficient range queries and point lookups on scalar columns. |
| `fts`           | scalar | Full-text search (inverted) index for text search on string columns.          |
| `ivf_flat`      | vector | IVF with full-precision vectors. Largest index, best recall.                  |
| `ivf_pq`        | vector | IVF with PQ-compressed vectors. Smallest index, good recall.                  |
| `ivf_hnsw_pq`   | vector | HNSW graph + PQ codes. Fast and compressed; latency-sensitive on large corpora. |
| `ivf_hnsw_sq`   | vector | HNSW graph + scalar quantisation. Fast, medium size; balanced workloads.      |

A vector index requires a **vector column** (see [CREATE TABLE → Vector Columns](create-table.md)): an `ARRAY<FLOAT>` or `ARRAY<DOUBLE>` with the `arrow.fixed-size-list.size` property set to the vector dimension. Asking for a vector index on a scalar column fails fast with a clear error.

## Distance Metrics

Vector indexes accept a `metric` option under the `WITH (...)` clause. The metric is stored in the index — queries that don't specify a metric will use the index-stored default, but they can override it in the [`lance_vector_search`](../dql/vector-search.md) TVF call if needed.

| Metric    | Alias                       | Notes                                                         |
|-----------|-----------------------------|---------------------------------------------------------------|
| `l2`      | `euclidean`                 | Default. Euclidean distance.                                  |
| `cosine`  | —                           | Cosine distance (implemented as L2 on normalised vectors).    |
| `dot`     | `inner_product`, `ip`       | Negative inner product — larger magnitude ⇒ *closer*.         |
| `hamming` | —                           | Hamming distance over binary-encoded vectors.                 |

## Options

The `CREATE INDEX` command supports options via the `WITH` clause. These options are specific to the chosen index method.

### BTree Options

For the `btree` method:

| Option           | Type   | Description                                                                                                                                                                                              |
|------------------|--------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `zone_size`      | Long   | The number of rows per zone in the B-tree index.                                                                                                                                                         |
| `build_mode`     | String | Index building mode: 'fragment' builds indexes in parallel by fragment; 'range' sorts data by indexed columns first, then partitions and builds indexes in parallel by partition. Default is 'fragment'. |
| `rows_per_range` | Long   | The number of rows per range when built using range mode. Default is 1000000.                                                                                                                            |


### FTS Options

For the `fts` method, the following options are required:

| Option             | Type    | Description                                                    |
|--------------------|---------|----------------------------------------------------------------|
| `base_tokenizer`   | String  | Tokenizer type: "simple" (whitespace/punctuation) or "ngram".  |
| `language`         | String  | Language for text processing (e.g., "English").                |
| `max_token_length` | Integer | Maximum token length (e.g., 40).                               |
| `lower_case`       | Boolean | Convert text to lowercase.                                     |
| `stem`             | Boolean | Enable stemming to reduce words to root form.                  |
| `remove_stop_words`| Boolean | Remove common stop words from index.                           |
| `ascii_folding`    | Boolean | Normalize accented characters (e.g., 'é' to 'e').              |
| `with_position`    | Boolean | Enable phrase queries. Increases index size.                   |

For advanced tokenizer configuration, refer to the [Lance FTS documentation](https://lance.org/format/table/index/scalar/fts/#tokenizers).

### FTS Format Version

Lance FTS index format v2 is selected by the Lance runtime environment variable `LANCE_FTS_FORMAT_VERSION=2`. Configure it on both the Spark driver and executors before creating the index.

=== "spark-submit"
    ```bash
    LANCE_FTS_FORMAT_VERSION=2 spark-submit \
        --conf spark.executorEnv.LANCE_FTS_FORMAT_VERSION=2 \
        ...
    ```

Spark SQL currently does not expose a per-index `fts_version` option. Use `USING fts` with the normal FTS options shown above; Spark records the index details and version returned by Lance.

### Vector Index Options

All options go under the `WITH (...)` clause. Every knob is optional; unspecified values fall back to the Lance defaults, which are chosen to work well for most workloads.

#### Shared IVF options

| Option           | Type    | Default     | Meaning                                    |
|------------------|---------|-------------|--------------------------------------------|
| `metric`         | String  | `l2`        | See [Distance Metrics](#distance-metrics) above. |
| `num_partitions` | Integer | `256`       | IVF centroid count (k-means training).     |
| `sample_rate`    | Integer | `256`       | Training sample ratio.                     |
| `max_iterations` | Integer | `50`        | Max k-means / PQ training iterations.      |

#### Additional `ivf_pq`, `ivf_hnsw_pq` options

| Option            | Type    | Default         | Meaning                                   |
|-------------------|---------|-----------------|-------------------------------------------|
| `num_sub_vectors` | Integer | `dim / 16`      | Number of PQ subquantisers. Must divide `dim` evenly. |
| `num_bits`        | Integer | `8`             | Bits per PQ code (4 or 8).                |

#### Additional `ivf_hnsw_sq` options

| Option            | Type    | Default | Meaning                                    |
|-------------------|---------|---------|--------------------------------------------|
| `num_bits`        | Integer | `8`     | Bits of scalar quantisation.               |

#### Additional HNSW options (`ivf_hnsw_*`)

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

### Basic scalar index

Create a simple B-tree index on a single column:

=== "SQL"
    ```sql
    ALTER TABLE lance.db.users CREATE INDEX idx_id USING btree (id);
    ```

### Indexing Multiple Columns

Create a composite scalar index on multiple columns.

=== "SQL"
    ```sql
    ALTER TABLE lance.db.logs CREATE INDEX idx_ts_level USING btree (timestamp, level);
    ```

### Indexing with Options

Create a B-tree index and specify the `zone_size`:

=== "SQL"
    ```sql
    ALTER TABLE lance.db.users CREATE INDEX idx_id_zoned USING btree (id) WITH (zone_size = 2048);
    ```

### Full-Text Search Index

Create an FTS index on a text column:

=== "SQL"
    ```sql
    ALTER TABLE lance.db.documents CREATE INDEX doc_fts USING fts (content) WITH (
        base_tokenizer = 'simple',
        language = 'English',
        max_token_length = 40,
        lower_case = true,
        stem = false,
        remove_stop_words = false,
        ascii_folding = false,
        with_position = true
    );
    ```

### IVF-PQ vector index with cosine similarity

```sql
ALTER TABLE lance.db.items CREATE INDEX emb_idx USING ivf_pq (embedding)
  WITH (num_partitions = 256, num_sub_vectors = 16, metric = 'cosine');
```

### IVF-Flat vector index on a small corpus

Highest recall, largest footprint:

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

Running `CREATE INDEX` with the same name **replaces** the previous index atomically. There is no separate "rebuild" statement.

## Output

The `CREATE INDEX` command returns the following information about the operation:

| Column              | Type   | Description                            |
|---------------------|--------|----------------------------------------|
| `fragments_indexed` | Long   | The number of fragments that were indexed. |
| `index_name`        | String | The name of the created index.         |

## When to Use an Index

Consider creating a **scalar** index when:

- You frequently filter a large table on a specific column.
- Your queries involve point lookups or small range scans.

Consider creating a **vector** index when:

- You run nearest-neighbour search on a vector column ([`lance_vector_search`](../dql/vector-search.md)) and want sub-linear lookup time.
- The corpus is large enough that a brute-force scan exceeds your latency budget.

## How It Works

### Scalar indexes (`btree`, `fts`)

1.  **Distributed Index Building**: For each fragment in the Lance dataset, a separate task is launched to build an index on the specified column(s).
2.  **Metadata Merging**: Once all per-fragment indexes are built, their metadata is collected and merged.
3.  **Transactional Commit**: A new table version is committed with the new index information. The operation is atomic and ensures that concurrent reads are not affected.

### Vector indexes (`ivf_*`)

1. **Driver-side validation** — the column is verified to be a vector column (correct dtype + `arrow.fixed-size-list.size` metadata). Misconfigured columns fail fast.
2. **Driver-side training + encoding** — Lance trains the IVF / PQ / HNSW / SQ parameters in a single call on the driver. Distributed per-fragment training requires pre-computed centroids and is not yet enabled in this connector.
3. **Transactional commit** — the new index is committed in a single Lance transaction; existing readers continue to see the previous version until they refresh.

## Notes and Limitations

- **Index Methods**: scalar (`btree`, `fts`) and vector (`ivf_flat`, `ivf_pq`, `ivf_hnsw_pq`, `ivf_hnsw_sq`) are supported.
- **Index Replacement**: If you create an index with the same name as an existing one, the old index will be replaced by the new one. This is because the underlying implementation uses `replace(true)`.
- **Vector — column arity**: vector indexes take exactly **one vector column**. Composite vector indexes are not supported.
- **Vector — dimension**: the `arrow.fixed-size-list.size` metadata must be set on the column. Without it the DDL refuses to build the index.
- **Vector — `num_sub_vectors`** must divide the vector dimension. A dimension of 128 is compatible with `num_sub_vectors ∈ {1, 2, 4, 8, 16, 32, 64, 128}`.
- **Vector — float16**: requires Spark 4.0+ (Arrow 18+). On earlier Spark versions, use `ARRAY<FLOAT>` without the `arrow.float16` property.
- **Vector — interaction with OPTIMIZE/VACUUM**: both commands preserve vector indexes. Running `OPTIMIZE` after large writes is recommended so the ANN search sees a consolidated layout.
