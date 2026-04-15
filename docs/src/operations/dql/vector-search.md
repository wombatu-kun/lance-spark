# Vector Search (`lance_vector_search`)

Executes an Approximate-Nearest-Neighbour (kNN) search over a Lance vector column from Spark SQL.
Implemented as a **table-valued function**, so it composes cleanly with `WHERE`, `JOIN`,
`GROUP BY`, and projections — no grammar extension required.

!!! warning "Spark Extension Required"
    This feature requires the Lance Spark SQL extension to be enabled. See
    [Spark SQL Extensions](../../config.md#spark-sql-extensions) for configuration details.

!!! tip "Also see"
    - [CREATE VECTOR INDEX](../ddl/create-vector-index.md) — build the index the search uses.
    - [Select](select.md) — general read path.

## Syntax

The function takes four required positional arguments plus five optional ones:

```
lance_vector_search(
  table,           -- STRING   required    catalog-qualified name OR filesystem URI
  column,          -- STRING   required    name of the vector column
  query,           -- ARRAY<FLOAT|DOUBLE> required    query vector, dimension must match column
  k,               -- INT      required    number of neighbours (> 0)
  [metric],        -- STRING   optional    l2 (default) | cosine | dot | hamming
  [nprobes],       -- INT      optional    IVF probe count, default 20
  [refine_factor], -- INT      optional    PQ re-rank factor, default 1
  [ef],            -- INT      optional    HNSW search depth
  [use_index]      -- BOOLEAN  optional    default true; false = brute force
)
```

Spark 3.5+ also accepts **named** arguments (`query => array(...)`, `k => 10`, …).
Spark 3.4 only accepts positional arguments.

## Basic Usage

=== "SQL"
    ```sql
    SELECT id, category
    FROM lance_vector_search(
      'lance.db.items',
      'embedding',
      array(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8),
      10
    );
    ```

=== "PySpark"
    ```python
    from pyspark.sql import functions as F

    q = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]
    spark.sql(f"""
        SELECT id, category
        FROM lance_vector_search(
          'lance.db.items',
          'embedding',
          array({', '.join(str(x) for x in q)}),
          10
        )
    """).show()
    ```

## Arguments

### `table`
A catalog-qualified name (e.g. `lance.db.items`) **or** a filesystem URI
(e.g. `s3://bucket/path/to/items.lance`). Catalog-qualified names are resolved through the
currently configured Spark catalog; URIs are passed straight through to the Lance DataSource.

### `column`
The name of the vector column to search. Must be a vector column (see
[CREATE TABLE → Vector Columns](../ddl/create-table.md)).

### `query`
The query vector, as a Spark `ARRAY<FLOAT>` or `ARRAY<DOUBLE>` literal / foldable expression. The
length must match the column's `arrow.fixed-size-list.size` metadata, otherwise Lance raises an
error at scan time. Double-precision arrays are automatically down-cast to float32.

### `k`
Number of neighbours to return. Must be positive. Lance may return fewer rows if the table is
smaller than `k` (or the pre-filter eliminates enough rows).

### `metric`
Which distance metric to use. See the metric table in
[CREATE VECTOR INDEX → Distance Metrics](../ddl/create-vector-index.md#distance-metrics).
If omitted, the metric stored inside the index is used.

### `nprobes`
Number of IVF partitions to probe. Higher values improve recall at the cost of latency. Default
`20`. Only relevant for IVF-family indexes.

### `refine_factor`
PQ re-rank factor. The scan returns `k × refine_factor` PQ-approximate candidates, then re-ranks
them using the exact codebook centroids. `1` (default) disables re-ranking.

### `ef`
HNSW candidate-list size at search time. Higher values improve recall. Relevant for
`ivf_hnsw_*` indexes.

### `use_index`
`true` (default) uses the ANN index; `false` forces a brute-force scan of every fragment. Useful
for recall evaluation or when no index exists yet.

## Composing with the Rest of SQL

### Projection

Project any subset of the source columns after the TVF:

```sql
SELECT id FROM lance_vector_search('lance.db.items', 'embedding', array(...), 10);
```

### Pre-filters

Filters on scalar columns that sit directly above the TVF are pushed into Lance and applied
**before** the kNN search — meaning `k` applies to the filtered subset, not the whole table.

```sql
SELECT id, category
FROM lance_vector_search('lance.db.items', 'embedding', array(...), 10)
WHERE category = 'books' AND price < 50.0;
```

### Joins / group-by

The TVF result is a regular Dataset, so all downstream operators work unchanged:

```sql
SELECT s.id, s.category, i.name
FROM lance_vector_search('lance.db.items', 'embedding', array(...), 50) s
JOIN lance.db.inventory i ON i.id = s.id
WHERE i.in_stock;
```

## Brute Force vs. Indexed Search

When you want ground truth — for recall evaluation or for tables that are too small to justify an
index — pass `use_index => false`:

```sql
SELECT id
FROM lance_vector_search('lance.db.items', 'embedding', array(...), 10, 'l2', 20, 1, 64, false);
```

Brute force scans every row in every fragment. It returns exact top-k per fragment; Spark unions
the per-fragment results.

## Tuning Recall vs. Latency

| Knob              | Effect on recall | Effect on latency |
|-------------------|------------------|-------------------|
| `nprobes` ↑       | ↑                | ↑                 |
| `ef` ↑            | ↑                | ↑                 |
| `refine_factor` ↑ | ↑                | ↑                 |
| `num_partitions` ↑ at index time | neutral | ↓ (each probe is smaller) |
| `m` / `ef_construction` ↑ at index time | ↑ | neutral (one-time cost) |

A common starting recipe for IVF-PQ on a few million rows:
`num_partitions = 256`, `num_sub_vectors = 16`, `nprobes = 20`, `refine_factor = 10`.

## Errors

| Condition                                              | Result                                                        |
|--------------------------------------------------------|---------------------------------------------------------------|
| `k <= 0`                                               | `IllegalArgumentException("… 'k' must be positive")`          |
| Unknown metric (`'manhattan'`, etc.)                   | `IllegalArgumentException("… unsupported metric …")`          |
| Non-constant `query` / `k` / `column`                  | `IllegalArgumentException("… must be a constant expression")` |
| `column` not a vector column                           | Raised by Lance at scan time (dimension mismatch).            |
| `table` not found                                      | `IllegalArgumentException("… could not resolve table …")`     |

## Notes and Limitations

- **Fragment-local top-k**: the scan today performs search per fragment and unions the results, so
  the raw TVF output may contain up to `k × num_fragments` rows. Add a global
  `ORDER BY … LIMIT k` on top if you need the true global top-k.
- **Single column**: the `column` argument is a single string — you cannot combine two vector
  columns in one call.
- **Query vector is a driver-side literal**: Spark evaluates the `query` expression on the driver
  when planning the scan. Non-foldable expressions (e.g. a column reference) are rejected.
- **Named arguments**: require Spark 3.5+. On Spark 3.4 pass all arguments positionally.
- **`_distance` column**: the TVF surfaces the per-row distance as a non-nullable `FLOAT`
  column named `_distance`. Its units depend on the `metric` argument: `l2` returns squared
  L2 distance, `cosine` returns `1 − cos(θ)`, `dot` returns negative inner product, and
  `hamming` returns integer Hamming distance (cast to FLOAT). Filters referencing
  `_distance` (e.g. `WHERE _distance < 0.5`) are evaluated by Spark after the scan; they are
  not pushed into the Lance native WHERE.
