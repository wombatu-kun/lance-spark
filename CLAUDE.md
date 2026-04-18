# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Apache Spark Connector for Lance — enables Spark to read/write datasets in Lance columnar format. Implements Spark DataSource V2 API. Supports Spark 3.4–4.1 across Scala 2.12 and 2.13.

## Build Commands

Maven-based build with Make wrapper. Defaults: `SPARK_VERSION=3.5`, `SCALA_VERSION=2.12`.

```shell
make install                                    # Install module (skip tests)
make install SPARK_VERSION=3.4 SCALA_VERSION=2.13  # Specific version
make test                                       # Run unit tests for default module
make test SPARK_VERSION=4.0 SCALA_VERSION=2.13  # Test specific version
make install-all                                # Install all modules
make test-all                                   # Run all tests
make lint                                       # checkstyle + spotless check
make format                                     # Auto-format code (spotless:apply)
make bundle                                     # Build fat JAR for distribution
make clean                                      # Clean all modules
```

Run a single test class:
```shell
./mvnw test -pl lance-spark-3.5_2.12 -Dtest=ClassName
```

## Module Structure

Multi-module Maven project with 14 modules following a shared-base pattern:

- **`lance-spark-base_2.{12,13}`**: Core shared logic (read/write, catalog, SQL extensions, vectorized accessors). All version-specific modules depend on this.
- **`lance-spark-{3.4,3.5,4.0,4.1}_2.{12,13}`**: Spark version-specific modules. Inherit base test classes and extend with version-specific behavior.
- **`lance-spark-bundle-{version}_{scala}`**: Fat JAR assembly modules for distribution.

Scala 2.12 modules exist only for Spark 3.4 and 3.5. Spark 4.0+ is Scala 2.13 only.

## Architecture

Source lives in `lance-spark-base_2.12/src/main/` (Java + Scala):

- **`org.lance.spark.LanceDataSource`** — DataSource V2 entry point (`SupportsCatalogOptions`, `DataSourceRegister`)
- **`org.lance.spark.catalog.BaseLanceNamespaceSparkCatalog`** — Catalog implementation (DDL, namespace management)
- **`org.lance.spark.read/`** — Vectorized batch scanning with Arrow, filter/projection/aggregation pushdown
- **`org.lance.spark.write/`** — Staged commit protocol for ACID writes via `StagedCommit`
- **`org.lance.spark.join.FragmentAwareJoinRule`** (Scala) — Catalyst optimizer rule for fragment-aware joins
- **`org.lance.spark.vectorized/`** — ~62 Arrow column vector accessors for all data types
- **`org.apache.spark.sql.catalyst.parser.extensions/`** (Scala) — SQL parser for Lance commands (`OPTIMIZE`, `VACUUM`, `ADD INDEX`, `ADD COLUMNS`, `UPDATE COLUMNS`)
- **`org.apache.spark.sql.execution.datasources.v2/`** (Scala) — Physical execution plans for Lance operations

Tests are in `lance-spark-base_2.12/src/test/` as base classes (e.g., `BaseSparkConnectorReadTest`). Version-specific modules inherit these.

## Code Style

- **Java**: Google Java Format (enforced by spotless)
- **Scala**: scalafmt 3.7.5 (config in `.scalafmt.conf`)
- **Line limit**: 100 characters
- **Import order**: `com.lancedb.lance.*`, then others, then `javax/java`, then `scala`
- Run `make format` before committing to fix style issues
- CI runs `make lint` — PRs must pass checkstyle + spotless checks

## Integration Tests

Docker-based, run via:
```shell
make bundle && make docker-build-test-full && make docker-test
```
Integration test code is in `docker/tests/` (Python/pytest).

## Key Dependencies

- **lance-core**: Native Lance library (JNI)
- **Apache Arrow**: Vectorized data exchange between Lance and Spark
- **ANTLR 4.9.3**: SQL parser generation for Lance extensions
