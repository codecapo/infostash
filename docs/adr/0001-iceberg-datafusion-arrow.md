# 0001. Build on Apache Iceberg, DataFusion and Arrow

- **Status:** Accepted
- **Date:** 2026-10-27 (recorded 2026-10-29)

## Context

Infostash must store data in the user's own object storage, run SQL fast on
one laptop or many machines, and let other engines read the same tables.
Writing a query engine and a table format from scratch would take years and
would lock users into our format.

## Decision

- **Apache Arrow** for data in memory.
- **Apache DataFusion** to parse, optimise and execute SQL.
- **Apache Iceberg** for tables, with **Parquet** data files.

We build what these projects lack (incremental planning, distributed
execution, quarantine, connectors) on top, and offer generic pieces upstream.

## Consequences

- Spark, Trino, Snowflake and Databricks can read every table we write.
  Users can leave without migrating data, which makes it easier to adopt us.
- We inherit years of optimiser and execution work and a large community.
- We follow their release cycles. DataFusion and Arrow ship breaking
  releases often, and iceberg-rust lags behind them (see
  [ADR 0002](0002-pin-iceberg-rust-from-git.md)).
- Iceberg's Rust library has no incremental scan, so we read snapshot history
  and manifests ourselves in `crates/storage`.
