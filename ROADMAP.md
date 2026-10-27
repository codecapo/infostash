# Roadmap

This is the public plan. Dates are targets, not promises. Each phase ends with
something people can run.

## Phase 0: First slice on a laptop

Goal: one binary that turns CSV files into Raw, Clean and Ready Iceberg tables.

- `infoctl init`, `infoctl run`, `infoctl query`
- SQL models with `-- @key: value` directives, dependency graph from the SQL
- Incremental runs: the second run processes only new rows
- Row checks with a quarantine table for bad rows
- Local Iceberg tables on disk; the distributed plan runs in one process
- CI, docs site, first `good first issue` list

## Phase 1: Real sources and real storage

Goal: a small team can run it on their own cloud account.

- Object storage on S3, GCS, Azure Blob and MinIO
- Iceberg REST catalog
- Connector SDK v1: YAML connectors, Rust SDK, conformance tests
- First connectors: Postgres, MySQL, S3 files, REST APIs, Kafka
- Postgres wire protocol, so BI tools can connect
- Web UI: project browser, run history, table preview
- Compaction and snapshot expiry

## Phase 2: Scale and agents

Goal: production use at terabyte scale.

- Distributed execution across many executors, with shuffle and task retry
- Compute pools for ingest, pipelines, BI and agents, scaling to zero
- Result, metadata and disk caches
- Semantic layer for metrics and business terms
- Chat agent (read-only) and builder agent (works on a branch, needs approval)
- MCP server and Arrow Flight SQL
- Kubernetes deployment (Helm chart)

## Phase 3: Enterprise and petabyte scale

- Petabyte-scale benchmarks, published with the method
- Wasm connectors in any language; Airbyte and Singer adapters
- Commercial: hosted cloud, SSO and SCIM, audit, private connector registry

## How to influence the roadmap

Comment on or upvote roadmap issues, open a Discussion, or write an RFC.
Maintainers review the roadmap at each release.
