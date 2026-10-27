# Architecture

A short tour of how data-in-motion fits together. Big changes to anything here
go through an RFC.

## The three layers

| Layer | Holds | Written by |
|---|---|---|
| **Raw** | Data as it arrived, plus load metadata | Connectors |
| **Clean** | Typed, deduplicated, checked rows | SQL models |
| **Ready** | Business tables and metrics for reports and agents | SQL models |

Every table is an Apache Iceberg table in object storage the user owns. Rows
that fail checks go to a quarantine table next to the target instead of
failing the run.

## Planes

**Control plane** (small, always on):
- API gateway: auth, roles, rate limits (axum)
- Project service: SQL and YAML files, kept in git
- Compiler: parses SQL, builds the dependency graph, makes incremental plans
- Scheduler and job queue (Postgres)
- Iceberg REST catalog
- Semantic layer and agent service
- Metering and audit

**Data plane** (scales with work, down to zero):
- Coordinator: splits a plan into stages and tasks
- Executors: run tasks on Apache DataFusion and Arrow
- Shuffle: Arrow Flight between executors, local disk first, object storage when full
- Ingest workers for connectors
- Maintenance workers: compaction, clustering, snapshot expiry

Executors run in separate pools (ingest, pipelines, BI, agents) so a large
pipeline never slows down a dashboard.

## How a pipeline run works

1. The scheduler creates a `Job`: project version, target table, snapshot range.
2. The compiler makes a plan that reads only snapshots added since the last run.
3. The coordinator splits it into stages; executors run tasks in parallel.
4. Failed tasks retry; a lost shuffle reruns only the stage that made it.
5. Results are written to staging files, then committed to Iceberg in one
   atomic commit. Readers see either the old table or the new one.

The same plan runs in one process on a laptop and across many machines in
production. Only the `Runner` changes.

## Handling very large data

The engine avoids reading data it doesn't need:

- Partition pruning and file statistics skip whole files
- Clustering keeps related rows together so statistics work
- Only the needed columns are read (Parquet is columnar)
- Bloom filters skip files for point lookups
- Incremental runs read only new snapshots

Writes stay fast at scale with right-sized files, compaction, deletion vectors
for updates, bucketed merges and hashed object prefixes.

## Speed and cold starts

- Result cache keyed by table snapshot: an unchanged table answers instantly
- Metadata cache for Iceberg manifests; local disk cache for hot files
- Executors are long-lived processes, not one per request. A pool stays warm
  while it is busy and shuts down after an idle timeout (about 10 minutes for
  BI and agents). The first query after idle waits for a machine to start.
- Hosted users share a warm fleet, so cold starts are rare. Self-hosted users
  choose "always ready" (keep one executor warm) or "lowest cost" (scale to zero).

## Cost

- Compute runs only when there is work
- Spot or preemptible machines for retryable work
- Incremental processing instead of full rebuilds
- Data stays in the user's storage, so there is no storage markup

## Connectors

A connector author writes two functions: `discover()` (what tables and columns
exist) and `read(position)` (the next batch after a saved position). The
framework does the rest:

- Saves the position in the same Iceberg commit as the data, so a crash never
  duplicates or loses rows
- Handles schema changes, retries, secrets and sandboxing

Ways to build one: YAML (no code), the Rust SDK, Wasm in any language, or an
adapter for existing Airbyte and Singer connectors. Every connector must pass
the conformance tests. The registry marks connectors as Certified, Community
or Experimental.

## Ways to connect

| Client | Protocol |
|---|---|
| BI tools (Tableau, Power BI, Metabase) | Postgres wire protocol |
| Python, notebooks, fast clients | Arrow Flight SQL, ADBC |
| Apps and scripts | HTTP API |
| AI assistants | MCP |
| Spark, Trino, Snowflake, Databricks | The same Iceberg tables |

## Agents

- **Chat agent:** answers questions using the semantic layer. Read-only. Runs
  every query with the user's own permissions. Marks answers from defined
  metrics as verified.
- **Builder agent:** drafts new models and connectors on an Iceberg branch,
  runs tests, and opens a change for a person to approve. It never writes to
  the main branch.
- **Guardrails:** permission checks, masking of personal data, per-user
  budgets, and a full audit log. A model gateway lets teams use any LLM,
  including one they host themselves.

## Crate map

See the table in [CONTRIBUTING.md](../CONTRIBUTING.md#repository-layout).
