# Architecture

A short tour of how Infostash fits together. Big changes to anything here
go through an RFC. The reasons behind key choices are recorded as
[architecture decision records](adr/README.md).

![Infostash architecture: clients, control plane, data plane and object storage](architecture.svg)

Blue arrows follow one pipeline run: the compiler plans it, the scheduler
hands the coordinator a job, executors read only new snapshots and write
staged files, and the coordinator makes one atomic Iceberg commit.

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

## What we build and what we reuse

We reuse proven open-source foundations and build the parts that make
Infostash useful, fast and cheap. Reusing the foundations keeps data in open
formats any engine can read, and lets us spend our effort where users feel it.

**Reused foundations**

| Part | Project | Why we reuse it |
|---|---|---|
| In-memory columnar data | Apache Arrow | The standard format; zero-copy exchange with other tools |
| Query engine (parse, optimize, execute SQL) | Apache DataFusion | Fast, extensible, and years of work by a large community |
| Table format | Apache Iceberg | Open tables on object storage, readable by Spark, Trino, Snowflake and others |
| File format | Apache Parquet | Columnar, compressed, supported everywhere |

Many products are built the same way: InfluxDB 3, GreptimeDB and Spice AI on
DataFusion, Dremio on Arrow and Iceberg. When we need something these projects
lack, we build it, then offer it upstream.

**What Infostash builds**

1. **Pipeline compiler:** turns SQL models into a dependency graph and
   incremental plans that decide what to run and what to skip.
2. **Incremental engine:** processes only data added since the last run.
   Iceberg's Rust library has no incremental scan, so we read the snapshot
   history and manifests ourselves (`crates/storage`).
3. **Data quality:** row checks that send bad rows to a quarantine table
   instead of failing the run.
4. **Distributed execution:** stages, tasks, shuffle, retries, compute pools
   and scale to zero, so the same plan runs on a laptop or many machines.
5. **Connector framework:** saves each source's read position in the same
   Iceberg commit as its data, so a crash never loses or duplicates rows.
6. **Caching and cost control:** results cached per table snapshot, warm
   executor pools, spot machines for retryable work.
7. **Semantic layer and agents:** a read-only chat agent answering from
   defined metrics with the user's own permissions, and a builder agent that
   works on a branch and needs approval.
8. **Developer experience:** `infoctl`, project files, the web UI and a
   five-minute quick start.

The hard engineering, and the reason users choose Infostash, is in 1, 2, 4
and 5: getting incremental, distributed and exactly-once processing right at
petabyte scale is what makes it cheaper and faster.

## Crate map

See the table in [CONTRIBUTING.md](../CONTRIBUTING.md#repository-layout).
