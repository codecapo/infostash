# 0004. Tasks stage files; only the job commits

- **Status:** Superseded by [0006](0006-ordered-job-commits.md)
- **Date:** 2026-10-27 (recorded 2026-10-29)

## Context

A job runs as many tasks, possibly on many machines. Tasks fail, retry and
sometimes run twice. Readers must never see a half-written table, and a
crashed connector must never lose or duplicate rows.

## Decision

- A task writes only to its own staging or shuffle location. It never
  touches a table. Each attempt of a task stages under its own path, which
  includes the task id and the attempt number.
- The coordinator records which attempt of each task succeeded. The job
  commit adds only the files that attempt reported, so output from failed or
  duplicate attempts is never committed.
- When every task has finished, the job makes **one** Iceberg commit that
  adds all staged files. Iceberg commits are atomic, so readers see the old
  table or the new one, never a mix.
- A connector's read position is saved in the same commit as the data it read.

These are listed as invariants in [AGENTS.md](../../AGENTS.md).

## Consequences

- Tasks are safe to retry or run twice: only one attempt per task is
  committed, however many wrote files.
- Failed attempts, and a crash before the commit, leave orphan staged files
  but no visible change. We need a cleanup step for orphan files (part of
  maintenance workers).
- One commit per job means one writer per table at a time. Concurrent jobs on
  the same table must be serialised by the scheduler or retried on conflict.
- Exactly-once ingest comes from the position living in the commit, not from
  the source.
