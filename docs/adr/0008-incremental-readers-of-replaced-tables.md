# 0008. Incremental readers of a replaced table fall back to a full refresh

- **Status:** Accepted
- **Date:** 2026-11-09

## Context

An incremental model reads only the snapshots added to its inputs since its
last run. That works while every new snapshot is an append: the rows those
snapshots added are exactly the rows to process. A replace commit
([ADR 0007](0007-hand-built-replace-commit.md)) breaks this. An `overwrite`
snapshot removes files as well as adding them, so the rows it added do not
describe the change. Today `appends_between` in `crates/storage` refuses to
read a range that holds a non-append snapshot. Retrying cannot get past
that refusal, since running the job again meets the same snapshot, yet it
surfaces as a plain storage error, which nothing tells apart from the
transient failures a runner retries.

A job carries the snapshot range pinned for each input, and
[ADR 0006](0006-ordered-job-commits.md) makes a rerun read the same pinned
snapshots as the first attempt, so the choice of how to read a replaced
input belongs before the job exists, not inside it. RFC 0001 lets a model
be rebuilt in full only when its author declares `@full_refresh`; this
rule adds a second cause, decided at plan time.

## Decision

Before creating a job, the scheduler checks the operation of every
snapshot in the range it would pin for each input. If any input holds a
snapshot that is not an append, or the input's `from` snapshot has been
expired, the scheduler pins every input of that model with `from = None`,
so the model is rebuilt from all of its inputs in full, reports the
fallback and its cause in the run output, and has the job replace its
target instead of appending to it. This is a new job with newly pinned
inputs; nothing is patched into an existing job.

The refusal in `appends_between` stays as a guard. Reaching it ends the
job as a failure that is not retried, with a message saying the plan-time
check missed that input. Until the plan-time check (#89) lands, the
compile-time rule that #73 adds to RFC 0001 and #84 enforces applies
instead: no incremental model reads a table that declares `@unique_key`
or `@full_refresh`, because those are the tables that get replaced. The
rule matters from the first replace commit (#88, used by #35 and #90),
and #89 removes it.

## Consequences

- A model downstream of a deduplicated Clean table or of a fully refreshed
  table is rebuilt from scratch whenever that table was replaced since the
  model's last run. That is correct and costs a full read of every input,
  so for these models a run no longer processes only the rows that
  changed, as the README and the 0.1.0 roadmap promise; those statements
  need that qualification (#94). Reading a replace as a set of changes is
  a later decision.
- The fallback cascades. The rebuilt target is itself an `overwrite`
  snapshot, so every model downstream of it falls back on its next run,
  and everything downstream of a `@full_refresh` model is rebuilt after
  every run in which that model committed. How often a merge triggers this
  is #35's call: a merge that removes no file can commit an append.
- Compaction (Phase 1) also writes snapshots that are not appends while
  changing no rows. Before it lands, the check needs a way to tell such a
  snapshot from a replace, or every compaction rebuilds everything
  downstream.
- #73 records in RFC 0001 that a model can be rebuilt in full without
  declaring `@full_refresh`, and why.
- The run output names each model that fell back and the input that
  caused it, so a slow run has a visible reason.
- The check reads snapshot summaries only, not data, so it costs little
  at plan time.
