# 0006. Jobs commit each table once, in order, under a job id

- **Status:** Accepted
- **Date:** 2026-10-30
- **Supersedes:** [0004](0004-stage-then-atomic-commit.md)

## Context

ADR 0004 said a job makes **one** Iceberg commit that adds all its staged
files. R&D spikes against the pinned iceberg-rust (rev `5b82ab9`) with the
SQLite catalog showed three things it did not account for:

- **There is no multi-table commit.** `Catalog::update_table` takes one
  table, and `TableCommit::build` is `pub(crate)`. The REST catalog's
  `V1_COMMIT_TRANSACTION` endpoint exists in the code but nothing calls it.
  A job that writes a target and its `__quarantine` table (#12) cannot make
  both changes in one atomic commit.
- **Stale commits are rebased silently.** `Transaction::do_commit` reloads
  the table and applies the actions again on top of the current head. A run
  that read a position, and then lost a race to another run, does not
  conflict: it commits and duplicates rows. "Retry on conflict" never fires.
- **Files are committed where they were written.** `fast_append` adds files
  by path and does not move them, so a staging area that files are later
  moved out of would cost a copy on object stores.

The parts of 0004 that the spikes proved still hold: tasks stage under an
attempt-specific name, the coordinator commits only the winning attempt of
each task, and a read position committed with its rows survives a crash
between read and commit.

## Decision

- A task writes only to its own staging or shuffle location. Staged data
  files live in the table's data location under a name that includes the
  job, task and attempt. Each attempt also writes a staging manifest that
  lists its data files.
- The coordinator records which attempt of each task succeeded and commits
  only the files from those manifests.
- A job makes **one commit per output table**. Each commit is atomic and
  carries the job id as `infostash.job-id` in its snapshot summary.
- When a job writes several tables, they commit in a fixed order: side
  tables such as quarantine first, the main target last. The target's
  commit marks the job as done. If the target gets no rows, it still gets a
  snapshot carrying the job id: a `fast_append` with no data files but with
  snapshot properties succeeds and adds a snapshot with no rows (the spike
  for this ADR checked it).
- A rerun of a job with the same id commits only to tables that do not yet
  have that job id, so reruns are idempotent.
- A connector's read position, and a model's last processed snapshot per
  input, are stored as a table property and mirrored in the snapshot
  summary. They are written in the same commit as the rows they describe.
- Each commit checks that the table head is still the one the job read, and
  fails if it moved. iceberg-rust would rebase the commit instead, so we wrap
  the catalog's compare-and-swap to do this.

`Warehouse::append` predates this decision: it commits one table with a
plain `fast_append`, with no job id and no head check. The commit path above
is built in #13, and the quarantine ordering in #12.

We revisit multi-table atomic commits when iceberg-rust calls
`V1_COMMIT_TRANSACTION` against a REST catalog that supports it.

## Consequences

- Tasks remain safe to retry or run twice, and a crash before a table's
  commit leaves no visible change to that table.
- Between a job's quarantine commit and its target commit, a reader can see
  quarantine rows whose target rows are not there yet. Readers that need a
  consistent view should trust only quarantine rows whose job id is in the
  target's history.
- A rerun must read the same pinned input snapshots as the first attempt so
  that it produces the same split between target and quarantine.
- Concurrent jobs on one table fail at commit instead of duplicating rows.
  The scheduler should still run one writer per table at a time.
- A run that read nothing new skips its commits. A `fast_append` with no
  data files and no properties is an error in iceberg-rust.
- Failed attempts and crashes leave orphan files. Their names identify the
  job, task and attempt, so a cleanup step (#40) can find them.
- The staging manifest format is a contract between tasks and the
  coordinator. It needs a version field.
