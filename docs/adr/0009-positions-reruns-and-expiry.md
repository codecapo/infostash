# 0009. Table positions, same-id reruns and expiry at checkpoints

- **Status:** Accepted
- **Date:** 2026-11-10
- **Amends:** [0006](0006-ordered-job-commits.md)

## Context

ADR 0006 stores a connector's read position, and a model's last processed
snapshot per input, as a table property mirrored in the snapshot summary.
`Warehouse::append_if_head` writes one properties map into both places, so
every snapshot of a connector table keeps its own copy of the position. For
the CSV connector the position is the list of every file loaded so far, and
Iceberg keeps every snapshot's summary in every later `metadata.json`. The
file written by run n therefore holds n copies of a list that is n entries
long, and the metadata directory keeps every file ever written. With paths
of 65 characters and one new file per run:

| Hourly one-file runs | Latest `metadata.json` | Metadata on disk |
|---|---|---|
| 100 | 0.3 MB | 11 MB |
| 1000 | 35 MB | 12 GB |

Only the table property is read: `connectors::prepare` takes the position
from it, and no production code reads the summary copy. Four alternatives
were ruled out. Compressing `metadata.json` does not help a reader that
still inflates the whole file. A per-connector delta position cannot be
defined, because the position is opaque to everything except the connector.
Reading the head snapshot's summary as the position keeps the growth. Tags,
or `gc.enabled = false`, are not a retention rule: a tag keeps one snapshot
and not the ancestors a delta read walks back through, and with
`gc.enabled = false` nothing can ever be expired.

Two rules that the connector, runner and maintenance work need are also
missing. ADR 0006 makes a rerun skip the tables that already carry its job id,
but does not say that a job which failed after one of its commits is rerun at
all, or before what. No ADR covers snapshot expiry, although any engine that
expires under a table's own settings removes every snapshot older than five
days except the head and those a branch or tag points at, including the one a
consumer's checkpoint names, and `appends_between` then fails with the same
`NotFound` as a snapshot that was never an ancestor. RFC 0002 (proposed in
#102) gives each model job a record under `.infostash/jobs/` that says what it
committed, classes each error as retried or not, and asks for one exception to
the rerun rule and one addition to the retention rule; both are below.

## Decision

- **The `infostash.position` table property is the one authoritative
  position.** Every commit, by a connector or a model and to a target or a
  quarantine table, sets `infostash.job-id` as a table property and in the
  snapshot summary. A connector commit also sets `infostash.position` as
  a table property only, with no digest of it in the summary: the job id
  is enough to tie a snapshot to the commit that moved the position. A model's
  per-input checkpoints stay mirrored in the summary as ADR 0006 says. They
  grow with the number of inputs, not with the number of runs, and the copy
  in the summary records which input snapshots each target snapshot was
  built from.
- **A run refuses to start when the table disagrees with its head.** If the
  `infostash.job-id` table property and the `infostash.job-id` in the head
  snapshot's summary differ, or one is present and the other is not, the
  table was changed outside Infostash, as after a rollback by another
  engine, and the position can no longer be trusted. `connectors::prepare`
  (#81) and the runner's precheck over every table the job writes (#13)
  fail with a new `Error::Diverged`, naming the table and both ids, before
  anything is written. `Diverged` maps to `Retry::Never` (RFC 0002). A
  person sets `infostash.job-id`, and for a connector table
  `infostash.position`, to match the head, then runs again; the command
  for that is not yet ticketed. When `Diverged` fires after a partial
  commit the abandon exception below applies, so the run after the fix is
  a new job. A table with no snapshot and no property is not a
  disagreement.
- **A job that failed after any of its commits is rerun under the same job
  id**, skipping the tables that already carry that id, before any new job
  for that target is created. The skip rule of ADR 0006 is unchanged. One
  exception, from RFC 0002: the job is abandoned, and the next run prepares
  new work under a new id, when its rerun finds a changed head, schema or
  partition spec on a table it has not committed to, when a run fails with
  a `Retry::Never` error after a partial commit, or when the project
  version it was prepared from is no longer the current one.
- **Snapshot expiry never goes past the oldest consumer checkpoint.** The
  cutoff for a table is the timestamp of the oldest snapshot that any
  consumer's checkpoint still names, read from the checkpoint table properties
  consumers already store; there is no separate watermark store. A table with
  no consumer is expired under its own settings. The consumers of a table are
  the models that read it in the project, so a reader outside the project gets
  no guarantee. The pinned iceberg-rust keeps every snapshot whose timestamp
  is at or after the cutoff and every later ancestor on the branch, as the
  last test of #91 confirms, so a checkpoint snapshot and everything after it
  survive. Expiry also keeps every snapshot committed by a model job whose
  record still exists without a `Committed` outcome, so that a rerun can find
  its job id on each table. A connector job keeps no record; its rerun runs
  before any new job for the table, so the head it needs is never expired. The
  expiry properties that #91 writes at `create_table` are a floor under any
  engine that expires on its own, not the guarantee. The rule is decided now
  and implemented with the Phase 1 snapshot expiry.
- **A reader whose `from` snapshot was expired gets its own error.** #89
  adds a variant of `infostash_model::Error` that names truncated history,
  distinct from the `NotFound` of a snapshot that was never an ancestor, and
  it is never retried. Once a full refresh exists (it needs the replace
  commit of ADR 0007), the plan-time fallback of ADR 0008 takes over.

## Consequences

- #80 splits `append_if_head` into a summary map and a table-properties
  map and takes the position out of the summary; #81 implements the connector
  skip and the `Diverged` check in `prepare`, with the rollback test
  below; #12 and #13 implement the rerun rule, the model skip and the
  runner precheck; #91 writes the expiry floor; #40 removes the orphan
  files of abandoned jobs and the old `metadata.json` files that nothing
  deletes today.
- Three tests check the rules: the size of each `metadata.json` grows
  linearly over 200 one-file commits; a run after the head is rolled back
  by hand through the catalog refuses with `Diverged` and writes nothing;
  and a checkpoint snapshot and every later one survive expiry at the
  checkpoint's timestamp, while expiry past the checkpoint gives the
  truncated-history error and the planned fallback.
- Rows that an abandoned job committed to side tables stay there and are
  hidden by the reader rule of ADR 0006. The step that removes them is
  not yet ticketed.
- A consumer that never runs holds expiry back forever on every table it
  reads. Removing a model from the project removes its checkpoints with it.
- Branch writes will need a position key per branch, since one
  `infostash.position` property names one line of history.
