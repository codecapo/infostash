# RFC 0002: Job and runner contracts

- **Author(s):** @codecapo
- **Status:** Draft
- **Discussion:** [issue #74](https://github.com/codecapo/infostash/issues/74)
- **Created:** 2026-11-10

## Summary

This RFC fixes the contracts between a job, the tasks that run it and the
commits it makes, so that the in-process runner of Phase 0 and the remote
runners that come later share one set of types. It replaces the single
snapshot in `JobOutcome::Committed` with one commit record per output
table, gives every `Error` a retry class, adds a `Data` error for bad input
values, defines what a `JobId` identifies and adds a separate run id, splits
`Warehouse::append_if_head` into public `stage` and `commit_staged` halves
that the runner calls at different times, makes `run_task` and `Runner::run`
async with a `TaskContext` in and a `TaskOutput` out, and settles the shape
of the `QueryContext` that `engine::query` takes. The types involved become
`#[non_exhaustive]` so later phases can add fields without another round of
breaking changes.

## Motivation

ADR 0006 settled that a job makes one commit per output table, side tables
first and the target last, with every snapshot carrying the job id, and
that a rerun under the same id commits only to tables that do not yet carry
it. The types in `crates/model`, `crates/jobs`, `crates/dist` and
`crates/storage` predate that decision and still describe a job as one
commit to one table:

- `JobOutcome::Committed` carries one snapshot, so a job with a quarantine
  table has nowhere to report its second commit, and a rerun that finds a
  table already committed has no way to say so. `Failed` carries a message
  and a bare `retryable` flag, which cannot say whether to retry a task, the
  whole job or nothing.
- `Error` has no retry rule. `run_in_process` retries every error, so a
  task that fails on a bad cast is run three times, and a `Conflict`, which
  only a fresh preparation can resolve, is retried in place. The mapping
  from iceberg-rust errors drops the kind of the error, so storage I/O
  failures and invalid-data errors look the same.
- `JobId` is a bare number. Nothing says what it identifies, when it is
  assigned, where it is stored or how a rerun after a crash finds it. There
  is no run id, so two attempts to run one job cannot be told apart in file
  names or logs.
- `Warehouse::append_if_head` stages and commits in one call. The runner
  needs to stage from a task and commit from the coordinator, which are
  different places even when both are in one process.
- `ExecutionStage::run_task` is synchronous and returns only row counts.
  Storage and DataFusion are async, and a task that stages files has to
  hand them to whoever commits.
- `engine::query` takes no caller identity, so authorisation cannot be added
  later without changing its signature.

If nothing changes, #12, #13 and #82 each invent part of these contracts,
and the first remote runner has to break them all again.

## Design

### Types in `crates/model`

Everything below lives in `crates/model` unless stated otherwise. The crate
keeps no dependencies. Every struct and enum shown is `#[non_exhaustive]`
except `JobId` and `RunId`, which stay plain newtypes over `u64` so that
tests and the CLI can write `JobId(7)`. A `#[non_exhaustive]` struct cannot
be built with a literal outside its crate, so each one has a constructor,
and structs whose fields are all counters derive `Default` so a caller
writes `let mut s = TaskStats::default(); s.rows_in = 3;`.

```rust
/// Identifies one unit of work. See "What a JobId identifies".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct JobId(pub u64);

/// One execution of a job. Never reused, even across reruns of one job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RunId(pub u64);

#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct Job {
    pub id: JobId,
    pub project: ProjectVersion,
    pub target: TableRef,
    /// Snapshot range to read for each upstream table.
    pub inputs: Vec<(TableRef, SnapshotRange)>,
    /// State of each output table the job was prepared against. The
    /// target is always present; side tables such as quarantine follow.
    pub expected: Vec<(TableRef, TableState)>,
    /// Start time of the job in milliseconds since the Unix epoch. Every
    /// clock read inside the job returns this value.
    pub started_at: u64,
    pub pool: Pool,
}

/// What a job expects to find on an output table when it commits.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableState {
    pub head: Option<SnapshotId>,
    pub schema_id: i32,
    pub partition_spec_id: i32,
}

impl TableState {
    pub fn new(head: Option<SnapshotId>, schema_id: i32,
               partition_spec_id: i32) -> Self;
}

impl Job {
    /// `expected` starts with `(target, target_state)`.
    pub fn new(
        id: JobId,
        project: ProjectVersion,
        target: TableRef,
        target_state: TableState,
        pool: Pool,
        started_at: u64,
    ) -> Self;
    pub fn with_input(self, table: TableRef, range: SnapshotRange) -> Self;
    pub fn with_expected(self, table: TableRef, state: TableState) -> Self;
}
```

`Job` carries neither a run id nor an attempt number. A job is the same
work however many times it runs; the run id and the attempt belong to the
`TaskContext` and the `AttemptKey` below.

`started_at` is the job's clock. Every function that reads the clock
inside a model (`now()`, `current_date` and the like) returns this value,
so two runs of one job see the same time. The revision of RFC 0001 under
#73 states the same rule from the model's side; this RFC is the one the
runner implements, and it stands whichever lands first.

```rust
/// Why a job stopped and what the caller may do about it.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Retry {
    /// Running the same thing again cannot help.
    Never,
    /// The task may run again in place, on the same pinned inputs.
    Task,
    /// Only a fresh preparation of the whole job can help.
    Job,
}

#[non_exhaustive]
#[derive(Debug)]
pub enum Error {
    Parse(String),
    Plan(String),
    /// An input row holds a value the model cannot process: a cast that
    /// fails, a string that does not parse, arithmetic that overflows, a
    /// division by zero. The message names the model.
    Data(String),
    NotFound(String),
    /// The warehouse failed. `retryable` is derived from the iceberg-rust
    /// error kind, or set by `crates/storage` for its own errors; see
    /// "Errors from iceberg-rust and DataFusion".
    Storage { message: String, retryable: bool },
    /// The engine failed for a reason outside the data: I/O, a timeout,
    /// memory, an internal error.
    Execution(String),
    /// A commit lost a race: a table changed after the job was prepared.
    Conflict(String),
    /// An exchange input this task needs no longer exists. Reserved;
    /// Phase 0 has no exchange, so only the reserved `TaskContext::input`
    /// returns it.
    InputLost(String),
}

#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    Parse, Plan, Data, NotFound, Storage, Execution, Conflict, InputLost,
}

impl Error {
    pub fn kind(&self) -> ErrorKind;
    pub fn retry(&self) -> Retry;
}
```

`Error::retry` is total over the variants:

| Variant | `retry()` |
| --- | --- |
| `Parse`, `Plan`, `Data`, `NotFound` | `Never` |
| `Storage { retryable: true, .. }` | `Task` |
| `Storage { retryable: false, .. }` | `Never` |
| `Execution` | `Task` |
| `Conflict` | `Job` |
| `InputLost` | `Job` |

`Execution` is `Task` because, once data errors have their own variant,
what remains in it is the environment: I/O, timeouts, memory and internal
failures. A retry on a laptop costs a few seconds and often succeeds; a
retry of a `Data` error never can. `InputLost` is `Job` for now because a
single-stage job has no narrower unit to rerun; a `Retry::Stage` class can
be added when a multi-stage runner exists, since `Retry` is
`#[non_exhaustive]`. The doc comment on `RetryPolicy` in `crates/dist` and
step 4 of the overview in `docs/architecture.md` describe that later
runner, which reruns only the stage that made a lost exchange output; #94
notes that they describe the target, not Phase 0.

The `Display` prefixes stay as they are (`parse error: `, `plan error: `,
`storage error: `, `execution error: `, `not found: `, `conflict: `) and
`Data` prints `data error: `, `InputLost` prints `input lost: `.

```rust
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitRecord {
    pub table: TableRef,
    pub snapshot: SnapshotId,
    /// True when this run committed nothing because the table already
    /// carried the job id from an earlier run of the same job.
    pub already_committed: bool,
}

impl CommitRecord {
    pub fn committed(table: TableRef, snapshot: SnapshotId) -> Self;
    pub fn already_committed(table: TableRef, snapshot: SnapshotId) -> Self;
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub enum JobOutcome {
    /// One record per output table, in commit order: side tables first,
    /// the target last.
    #[non_exhaustive]
    Committed { commits: Vec<CommitRecord>, metrics: JobMetrics },
    /// No input had a snapshot past its checkpoint. Decided by the
    /// preparer, before a job record exists; nothing was written.
    NoChange,
    #[non_exhaustive]
    Failed { kind: ErrorKind, message: String, retry: Retry },
}

impl JobOutcome {
    pub fn committed(commits: Vec<CommitRecord>, metrics: JobMetrics)
        -> Self;
    pub fn failed(error: &Error) -> Self;
}
```

The name `CommitRecord` is chosen over `TableCommit` so that it does not
collide with iceberg-rust's `TableCommit`, which `crates/storage` imports.

`NoChange` is narrow on purpose. ADR 0006 says an empty target still gets a
snapshot, so a job whose inputs have new snapshots but whose model produces no
rows returns `Committed` with a record per table. A rerun that finds every
table already carrying its id also returns `Committed`, with every record
flagged `already_committed` and `JobMetrics::default()` for the metrics. Such
a rerun only happens when the process stopped between the last commit and the
rewrite of the job record, so the metrics of those rows are lost; Phase 0 does
not recover them; a queue that keeps outcomes, in a later phase, will. A rerun
that commits to some tables and skips the rest reports the stats of the tasks
it ran, which in Phase 0 is every task, since one stage produces every table.
That closes the gap #81 describes, where a rerun after a lost result reported
`NoChange` and could not be told from a job that had nothing to do.

`TaskStats` stays in `crates/dist` and gains the counts the runner needs:

```rust
// crates/dist
#[non_exhaustive]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TaskStats {
    pub rows_in: u64,
    pub rows_out: u64,
    pub rows_quarantined: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub bytes_spilled: u64,
}

impl TaskStats {
    pub fn add(&mut self, other: &TaskStats);
}
```

`JobMetrics` is built by the runner: `rows_read` and `bytes_read` sum
`rows_in` and `bytes_in` over the stages that read tables rather than an
exchange, `rows_written`, `rows_quarantined` and `bytes_written` sum
`rows_out`, `rows_quarantined` and `bytes_out` over the stages that stage
files, and `compute_millis` is measured by the runner. With one stage in
Phase 0 every sum has one term. The struct literal that builds `TaskStats`
in the `Fake` stage of the dist tests gains the new fields or becomes
`TaskStats::default()` plus assignments.

```rust
/// Who asked for a query or a job to run.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Principal {
    /// The operating system user running this process.
    Local,
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryContext {
    /// The principal this process runs as.
    pub local: Principal,
    /// The principal a service acts for. `None` until there are services.
    pub on_behalf_of: Option<Principal>,
}

impl QueryContext {
    pub fn local() -> Self;
}
```

`engine::query`, which #46 adds, takes `&QueryContext` (#95), and the
`infoctl query` command builds it with `QueryContext::local()`. Jobs in
every `Pool` run as `Principal::Local` in Phase 0; a principal field on
`Job` is left until a pool runs under its own service account, and is
then an addition, not a
change.

### Staged files

```rust
/// Names one attempt of one task of one run of one job.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AttemptKey {
    pub job: JobId,
    pub run: RunId,
    pub stage: u32,
    pub partition: u32,
    pub attempt: u32,
}

impl AttemptKey {
    pub fn new(job: JobId, run: RunId, stage: u32, partition: u32,
               attempt: u32) -> Self;
    /// Stage 0, partition 0, attempt 1: the key of work that has one
    /// task and is never retried in place, such as a connector load.
    pub fn single(job: JobId, run: RunId) -> Self;
}
```

Stages and partitions count from 0, as `StageId` and `TaskId` do today;
attempts count from 1, as `run_in_process` does today, so `attempt` in
`AttemptKey` and `TaskContext` is 1 on the first try.

```rust

/// One task attempt's output for one table, written to the table's data
/// directory and not yet referenced by any snapshot. This is the staging
/// manifest of ADR 0006 in its in-memory form.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedFiles {
    /// Layout version of this record. 1 for the layout in this RFC.
    pub version: u32,
    pub table: TableRef,
    pub attempt: AttemptKey,
    /// Iceberg schema id and partition spec id of the table when the
    /// files were written. `commit_staged` looks both up in the table's
    /// metadata to read `data_file` back, and refuses the record when
    /// either is no longer current.
    pub schema_id: i32,
    pub partition_spec_id: i32,
    pub files: Vec<StagedFile>,
}

impl StagedFiles {
    /// `version` is set to the current layout version.
    pub fn new(table: TableRef, attempt: AttemptKey, schema_id: i32,
               partition_spec_id: i32) -> Self;
    pub fn push(&mut self, file: StagedFile);
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedFile {
    pub path: String,
    pub rows: u64,
    pub bytes: u64,
    /// The iceberg `DataFile`, serialised by iceberg-rust's own JSON form
    /// (`serialize_data_file_to_json`), so that this record can be
    /// written to disk without `crates/model` depending on iceberg.
    pub data_file: String,
}

impl StagedFile {
    pub fn new(path: String, rows: u64, bytes: u64, data_file: String)
        -> Self;
}
```

`StagedFiles` is plain data so that `crates/dist` can carry it in a task's
output without depending on `crates/storage`, and so that the same record
is what `stage` writes to disk as the per-attempt staging manifest that
ADR 0006 requires. `crates/model` has no serde dependency; `crates/storage`
serialises the record to JSON with the field names above and gains
`serde_json` as a direct dependency for it. The `version` field lets a
build tell a manifest it understands from one it should leave alone.

### Storage: `stage` and `commit_staged`

`crates/storage` gains two public methods built from today's private halves
of `append_if_head` (the property and head prechecks, `write_files` and
`commit_if_head`):

```rust
impl Warehouse {
    /// Write `batches` to the table's data directory under names that
    /// carry `attempt`, write the attempt's staging manifest next to
    /// them, and return the manifest's contents. Nothing is committed.
    /// Fails before any file exists when a batch does not match the
    /// table's columns.
    pub async fn stage(
        &self,
        table: &TableRef,
        attempt: AttemptKey,
        batches: &[RecordBatch],
    ) -> Result<StagedFiles>;

    /// Append every file in `staged` to `table` in one snapshot that
    /// carries `properties`, if and only if the table's head is still
    /// `expected_head`. Several `StagedFiles` for one table, from several
    /// tasks or from the winning attempt of each, make one snapshot.
    /// Deletes the staging manifests of `staged` once the commit lands;
    /// a failed delete is logged and does not fail the commit.
    pub async fn commit_staged(
        &self,
        table: &TableRef,
        expected_head: Option<SnapshotId>,
        staged: &[StagedFiles],
        properties: &HashMap<String, String>,
    ) -> Result<Commit>;
}
```

Staged data files are named

```text
{job}-{run}-{stage}-{partition}-{attempt}-{n:05}-{uuid}.parquet
```

where `n` counts files within the attempt and `uuid` is a fresh random
suffix, as #82 requires: a reused key must never overwrite a committed
file, and iceberg-rust's local file writer truncates. The staging manifest
of an attempt is written after its data files, in the same directory, as
`{job}-{run}-{stage}-{partition}-{attempt}.staged.json`, with no random
suffix: a key names one attempt, so a manifest under a reused key belongs
to a bug, and overwriting it costs nothing because only the attempt that
wrote it and #40 read it. Iceberg's own manifests, manifest lists and
`metadata.json` keep iceberg-rust's names.

`stage` checks neither the table's head nor its schema. Files from a job
whose commit later loses its race are orphans, like the files of a failed
attempt, and #40 finds both by name and age. A staging manifest older than
#40's minimum age goes too, whether or not its files are referenced: a
crash between the commit and the delete leaves a manifest for committed
files, and the manifest is not needed once the files are in a snapshot.
The runner checks the expected state of every output table before it
runs any task, so a job that is already stale stages nothing.

`commit_staged` only appends. Replacing rows for `@full_refresh` or
merging on `@unique_key` (ADR 0007) is a separate
operation that #88 defines; it will take the same `StagedFiles`.

`commit_staged` checks, before touching the catalog, in this order:

1. every property key starts with `infostash.`, and the call has either
   files or properties, as `append_if_head` checks rows and properties
   today; a commit with no files and the job id property is valid, because
   ADR 0006 gives an empty target a snapshot;
2. every record is for `table` and has a `version` this build understands;
   a record that fails this is a `Storage { retryable: false }` error,
   because no rerun fixes it;
3. every record's `schema_id` and `partition_spec_id` are the table's
   current ones; one that is not is `Conflict`, because the table changed
   after the job was prepared, which is exactly what `Conflict` means.

Then it reads each `data_file` back with the schema and partition type those
ids name, runs the head check through the wrapped catalog as `append_if_head`
does today, widened so that the guard compares the head, the current schema id
and the default partition spec id on the load that `update_table` itself
makes. The SQL catalog then swaps the metadata location only if it is still
the one that load returned, so a metadata-only change that lands after the
check, which moves no snapshot, fails the swap and is mapped to `Conflict`
like a moved head. A caller that gets `Conflict` after its own commit may have
landed, for example a rerun after a crash, repeats the check #81 defines
(`Warehouse::committed_by`) before giving up, and reports the table as
`already_committed` when it finds its own id.

The job id, the run id and the attempt go into file names only; the
snapshot carries `infostash.job-id` and the other properties the caller
passes, as today. `append_if_head` stays as a convenience that calls
`stage` with an `AttemptKey` the caller supplies and then `commit_staged`,
so the CSV connector keeps one call. The connector builds its key with
`AttemptKey::single(job, run)`; `connectors::prepare` takes the `RunId`
next to the `JobId` (#82), both assigned by the caller, as for every job.
`Prepared::commit` keeps returning `Result<Option<Commit>>` until #81,
which makes it `Result<Option<CommitRecord>>`: `None` still means nothing
to load, and an already-committed rerun is reported through the same flag
as everywhere else.

Model runs never commit through datafusion-iceberg's `IcebergCommitExec`.
That operator commits with a plain `fast_append`, with no head check and
no snapshot properties, so it can neither carry the job id nor refuse a
stale commit. The engine executes a model's plan to record batches, the
task hands them to `stage`, and the coordinator commits with
`commit_staged`.

### Errors from iceberg-rust and DataFusion

`crates/storage` maps every iceberg-rust error by kind:

| iceberg-rust `ErrorKind` | `Error` |
| --- | --- |
| `TableNotFound`, `NamespaceNotFound` | `NotFound` |
| `CatalogCommitConflicts`, a tripped head guard | `Conflict` |
| `Unexpected` | `Storage { retryable: true }` |
| `DataInvalid`, `PreconditionFailed`, `FeatureUnsupported`, `TableAlreadyExists`, `NamespaceAlreadyExists` | `Storage { retryable: false }` |
| any other kind | `Storage { retryable: e.retryable() }` |

The rows apply in order, so a conflict is `Conflict` whatever its flag.
iceberg-rust sets its own retryable flag only on
`CatalogCommitConflicts`, and it converts every `std::io::Error` into
`Unexpected` without the flag. The flag alone would therefore make every
storage I/O failure `Never`, which is the opposite of what the ticket
asks. `Unexpected` is the kind iceberg-rust uses for I/O, poisoned locks
and arrow failures inside the library, all environment rather than data,
so it is `Task`. `ErrorKind` is `#[non_exhaustive]`, so a kind added by a
later iceberg-rust falls into the last row and keeps whatever flag the
library gives it; the mapping test lists every kind this RFC names, so a
new one is noticed when the dependency is updated.

Errors that `crates/storage` raises itself follow the same split between
environment and data: a failed read or write of the local filesystem
(`open_local`, writing or deleting a staging manifest) is
`Storage { retryable: true }`; a validation failure (a property key
without the prefix, nothing to commit, a batch whose columns do not match
the table, a manifest that does not parse) is `Storage { retryable: false }`.

`crates/engine` maps a `DataFusionError` by its root cause, found with
`DataFusionError::find_root`, so that a `Context` wrapper never hides the
class:

| Root | `Error` |
| --- | --- |
| `SQL` | `Parse` |
| `Plan` naming a table that does not exist (as #46 maps it) | `NotFound` |
| `Plan`, `SchemaError`, `Configuration`, `NotImplemented` | `Plan` |
| `ArrowError(SchemaError)`, `ArrowError(InvalidArgumentError)` | `Plan` |
| `ArrowError(CastError)`, `ArrowError(ParseError)` | `Data` |
| `ArrowError(DivideByZero)`, `ArrowError(ArithmeticOverflow)` | `Data` |
| `ResourcesExhausted`, `IoError`, `ObjectStore`, `Internal`, the rest | `Execution` |

The `Data` message names the model when there is one (an ad hoc
`engine::query` has none), for example

```text
data error: model clean.orders: Cast error: Cannot cast string 'x' to Int64
```

An arrow schema or argument error at execution time means the plan and the
data disagree about shape, which a rerun cannot change, so it is `Plan` and
`Never`. Other `ArrowError` variants, `ComputeError` among them, stay
`Execution` until a case shows they belong in `Data`; a wrong `Execution`
costs a retry, a wrong `Data` hides a transient failure.
`ResourcesExhausted` is `Execution` because Phase 0 sets no memory limit,
so the failure depends on what else the machine is doing; once a task runs
under `Limits::memory_bytes` the same input will exhaust the same limit
every time, and a later revision may move it to `Never`. The checks RFC
0001 defines route most bad values to quarantine before they can fail a
cast; `Data` covers what still reaches the engine, such as an overflow in
an expression after the checks.

### What a `JobId` identifies

A `JobId` identifies one unit of work, fixed by:

- the project version,
- the target table,
- the pinned inputs: a snapshot range per upstream table for a model, or
  the position range a connector read, and
- the expected state of each output table: its head, schema id and
  partition spec id, and
- `started_at`.

Two jobs that differ in any of these are different work and get different
ids. The id is assigned by the control plane (in Phase 0, `infoctl run`)
as a random `u64` and is never reused. A rerun keeps the id and gets a new
`RunId`.

Preparation is the step that turns a project version and a target into a
`Job`: it reads the target's checkpoints, pins the input ranges, reads the
state of every output table and assigns the ids. `crates/jobs` gains a
`prepare` function for it, which `infoctl run` calls before `Runner::run`;
it returns `NoChange` when no input has a snapshot past its checkpoint,
so #13's criterion that the runner returns `NoChange` moves to this
function. A connector load decides the same inside `Prepared::commit`.

A connector load (`connectors::prepare` and `Prepared::commit`) is a job
with one output table and one commit, so it has nothing to repair after a
crash: either its commit landed, which the next load sees in
`infostash.position`, or nothing did. It takes an id and a run id like any
job, writes the id into its snapshot as every job does, and keeps no job
record. Its inputs are pinned by `Prepared`, which holds the position
range in memory; a new preparation after a crash is new work with a new
id even when it reads the same bytes, so the skip that #81 adds helps a
caller that lost the result of a commit, not one that crashed before it.

A model job commits to several tables, so a crash between commits leaves work
that must be finished under the same id (ADR 0006). For that the control plane
keeps a job record. In Phase 0 the record is a JSON file at
`.infostash/jobs/<job-id>.json` under the project directory, beside the
default warehouse path RFC 0001 gives, and it stays there when the warehouse
path is configured elsewhere, because it belongs to the project that runs the
job. `infoctl run` writes it with `serde_json`, each time to a temporary file
in the same directory that is synced and renamed over the record, so a crash
leaves the old record or the new one and never a truncated file. The first
write completes before the first commit. Its lifecycle:

- **Written** after preparation and before the first run, with the fields
  of `Job` and no outcome. A job that preparation decides is `NoChange`
  never gets a record.
- **Rewritten** after a run that ends in `Failed` and keeps its record,
  with the outcome added, so that the next `infoctl run` can say why it
  is rerunning.
- **Rewritten** with the `Committed` outcome after the last commit, so that a
  caller whose report was lost can still read the result. Such a record is not
  rerun; #40 removes it with the staging manifests older than the minimum age.
- **Deleted** after a `Failed` run when `committed_by` finds the id on no
  output table, because nothing is then referenced and the next preparation
  repeats the work with a new id; and when a job is abandoned, see below.

A record without a `Committed` outcome that exists when `infoctl run` starts
is a job that may have committed to some, none or all of its tables;
`committed_by` tells which. `infoctl run` reruns every such record for its
target before it prepares a new job, and refuses to prepare a new job while
one remains. It holds an exclusive lock on `.infostash/jobs/lock`, taken with
the operating system's file lock, from before it reads any record until after
its last record write, so in Phase 0 one process at a time recovers, runs and
records jobs for a project. A second `infoctl run` on the same project fails
at once with an error naming the lock file, before it reads any record.

A rerun creates a new `RunId`, reads the current state of every output
table, skips each table whose head already carries the job id (and, where
the table property names another job, each table where `committed_by`
finds the id on an earlier snapshot), and checks that every remaining
table still has the head, schema id and partition spec id the record
expects. A changed state on a remaining table means a stranger changed
it, so the job is stale: the rerun stops with `Conflict`, which is
`Retry::Job`.

`Retry::Job` means the control plane abandons the job and prepares again
from the current state of the tables. Because the previous job's expected
state no longer holds, the new preparation is new work and gets a new id,
and the stale record is deleted. A job is abandoned the same way in two
more cases, because rerunning it could only fail again:

- a run ends in `Failed` with `Retry::Never` after a partial commit (for
  example a non-retryable `Storage` error on the target commit), and
- the record names a project version that is not the current one. The
  record holds the version hash, not the project files, so the work can
  no longer be rebuilt once the model is edited.

A run that ends in `Failed` with `Retry::Task` after a partial commit,
with every attempt used up, keeps its record and is rerun next time.

Abandoning is an exception to the rule of #75 that a job with partial
commits is finished under its own id before a new one is prepared. Rows
the abandoned job committed to side tables stay where they are, and the
new job quarantines the same bad rows again under its own id. Readers
that need a view consistent with the target already keep only quarantine
rows whose job id is in the target's history (ADR 0006), so the duplicate
rows are invisible to them, and #12's crash test ("each row in exactly
one table once") is stated over the rows that rule shows. Removing
abandoned side-table rows is maintenance work alongside #40. If the new
preparation finds no input past its checkpoint, the outcome is `NoChange`.

The record format is internal to the CLI until a queue replaces it in a
later phase. The run id is a random `u64` assigned by the control plane for
every run, including the first. It appears in staged file names, in
`TaskContext` and in logs, and in no snapshot property. The committed
metadata still names the data files, so a committed file's path tells
which run wrote it.

### Tasks and the runner in `crates/dist`

```rust
#[non_exhaustive]
pub struct TaskContext {
    pub job: JobId,
    pub run: RunId,
    pub task: TaskId,
    pub attempt: u32,
    // Reserved. Phase 0 has one stage and no exchange, so `input`
    // returns `Error::InputLost` for every stage, `cancelled` is always
    // false and `limits` holds no bound.
}

impl TaskContext {
    pub fn new(job: JobId, run: RunId, task: TaskId, attempt: u32) -> Self;
    pub fn attempt_key(&self) -> AttemptKey;
    pub fn input(&self, stage: StageId) -> Result<ExchangeInput>;
    pub fn cancelled(&self) -> bool;
    pub fn limits(&self) -> &Limits;
}

/// Placeholder for the exchange reader of a later phase.
#[non_exhaustive]
pub struct ExchangeInput {}

#[non_exhaustive]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Limits {
    pub memory_bytes: Option<u64>,
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct TaskOutput {
    pub stats: TaskStats,
    /// What the task staged, one record per output table it wrote to.
    pub staged: Vec<StagedFiles>,
}

impl TaskOutput {
    pub fn new(stats: TaskStats, staged: Vec<StagedFiles>) -> Self;
}

#[async_trait]
pub trait ExecutionStage: Send + Sync {
    fn id(&self) -> StageId;
    fn depends_on(&self) -> &[StageId];
    fn task_count(&self) -> u32;
    fn output(&self) -> &Exchange;
    async fn run_task(&self, ctx: &TaskContext) -> Result<TaskOutput>;
}

#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    pub max_task_attempts: u32,
}

impl RetryPolicy {
    pub fn new(max_task_attempts: u32) -> Self;
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct TaskRun {
    pub task: TaskId,
    /// Attempts made, counting the first.
    pub attempts: u32,
    pub output: TaskOutput,
}

pub async fn run_in_process(
    stages: &[&dyn ExecutionStage],
    job: JobId,
    run: RunId,
    retry: RetryPolicy,
) -> Result<Vec<TaskRun>>;
```

`run_task` is async, and so is `Runner::run` below. Every task reads
through `crates/storage` and executes through DataFusion, both of which
are async, so a synchronous signature would make every task block a
runtime thread on a nested executor. `crates/dist` and `crates/jobs` take
`async-trait` as a dependency, the same version `crates/storage` already
uses, so that the traits stay object safe.

`run_in_process` runs the stages in the order `schedule` gives and each
task up to `max_task_attempts` times. It retries a task only when the
error's `retry()` is `Retry::Task`; any other error ends the run at once
with that error. After the last attempt the last error is returned
unchanged. A retried task starts again immediately. Phase 0 adds no wait,
because the only transient failures a laptop sees are local I/O, and a
delay would make the retry tests slower without making them stronger.
`RetryPolicy` is `#[non_exhaustive]` so that a delay can be added when a
remote runner needs one. How `max_task_attempts` counts, and what `0`
means, stays with #29.

`run_in_process` returns each task's output with the number of attempts it
took, instead of one summed `TaskStats`. The runner in `crates/jobs` sums
the stats, collects the staged files of the attempt that succeeded, and
commits them. Files written by attempts that failed are never committed;
#40 removes them.

A task never returns `Conflict`. Tasks read inputs pinned by snapshot id
and write only staged files, so there is nothing for them to race on, and
`Conflict` comes from the commit step or the runner's precheck of the
expected table states, never from a task. The contract is not enforced
at runtime: a `Conflict` from a task is passed through like any other
error, `JobOutcome::failed` gives it `Retry::Job`, and the cost of the bug
is one needless preparation rather than a wrong commit.

In Phase 0 a model compiles to one `ExecutionStage` with one task; the
stage runs the model's plan over its pinned inputs, splits the rows into
target and quarantine as RFC 0001 says, and stages each. One stage per
model and input partition, with a `Hash` exchange between stages, is a
later phase and needs no change to these types.

### The runner in `crates/jobs`

```rust
#[async_trait]
pub trait Runner: Send + Sync {
    fn name(&self) -> &str;
    fn serves(&self, pool: Pool) -> bool;
    /// Run `job` once, under `run`. Returns `Failed` rather than an error
    /// so that a caller sees every outcome through one type.
    async fn run(&self, job: &Job, run: RunId) -> JobOutcome;
}
```

The in-process runner that #13 builds:

1. loads the state of every output table, skips the tables that already
   carry the job id (see "What a `JobId` identifies"), and checks the
   expected head, schema id and partition spec id of the rest; a change is
   `Conflict` before any task runs; when every table already carries the id it
   returns `Committed`, with every record `already_committed` and default
   metrics, without planning or running a task;
2. plans the stages, runs them with `run_in_process`, and maps an error
   to `Failed` through `JobOutcome::failed`;
3. commits side tables first and the target last, one `commit_staged` per
   table, each carrying `infostash.job-id` and, on the target, the input
   checkpoints; a `Storage { retryable: true }` from `commit_staged` is
   retried in place up to `max_task_attempts` times, since the commit runs
   in the coordinator and no task exists to retry; a `Conflict` on any
   table ends the run with `Failed { retry: Job }` after the
   `committed_by` check;
4. returns `Committed` with one record per output table, in commit order.

The compiler, not the runner, guarantees that a rerun produces the same
rows. Its obligation is that a model's result is a function of the project
version, the pinned inputs and `started_at` alone: no other clock, no
random source, no read outside the pinned snapshot ranges. The runner
guarantees only that attempts of one task are independent, that nothing
from a failed attempt is committed, and that a rerun commits to no table
that already carries the id.

### Tests

The code that implements this RFC proves at least:

- A `Fake` stage whose task fails with `Error::Conflict` is not retried by
  `run_in_process`: the call count is 1 and the error returned is the
  `Conflict`.
- A `Fake` stage whose task fails once with an I/O `Error::Execution` and
  then succeeds is retried: the call count is 2 and the returned `TaskRun`
  reports two attempts.
- A `Fake` stage whose task fails with `Error::Data` is not retried.
- A job with a target and a quarantine table returns `Committed` with two
  records, quarantine first, and each snapshot carries the job id.
- A rerun of a finished job returns `Committed` with every record flagged
  `already_committed`, zero metrics, runs no task and makes no snapshot.
- A rerun whose target schema id differs from the recorded one returns
  `Failed { kind: Conflict, retry: Job }` before any task runs.
- A schema change injected after the id checks of `commit_staged` and before
  its catalog update, which moves no snapshot, returns `Conflict` and makes
  no snapshot.
- `commit_staged` with a `StagedFiles` whose `schema_id` is no longer
  current returns `Conflict` and makes no snapshot; one whose `table` is
  another table returns `Storage { retryable: false }`.
- `stage` writes a staging manifest that reads back equal to the
  `StagedFiles` it returned, and `commit_staged` deletes it.
- Two attempts of one task stage under different file names, and a
  reused `AttemptKey` never overwrites a committed file (#82).
- An iceberg-rust error of kind `Unexpected` maps to
  `Storage { retryable: true }` and one of kind `DataInvalid` to
  `Storage { retryable: false }`.
- `Error::retry` is covered by a table test with one row per variant, so
  that a new variant without a retry class fails the test.
- A `DataFusionError::Context` wrapping an arrow `CastError` maps to
  `Error::Data` with the model name in the message.

## Alternatives

**`Failed(Error)` instead of kind, message and retry.** Simpler, but
`Error` is neither `Clone` nor `PartialEq`, and `JobOutcome` is both, which
metering and tests rely on. Three plain values keep that.

**A boolean `retryable` on every error.** That is what `JobOutcome::Failed`
has today. It cannot say that a `Conflict` needs a fresh preparation while
an I/O error needs a repeated task, and that difference is the whole
reason `run_in_process` must stop retrying `Conflict`.

**Keep `run_task` synchronous and block inside.** It keeps the trait
simple, but every task would create or borrow a runtime to call storage,
and the in-process runner could not run tasks concurrently on one runtime.

**Hold staged files in memory and write no manifest in Phase 0.** In one
process the manifest is written and read back by the same code, and the
runner never reads it. But ADR 0006 is accepted and requires a manifest
per attempt, #13's Done-when repeats it, and the manifest is what lets
#40 tell a complete attempt from a half-written one. Writing a small JSON
file costs less than amending an ADR.

**Let `crates/dist` depend on `crates/storage` and carry iceberg's
`DataFile` directly.** It saves the JSON round trip, but ties the task
contract to the storage library and makes `StagedFiles` unserialisable
without iceberg, which the manifest needs.

**A content-derived `JobId` (a hash of the fields that fix it).** It
removes the need to assign the id before the record is written, but the
hash would have to cover a serialisation of the inputs that every runner
agrees on, and it would still need the record to be found after a crash.
A random id and a record are simpler and are what a queue will hold
anyway.

**A `Timeout` error variant.** Nothing in Phase 0 times out. The class is
`Task` either way, and a variant can be added later without breaking
callers because `Error` is `#[non_exhaustive]`.

## Compatibility

This RFC changes the public API of seven crates. No committed data changes
shape: snapshots carry the same properties, and tables keep the same
schemas. Staged files get longer names and a manifest beside them, which
only #40 reads.

- `crates/model`: `Job` gains `expected` and `started_at` and
  becomes `#[non_exhaustive]`, so the struct literal in the test fixture
  of `crates/jobs` is replaced by `Job::new` (#13 does this). `JobOutcome`
  and `Error` change variants as shown; a `Storage(String)` becomes
  `Storage { message, retryable }`, and an iceberg `TableNotFound` or
  `NamespaceNotFound`, which was `Storage`, becomes `NotFound`. `RunId`,
  `TableState`, `Retry`, `ErrorKind`, `CommitRecord`, `AttemptKey`,
  `StagedFiles`, `StagedFile`, `Principal` and `QueryContext` are new.
- `crates/jobs`: `Runner::run` becomes async and takes a `RunId`;
  `prepare` is new. The crate gains `async-trait`.
- `crates/dist`: `run_task` becomes async and takes a `TaskContext`;
  `run_in_process` becomes async, takes the job and run ids and returns
  `Vec<TaskRun>`; `TaskStats` gains three fields and becomes
  `#[non_exhaustive]`; `RetryPolicy` becomes `#[non_exhaustive]`, keeps
  its `Default` and gains `new`; `TaskContext`, `ExchangeInput`, `Limits`,
  `TaskOutput` and `TaskRun` are new. The crate gains `async-trait`.
- `crates/storage`: `stage` and `commit_staged` are new; `append_if_head`
  stays and takes an `AttemptKey`; the iceberg error mapping is by kind as
  shown. The crate gains `serde_json` for the staging manifest.
- `crates/engine`: `query` takes `&QueryContext` (#95) and maps DataFusion
  errors as shown.
- `crates/connectors`: `prepare` takes a `RunId` next to the `JobId`,
  `Prepared::commit` passes `AttemptKey::single` to `append_if_head`
  (#82) and, under #81, returns `Result<Option<CommitRecord>>`.
- `crates/cli`: `infoctl run` assigns job and run ids for model jobs and
  connector loads alike, and keeps the job record; it gains `serde_json` and a
  file-locking crate for the run lock.

There are no released users of these crates, so no migration is needed
beyond the call sites in this repository, which the implementing tickets
(#12, #13, #81, #82, #95) update.

## Open questions

Answered here:

1. Who recovers a job id after a crash with no queue: `infoctl run`, from the
   job record under `.infostash/jobs/`. The table property alone is not
   enough, because a crash between commits leaves the id on a side table only,
   and nothing names the target it was for. A record without a `Committed`
   outcome is rerun or abandoned; a record with one, or none at all, is
   finished or abandoned.
2. One `ExecutionStage` per model, or per model and input partition: per
   model in Phase 0, with one task, run by the in-process runner.
3. Whether Phase 0 writes per-attempt staging manifests: yes, as
   ADR 0006 requires. `stage` writes the manifest, `commit_staged` deletes
   it, and #40 removes the ones left behind.
4. Who guarantees that a rerun produces the same output: the compiler, by
   fixing everything the result depends on in the job. The runner
   guarantees only isolation between attempts and the skip rule.
5. What records a job as done once snapshot summaries expire: in Phase 0, its
   job record with a `Committed` outcome, or the absence of one. Snapshot
   expiry does not exist in Phase 0, and the retention rule that #75 decides
   must keep every snapshot that a job whose record is present without a
   `Committed` outcome committed, so that `committed_by` can find them.
6. Whether `run_task` may return `Conflict`: no. Only `commit_staged` and
   the runner's precheck do.
7. Which principal jobs in each `Pool` run as: `Principal::Local` for
   every pool in Phase 0.

Still open:

- Whether a remote runner needs a `Retry::Stage` class for `InputLost`,
  and whether a wait between task attempts should be fixed or grow.
- When side-table rows of an abandoned job are removed, and by what: #40
  or a separate maintenance task.
- Whether the job record moves into the warehouse directory when a second
  machine first needs to read it, or straight into a queue.
