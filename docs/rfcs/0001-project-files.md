# RFC 0001: Project file format

- **Author(s):** @codecapo
- **Status:** Draft
- **Discussion:** [issue #5](https://github.com/codecapo/infostash/issues/5)
- **Created:** 2026-10-29

## Summary

An Infostash project is a directory of plain files: one `infostash.toml`,
one TOML file per source, and one SQL file per model. The directory a model
sits in sets its layer, and the file name sets its table name. Settings are
`-- @key: value` comments at the top of the SQL file. Dependencies come from
the tables each query reads. There is no templating language. Models run
incrementally by default, and this RFC states which queries may.

## Motivation

`infoctl init`, `infoctl run` and the compiler all need to agree on what a
project looks like, and users will commit these files to git and keep them
for years. Once people depend on the format, changing it breaks their
projects, so we agree on it before writing the compiler.

Goals:

- An analyst who knows SQL can read and write a project with no new syntax.
- Every file is valid on its own: SQL files run in any SQL editor.
- The format can grow (new directives, new connectors) without breaking
  existing projects.

## Design

### Layout

```text
shop/
├── infostash.toml
├── sources/
│   └── orders.toml          -> bronze.orders
├── clean/
│   └── orders.sql           -> silver.orders
└── ready/
    └── daily_revenue.sql    -> gold.daily_revenue
```

- `sources/` defines Raw tables. Each file is one source, loaded by a
  connector. Raw tables are never written by SQL.
- `clean/` and `ready/` hold models. Each `.sql` file is one table in that
  layer, named after the file (without `.sql`).
- Subdirectories inside `clean/` and `ready/` are for organising files only;
  they don't change the table name. Two files with the same name in one layer
  are an error.
- Table and file names use lower case letters, digits and `_`, start with a
  letter, and must not end in `__quarantine` (see
  [ADR 0003](../adr/0003-layers-as-namespaces.md)).
- Other files and directories are ignored, so a project can hold a README,
  tests or data.

### `infostash.toml`

```toml
[project]
name = "shop"
format = 1             # this RFC; bumped only for breaking changes

[warehouse]
path = ".infostash/warehouse"   # Phase 0: a local directory, relative to the project
```

Unknown keys are an error, so typos fail fast. Later phases add other
warehouse kinds (S3, GCS, Azure, an Iceberg REST catalog) as new keys.

### Sources

```toml
# sources/orders.toml
connector = "csv"
path = "data/orders/*.csv"     # relative to the project
```

`connector` picks the connector; every other key belongs to it and is checked
by it. Phase 0 ships only `csv`.

The CSV connector reads every column as nullable text, so Raw keeps each
value as the file holds it and typing happens in Clean models. It adds three
columns to every row: `_source_file`, the path of the file the row came from;
`_loaded_at`, when the row was loaded; and `_source_row`, the row's position
among the data rows of its file, counting from 1. A file whose header uses one
of these names is an error.

Raw tables only grow. No row is ever updated or deleted, so a correction
arrives as a new row in a new file. Each run reads only files it has not read
before, in lexical order of their paths, and the position (the files read so
far, and how far into the current file the run got) is saved in the same
commit as the data ([ADR 0006](../adr/0006-ordered-job-commits.md)). The
position also records the `path` pattern it was taken under. A run whose
pattern differs from the stored one fails and names both, because the
position knows files by path alone, and the same file reached through another
pattern would load again. A position saved before the pattern was recorded is
read as having none, and the next run records it. Which files a pattern
lists, and what a producer may do to a directory a connector reads, is
documented with the connector.

### Models

```sql
-- clean/orders.sql
-- @unique_key: order_id
-- @check not_negative: amount >= 0
-- @check: customer_id is not null
-- @description: One row per order, latest version wins.
select
    try_cast(order_id as bigint)          as order_id,
    try_cast(customer_id as bigint)       as customer_id,
    try_cast(amount as decimal(12, 2))    as amount,
    try_cast(ordered_at as timestamp)     as ordered_at
from raw.orders
```

```sql
-- ready/daily_revenue.sql
-- @full_refresh: true
select date_trunc('day', ordered_at) as day, sum(amount) as revenue
from clean.orders
group by 1
```

A model is one `select` (or `with ... select`) statement in the DataFusion
SQL dialect. A trailing `;` is allowed; more than one statement is an error.

**Directives.** The header of a model is the run of blank lines and `--`
comment lines at the top of the file; it ends at the first line that is
neither. A directive is a header comment of the form `-- @key: value`,
starting at column one with `-- @`; a header comment of any other shape,
such as `--@key: value` or one indented by spaces, is an ordinary comment.
The key, and for `@check` its optional name, end at the first `:`, so a
value may hold colons, as `amount::decimal` does. Whitespace around the key
and the value is trimmed, and an empty value is an error, as is a header
comment that starts with `-- @` but has no `:`.
Only `@check` may carry a name between the key and the colon
(`-- @check not_negative: amount >= 0`); a name on any other directive is an
error. Keys are case sensitive. An unknown key is an error, and so is
repeating a directive whose row below says it may not repeat. Any other
comment in the header is ignored, and so is a line of the form
`-- @key: value` after the SQL starts: it is an ordinary comment, not an
error.

| Directive | Repeat | Meaning |
|---|---|---|
| `@unique_key: col[, col]` | no | One row per key: of the rows that share these columns, the latest wins (see Ordering below). Without it, rows are appended. Not accepted until the replace commit is in use (see Compatibility). |
| `@check[ name]: <SQL boolean expression>` | yes | The checks of a model form a list. A row for which any check is false or null goes to the quarantine table instead of the target. The name is optional (see Checks below). |
| `@full_refresh: true` | no | Rebuild the whole table every run: read every input whole and replace the table, instead of processing only new rows. Its only value is `true`; any other value is an error. Required when the query cannot run incrementally (see Incremental models below). Not accepted until the replace commit is in use (see Compatibility). |
| `@description: <text>` | no | Shown in the UI and docs, and given to agents. |

**Checks.** Each `@check` is a SQL boolean expression over the model's output
columns, so `amount >= 0` tests the decimal the model produces, not the Raw
text. A row for which any check is false or null is quarantined: a check is a
promise about every row that reaches the target, and a null result is not a
kept promise. A check over a column that may be empty says so:
`discount is null or discount >= 0`. Checks run on every row the query
produces, after its `where` and before any deduplication, so a quarantined
row never displaces an accepted one.

A check may carry a name, which follows the rules for table names (lower case
letters, digits and `_`, starting with a letter) and is unique within the
model. Quarantine rows report a named check by its name and an unnamed one by
its expression as written. Two kinds of implicit check also apply. Their names
hold a `:`, so they cannot collide with a written name.

- Every `try_cast` (or `safe_cast`) whose argument is a bare Raw column
  adds the check `<raw> is null or try_cast(<raw> as <type>) is not null`,
  named `cast:<col>`, where `<col>` is the output column the cast produces,
  so two casts of one Raw column get two names; a cast that is not by itself
  an output column, inside a larger select expression or in the `where`,
  takes the name of the Raw column instead. A `try_cast` over any other
  expression, such as `try_cast(trim(amount) as decimal)`, adds no implicit
  check; a written check covers it. The implicit check tells an empty cell,
  which stays null, from a value the type cannot hold. It runs before the
  written checks, which see the typed value, and a row that fails it is
  reported under `cast:<col>` only: the written checks that read that column
  are not reported for the row, because their input is missing rather than
  wrong. Written checks over other columns still apply to it.
- In a model with `@unique_key`, each key column adds the check
  `<col> is not null`, named `key_not_null:<col>`, so that rows without a key
  never collapse into one.

In Phase 0 a model with a check must take each output row from one row of
one input: its plan is a projection, with an optional filter, over one
declared table, with CTEs and aliases allowed. Implicit checks count, so the
rule also covers every model with `@unique_key` and every model that
`try_cast`s a bare Raw column. A check on any other model is a compile error
that names the model, until open question 6 settles what its quarantine
rows hold.

**Raw columns are text.** A column read from a Raw table is text, so a strict
`cast` over it fails the whole run on the first value the type cannot hold,
before any check runs. A strict cast of a Raw column is therefore a compile
error that names the column, whether written as `cast(...)`, as `::`, or
inserted by type coercion: `where amount > 0` over a Raw column casts the
column to a number. The lint reads the analysed, unoptimised plan, where every
form appears as a cast and `try_cast` and `safe_cast` appear as a try cast.
Convert Raw columns with `try_cast`, as the example does, and compare or
compute with the typed value.

### Incremental models

**What a model's table holds.** In every mode, a model's table equals its
query evaluated over its inputs as they stood at the snapshots the run
pinned, minus the rows its checks quarantine, with one row per key, the
latest, under `@unique_key`. A model processed one run at a time must end
with the same table as one rebuilt in a single pass, with one exception to
both statements: a check that reads the clock judges each row by the start
time of the run that first read it (see Time below). For the example, orders
of 100 and then 20 placed on 2026-10-29 give `daily_revenue` one row,
(2026-10-29, 120), however many runs brought them in.

**Incremental by default.** A model without `@full_refresh` reads only the
snapshots each input added since the model's last successful run, and appends
what its query yields over those rows. That gives the right table only when
the query treats each row on its own, so the compiler decides, when it
compiles the project and before any job is created, whether a query may run
incrementally; if it may not, compiling fails rather than silently
rebuilding. The rule is an allow-list over the model's unoptimised DataFusion
logical plan, built against the project's tables:

- The plan may hold only these nodes: `TableScan` of a table the project
  declares, `Filter` (`where`), `Projection` (the select list),
  `SubqueryAlias` (a CTE or a named subquery in `from`), `Unnest`, and
  `Union` from `union all`. No expression may contain a subquery (`exists`,
  `in (select ...)` or a scalar subquery).
- Any other node fails compilation with an error that names the model and
  the construct and offers `-- @full_refresh: true`. In Phase 0 that covers
  every join (`intersect` and `except` included), every aggregate with or
  without `group by`, window functions, `distinct` and plain `union`,
  `order by`, `limit`, `values`, a `select` without `from`, and a scan of
  anything the project does not declare, such as the `generate_series` table
  function.
- An incremental body may call only functions whose result depends on their
  arguments alone (DataFusion's `Immutable` volatility), because each row is
  seen by one run: `now()` in a `where` would judge each row by the time of
  the run that first read it. Functions that hold still within a run but not
  across runs (`Stable`, such as `now()` and `current_date`) are allowed in
  `@check` expressions and in `@full_refresh` models. Functions that change
  between calls (`Volatile`, such as `random()` and `uuid()`) are rejected
  everywhere, `@check` included, with an error that does not offer
  `@full_refresh`, because a rerun must split rows between target and
  quarantine exactly as the first attempt did (ADR 0006).

The example `clean/orders.sql` passes: it projects one Raw table. The example
`daily_revenue` aggregates, so it declares `@full_refresh: true`.

**Full refresh.** A model with `@full_refresh: true` is exempt from the node
allow-list but not from the function rules. Each run reads every input whole,
as of the snapshot pinned for it, and replaces the table in one commit that
carries the job id and the checkpoints, so readers see the old rows or the
new, never both ([ADR 0007](../adr/0007-hand-built-replace-commit.md)). The
table keeps its identity and history across refreshes, and a query that
returns no rows still replaces the table, leaving it empty. The quarantine
table is replaced in the same way, in the same commit, so a rebuild never
appends the same quarantined rows again.

**Rebuilt without `@full_refresh`.** A model may also be rebuilt in full
without declaring `@full_refresh`. An incremental read works only over
appends, and a table that merges on `@unique_key` or is fully refreshed is
replaced, not appended to, when it changes. Before a run creates a job, it
looks at every snapshot it would read from each input. If any is not an
append, or the snapshot the model's last run stopped at has expired, the job
pins every input of the model whole, rebuilds the model from all of them and
replaces its table, and the run output names the model, the input and the
cause ([ADR 0008](../adr/0008-incremental-readers-of-replaced-tables.md)).
Such a run no longer processes only the rows that changed, and everything
downstream of a `@full_refresh` model is rebuilt after each run in which that
model committed. Until this check exists, the compiler rejects an incremental
model that reads such a table (see Compatibility), and a model that has been
rebuilt by declaring `@full_refresh` keeps the directive, because dropping
it would let the compiler accept an incremental reader that then meets the
replaced snapshot.

**Time.** Every function that reads the clock (`now()`, `current_date` and
the rest) returns the job's start time. That time is stored with the job id
and used again when the job is rerun, so a check such as
`ordered_at <= now()` splits rows exactly as the first attempt did.

**Ordering.** Under `@unique_key` the latest row wins. Rows are ordered by the
data sequence number of the input snapshot that added the row's data file (a
file carried forward by a later snapshot keeps its own), then by
`_source_file`, then by `_source_row`. Sequence numbers belong to one table,
which is why a `@unique_key` model reads one input in Phase 0 (see Checks
above). Every Raw table has `_source_file` and `_source_row`, and within one
load they give the order the rows were read: files in lexical path order,
rows in file order. Because Raw tables only grow, the
latest row for a key is its current version, and a key cannot be deleted in
Phase 0. A model's table has the two columns when its query selects them
under their own names. Over a table without them, rows are ordered by
snapshot alone, and when two rows of one input snapshot share a key, the run
fails and names the key rather than pick one.

**Quarantine rows.** Rows that fail a check go to `<table>__quarantine`
(ADR 0003) as the input row as read, so for a Clean model the Raw text before
any conversion, with its load columns, plus `_checks_failed`, the list of the
checks the row failed, by name, and `_job_id`, the id of the job that wrote
it. The job id is a column because a reader that wants a view consistent with
the target trusts only quarantine rows whose job id appears in the target's
commit history (ADR 0006), and the id in a snapshot summary cannot be joined
from SQL. The quarantine table commits before the target, under the same job
id.

**Model versions.** Each model commits a version hash under an `infostash.`
key in the same commit as its checkpoint, so a table always records which SQL
produced its rows. The hash covers the SQL as parsed and printed again by the
parser, so that whitespace, comments and formatting leave it unchanged while
any change to an expression changes it; the `@unique_key` directive and the
`@check` directives, each with its name and expression, in file order, which
define what the table holds; and the current schema id of
each input, read with the table state the commit is checked against. It
leaves out `@description`, which changes nothing in the table, and
`@full_refresh`, which changes how the table is produced rather than what it
holds, so a model can be rebuilt by declaring it. Before an incremental run
reads anything, the stored hash is compared with the computed one; a table
without a stored hash, or with a matching one, runs normally. On a mismatch
the run is refused, with an error that names the model and says that its
SQL, its checks or key, or an input's schema changed since its last run, and
nothing is committed. A run of a model that declares `@full_refresh: true`
does not compare the stored hash: it replaces the table and commits the new
hash in the same commit. In Phase 0 the way forward after a mismatch is
therefore to declare `@full_refresh: true`, once the directive is accepted
(see Compatibility), and to keep it until the plan-time check of ADR 0008
exists (see Rebuilt without `@full_refresh` above). Until then, every
incremental model that reads its table must declare `@full_refresh: true`
as well. Rebuilding automatically on a mismatch is a later change. A new
column in a source file fails its load today; growing a Raw table's schema
with its files is a later, opt-in feature.

### Dependencies

- A model reads tables by qualified name, `<layer>.<table>`, using either
  name for the layer (`raw.orders` or `bronze.orders`).
- Unqualified table names are an error, so a reader always knows the layer.
- Clean models may read Raw and Clean tables. Ready models may read any layer.
  Reading a higher layer (Clean reading Ready) is an error.
- The dependency graph must have no cycles; the error names the cycle.
- Reading a table no source or model defines is an error.

## Alternatives

- **dbt-style Jinja (`{{ ref('orders') }}`).** Powerful, but it's a second
  language, files no longer run in a SQL editor, and errors point at
  generated code. Table names in plain SQL give the same graph.
- **YAML next to each SQL file for settings.** Keeps SQL pure but splits one
  model across two files. Header comments keep a model in one place, and SQL
  editors ignore them.
- **Layer as a directive (`-- @layer: clean`).** Works, but the directory
  makes the layer visible in a file tree and in `git diff`.
- **One `project.yaml` listing every model.** A central list conflicts on
  every pull request; one file per model doesn't.
- **A `@partition_by` directive now.** Earlier drafts had one. Tables are
  created without a partition spec and written unpartitioned, and the replace
  commit of ADR 0007 covers unpartitioned tables only, so a partitioned table
  could not be merged or refreshed. A later revision can add partitioning as
  a new directive without breaking anything; until then the key is unknown
  and fails compilation.
- **Null passes a check, as a SQL `CHECK` constraint lets it.** Reads
  naturally, but a blank cell would then pass every check except an explicit
  `is not null`. Quarantining null fails closed, and an optional column costs
  one `is null or` clause.
- **Strict casts that fail the run.** One bad value would fail the run after
  other models have committed, and the row that caused it would be hard to
  find. `try_cast` with an implicit check keeps the run going and puts the
  value in quarantine with its source file and row.
- **Checking eligibility when a job runs.** The runner could inspect each
  plan as it reads the catalog, but a project would then compile and fail
  only when run, and the same query would be judged on every run. The
  compiler sees the plan once, before any job exists, and its error points at
  the file.
- **A name on every check.** More to type, and a short expression is its own
  best description. An optional name serves when the expression is long.

## Compatibility

This is the first format, so nothing breaks. `format = 1` lets a later
version detect old projects and migrate them. Adding a directive or a
connector key is not a breaking change; changing what an existing one means
is, and needs a new RFC and a `format` bump. `@partition_by`, present in
earlier drafts, was removed while this RFC was still a Draft.

**Implementation status.** The code at the time of this revision does not
implement these rules yet:

- `directives()` in `crates/compiler` reads `-- @` lines from the whole
  file, keeps one value per key and does not recognise a check name (#7).
- No eligibility check, cast lint, implicit check or version hash exists
  (#84, #12, #87), and the clock is not pinned to the job.
- Raw tables carry `_source_file` and `_loaded_at` but not `_source_row`
  (#86), and the CSV position does not record the pattern (#78).
- The replace commit of ADR 0007 is not built yet (#88). Until it is in
  use, the compiler rejects `@unique_key` and `@full_refresh` with an error
  that names the directive (#84; lifted by #35 and #90). Until the plan-time
  check of ADR 0008 exists (#89), the compiler also rejects an incremental
  model that reads a table whose model declares either directive, naming
  that input, so that incremental models read only append-only tables.

Tables are created without a partition spec and written unpartitioned, as
they are today, until a later revision brings partitioning back.

## Open questions

1. **Directory names:** `clean/` and `ready/` (the UI names), or `silver/`
   and `gold/` (the namespaces), or accept both?
2. **Checks:** answered. A check may carry a name
   (`@check not_negative: amount >= 0`); an unnamed check is reported by its
   expression, and the implicit checks as `cast:<col>` and
   `key_not_null:<col>`. See Checks.
3. **CSV schema:** answered. Every Raw column is nullable text, and Clean
   models type it with `try_cast`. A declared schema in the source file can
   come later as a connector key. See Sources.
4. **Merge semantics:** answered. Rows are ordered by the data sequence
   number of the snapshot that added their file, then `_source_file`, then
   `_source_row`, and the latest wins; a tie that nothing orders fails the
   run. See Ordering.
5. **Config format:** TOML matches Cargo and is strict about types; YAML is
   more familiar to data teams. This RFC picks TOML.
6. **Checks on models that are not row-wise:** what does a quarantine row
   hold when an output row does not come from exactly one input row (a
   join, an aggregate, a `union all` of inputs with different columns), or
   when one input row yields several output rows (`unnest`)? The output
   row, the input rows, or something else? Until this is settled, `@check`
   is allowed only on models whose output rows each come from one row of
   one input.
