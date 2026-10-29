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
the tables each query reads. There is no templating language.

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
by it. Phase 0 ships only `csv`. Each run reads only files it has not read
before; the position is saved in the same commit as the data
([ADR 0004](../adr/0004-stage-then-atomic-commit.md)).

### Models

```sql
-- clean/orders.sql
-- @unique_key: order_id
-- @check: amount >= 0
-- @check: customer_id is not null
-- @description: One row per order, latest version wins.
select
    cast(order_id as bigint)          as order_id,
    cast(customer_id as bigint)       as customer_id,
    cast(amount as decimal(12, 2))    as amount,
    cast(ordered_at as timestamp)     as ordered_at
from raw.orders
```

```sql
-- ready/daily_revenue.sql
-- @partition_by: day
select date_trunc('day', ordered_at) as day, sum(amount) as revenue
from clean.orders
group by 1
```

A model is one `select` (or `with ... select`) statement in the DataFusion
SQL dialect. A trailing `;` is allowed; more than one statement is an error.

**Directives** are comment lines of the form `-- @key: value` in the header,
before the first line of SQL. Comments after the SQL starts are plain
comments. Keys are case sensitive. An unknown key is an error.

| Directive | Repeat | Meaning |
|---|---|---|
| `@unique_key: col[, col]` | no | Merge on these columns: a new row replaces the old one with the same key. Without it, rows are appended. |
| `@check: <SQL boolean expression>` | yes | Rows where the expression is false or null go to the quarantine table instead of the target. |
| `@partition_by: <column or transform>` | no | Iceberg partitioning, such as `day` or `day(ordered_at)`. |
| `@full_refresh: true` | no | Rebuild the whole table every run instead of processing only new rows. |
| `@description: <text>` | no | Shown in the UI and docs, and given to agents. |

**Incremental by default.** A model reads only the upstream snapshots added
since its last successful run. The compiler decides whether a query can be
run incrementally; if it can't (for example a window over the whole table),
compiling fails and asks for `@full_refresh: true` rather than silently
rebuilding.

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

## Compatibility

This is the first format, so nothing breaks. `format = 1` lets a later
version detect old projects and migrate them. Adding a directive or a
connector key is not a breaking change; changing what an existing one means
is, and needs a new RFC and a `format` bump.

The current `directives()` function in `crates/compiler` reads directives
from every comment line and keeps one value per key. It must change to read
the header only and to allow repeated `@check`.

## Open questions

1. **Directory names:** `clean/` and `ready/` (the UI names), or `silver/`
   and `gold/` (the namespaces), or accept both?
2. **Checks:** is a one-line SQL expression enough, or do we also want named
   checks (`@check not_negative: amount >= 0`) so the quarantine table can
   say which check a row failed?
3. **CSV schema:** infer types from the files, read them all as strings and
   cast in Clean, or require a schema in the source file? Strings keep Raw
   faithful to the source and make Clean do the typing.
4. **Merge semantics:** with `@unique_key`, which row wins when one batch has
   two rows with the same key? Proposal: the last one read.
5. **Config format:** TOML matches Cargo and is strict about types; YAML is
   more familiar to data teams. This RFC picks TOML.
