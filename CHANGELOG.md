# Changelog

All notable changes are listed here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org). Before 1.0, a minor version may
include breaking changes; the release notes say so.

## [Unreleased]

### Added

- Rust workspace with crates for the model, compiler, engine, distributed
  execution, storage, connectors, jobs, server, agent and the `infoctl` CLI.
- Iceberg tables on local disk: create, append in one atomic commit, read the
  current snapshot, read only rows added since a snapshot, and read a table
  from its metadata file with no catalog.
- The table catalog is kept in `catalog.db` (SQLite) in the warehouse
  directory, so tables and their snapshot history survive between runs.
- `infoctl query` runs one read-only SQL statement against the warehouse and
  prints the result as a table, CSV or line-delimited JSON. Tables are named
  `<layer>.<table>`, by namespace or alias; statements that write are rejected,
  and a directory without a catalog is refused rather than initialised.
