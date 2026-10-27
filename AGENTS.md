# Instructions for AI coding agents

This file tells AI assistants (Claude Code, Codex, Cursor, Copilot and others)
how to work in this repository. Humans should read CONTRIBUTING.md; this file
is the short, strict version of it.

## Project in one paragraph

data-in-motion is an open-source data platform in Rust. SQL pipelines move
data through Raw, Clean and Ready layers stored as Apache Iceberg tables on
object storage. Execution uses Apache DataFusion and Arrow, split into stages
and tasks so the same plan runs in one process or across many machines. See
`docs/architecture.md`.

## Before you change anything

- Read the crate's `lib.rs` doc comment and the types in `crates/model`.
- For a new public API, file format or protocol, stop and ask for an RFC
  (`docs/rfcs/0000-template.md`) instead of implementing directly.

## Rules

- Run `cargo fmt --all`, `cargo clippy --all-targets -- -D warnings` and
  `cargo test` before saying a change is done. Report failures as they are.
- Add or update tests for every behavior change.
- No `unwrap()` or `expect()` in library code paths that can fail at runtime.
- Do not add dependencies without saying why in the pull request.
- Keep changes to the task asked. No drive-by refactors or renames.
- Never commit secrets, tokens or `.env` files.
- Commits use `git commit -s` (DCO sign-off) and an imperative summary line.

## Key interfaces

| Trait or type | Crate | Purpose |
|---|---|---|
| `Job`, `JobOutcome` | `model` | Unit of work: project version + table + snapshot range in, commit + metrics out |
| `ExecutionStage` | `dist` | One stage of a distributed plan; tasks must be safe to retry |
| `Runner` | `jobs` | Where a job runs: in process, Kubernetes, hosted fleet |
| `Connector` | `connectors` | Reads from a resumable position into Raw |

## Invariants that must never break

- A task writes only to its own shuffle or staging location. Only the job
  commit touches a table, and the commit is atomic.
- A connector's read position is committed in the same Iceberg commit as its data.
- Agents run queries with the requesting user's permissions, never more.
