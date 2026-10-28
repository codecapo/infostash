# Instructions for AI coding agents

This file tells AI assistants (Claude Code, Codex, Cursor, Copilot and others)
how to work in this repository. Humans should read CONTRIBUTING.md; this file
is the short, strict version of it.

## Project in one paragraph

Infostash is an open-source data platform in Rust. SQL pipelines move
data through Raw, Clean and Ready layers stored as Apache Iceberg tables on
object storage. Execution uses Apache DataFusion and Arrow, split into stages
and tasks so the same plan runs in one process or across many machines. See
`docs/architecture.md`.

## Before you change anything

- Read the crate's `lib.rs` doc comment and the types in `crates/model`.
- For a new public API, file format or protocol, stop and ask for an RFC
  (`docs/rfcs/0000-template.md`) instead of implementing directly.

## Rules

- Run `cargo fmt --all`, `cargo clippy --workspace --all-targets -- -D warnings`,
  `cargo test --workspace`, `cargo deny check`, `typos` and `cargo machete`
  before saying a change is done. Report failures as they are. If you touch
  docs comments, also run `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`;
  if you touch `.github/`, also run `actionlint` and `zizmor .`.
- Add or update tests for every behavior change.
- No `unwrap()`, `expect()`, `panic!`, `todo!` or `dbg!` in library code; clippy
  enforces this (tests may unwrap). No `unsafe`; the workspace forbids it.
- Do not add dependencies without saying why in the pull request.
- Keep changes to the task asked. No drive-by refactors or renames.
- Never commit secrets, tokens or `.env` files.
- Work on a branch from `main` named `<type>/<topic>`; never commit to `main`.
- Commits use `git commit -s` (DCO sign-off). Pull request titles, and any
  commit that lands on `main`, follow Conventional Commits:
  `feat(storage): add warehouse`. See CONTRIBUTING.md.
- Do not add AI attribution trailers (`Co-Authored-By`) or "generated with"
  footers to commits or pull requests.

## Working correctly

- **Verify, don't recall.** Before using a library API, check it exists in the
  version pinned in `Cargo.lock` (read the source under `~/.cargo/registry` or
  `~/.cargo/git`, or docs.rs for that exact version).
- **Done means checked.** A change is done only when every check above passes.
  Show the command output; never say something passes without running it.
- **Tests must prove behavior.** Each new test must fail if the code it covers
  is broken. Don't weaken, skip or delete tests to get green.
- **Don't silence checks.** No new `#[allow]`, `cargo deny` ignores, typos
  exceptions or CI edits unless the task is about them; explain any you add.
- **Stop and ask** when requirements are unclear, a decision is hard to
  reverse, or the fix needs a public API, format or protocol change.
- **Stay local.** Never push, open or merge pull requests, publish crates,
  change repo settings or contact external services unless a person asks.
- **No secrets.** Never read, print or commit credentials, tokens or `.env`
  files, and never send them to external services.
- **Report honestly.** Say what you did not do, what failed and what you are
  unsure of.

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
