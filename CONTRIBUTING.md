# Contributing

Thanks for helping build Infostash. This guide gets you from a fresh
machine to a merged pull request.

## Ways to help

- **Report a bug or ask for a feature:** open an issue using one of the templates.
- **Request or build a connector:** use the "Connector request" issue template.
  Connectors are the easiest way to make your first contribution.
- **Fix something:** issues labeled `good first issue` are small and
  well described. Comment on the issue so nobody else picks it up at the same time.
- **Improve docs:** typos, unclear steps and missing examples are all welcome.
- **Propose a big change:** write an RFC first (see below).

## Set up your machine

1. Install [rustup](https://rustup.rs). You don't need to pick a Rust version:
   `rust-toolchain.toml` pins it, and rustup installs it on your first build.
2. Clone and build:

   ```sh
   git clone https://github.com/codecapo/infostash
   cd infostash
   cargo build
   cargo test
   ```

3. Before every pull request, run the same checks CI runs:

   ```sh
   cargo fmt --all
   cargo clippy --workspace --all-targets -- -D warnings
   cargo test --workspace
   ```

   CI also checks licenses and security advisories, spelling and unused
   dependencies. To run those locally, install the tools once
   (`cargo install cargo-deny typos-cli cargo-machete`), then:

   ```sh
   cargo deny check
   typos
   cargo machete
   ```

## Repository layout

| Path | What lives there |
|---|---|
| `crates/model` | Shared types: layers, tables, snapshots, `Job` |
| `crates/compiler` | Turns SQL project files into a dependency graph and incremental plans |
| `crates/engine` | Vectorized execution on Apache DataFusion and Arrow |
| `crates/dist` | Distributed stages, tasks, shuffle and retries |
| `crates/storage` | Object storage and Iceberg tables |
| `crates/connectors` | Sources and destinations, plus the connector SDK |
| `crates/jobs` | Job queue, scheduler and runners |
| `crates/agent` | Semantic layer, chat agent and builder agent |
| `crates/server` | HTTP API, Postgres wire protocol, Flight SQL, MCP |
| `crates/cli` | The `infoctl` command line |
| `ui/` | Web UI (TypeScript), embedded into the binary |
| `docs/` | Architecture, RFCs and maintainer guides |

## Making a change

1. **Small fixes:** open a pull request directly.
2. **New features:** open an issue first so we can agree on the approach
   before you spend time on it.
3. **Big or hard-to-reverse changes** (new crate, public API, file format,
   protocol, anything users will depend on): write an RFC. Copy
   `docs/rfcs/0000-template.md`, open it as a pull request, and discuss it there.

### Pull request checklist

- One topic per pull request. Small pull requests get reviewed faster.
- Tests for new behavior and for every bug fix.
- All CI checks pass.
- The title follows Conventional Commits (see below).
- Public items have doc comments.
- The description says what changed and why, and links the issue.

### Branches, titles and merging

- Branch from `main` and name the branch `<type>/<topic>`, for example
  `feat/csv-connector`.
- Pull requests are **squash merged**: the whole pull request becomes one
  commit on `main`, and its title becomes that commit's message.
- So the **pull request title** must follow
  [Conventional Commits](https://www.conventionalcommits.org):
  `<type>(<optional scope>): <summary>`, for example
  `feat(connectors): add CSV connector` or `fix(storage): close writer on error`.
  CI checks it.
- Types: `feat`, `fix`, `docs`, `perf`, `refactor`, `test`, `build`, `ci`,
  `deps`, `chore`, `revert`. Add `!` after the type for a breaking change
  (`feat(model)!: rename Layer::Ready`).
- Scopes are crate names (`storage`, `engine`, `cli`, ...) or areas (`docs`, `ci`).
- Write the summary in the imperative and lower case, with no full stop.
- Commits inside your branch can be anything you like; only the title lands on `main`.
- Add a line to `CHANGELOG.md` under `Unreleased` for anything users will notice.

### Sign your commits (DCO)

We use the [Developer Certificate of Origin](https://developercertificate.org).
It is a one-line statement that you have the right to submit the code. Add it
with `-s`:

```sh
git commit -s -m "feat(connectors): add CSV connector"
```

This adds `Signed-off-by: Your Name <you@example.com>` to the commit. A bot
checks every pull request for it. There is no separate agreement to sign.

## Using AI tools

AI coding assistants (Claude, Copilot, Cursor and others) are welcome here. The
core team uses them every day. The rules:

- **You are the author.** You must understand every line you submit, be able
  to explain it in review, and have run the tests yourself.
- **Say so in the pull request** when an AI tool wrote a significant part of
  the change. There is a checkbox in the template. It doesn't count against
  you; it helps reviewers know where to look carefully.
- **No unreviewed bulk changes.** Don't open pull requests that reformat,
  rename or "improve" large areas of code without an issue agreeing to it.
- **Agent instructions live in [AGENTS.md](AGENTS.md).** Point your assistant
  at it so it follows the project's conventions.

## Code style

- Follow `rustfmt` and fix every `clippy` warning.
- Prefer clear names and small functions over comments that explain clever code.
- No `unwrap()` or `expect()` in library code paths that can fail at runtime;
  return an error instead. Tests may unwrap.
- Keep dependencies few. Discuss any new dependency in the pull request.

## Getting help

Ask in GitHub Discussions or the community chat (link in the README once it
opens). Maintainers aim to reply to new issues and pull requests within three
working days.
