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
   git clone https://github.com/infostash/infostash
   cd infostash
   cargo build
   cargo test
   ```

3. Before every pull request, run the same checks CI runs:

   ```sh
   cargo fmt --all
   cargo clippy --all-targets -- -D warnings
   cargo test
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
- `cargo fmt`, `cargo clippy` and `cargo test` pass.
- Public items have doc comments.
- The description says what changed and why, and links the issue.

### Commit messages

Write a short summary line in the imperative ("Add CSV connector", not
"Added CSV connector"), then a blank line, then any detail.

### Sign your commits (DCO)

We use the [Developer Certificate of Origin](https://developercertificate.org).
It is a one-line statement that you have the right to submit the code. Add it
with `-s`:

```sh
git commit -s -m "Add CSV connector"
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
