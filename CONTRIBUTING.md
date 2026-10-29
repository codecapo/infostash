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

   CI also checks licenses and security advisories, spelling, unused
   dependencies and docs. To run those locally, install the tools once
   (`cargo install cargo-deny typos-cli cargo-machete`), then:

   ```sh
   cargo deny check
   typos
   cargo machete
   RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
   ```

   If you change files under `.github/`, also run
   [actionlint](https://github.com/rhysd/actionlint) and
   [zizmor](https://docs.zizmor.sh) (`actionlint`, `zizmor .`).

   CI also runs the tests on macOS, measures coverage, runs CodeQL security
   analysis and OpenSSF Scorecard, and reviews new dependencies for known
   vulnerabilities. Library code may not use `unwrap`, `expect`, `panic!` or
   `unsafe`; clippy and the compiler enforce this.

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
- [CodeRabbit](https://coderabbit.ai) reviews every pull request automatically
  (config in `.coderabbit.yaml`). Treat its comments as advice: fix what is
  right, reply to what is not. A maintainer's review decides the merge.
- Every review thread must be resolved before merging. If a finding is real
  but belongs in another pull request, reply `@coderabbitai create an issue
  for this` and resolve the thread once the issue exists, so nothing is lost.

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

AI coding assistants and agents (Claude Code, Codex, Cursor, Copilot and
others) are welcome here. The core team uses them every day. The standard for
the result is the same as for code you typed yourself.

**You are responsible**

- **You are the author.** You must understand every line you submit, be able
  to explain it in review, and have run the checks yourself.
- **Say so in the pull request** when an AI tool wrote a significant part of
  the change. There is a checkbox in the template. It doesn't count against
  you; it helps reviewers know where to look carefully.
- **Don't add AI attribution to commits or pull requests.** No
  `Co-authored-by` trailers for AI tools and no "Generated with" footers.
  The pull request checkbox is the disclosure. CI blocks attribution lines;
  to catch them before you push, install the check as a commit hook:
  `ln -s ../../.github/scripts/check-ai-attribution.sh .git/hooks/commit-msg`.

**Check what the tool gives you**

- **APIs must exist in the version we pin.** Assistants often invent functions
  or use an older version's API. Check against the source in `Cargo.lock`'s
  version (docs.rs or `~/.cargo/registry`), not memory.
- **Tests must be able to fail.** A generated test can pass without testing
  anything. Break the code on purpose once and make sure the test catches it.
- **Don't let it make checks pass by weakening them:** no new `#[allow]`,
  ignored advisories, skipped tests, loosened assertions or edits to CI
  config without saying why in the pull request.
- **Read the whole diff** before you push. Look for leftover debug code,
  changes outside the task, and claims in comments that the code doesn't keep.

**Keep it safe and legal**

- **Never paste secrets, credentials or customer data** into a prompt.
- **No copied code under incompatible licenses.** If output looks lifted from
  another project, don't use it. Everything here must be Apache 2.0
  compatible.
- **No unreviewed bulk changes.** Don't open pull requests that reformat,
  rename or "improve" large areas of code without an issue agreeing to it.
- **Agents don't act on their own outside your machine.** A person decides
  when to push, open a pull request, publish or change repo settings.

**One set of instructions**

[AGENTS.md](AGENTS.md) holds the rules for every assistant. Codex and Cursor
read it directly; `CLAUDE.md` and `.github/copilot-instructions.md` point to
it. Change the rules there, not in tool-specific files, and update it in the
same pull request as any convention change.

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
