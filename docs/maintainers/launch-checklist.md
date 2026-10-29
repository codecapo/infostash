# Launch checklist for maintainers

Steps to take the project from a private repo to a public open-source
community. Tick them off in order. Items marked **TODO** need a decision.

## 1. Decide the basics

- [x] Project name: **Infostash** (CLI: `infoctl`).
- [ ] Check the name is free: trademark search, crates.io and npm (both `infostash` and `infoctl`), the
      `infostash` GitHub org and domains. Reserve them all.
- [ ] **TODO: domain** for the website, docs and contact emails.
- [ ] **TODO: contact emails:** `security@`, `conduct@`, `hello@`.
- [ ] File the trademark for the name and logo in your main markets.
- [ ] Set up the company that owns the trademark and runs the commercial service.

## 2. Contributor agreement: DCO or CLA

| | DCO (sign-off) | CLA (signed agreement) |
|---|---|---|
| Effort for contributors | One flag: `git commit -s` | Sign a legal form once |
| Effect on community | Low friction | Some people and companies won't sign |
| Can the company relicense later | No | Yes |
| Can the company sell it | Yes, Apache 2.0 allows it | Yes |

**Recommendation: DCO.** Apache 2.0 already lets the company use community code
in commercial products. A CLA mainly enables relicensing, which our governance
promises not to do. The docs in this repo assume DCO.

## 3. GitHub

- [x] Repository created at `github.com/codecapo/infostash`.
- [ ] Later: create a GitHub organization and transfer the repo to it
      (GitHub redirects the old URL). Require two-factor auth for members.
- [x] Repo settings: Issues on, Discussions on, Wiki off, squash merge only
      (title and description as the commit), delete branches after merge.
- [x] Protect `main`: pull request required, CI must pass, no force pushes,
      no deletion, linear history. Require one approval once there is a
      second maintainer (a sole maintainer cannot approve their own PR).
- [ ] Install the [DCO GitHub App](https://github.com/apps/dco) and make its
      check required.
- [x] Turn on private vulnerability reporting, Dependabot alerts and secret scanning.
- [x] Dependabot version updates (`.github/dependabot.yml`), cargo-deny,
      typos and cargo-machete in CI, Conventional Commit titles checked.
- [x] Quality and security in CI: strict clippy lints, rustdoc, macOS tests,
      coverage, CodeQL, OpenSSF Scorecard, dependency review, actionlint and
      zizmor. Every action is pinned to a commit SHA.
- [ ] Add required checks for the new CI jobs once they have run once.
- [ ] Sign in to [Codecov](https://codecov.io) with GitHub, add the repo, and
      save its upload token as the `CODECOV_TOKEN` Actions secret.
- [ ] Register at the [OpenSSF Best Practices](https://www.bestpractices.dev)
      program, reach the "passing" level, and add its badge to the README.
- [x] AI code review with CodeRabbit (free for public repos), advisory only.
- [ ] Optional: import the repo into [SonarQube Cloud](https://sonarcloud.io)
      (free for public repos) for a code quality dashboard.
- [ ] Before 0.1: release workflow with binaries, SBOM (`cargo cyclonedx`),
      build provenance attestations and `cargo semver-checks`.
- [ ] Once the SQL compiler exists: fuzz the parser with `cargo fuzz`, then
      apply to [OSS-Fuzz](https://google.github.io/oss-fuzz/).
- [x] Release process written down in [releasing.md](releasing.md).
- [x] `CODEOWNERS` file; add area owners as maintainers join.
- [ ] Labels: `good first issue`, `help wanted`, `bug`, `enhancement`,
      `connector`, `rfc`, `needs triage`, plus one per area (`area/engine`, ...).
- [x] Topics and description on the repo page.
- [ ] Social preview image.

## 4. Before going public

- [ ] Phase 0 works end to end with a 5-minute quick start.
- [ ] Remove placeholder text in README, CODE_OF_CONDUCT and SECURITY.
- [ ] Docs site live (mdBook or Docusaurus) with install, quick start,
      concepts and "build a connector".
- [ ] 15 to 20 `good first issue` items, each with clear steps.
- [ ] Roadmap issues opened for each phase.
- [ ] Check the git history for secrets before making the repo public.

## 5. Community

- [ ] Community chat (Discord or Slack) with channels: `#general`, `#help`,
      `#contributors`, `#connectors`, `#announcements`.
- [ ] Monthly community call, recorded and posted.
- [ ] Reply to every new issue and pull request within three working days.
- [ ] Thank and credit contributors in release notes.

## 6. Releases

- [ ] Semantic versioning. Before 1.0, minor versions may break things;
      say so in the release notes.
- [ ] Release every 4 to 6 weeks from `main`, with a tagged changelog.
- [ ] Publish binaries for Linux, macOS and Windows, a container image and
      the crates.
- [ ] Sign release artifacts.

## 7. Measure

Track monthly: GitHub stars, unique contributors, first-time contributors,
time to first reply on issues, merged pull requests from outside the core team,
downloads, and active community members.

## Working with AI agents

The core team builds with AI coding agents. Keep [AGENTS.md](../../AGENTS.md)
current: when a convention changes, update it in the same pull request, so
every agent and contributor follows the same rules.
