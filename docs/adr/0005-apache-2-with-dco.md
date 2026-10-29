# 0005. Apache 2.0 with a DCO, not a CLA

- **Status:** Accepted
- **Date:** 2026-10-27 (recorded 2026-10-29)

## Context

Infostash is open core: an open-source core plus a commercial hosted service
and enterprise features. We want companies to adopt and contribute without a
legal review, and we need to be able to use community code commercially.

## Decision

- The core is licensed under **Apache 2.0**.
- Contributors certify their right to contribute with the **Developer
  Certificate of Origin**: each commit carries a `Signed-off-by` line
  (`git commit -s`). There is no contributor licence agreement.

The comparison is in the
[launch checklist](../maintainers/launch-checklist.md#2-contributor-agreement-dco-or-cla).

## Consequences

- Apache 2.0 is familiar to company lawyers and includes a patent grant.
- Contributing takes one flag, not a signed form.
- Apache 2.0 already lets the company use community code in commercial
  products. Without a CLA the company cannot relicense community code, which
  matches the promise in [GOVERNANCE.md](../../GOVERNANCE.md) never to change
  the license of released code.
- Commits without a sign-off can't be merged. The DCO check is still to be
  installed (see the launch checklist).
