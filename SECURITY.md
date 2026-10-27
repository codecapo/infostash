# Security policy

## Reporting a vulnerability

**Do not open a public issue for security problems.**

Report privately through GitHub: go to the **Security** tab of this repository
and click **Report a vulnerability**. Include what you found, how to reproduce
it, and the impact you expect.

What happens next:

1. We confirm receipt within 3 working days.
2. We confirm or rule out the issue within 10 working days.
3. We fix it and agree a disclosure date with you, normally within 90 days.
4. We credit you in the release notes, unless you prefer not to be named.

## Supported versions

Until version 1.0, only the latest release gets security fixes.

## Scope

In scope: the engine, server endpoints, CLI, connectors in this repository,
the connector sandbox, and the agents (including prompt injection that leads
to data access beyond the user's permissions).

Out of scope: third-party connectors in the community registry (report those to
their maintainers), and denial of service through queries that are simply
very expensive.
