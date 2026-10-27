# Governance

How decisions are made, who can merge, and what stays open source.

## Roles

| Role | Who | Can do |
|---|---|---|
| **Contributor** | Anyone who opens an issue, pull request or review | Propose changes, review, vote informally in discussions |
| **Committer** | Contributors with a record of good work in an area | Approve and merge pull requests in the areas they own (see `CODEOWNERS`) |
| **Maintainer** | Committers who look after the whole project | Approve RFCs, cut releases, add committers, set the roadmap |
| **Project lead** | The founder, during the early phase | Final call when maintainers can't agree |

### Becoming a committer

Any maintainer can nominate a contributor who has made several solid
contributions and helped others in review. Maintainers decide by consensus.
New committers start with ownership of the area they've worked in.

### Stepping down

Committers and maintainers who have been inactive for six months move to
"emeritus" and can return by asking.

## Making decisions

- **Day-to-day changes:** one committer approval for the affected area.
- **Public APIs, formats and protocols:** an RFC, approved by two maintainers
  after at least one week open for comment.
- **Disagreements:** discuss in the issue or RFC and aim for consensus. If
  that fails, maintainers vote by simple majority; the project lead breaks ties.

All decisions happen in public: issues, pull requests, RFCs and Discussions.
Decisions made in private chats are written up in a public issue.

## The open-core boundary

data-in-motion is open core. The company behind it sells a hosted cloud
service and enterprise features. To keep trust with the community, we commit to:

**Always open source (Apache 2.0):**
- The engine, distributed execution, pipelines, storage and Iceberg support
- The CLI, web UI, HTTP API, Postgres wire protocol, Flight SQL and MCP server
- The connector SDK, framework, protocol, conformance tests and community connectors
- The semantic layer and the chat and builder agents
- Everything a single team needs to run it in production themselves

**Commercial:**
- The hosted cloud service (control plane, autoscaling, billing)
- Organization-scale features: SSO and SCIM, fine-grained audit, cross-region
  disaster recovery, private connector registry, certified enterprise
  connectors (for example SAP and mainframe), support and SLAs

**Our promises:**
- We will never move a feature from open source to commercial.
- We will never change the license of code already released.
- Commercial features are built on public extension points, so the community
  can build alternatives.

## Trademark

The code is free to use, change and redistribute under Apache 2.0. The project
name and logo are trademarks of the company. You may say your product "works
with" or "is built on" the project. You may not use the name or logo for a
fork or a hosted service in a way that suggests it is the official project.
A full trademark policy will be published before the first release.

## Changing this document

Changes to governance need an RFC and approval from a majority of maintainers.
