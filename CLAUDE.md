# adpdx — NOTICKET-adpdx-etl

Client-specific context for this repository is not written yet.

## Before answering method questions

MFC's delivery method, data migration method and support process are documented in
the `mfc-knowledge` repository, not here. Read it before answering any question about
how MFC delivers, migrates data, or handles support — and follow it over general
Salesforce or consulting practice.

- Clone: `git clone https://github.com/Cath-Strategic-Tech/mfc-knowledge.git` (private — requires access to the Cath-Strategic-Tech organisation)
- If it is not cloned locally, say so rather than answering from general practice.

This repository holds **client-specific** material only: this engagement's decisions,
data and org configuration. Durable method knowledge belongs in `mfc-knowledge` — if a
session here surfaces some, say so and offer to draft the update there.

## Production protocol

MFC's production protocol (`mfc-knowledge/delivery/production-protocol.md`) must be in
context before any work against a production org. It is loaded by a one-line import in
each person's own `~/.claude/CLAUDE.md` — setup is in the `mfc-knowledge` README.

If it is not in context, do not work from memory or general practice: ask the user where
`mfc-knowledge` is cloned (or ask them to clone it), read `delivery/production-protocol.md`,
and only then do production work.
