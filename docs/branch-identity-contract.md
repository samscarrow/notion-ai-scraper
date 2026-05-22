# Lab Branch Identity Contract

## Purpose

Work Item identity is canonical. Git branch names are execution artifacts.

This contract prevents agents from rebranching or rewriting useful work solely because the branch name does not match the Work Item handle.

## Canonical identity

The Lab Work Item owns the durable identity of a unit of work.

Examples:

- `SRA-IMP-1`
- `HCA-DS-2`
- `CS-EXP-4`

A branch is only the execution location for that work.

## Branch policy

Preferred branch names should include the Work Item handle:

```text
work/sra-imp-1
```

or:

```text
sra-imp-1
```

Branch-name handles are case-insensitive. Lowercase is preferred for branch names, even when the Work Item handle is displayed uppercase in Notion.

Existing substantial work on a noncanonical branch is acceptable when all of these are true:

1. The branch maps cleanly to one Work Item.
2. The branch is pushed or otherwise inspectable.
3. The Work Item records the actual branch name in `Branch`.
4. The issue or PR title/body references the Work Item handle.
5. The branch passes the Work Item objective, `Kill/Stop Condition`, metrics, and acceptance checks.

Branch-name mismatch is metadata, not failure.

## Rebranch only for contamination

A fresh canonical branch is justified only when the existing branch is contaminated or unsuitable for review.

Rebranch when:

- unrelated Work Items are mixed into the diff
- `git log main..HEAD --oneline` contains unrelated commits
- the branch base is wrong enough to make review or merge unsafe
- the branch contains discarded experiments or failed approaches that should not ship
- the branch cannot be mapped cleanly to one Work Item

Do not rebranch solely for naming hygiene.

## Dispatch packet meaning

The `branch` field in a dispatch packet means the actual execution branch, not necessarily the canonical-preferred branch name.

If the Work Item `Branch` property is populated, execution agents should treat that value as authoritative.

If the Work Item `Branch` property is empty, the control plane dispatches `branch=main` for non-sandbox items. Execution agents should follow the dispatch packet. Creating a new preferred branch for implementation work requires setting the Work Item `Branch` property before dispatch.

## PR convention

A PR from a noncanonical branch must carry the Work Item identity in the title or body.

Example title:

```text
SRA-IMP-1: Normalize scholarly corpus passage layer
```

Example body block:

```text
Lab Work Item: SRA-IMP-1
Implementation Branch: fix/issue-25
Canonical Branch Preference: work/sra-imp-1
Branch Exception: accepted; existing work predates branch discipline
```

## Acceptance rule

Acceptance is determined by:

- Work Item objective
- Kill/Stop Condition
- Metrics
- tests and validation logs
- scope cleanliness
- review outcome

Acceptance is not determined by whether the branch string is canonical.
