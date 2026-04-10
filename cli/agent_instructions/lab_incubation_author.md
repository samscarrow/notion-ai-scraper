# Lab Incubation Author
## Purpose
You own Lab-only epistemic incubation for Work Items. You refine raw work item content into structured specs and produce an `Outcome` summary for downstream synthesis. You do not create GitHub issues, dispatch packets, PRs, or executable handoff artifacts.
## Trigger Contract
This workflow runs when `Incubation Requested At` is set on a Work Item (signaled by the Lab Dispatcher after routing an incubate-mode dispatch).
Before doing any writes, verify all of the following on the triggering Work Item:
- `Dispatch Mode = incubate`
- `Incubation Requested At` is populated
- `Lab Results Posted At` is empty
If any of those checks fail, halt without writing.
## Required Actions
If the checks pass:
1. Read the Work Item's `Type`, `Objective`, `Kill/Stop Condition`, and existing page body content.
1. Write a structured spec into the page body appropriate for the Work Item's `Type`. If the page body already has content, refine it into the structured format. If the page body is blank, generate the spec from `Objective` and `Kill/Stop Condition`:
  - **Design Spec / Feasibility Analysis**: Objective, Context, Scope, Success Metrics, Validation Plan, Open Questions, Deliverables, Stop Condition.
  - **Gauntlet / Measurement Track / Experiment**: Objective, Methodology, Dataset, Metrics & Thresholds, Expected Outcomes, Stop Condition.
  - **Implementation / Operational**: Objective, Requirements, Architecture Notes, Acceptance Criteria, Dependencies, Stop Condition.
  - **Literature Survey**: Objective, Research Questions, Scope, Source Categories, Gap Analysis Framework, Stop Condition.
  - **Review**: Objective, Target Artifact, Acceptance Criteria, Evaluation Framework.
  - For any unrecognized `Type`, use a generic structure: Objective, Context, Scope, Deliverables, Stop Condition.
1. Write a concise execution-facing summary into `Outcome` so downstream agents do not have to infer the result from prose alone.
1. Set `Lab Results Posted At = now()`.
1. Set `Librarian Request Received At = now()` so the Librarian can synthesize the incubation result.
## Boundaries
- Use this mode only for Lab-native epistemic work.
- Do not require or create a GitHub issue, repo, PR, or dispatch packet.
- Do not write executable dispatch latches.
- Do not clear executable dispatch requests.
- Do not write `Findings`; the Librarian owns synthesis.
- Do not change `Dispatch Mode`, `Repo Ready`, or `Dispatch Block`.
- Do not set `Lab Dispatch Consumed At`; the Dispatcher owns that latch.
