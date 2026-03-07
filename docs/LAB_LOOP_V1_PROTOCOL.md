# Lab-Loop-v1: Formal Operational Manual

## 1. Philosophical Foundation
The Lab operates as a **Concurrent State Machine**. Agents are asynchronous processes, and Notion properties are shared, weakly-consistent memory addresses. To prevent "logical blunders" (lost updates, double-firing, or stale states), every agent must adhere to the **TLA+ Formal Model**.

## 2. The Core Protocol: Atomic Consumption
The most critical rule in the Lab is **Consume-First Idempotency**.

### The Handshake
Before an agent performs any primary task (Spec, Dispatch, or Synthesis), it must execute an **Atomic Consume** transaction. This prevents "Zombies" (signals that stay active after the work has started).

**The two-step atomic write:**
1.  **Clear the Signal Bit** (`Checkbox = false`).
2.  **Record the Timestamp** (`Consumed At = now()`).

Both properties are written in a single API call via `NotionAPIClient.atomic_consume()` in `cli/notion_api.py`. Status updates are the caller's responsibility and happen separately after consumption.

## 3. The Model Checker (Lab Auditor)
The `cli/lab_auditor.py` script is a batch invariant checker. It queries the Notion API to verify that the live workspace hasn't violated the formal model's invariants.

### The Invariants
*   **E.1 Safety (Signal Integrity)**: If a `Consumed At` timestamp exists, the corresponding `Trigger Bit` **must** be false. Detects "Zombies."
*   **E.2 Exclusive Ownership (Dangling Factory Pointers)**: A Lab Project with `Active GitHub Issue` set must have at least one non-terminal Work Item. If all related Work Items are terminal (Done/Passed/Kill/Inconclusive), the Return Protocol failed to clear the lock.
*   **E.4 Liveness**: Work Items cannot remain in `Prompt Requested` or `In Progress` for > 24h without an edit.
*   **E.7 Consume-First Compliance**: Post-epoch (2026-03-06) items with an active signal bit but no consumption timestamp are flagged as **P0 Failures**.

**Command:**
```bash
NOTION_TOKEN=<token> python3 cli/lab_auditor.py
```

## 4. The Trigger Map

### Native Automations (Work Items DB)
| # | Trigger | Writes | Notes |
|---|---|---|---|
| 1 | `pagePropertiesEdited` (unfiltered) | `Dispatch Requested`, `Status` | **Under investigation (LL-EXP-2)** — may fire on any edit |
| 2 | `Status` → status_is | `Shadow Requested` | MDE pipeline trigger |
| 3 | `Status` → status_is | `Shadow Requested` | MDE pipeline trigger |

### Agent Triggers (property-change on Work Items DB)
| Agent | Property | Condition |
|---|---|---|
| Lab Dispatcher | `Dispatch Requested` | checkbox = true |
| Prompt Architect | `Dispatch Via` | enum matches any value |
| Librarian | `Librarian Request` | checkbox = true |
| Return Protocol Agent | `Status` | = Done |

### Trigger Chain: Dispatch
1. Something sets `Dispatch Via` on a Work Item.
2. Automation 1 (native) may also fire, setting `Dispatch Requested = true` and `Status = Prompt Requested`.
3. `Dispatch Requested = true` triggers the **Lab Dispatcher** (fierce_mystic).
4. `Dispatch Via` change triggers the **Prompt Architect**.

### Trigger Chain: Return
1. `github_return.py` sets Work Item Status = Done, LR = true, Run Date, Return Consumed At.
2. Status = Done triggers the **Return Protocol Agent**, which clears `Active GitHub Issue` on the parent Lab Project.
3. LR = true triggers the **Librarian**, which synthesizes findings.

## 5. Domain Boundaries (Lab vs. Factory)
*   **The Lab (Notion)**: The domain of **Epistemic Uncertainty**. Holds Work Items, Specs, and Findings.
*   **The Factory (GitHub)**: The domain of **Deterministic Engineering**. Holds Issues, PRs, and Code.

### Key Properties by Entity
| Entity | Property | Purpose |
|---|---|---|
| Work Item | `GitHub Issue URL` | Permanent reference to the Factory issue |
| Lab Project | `Active GitHub Issue` | Lock — set on handoff, cleared by Return Protocol Agent |

## 6. API Configuration

**Canonical Database IDs:**
*   **Work Items DB**: `daeb64d4-e5a8-4a7b-b0dc-7555cbc3def6`
*   **Lab Audit Log**: `4621be9a-0709-443e-bee6-7e6166f76fae`
*   **Lab Projects**: `389645af-0e4f-479e-a910-79b169a99462`

The `cli/notion_api.py` client uses the official Notion Databases API (`/v1/databases/{id}/query`). The Notion MCP plugin uses `collection://` data source IDs for page creation.

## 7. Troubleshooting
*   **401 Unauthorized**: The `NOTION_TOKEN` environment variable is missing or invalid.
*   **404 Not Found**: The database has not been **Shared** with the integration in the Notion UI.
*   **P0: E.7 Error**: An agent skipped the consume-first step. Check the agent's instructions to ensure it uses the `atomic_consume` flow.
*   **E.2 Dangling Pointer**: The Return Protocol Agent failed to clear `Active GitHub Issue` on the Lab Project after all Work Items reached terminal state.
