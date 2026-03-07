# Lab-Loop-v1: Formal Operational Manual

## 1. Philosophical Foundation
The Lab operates as a **Concurrent State Machine**. Agents are asynchronous processes, and Notion properties are shared, weakly-consistent memory addresses. To prevent "logical blunders" (lost updates, double-firing, or stale states), every agent must adhere to the **TLA+ Formal Model**.

## 2. The Core Protocol: Atomic Consumption
The most critical rule in the Lab is **Consume-First Idempotency**. 

### The Handshake
Before an agent performs any primary task (Spec, Dispatch, or Synthesis), it must execute an **Atomic Consume** transaction. This prevents "Zombies" (signals that stay active after the work has started).

**Mathematical Transition ($\alpha_{Consume}$):**
1.  **Clear the Signal Bit** (`Checkbox = false`).
2.  **Record the Timestamp** (`Consumed At = now()`).
3.  **Update the Status** (e.g., `Not Started` → `Prompt Requested`).

**Implementation:**
Always use the `NotionAPIClient.atomic_consume()` method in `cli/notion_api.py`. Never perform these updates as separate calls.

## 3. The Model Checker (Lab Auditor)
The `cli/lab_auditor.py` script is the Lab's "Central Nervous System." It runs SQL-based model checking to verify that the live workspace hasn't violated mathematical invariants.

### The Invariants
*   **E.1 Safety (Signal Integrity)**: If a `Consumed At` timestamp exists, the corresponding `Trigger Bit` **must** be false.
*   **E.2 Exclusive Ownership**: A project cannot have an `Active GitHub Issue` if the Work Item status is `Done`. (Resolves "Dangling Factory Pointers").
*   **E.4 Liveness**: Work Items cannot remain in `Prompt Requested` or `In Progress` for > 24h.
*   **E.7 Consume-First Compliance**: Post-epoch (2026-03-06) items with an active bit but no timestamp are flagged as **P0 Failures**.

**Command:**
```bash
export PYTHONPATH=$PYTHONPATH:.
python3 cli/lab_auditor.py
```

## 4. Domain Boundaries (Lab vs. Factory)
*   **The Lab (Notion)**: The domain of **Epistemic Uncertainty**. Holds Work Items, Specs, and Findings.
*   **The Factory (GitHub)**: The domain of **Deterministic Engineering**. Holds Issues, PRs, and Code.

### The Return Protocol
The loop is closed when the **Factory** signals completion to the **Lab**:
1.  **Signal**: Agent closes a GitHub Issue or merges a PR.
2.  **Intake**: Webhook Bridge (`github_return.py`) detects the event.
3.  **Handoff**: Bridge moves Notion status to `Done` and triggers the **Librarian** (`LR=true`).
4.  **Terminal Action**: The `Active GitHub Issue` URL **must** be cleared from the Project page to release the lock.

## 5. API Configuration
The fleet has migrated to the **Official Notion Databases API**. Do not use experimental "Data Source" endpoints.

**Canonical IDs:**
*   **Work Items DB**: `daeb64d4-e5a8-4a7b-b0dc-7555cbc3def6`
*   **Lab Audit Log**: `4621be9a-0709-443e-bee6-7e6166f76fae`
*   **Lab Projects**: `389645af-0e4f-479e-a910-79b169a99462`

## 6. Troubleshooting
*   **401 Unauthorized**: The `NOTION_TOKEN` environment variable is missing or invalid.
*   **404 Not Found**: The database has not been **Shared** with the integration in the Notion UI.
*   **P0: E.7 Error**: An agent has "hallucinated" consumption. Check the agent's instructions to ensure it uses the `atomic_consume` flow.
