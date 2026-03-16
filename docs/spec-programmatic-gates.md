# Spec: Programmatic Gate Enforcement (Pre-Flight + Cascade Depth)

## Problem

Pre-Flight Mode and Cascade Depth Tracking are currently enforced inside agent
instructions using inline SQL + LLM arithmetic. This is:

- **Incomplete**: Only 5/28 agents have Pre-Flight; only 3/28 have cascade depth.
- **Unreliable**: LLMs doing `depth + 1` and comparison is non-deterministic.
- **Wasteful**: ~100-150 instruction tokens per agent for logic that a single
  Python function handles deterministically in <1ms.
- **Unmaintainable**: Changing the gate logic requires updating N agent
  instruction pages manually.

## Design

Move enforcement out of agent instructions and into two layers:

1. **`dispatch.py` validation** — gates the programmatic dispatch path
   (webhook receiver, MCP tools). Adds V13 (Pre-Flight) and V14 (Cascade Depth)
   to the existing V1-V12 validation pipeline.

2. **New MCP tool `check_gates`** — gates the Notion-native automation path
   (where DB automations invoke agents directly, bypassing `dispatch.py`).
   Agents call this tool instead of running inline SQL.

Both layers share the same Python gate logic. One implementation, two entry points.

## Architecture Context

There are two invocation paths for Lab agents:

```
Path A — Programmatic dispatch (OpenClaw, external execution planes)
  Notion automation → webhook /notion-dispatch → dispatch.py → execution plane
  Gate: V13/V14 in build_dispatch_packet()

Path B — Notion-native automation (internal Lab agents)
  Notion DB automation → directly invokes agent in a thread
  Gate: Agent calls check_gates() MCP tool as first action
```

Path A already has V1-V12 validation. Adding V13/V14 there is a one-line
addition. Path B has no programmatic middleware — the agent IS the compute
layer, so it must call the gate tool. But the tool does the logic, not the
agent.

## Changes

### 1. New Work Item property: `Cascade Depth` (number, nullable)

- `null` / empty = human-originated (treated as depth 1).
- Set programmatically when an agent creates a successor Work Item.
- Read by `build_dispatch_packet()` and `check_gates()`.

### 2. New config entry: `LAB_CONTROL_DB_ID`

Add to `cli/config.py`. Value: `60928daf-eb88-47eb-8cce-ccf2047c8bdc`
(the Lab Control database already used by agents in inline SQL).

### 3. Gate logic in `dispatch.py`

```python
def _query_lab_control(client, parameter: str) -> dict | None:
    """Query Lab Control DB for a named parameter row."""
    cfg = get_config()
    pages = client.query_all(
        cfg.lab_control_db_id,
        filter_payload={
            "property": "Parameter",
            "rich_text": {"equals": parameter},
        },
    )
    if not pages:
        return None
    props = pages[0].get("properties", {})
    return {
        "flag": _checkbox(props, "Flag"),
        "description": _text(props, "Description"),
    }


def check_gates(
    work_item_id: str,
    client: NotionAPIClient | None = None,
) -> dict:
    """Programmatic Pre-Flight + Cascade Depth gate check.

    Returns:
      {"proceed": True} or
      {"halt": True, "reason": "...", "detail": "..."}
    """
    if client is None:
        client = NotionAPIClient(get_config().notion_token)

    # G1: Pre-Flight Mode
    pf = _query_lab_control(client, "Pre-Flight Mode")
    if pf and pf["flag"]:
        return {
            "halt": True,
            "reason": "pre_flight_active",
            "detail": "Pre-Flight Mode is active. All dispatch suspended.",
        }

    # G2: Cascade Depth
    page = client.retrieve_page(work_item_id)
    props = page.get("properties", {})
    depth = _number(props, "Cascade Depth")  # None = human-originated
    if depth is None:
        depth = 1

    max_depth_row = _query_lab_control(client, "Max Cascade Depth")
    max_depth = int(max_depth_row["description"]) if max_depth_row else 5

    if depth >= max_depth:
        return {
            "halt": True,
            "reason": "cascade_depth_exceeded",
            "detail": f"Cascade depth {depth} >= limit {max_depth}.",
        }

    return {"proceed": True, "cascade_depth": depth}
```

### 4. Integration into `build_dispatch_packet()`

Add after existing field extraction, before V1:

```python
# V13: Pre-Flight Mode
pf = _query_lab_control(client, "Pre-Flight Mode")
if pf and pf["flag"]:
    errors.append("V13: Pre-Flight Mode active — all dispatch suspended")

# V14: Cascade Depth
depth = _number(props, "Cascade Depth") or 1
max_depth_row = _query_lab_control(client, "Max Cascade Depth")
max_depth = int(max_depth_row["description"]) if max_depth_row else 5
if depth >= max_depth:
    errors.append(f"V14: Cascade depth {depth} >= limit {max_depth}")
```

Add `cascade_depth` to the dispatch packet schema and payload:

```json
"cascade_depth": {"type": "integer", "minimum": 1, "default": 1}
```

### 5. New MCP tool: `check_gates`

Exposed in `mcp_server.py`:

```python
@mcp.tool()
def check_gates(work_item_id: str) -> str:
    """
    Check Pre-Flight and Cascade Depth gates for a Work Item.

    Call this before performing any writes. If result contains "halt",
    stop immediately and report the reason.

    Returns JSON: {"proceed": true, "cascade_depth": N}
    or {"halt": true, "reason": "...", "detail": "..."}
    """
    result = dispatch.check_gates(work_item_id)
    return json.dumps(result)
```

### 6. Cascade depth stamping

Agents that create successor Work Items must stamp `Cascade Depth` on the new
item. This happens in two places today:

| Creator | Trigger | Current behavior |
|---|---|---|
| Lab Projects (Inconclusive Handler) | `Synthesis Complete` on Inconclusive WI | Creates successor WI with `Status = Not Started` |
| Research Designer (Back-End Branch) | `Synthesis Completed At` updated | Creates successor WI(s) |

Both already set properties on the new WI. Add one more:

```
"Cascade Depth": parent_cascade_depth + 1
```

This is the ONLY place increment-by-one happens, and it happens in the
agent's property write (a tool call parameter), not in the agent's reasoning.
The agent doesn't do arithmetic — it reads `cascade_depth` from the
`check_gates` response and passes `cascade_depth + 1` as a literal value in
the create_page call.

Alternatively, successor creation could go through a new MCP tool
`create_successor_work_item(parent_id, properties)` that handles the depth
stamp server-side, making the agent completely arithmetic-free.

### 7. Agent instruction changes

**Remove entirely** (replaced by `check_gates` tool):

| Agent | Remove | Token savings (approx) |
|---|---|---|
| Headhunter | Pre-Flight SQL + Cascade Depth SQL + halt logic | ~200 tokens |
| Lab Auditor | Pre-Flight SQL + halt logic | ~100 tokens |
| Lab Projects | Pre-Flight SQL + halt logic | ~100 tokens |
| Lab Trailblazer | Pre-Flight SQL + Cascade Depth SQL + halt logic | ~200 tokens |
| Lab Ship Class | Pre-Flight SQL + Cascade Depth SQL + halt logic | ~200 tokens |
| Lab Research Designer | Pre-Flight SQL + halt logic | ~100 tokens |

**Replace with** (one line per agent):

```
## Pre-Flight & Cascade Gate
Call `check_gates(work_item_id)` before any writes. If it returns `halt`, stop
and report the reason verbatim. Do not proceed.
```

~30 tokens vs ~100-200 tokens per agent. Across 6 agents today: ~900 tokens
saved. If rolled out to all 28 agents: ~2800 tokens saved from instruction
budgets.

### 8. Rollout to remaining agents

Once `check_gates` exists, adding it to any agent is a single instruction
line. The gate logic (which parameters to check, what limits apply) lives in
Python and Lab Control — not in each agent's instructions.

Priority order for rollout beyond the current 6:
1. **Pipeline agents** (Dispatcher, Prompt Architect, Intake Clerk, Librarian)
   — these form the dispatch chain and are the primary cascade path.
2. **Operational agents** (MDE Executor, MDE Execution Auditor, Data Entry
   Instructors) — these have their own trigger chains.
3. **Advisory agents** (Study Buddy, Career Strategist, etc.) — low priority,
   no write chains.

## What stays in Lab Control DB

The Lab Control database remains the source of truth for:
- `Pre-Flight Mode` (checkbox flag)
- `Max Cascade Depth` (number in Description field)

No schema changes to Lab Control needed. The programmatic layer reads these
the same way agents did — just via API instead of inline SQL.

## What stays in agent instructions

Agents that have **domain-specific idempotency gates** (e.g., Dispatcher's
"halt if Dispatch Requested Consumed At is already set") keep those. The
`check_gates` tool handles system-wide safety; per-agent idempotency remains
the agent's job because it requires reading domain-specific properties.

## Files changed

| File | Change |
|---|---|
| `cli/config.py` | Add `LAB_CONTROL_DB_ID` |
| `cli/dispatch.py` | Add `_query_lab_control`, `check_gates`, `_number` helper; V13/V14 in `build_dispatch_packet`; `cascade_depth` in packet |
| `cli/mcp_server.py` | Expose `check_gates` tool |
| `cli/contracts/dispatch_packet.schema.json` | Add `cascade_depth` field |
| 6 agent instruction pages | Remove inline SQL gates, add `check_gates` call |

## Kill condition

If the `check_gates` MCP tool adds measurable latency to agent invocations
(the Lab Control query adds one Notion API round-trip), consider caching the
Pre-Flight and Max Cascade Depth values with a short TTL (30-60s). These
parameters change rarely.
