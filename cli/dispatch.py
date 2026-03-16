"""
dispatch.py — Dispatch adapter for the Lab control plane.

Reads Work Items from the Lab, validates them against the OpenClaw v1.1 contract,
produces dispatch packets that execution planes can consume, and ingests return
payloads when execution completes.

Entry points (exposed as MCP tools in mcp_server.py):
  - get_dispatchable_items()  — find ready work
  - build_dispatch_packet()   — validate + build packet for one item
  - stamp_dispatch_consumed() — mark item as consumed + In Progress
  - handle_final_return()     — ingest execution results, trigger intake
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any

import notion_api
from config import get_config

# ── Contract configs (loaded once) ───────────────────────────────────────────

_CONTRACTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "contracts")


def _load_contract(name: str) -> dict[str, Any]:
    with open(os.path.join(_CONTRACTS_DIR, name), "r") as f:
        return json.load(f)


LANE_CAPABILITIES = _load_contract("lane_capabilities.json")
ENV_RESTRICTIONS = _load_contract("environment_restrictions.json")
VERDICT_MAPPING = _load_contract("verdict_state_mapping.json")
REDACTION_CONFIG = _load_contract("redaction_patterns.json")

VALID_LANES = set(LANE_CAPABILITIES["lanes"].keys())
VALID_ENVIRONMENTS = set(ENV_RESTRICTIONS["environments"].keys())
DISPATCH_VIA_DEFAULTS = LANE_CAPABILITIES["dispatch_via_defaults"]
VALID_DISPATCH_VIA = set(DISPATCH_VIA_DEFAULTS.keys())
VALID_TYPES = {"Gauntlet", "Measurement Track", "Literature Survey", "Design Spec", "Feasibility Analysis", "Other"}


# ── Property extraction helpers ──────────────────────────────────────────────

def _text(props: dict, key: str) -> str:
    """Extract plain text from a rich_text property."""
    return "".join(
        t.get("plain_text", "") for t in (props.get(key, {}) or {}).get("rich_text", [])
    ).strip()


def _title(props: dict, key: str) -> str:
    """Extract plain text from a title property."""
    return "".join(
        t.get("plain_text", "") for t in (props.get(key, {}) or {}).get("title", [])
    ).strip()


def _select(props: dict, key: str) -> str | None:
    return ((props.get(key, {}) or {}).get("select") or {}).get("name")


def _status(props: dict, key: str = "Status") -> str | None:
    return ((props.get(key, {}) or {}).get("status") or {}).get("name")


def _url(props: dict, key: str) -> str | None:
    return (props.get(key, {}) or {}).get("url") or None


def _date_start(props: dict, key: str) -> str | None:
    return ((props.get(key, {}) or {}).get("date") or {}).get("start")


def _checkbox(props: dict, key: str) -> bool:
    return (props.get(key, {}) or {}).get("checkbox", False)


def _number(props: dict, key: str) -> int | float | None:
    return (props.get(key, {}) or {}).get("number")


def _relation_ids(props: dict, key: str) -> list[str]:
    return [r["id"] for r in (props.get(key, {}) or {}).get("relation", []) if r.get("id")]


# ── Lab Control queries ──────────────────────────────────────────────────────

# Cache Lab Control values with a short TTL to avoid repeated API calls.
_lab_control_cache: dict[str, tuple[float, dict]] = {}
_LAB_CONTROL_TTL = 60  # seconds


def _query_lab_control(
    client: notion_api.NotionAPIClient,
    parameter: str,
) -> dict[str, Any] | None:
    """Query the Lab Control database for a named parameter row.

    Returns {"flag": bool, "description": str} or None if not found.
    Results are cached for 60 seconds.
    """
    import time as _time

    now = _time.monotonic()
    cached = _lab_control_cache.get(parameter)
    if cached and (now - cached[0]) < _LAB_CONTROL_TTL:
        return cached[1]

    cfg = get_config()
    pages = client.query_all(
        cfg.lab_control_db_id,
        filter_payload={
            "property": "Parameter",
            "title": {"equals": parameter},
        },
    )
    if not pages:
        _lab_control_cache[parameter] = (now, None)
        return None

    props = pages[0].get("properties", {})
    result = {
        "flag": _checkbox(props, "Flag"),
        "value": _number(props, "Value"),
    }
    _lab_control_cache[parameter] = (now, result)
    return result


def check_gates(
    work_item_id: str | None = None,
    client: notion_api.NotionAPIClient | None = None,
) -> dict[str, Any]:
    """Programmatic Pre-Flight + Cascade Depth gate check.

    If work_item_id is provided, checks both Pre-Flight and Cascade Depth.
    If omitted, checks Pre-Flight only (for agents not operating on a
    specific Work Item).

    Returns:
        {"proceed": True, "cascade_depth": N}
    or:
        {"halt": True, "reason": "...", "detail": "..."}
    """
    if client is None:
        client = notion_api.NotionAPIClient(get_config().notion_token)

    # G1: Pre-Flight Mode
    pf = _query_lab_control(client, "Pre-Flight Mode")
    if pf and pf["flag"]:
        return {
            "halt": True,
            "reason": "pre_flight_active",
            "detail": "Pre-Flight Mode is active. All dispatch suspended.",
        }

    # G2: Cascade Depth (only when a Work Item is in scope)
    depth = 1
    if work_item_id:
        page = client.retrieve_page(work_item_id)
        props = page.get("properties", {})
        raw_depth = _number(props, "Cascade Depth")
        if raw_depth is not None:
            depth = int(raw_depth)

        max_depth_row = _query_lab_control(client, "Max Cascade Depth")
        max_depth = int(max_depth_row["value"]) if max_depth_row and max_depth_row["value"] is not None else 5

        if depth >= max_depth:
            return {
                "halt": True,
                "reason": "cascade_depth_exceeded",
                "detail": f"Cascade depth {depth} >= limit {max_depth}.",
            }

    return {"proceed": True, "cascade_depth": depth}


# ── Core functions ───────────────────────────────────────────────────────────

def get_dispatchable_items(client: notion_api.NotionAPIClient | None = None) -> list[dict[str, Any]]:
    """Query Work Items DB for items ready to dispatch.

    Criteria: Dispatch Requested Received At is set, Dispatch Requested Consumed At is empty,
    Status in {Not Started, Prompt Requested}.
    """
    if client is None:
        client = notion_api.NotionAPIClient(get_config().notion_token)

    cfg = get_config()
    filter_payload = {
        "and": [
            {"property": "Dispatch Requested Received At", "date": {"is_not_empty": True}},
            {"property": "Dispatch Requested Consumed At", "date": {"is_empty": True}},
            {
                "or": [
                    {"property": "Status", "status": {"equals": "Not Started"}},
                    {"property": "Status", "status": {"equals": "Prompt Requested"}},
                ]
            },
        ]
    }

    pages = client.query_all(cfg.work_items_db_id, filter_payload=filter_payload)
    results = []
    for page in pages:
        props = page.get("properties", {})
        # Resolve project name if linked
        project_ids = _relation_ids(props, "Project")
        project_name = None
        if project_ids:
            try:
                proj_page = client.retrieve_page(project_ids[0])
                proj_props = proj_page.get("properties", {})
                project_name = _title(proj_props, "Project Name")
            except Exception:
                pass

        results.append({
            "id": page["id"],
            "name": _title(props, "Item Name"),
            "dispatch_via": _select(props, "Dispatch Via"),
            "execution_lane": _select(props, "Execution Lane"),
            "environment": _select(props, "Environment"),
            "branch": _text(props, "Branch"),
            "project_name": project_name,
            "status": _status(props),
            "type": _select(props, "Type"),
        })

    return results


def build_dispatch_packet(
    work_item_id: str,
    client: notion_api.NotionAPIClient | None = None,
) -> dict[str, Any]:
    """Build and validate a dispatch packet for a single Work Item.

    Returns {"packet": {...}, "errors": []} on success,
    or {"packet": None, "errors": ["V1: ...", ...]} on validation failure.
    """
    if client is None:
        client = notion_api.NotionAPIClient(get_config().notion_token)

    # Fetch Work Item
    page = client.retrieve_page(work_item_id)
    props = page.get("properties", {})

    # Extract fields
    item_name = _title(props, "Item Name")
    objective = _text(props, "Objective")
    kill_condition = _text(props, "Kill/Stop Condition")
    dispatch_via = _select(props, "Dispatch Via")
    execution_lane = _select(props, "Execution Lane")
    environment = _select(props, "Environment")
    branch = _text(props, "Branch") or None
    item_type = _select(props, "Type") or "Other"
    prompt_notes = _text(props, "Prompt Notes") or None
    github_issue_url = _url(props, "GitHub Issue URL")
    consumed_at = _date_start(props, "Dispatch Requested Consumed At")
    existing_run_id = _text(props, "run_id") if "run_id" in props else None

    # Resolve project
    project_ids = _relation_ids(props, "Project")
    project_name = None
    project_id = None
    if project_ids:
        project_id = project_ids[0]
        try:
            proj_page = client.retrieve_page(project_id)
            proj_props = proj_page.get("properties", {})
            project_name = _title(proj_props, "Project Name")
        except Exception:
            pass

    # Default execution lane from dispatch_via if not explicitly set
    if not execution_lane and dispatch_via:
        execution_lane = DISPATCH_VIA_DEFAULTS.get(dispatch_via)

    # Default environment to "dev" if not set
    if not environment:
        environment = "dev"

    # ── Cascade depth ────────────────────────────────────────────────────
    cascade_depth = _number(props, "Cascade Depth")
    if cascade_depth is None:
        cascade_depth = 1
    else:
        cascade_depth = int(cascade_depth)

    # ── Validation ───────────────────────────────────────────────────────
    errors: list[str] = []

    # V13: Pre-Flight Mode (checked first — blocks everything)
    pf = _query_lab_control(client, "Pre-Flight Mode")
    if pf and pf["flag"]:
        errors.append("V13: Pre-Flight Mode active — all dispatch suspended")

    # V14: Cascade Depth
    max_depth_row = _query_lab_control(client, "Max Cascade Depth")
    max_depth = int(max_depth_row["value"]) if max_depth_row and max_depth_row["value"] is not None else 5
    if cascade_depth >= max_depth:
        errors.append(f"V14: Cascade depth {cascade_depth} >= limit {max_depth}")

    # V1: valid UUID
    try:
        uuid.UUID(work_item_id)
    except ValueError:
        errors.append(f"V1: work_item_id '{work_item_id}' is not a valid UUID")

    # V2: known dispatch_via
    if not dispatch_via:
        errors.append("V2: dispatch_via is empty")
    elif dispatch_via not in VALID_DISPATCH_VIA:
        errors.append(f"V2: dispatch_via '{dispatch_via}' is not a known value")

    # V3: valid execution lane
    if not execution_lane:
        errors.append("V3: execution_lane could not be resolved (set Execution Lane or use a non-Manual Dispatch Via)")
    elif execution_lane not in VALID_LANES:
        errors.append(f"V3: execution_lane '{execution_lane}' is not a valid lane")

    # V4: valid environment
    if environment not in VALID_ENVIRONMENTS:
        errors.append(f"V4: environment '{environment}' is not valid (must be dev/staging/production/sandbox)")

    # V5: lane compatible with environment
    if execution_lane and environment and environment in ENV_RESTRICTIONS["environments"]:
        allowed = ENV_RESTRICTIONS["environments"][environment]["allowed_lanes"]
        if allowed != "*" and execution_lane not in allowed:
            errors.append(
                f"V5: lane '{execution_lane}' is not allowed in '{environment}' environment "
                f"(allowed: {', '.join(allowed)})"
            )

    # V6: objective non-empty
    if not objective:
        errors.append("V6: objective is empty")

    # V7: kill_condition required for Gauntlet
    if item_type == "Gauntlet" and not kill_condition:
        errors.append("V7: kill_condition is required for Gauntlet type items")

    # V8: no active run_id (idempotency)
    if existing_run_id:
        errors.append(f"V8: work item already has an active run_id '{existing_run_id}'")

    # V9: dispatch request must exist
    received_at = _date_start(props, "Dispatch Requested Received At")
    if not received_at:
        errors.append("V9: Dispatch Requested Received At is empty (no dispatch request)")

    # V10: not already consumed
    if consumed_at:
        errors.append(f"V10: Dispatch Requested Consumed At is already set ({consumed_at})")

    # V11: production audit logging
    production_audit = False
    if environment == "production":
        production_audit = True
        # Not an error, just a flag — logged in the packet

    # V12: branch required for non-sandbox
    if environment != "sandbox" and not branch:
        # Soft warning, not blocking — default to "main" if not set
        branch = "main"

    if errors:
        return {"packet": None, "errors": errors}

    # ── Build packet ─────────────────────────────────────────────────────
    run_id = str(uuid.uuid4())
    lane_caps = LANE_CAPABILITIES["lanes"].get(execution_lane, {})

    packet = {
        "version": "1.1",
        "run_id": run_id,
        "work_item_id": work_item_id,
        "work_item_name": item_name,
        "project_name": project_name,
        "project_id": project_id,
        "objective": objective,
        "kill_condition": kill_condition or None,
        "dispatch_via": dispatch_via,
        "execution_lane": execution_lane,
        "environment": environment,
        "branch": branch,
        "type": item_type,
        "prompt_notes": prompt_notes,
        "github_issue_url": github_issue_url,
        "cascade_depth": cascade_depth,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "constraints": {
            "can_code": lane_caps.get("can_code", False),
            "can_browse": lane_caps.get("can_browse", False),
            "can_deploy": lane_caps.get("can_deploy", False),
            "write_scope": lane_caps.get("write_scope", "none"),
            "max_timeout_s": lane_caps.get("max_timeout_s", 300),
        },
    }

    return {"packet": packet, "errors": [], "_production_audit": production_audit}


def stamp_dispatch_consumed(
    work_item_id: str,
    run_id: str,
    client: notion_api.NotionAPIClient | None = None,
) -> dict[str, Any]:
    """Mark a Work Item as consumed: set timestamp, status, and run_id.

    Returns the updated page properties on success.
    """
    if client is None:
        client = notion_api.NotionAPIClient(get_config().notion_token)

    cfg = get_config()
    now = notion_api.now_iso()

    # Update Work Item properties
    properties: dict[str, Any] = {
        "Dispatch Requested Consumed At": {"date": {"start": now}},
        "Status": {"status": {"name": "In Progress"}},
        "run_id": {"rich_text": [{"type": "text", "text": {"content": run_id}}]},
    }

    result = client.update_page(work_item_id, properties)

    # Create audit log entry
    try:
        client.create_page(
            parent={"database_id": cfg.audit_log_db_id},
            properties={
                "Transition": {"title": [{"type": "text", "text": {"content": "NotStarted\u2192InProgress"}}]},
                "Work Item": {"relation": [{"id": work_item_id}]},
                "Agent": {"select": {"name": "Dispatch Adapter"}},
                "Consumption Timestamp": {"date": {"start": now}},
            },
        )
    except Exception:
        # Audit log failure should not block dispatch
        pass

    return {"status": "consumed", "work_item_id": work_item_id, "run_id": run_id, "consumed_at": now}


# ── Return ingestion ─────────────────────────────────────────────────────────

VALID_RETURN_STATUSES = {"ok", "error", "gated", "timeout"}
VALID_VERDICTS = {"PASS", "FAIL", "INCONCLUSIVE", "OBSERVATIONS"}


def _resolve_verdict_mapping(
    verdict: str | None, work_item_type: str | None, status: str,
) -> dict[str, Any]:
    """Map return status + verdict to Notion Status/Verdict properties.

    Uses the same verdict_state_mapping.json as the aws-ec2 webhook bridge.
    """
    if status != "ok":
        entry = VERDICT_MAPPING.get("error_states", {}).get(status)
        if entry:
            return entry
        return {"status": "Blocked", "verdict": None}

    if not verdict:
        return {"status": "Done", "verdict": None}

    is_gauntlet = work_item_type == "Gauntlet"
    key = "gauntlet" if is_gauntlet else "non_gauntlet"
    entry = VERDICT_MAPPING.get(key, {}).get(verdict)

    if entry is None:
        # OBSERVATIONS on Gauntlet → treat as INCONCLUSIVE + warning
        if verdict == "OBSERVATIONS" and is_gauntlet:
            fallback = VERDICT_MAPPING["gauntlet"]["INCONCLUSIVE"]
            return {**fallback, "warning": "OBSERVATIONS invalid for Gauntlet — treated as INCONCLUSIVE"}
        return {"status": "Done", "verdict": None}

    return entry


def _check_return_idempotency(
    client: notion_api.NotionAPIClient, page_id: str, run_id: str,
) -> bool:
    """Check if this run_id has already been ingested by scanning page content."""
    try:
        blocks = client.list_block_children(page_id, page_size=100)
        for block in blocks:
            if block.get("type") == "heading_3":
                texts = block.get("heading_3", {}).get("rich_text", [])
                for t in texts:
                    if run_id in t.get("text", {}).get("content", ""):
                        return True
    except Exception:
        pass
    return False


def _apply_redaction(text: str) -> str:
    """Apply redaction patterns from shared contract config."""
    import re
    for pattern_def in REDACTION_CONFIG.get("patterns", []):
        compiled = re.compile(pattern_def["regex"])
        replacement = REDACTION_CONFIG["replacement"].replace("{label}", pattern_def["label"])
        text = compiled.sub(replacement, text)
    return text


def handle_final_return(
    work_item_id: str,
    run_id: str,
    status: str,
    summary: str,
    raw_output: str,
    duration_ms: int,
    model: str,
    lane: str,
    verdict: str | None = None,
    error: str | None = None,
    metrics: dict | None = None,
    artifacts: list[dict] | None = None,
    files_changed: list[str] | None = None,
    commit_sha: str | None = None,
    pr_url: str | None = None,
    client: notion_api.NotionAPIClient | None = None,
) -> dict[str, Any]:
    """Ingest a final return payload from an execution plane.

    Mirrors the aws-ec2 webhook bridge's _ingest_final_return logic so both
    return paths (GitHub webhook and direct MCP) produce identical Notion state.

    Flow:
      1. Validate return fields (R1-R5)
      2. Idempotency check (duplicate run_id → reject)
      3. Map verdict → Status/Verdict properties
      4. Set Return Received At (triggers Lab Intake Clerk)
      5. Append content blocks (summary, raw output, artifacts)
      6. Write audit log entry
    """
    if client is None:
        client = notion_api.NotionAPIClient(get_config().notion_token)

    cfg = get_config()

    # ── R1-R5 validation ──────────────────────────────────────────────
    errors: list[str] = []
    if status not in VALID_RETURN_STATUSES:
        errors.append(f"R2: Invalid status '{status}' (must be ok/error/gated/timeout)")
    if status == "ok" and not verdict:
        errors.append("R3: status=ok requires a verdict")
    if status != "ok" and not error:
        errors.append(f"R4: status={status} requires an error message")
    if verdict and verdict not in VALID_VERDICTS:
        errors.append(f"R5: Invalid verdict '{verdict}' (must be PASS/FAIL/INCONCLUSIVE/OBSERVATIONS)")

    if errors:
        return {"ingested": False, "errors": errors}

    # ── Idempotency gate ──────────────────────────────────────────────
    if _check_return_idempotency(client, work_item_id, run_id):
        return {"ingested": False, "reason": "duplicate_run_id", "run_id": run_id}

    # ── Fetch Work Item for type resolution ───────────────────────────
    page = client.retrieve_page(work_item_id)
    props = page.get("properties", {})
    item_name = _title(props, "Item Name")
    wi_type = _select(props, "Type")
    from_status = _status(props)

    # ── Redaction ─────────────────────────────────────────────────────
    raw_output = _apply_redaction(raw_output)

    # ── Verdict mapping ───────────────────────────────────────────────
    mapping = _resolve_verdict_mapping(verdict, wi_type, status)
    now = notion_api.now_iso()

    # ── Update Work Item properties ───────────────────────────────────
    update_props: dict[str, Any] = {
        "Return Received At": {"date": {"start": now}},
        "Return Consumed At": {"date": {"start": now}},
    }

    if status == "ok":
        update_props["Status"] = {"status": {"name": mapping.get("status", "Done")}}
        mapped_verdict = mapping.get("verdict")
        if mapped_verdict:
            update_props["Verdict"] = {"select": {"name": mapped_verdict}}

        outcome_text = summary
        if mapping.get("warning"):
            outcome_text = f"[WARNING: {mapping['warning']}] {outcome_text}"
        if outcome_text:
            update_props["Outcome"] = {
                "rich_text": [{"type": "text", "text": {"content": outcome_text[:2000]}}]
            }

        if metrics:
            update_props["Metrics"] = {
                "rich_text": [{"type": "text", "text": {"content": json.dumps(metrics, indent=2)[:2000]}}]
            }

        # Signal Librarian
        update_props["Librarian Request Received At"] = {"date": {"start": now}}
    else:
        # Error/gated/timeout: set Blocked but still record Return Received At
        # so Intake Clerk trigger fires for triage
        update_props["Status"] = {"status": {"name": mapping.get("status", "Blocked")}}

    client.update_page(work_item_id, update_props)

    # ── Append content blocks to page body ────────────────────────────
    content_blocks: list[dict[str, Any]] = []

    if status == "ok":
        content_blocks.append(
            notion_api.heading_block("heading_3", f"Execution Result (run_id: {run_id})")
        )
        content_blocks.append(
            notion_api.paragraph_block(
                f"Lane: {lane} | Model: {model} | Duration: {duration_ms}ms | Verdict: {verdict}"
            )
        )

        # Raw output in a toggle (truncated to Notion limits)
        if raw_output:
            chunks = [raw_output[i:i + 2000] for i in range(0, min(len(raw_output), 10000), 2000)]
            content_blocks.append({
                "object": "block", "type": "toggle",
                "toggle": {
                    "rich_text": [{"type": "text", "text": {"content": "Raw Output"}}],
                    "children": [
                        {
                            "object": "block", "type": "code",
                            "code": {
                                "rich_text": [{"type": "text", "text": {"content": chunk}}],
                                "language": "plain text",
                            },
                        }
                        for chunk in chunks
                    ],
                },
            })
    else:
        content_blocks.append(
            notion_api.heading_block("heading_3", f"Execution Error (run_id: {run_id})")
        )
        content_blocks.append({
            "object": "block", "type": "callout",
            "callout": {
                "icon": {"emoji": "\u26a0\ufe0f"},
                "rich_text": [{"type": "text", "text": {
                    "content": f"Status: {status} | Lane: {lane} | Error: {error or 'Unknown'}"
                }}],
            },
        })

    # Artifacts section
    if artifacts or files_changed or commit_sha or pr_url:
        artifact_lines: list[str] = []
        if commit_sha:
            artifact_lines.append(f"Commit: {commit_sha}")
        if pr_url:
            artifact_lines.append(f"PR: {pr_url}")
        for a in (artifacts or []):
            artifact_lines.append(
                f"[{a.get('type', 'file')}] {a.get('path_or_url', '')} — {a.get('description', '')}"
            )
        if files_changed:
            artifact_lines.append(f"Files changed: {', '.join(files_changed[:20])}")

        content_blocks.append(
            notion_api.heading_block("heading_3", "Artifacts")
        )
        content_blocks.append(
            notion_api.paragraph_block("\n".join(artifact_lines)[:2000])
        )

    if content_blocks:
        client.append_block_children(work_item_id, content_blocks)

    # ── Audit log entry ───────────────────────────────────────────────
    to_status = mapping.get("status", "Done") if status == "ok" else "Blocked"
    try:
        client.create_page(
            parent={"database_id": cfg.audit_log_db_id},
            properties={
                "Transition": {"title": [{"type": "text", "text": {
                    "content": f"v1.1 Return: {item_name} ({status})"
                }}]},
                "Work Item": {"relation": [{"id": work_item_id}]},
                "Agent": {"select": {"name": "Dispatch Adapter (v1.1)"}},
                "To Status": {"select": {"name": to_status}},
                "Consumption Timestamp": {"date": {"start": now}},
                **({"From Status": {"select": {"name": from_status}}} if from_status else {}),
            },
        )
    except Exception:
        pass  # Audit log failure should not block return

    return {
        "ingested": True,
        "work_item_id": work_item_id,
        "item_name": item_name,
        "run_id": run_id,
        "status": status,
        "verdict": verdict,
        "mapped_status": to_status,
    }
