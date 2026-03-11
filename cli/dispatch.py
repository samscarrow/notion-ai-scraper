"""
dispatch.py — Dispatch adapter for the Lab control plane.

Reads Work Items from the Lab, validates them against the OpenClaw v1.1 contract,
and produces dispatch packets that execution planes can consume.

Three entry points (exposed as MCP tools in mcp_server.py):
  - get_dispatchable_items()  — find ready work
  - build_dispatch_packet()   — validate + build packet for one item
  - stamp_dispatch_consumed() — mark item as consumed + In Progress
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


def _relation_ids(props: dict, key: str) -> list[str]:
    return [r["id"] for r in (props.get(key, {}) or {}).get("relation", []) if r.get("id")]


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
    received_at = _date_start(props, "Dispatch Requested Received At")
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

    # ── Validation ───────────────────────────────────────────────────────
    errors: list[str] = []

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

    # V9: Dispatch Requested Received At must be set
    if not received_at:
        errors.append("V9: Dispatch Requested Received At is not set")

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
