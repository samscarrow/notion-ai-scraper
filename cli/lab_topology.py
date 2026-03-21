from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

import yaml

ROOT = os.path.dirname(os.path.abspath(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

try:
    from . import block_builder, cookie_extract, notion_agent_config, notion_api, notion_blocks
    from .config import get_config
    from .notion_http import _post, read_records
    from .utils import _name_to_key, _to_dashed_uuid
except ImportError:
    import block_builder
    import cookie_extract
    import notion_agent_config
    import notion_api
    import notion_blocks
    from config import get_config
    from notion_http import _post, read_records
    from utils import _name_to_key, _to_dashed_uuid

AGENTS_YAML = os.path.join(ROOT, "agents.yaml")
CONTRACTS_YAML = os.path.join(ROOT, "contracts", "lab_contracts.yaml")
DEFAULT_LOOKBACK_DAYS = 14
DEFAULT_LOOKBACK_LIMIT = 25
TERMINAL_STATUSES = {"Done", "Passed", "Kill Condition Met", "Inconclusive", "Closed", "Blocked"}

_ACCESS_STRENGTH = {
    "reader": 1,
    "read_and_write": 2,
}
_TIMELINE_FIELDS = [
    "Dispatch Requested Received At",
    "Dispatch Requested Consumed At",
    "Prompt Request Received At",
    "Prompt Request Consumed At",
    "Run Date",
    "Return Received At",
    "Return Consumed At",
    "Librarian Request Received At",
    "Librarian Request Consumed At",
    "Synthesis Completed At",
    "Synthesis Consumed At",
    "FOSS Recon Consumed At",
    "Triage Routed At",
]


def _chunked(items: list[str], size: int = 50) -> list[list[str]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso_from_millis(value: int | float | None) -> str | None:
    if value is None:
        return None
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc).isoformat()


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed
    except ValueError:
        return None


def _contract_rollout_applies(contract: dict[str, Any], item: dict[str, Any]) -> bool:
    rollout_started_at = _parse_iso(contract.get("rollout_started_at"))
    if rollout_started_at is None:
        return True
    candidate_times: list[datetime] = []
    for field in contract.get("upstream_complete_fields", []) or []:
        parsed = _parse_iso(item.get(field))
        if parsed is not None:
            candidate_times.append(parsed)
    if not candidate_times:
        parsed = _parse_iso(item.get("_last_edited_time")) or _parse_iso(item.get("_created_time"))
        if parsed is not None:
            candidate_times.append(parsed)
    if not candidate_times:
        return False
    return max(candidate_times) >= rollout_started_at


def _load_registry(path: str = AGENTS_YAML) -> dict[str, dict[str, Any]]:
    if not os.path.exists(path):
        return {}
    with open(path) as handle:
        data = yaml.safe_load(handle) or {}
    return data if isinstance(data, dict) else {}


def load_contracts(path: str = CONTRACTS_YAML) -> list[dict[str, Any]]:
    with open(path) as handle:
        data = yaml.safe_load(handle) or {}
    contracts = data.get("contracts", data)
    if not isinstance(contracts, list):
        raise ValueError(f"Invalid contract manifest format in {path}")
    return contracts


def _get_auth(token_v2: str | None = None, user_id: str | None = None) -> tuple[str, str | None]:
    if token_v2:
        return token_v2, user_id
    return cookie_extract.get_auth()


def _detect_space_id(registry: dict[str, dict[str, Any]], token_v2: str, user_id: str | None) -> str:
    for cfg in registry.values():
        if cfg.get("space_id"):
            return cfg["space_id"]
    spaces = notion_agent_config.get_user_spaces(token_v2)
    if len(spaces) == 1:
        return spaces[0]["id"]
    raise ValueError("Unable to determine Notion space ID for Lab topology compilation.")


def _load_page_chunk(page_id: str, token_v2: str, user_id: str | None) -> dict[str, Any]:
    payload = {
        "pageId": page_id,
        "limit": 100,
        "cursor": {"stack": []},
        "chunkNumber": 0,
        "verticalColumns": False,
    }
    return _post("loadPageChunk", payload, token_v2, user_id)


def _extract_block_title(block: dict[str, Any]) -> str:
    title_prop = block.get("properties", {}).get("title", [])
    if not isinstance(title_prop, list):
        return ""
    parts: list[str] = []
    for item in title_prop:
        if isinstance(item, list) and item:
            parts.append(str(item[0]))
    return "".join(parts).strip()


def _load_project_policies(project_database_id: str | None) -> dict[str, dict[str, Any]]:
    if not project_database_id:
        return {}
    cfg = get_config()
    token = getattr(cfg, "notion_token", None)
    if not token:
        return {}
    client = notion_api.NotionAPIClient(token)
    try:
        pages = client.query_all(project_database_id)
    except Exception:
        return {}

    policies: dict[str, dict[str, Any]] = {}
    for page in pages:
        props = page.get("properties", {}) or {}
        policies[page["id"]] = {
            "id": page["id"],
            "name": "".join(item.get("plain_text", "") for item in (props.get("Project Name", {}) or {}).get("title", [])) or page["id"],
            "focus": bool((props.get("Focus", {}) or {}).get("checkbox")),
            "max_active_items": (props.get("Max Active Items", {}) or {}).get("number"),
            "min_terminal_value": ((props.get("Min Terminal Value", {}) or {}).get("select") or {}).get("name"),
            "fork_budget": (props.get("Fork Budget", {}) or {}).get("number"),
        }
    return policies


def _collection_schema_summary(collection: dict[str, Any]) -> dict[str, Any]:
    raw_schema = collection.get("schema", {}) or {}
    property_id_to_name: dict[str, str] = {}
    property_name_to_type: dict[str, str] = {}
    property_name_to_id: dict[str, str] = {}
    options: dict[str, list[str]] = {}

    for prop_id, definition in raw_schema.items():
        name = definition.get("name", prop_id)
        prop_type = definition.get("type", "unknown")
        property_id_to_name[prop_id] = name
        property_name_to_type[name] = prop_type
        property_name_to_id[name] = prop_id

        option_values: list[str] = []
        for key in ("options", "groups"):
            for option in definition.get(key, []) or []:
                value = option.get("value") or option.get("name")
                if value:
                    option_values.append(value)
        if option_values:
            options[name] = option_values

    return {
        "property_id_to_name": property_id_to_name,
        "property_name_to_type": property_name_to_type,
        "property_name_to_id": property_name_to_id,
        "options": options,
    }


def _parse_filter_values(values: list[Any]) -> list[str]:
    parsed: list[str] = []
    for value in values or []:
        if not isinstance(value, dict):
            parsed.append(str(value))
            continue
        kind = value.get("type")
        if kind == "exact":
            parsed.append(str(value.get("value")))
        elif kind == "is_group":
            parsed.append(f"group:{value.get('value')}")
        else:
            parsed.append(str(value.get("value") or value.get("id") or kind or "?"))
    return parsed


def _extract_model_type(workflow_data: dict[str, Any]) -> str:
    model = workflow_data.get("model")
    if isinstance(model, dict):
        return model.get("type", "unknown")
    if isinstance(model, str):
        return model
    return "unknown"


def _extract_artifact_summary(artifact: dict[str, Any] | None) -> dict[str, Any]:
    data = (artifact or {}).get("data", {}) or {}
    return {
        "published_artifact_created_at": _iso_from_millis(artifact.get("created_at") if artifact else None),
        "published_artifact_publish_time": _iso_from_millis(data.get("publishTime")),
        "published_artifact_publish_version": data.get("publishVersion"),
        "published_artifact_workflow_version": data.get("workflowVersion"),
        "published_instruction_block_id": ((data.get("instructions") or {}).get("id")),
    }


def _stable_hash(value: str | None) -> str | None:
    if value is None:
        return None
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _normalize_markdown(markdown: str) -> str:
    lines = [line.rstrip() for line in markdown.strip().splitlines()]
    return "\n".join(lines).strip()


def _render_block_markdown(
    block_id: str | None,
    space_id: str | None,
    token_v2: str,
    user_id: str | None,
) -> str | None:
    if not block_id or not space_id:
        return None
    requested_root_id = block_id
    blocks_map: dict[str, Any] = {}
    seen: set[str] = set()
    while requested_root_id and requested_root_id not in seen:
        seen.add(requested_root_id)
        data = notion_blocks.get_block_tree(requested_root_id, space_id, token_v2, user_id)
        blocks_map = data.get("recordMap", {}).get("block", {})
        if not blocks_map:
            return None
        render_root_id = notion_blocks.resolve_render_root_id(requested_root_id, blocks_map)
        if render_root_id == requested_root_id or render_root_id in blocks_map:
            markdown = block_builder.blocks_to_markdown(blocks_map, render_root_id)
            return _normalize_markdown(markdown or "")
        requested_root_id = render_root_id
    return None


def _normalize_permissions_from_modules(
    modules: list[dict[str, Any]],
    database_by_public: dict[str, dict[str, Any]],
    page_resources: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    permissions: list[dict[str, Any]] = []
    for module in modules or []:
        if module.get("type") != "notion":
            continue
        for permission in module.get("permissions", []) or []:
            identifier = permission.get("identifier", {}) or {}
            actions = permission.get("actions", []) or []
            normalized = {
                "actions": sorted(actions),
                "access": _resolve_access_level(actions),
                "access_strength": _max_access(actions),
                "scope": identifier.get("type"),
                "resource_type": "unknown",
                "resource_key": None,
                "resource_label": None,
                "block_id": identifier.get("blockId"),
            }
            if identifier.get("type") == "workspacePublic":
                normalized.update(
                    {
                        "resource_type": "workspace_public",
                        "resource_key": "workspace_public",
                        "resource_label": "Workspace Public",
                    }
                )
            elif identifier.get("type") == "pageOrCollectionViewBlock" and identifier.get("blockId"):
                block_id = identifier["blockId"]
                database = database_by_public.get(block_id)
                if database:
                    normalized.update(
                        {
                            "resource_type": "database",
                            "resource_key": database["key"],
                            "resource_label": database["label"],
                            "database_public_id": database.get("notion_public_id"),
                            "database_internal_id": database.get("notion_internal_id"),
                        }
                    )
                else:
                    page = page_resources.get(block_id, {"label": block_id})
                    normalized.update(
                        {
                            "resource_type": "page",
                            "resource_key": block_id,
                            "resource_label": page["label"],
                        }
                    )
            permissions.append(normalized)
    permissions.sort(key=lambda item: (item["resource_type"], item["resource_key"] or "", item["access"]))
    return permissions


def _normalize_mcp_servers(modules: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for module in modules or []:
        if module.get("type") != "mcpServer":
            continue
        state = module.get("state", {}) or {}
        normalized.append(
            {
                "name": module.get("name"),
                "serverUrl": state.get("serverUrl"),
                "enabledToolNames": sorted(state.get("enabledToolNames", []) or []),
                "enabledResourceUris": sorted(state.get("enabledResourceUris", []) or []),
            }
        )
    normalized.sort(key=lambda item: (item.get("name") or "", item.get("serverUrl") or ""))
    return normalized


def _normalize_runtime_config(
    workflow_data: dict[str, Any],
    database_by_internal_id: dict[str, dict[str, Any]],
    database_by_public: dict[str, dict[str, Any]],
    page_resources: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    modules = workflow_data.get("modules", []) or []
    triggers = [
        _normalize_trigger(trigger, database_by_internal_id)
        for trigger in workflow_data.get("triggers", []) or []
    ]
    return {
        "name": workflow_data.get("name"),
        "description": workflow_data.get("description"),
        "model": _extract_model_type(workflow_data),
        "triggers": [
            {
                "enabled": trigger.get("enabled", True),
                "type": normalized["type"],
                "database_key": normalized.get("database_key"),
                "properties": [item["name"] for item in normalized.get("properties", [])],
                "conditions": [
                    {
                        "property_name": clause.get("property_name"),
                        "operator": clause.get("operator"),
                        "values": clause.get("values", []),
                    }
                    for clause in normalized.get("conditions", [])
                ],
            }
            for trigger, normalized in zip(workflow_data.get("triggers", []) or [], triggers, strict=False)
        ],
        "permissions": [
            {
                "resource_type": permission["resource_type"],
                "resource_key": permission["resource_key"],
                "access": permission["access"],
            }
            for permission in _normalize_permissions_from_modules(modules, database_by_public, page_resources)
        ],
        "mcp_servers": _normalize_mcp_servers(modules),
    }


def _resolve_access_level(actions: list[str]) -> str:
    if "read_and_write" in actions:
        return "read_and_write"
    if "reader" in actions:
        return "reader"
    return "unknown"


def _max_access(actions: list[str]) -> int:
    return max((_ACCESS_STRENGTH.get(action, 0) for action in actions), default=0)


def _property_names_for_ids(property_ids: list[str], database: dict[str, Any] | None) -> list[dict[str, str]]:
    schema = (database or {}).get("schema", {})
    name_map = schema.get("property_id_to_name", {})
    type_map = schema.get("property_name_to_type", {})
    resolved: list[dict[str, str]] = []
    for property_id in property_ids or []:
        property_name = name_map.get(property_id, property_id)
        resolved.append(
            {
                "id": property_id,
                "name": property_name,
                "type": type_map.get(property_name, "unknown"),
            }
        )
    return resolved


def _normalize_trigger(
    trigger: dict[str, Any],
    database_by_internal_id: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    state = trigger.get("state", {}) or {}
    trigger_type = state.get("type", "unknown")
    database = None
    collection_id = None
    if trigger_type == "notion.page.updated":
        collection_id = state.get("collectionId")
        database = database_by_internal_id.get(collection_id)

    properties = _property_names_for_ids(state.get("propertyIds", []), database)
    conditions: list[dict[str, Any]] = []
    filters = state.get("propertyFilters", {}) or {}
    for clause in filters.get("all", []) + filters.get("some", []):
        property_id = clause.get("property")
        filt = clause.get("filter", {}) or {}
        property_name = property_id
        property_type = "unknown"
        if database:
            property_name = database["schema"]["property_id_to_name"].get(property_id, property_id)
            property_type = database["schema"]["property_name_to_type"].get(property_name, "unknown")
        conditions.append(
            {
                "property_id": property_id,
                "property_name": property_name,
                "property_type": property_type,
                "operator": filt.get("operator", "unknown"),
                "values": _parse_filter_values(filt.get("value", [])),
            }
        )

    return {
        "id": trigger.get("id"),
        "enabled": trigger.get("enabled", True),
        "type": trigger_type,
        "database_key": database.get("key") if database else None,
        "database_label": database.get("label") if database else None,
        "database_public_id": database.get("notion_public_id") if database else None,
        "database_internal_id": collection_id,
        "properties": properties,
        "conditions": conditions,
        "raw_state": state,
    }


def _normalize_automation(database: dict[str, Any], raw: dict[str, Any]) -> dict[str, Any]:
    trigger = raw.get("trigger") or {}
    event = trigger.get("event", {}) or {}
    event_type = "unknown"
    trigger_properties: list[dict[str, str]] = []
    trigger_conditions: list[dict[str, Any]] = []

    if event.get("pagePropertiesEdited"):
        event_type = "pagePropertiesEdited"
        edited = event["pagePropertiesEdited"] or {}
        clauses = edited.get("all", []) + edited.get("some", [])
        for clause in clauses:
            prop_id = clause.get("property")
            trigger_properties.append(
                {
                    "id": prop_id,
                    "name": database["schema"]["property_id_to_name"].get(prop_id, prop_id),
                }
            )
            filt = clause.get("filter", {}) or {}
            trigger_conditions.append(
                {
                    "property_id": prop_id,
                    "property_name": database["schema"]["property_id_to_name"].get(prop_id, prop_id),
                    "operator": filt.get("operator", "unknown"),
                    "values": _parse_filter_values(filt.get("value", [])),
                }
            )
    elif event.get("pagesAdded"):
        event_type = "pagesAdded"

    actions: list[dict[str, Any]] = []
    for action in raw.get("actions", []):
        config = action.get("config", {}) or {}
        writes: list[dict[str, str]] = []
        for property_id in (config.get("values", {}) or {}).keys():
            writes.append(
                {
                    "id": property_id,
                    "name": database["schema"]["property_id_to_name"].get(property_id, property_id),
                }
            )
        actions.append(
            {
                "id": action.get("id"),
                "type": action.get("type"),
                "writes": writes,
                "config": config,
            }
        )

    return {
        "id": raw.get("id"),
        "database_key": database["key"],
        "database_label": database["label"],
        "database_public_id": database.get("notion_public_id"),
        "database_internal_id": database.get("notion_internal_id"),
        "enabled": raw.get("enabled", True),
        "event_type": event_type,
        "trigger_properties": trigger_properties,
        "trigger_conditions": trigger_conditions,
        "actions": actions,
    }


def _resolve_contract_entity(entity: str, snapshot_indexes: dict[str, Any]) -> dict[str, Any]:
    agent = snapshot_indexes["agent_by_key"].get(entity)
    if agent:
        return {"kind": "agent", "key": agent["key"], "label": agent["label"]}
    database = snapshot_indexes["database_by_key"].get(entity)
    if database:
        return {"kind": "database", "key": database["key"], "label": database["label"]}
    return {"kind": "external", "key": entity, "label": entity}


def _resolve_contracts(contracts: list[dict[str, Any]], snapshot_indexes: dict[str, Any]) -> list[dict[str, Any]]:
    resolved: list[dict[str, Any]] = []
    for contract in contracts:
        database = snapshot_indexes["database_by_key"].get(contract.get("database"))
        source = _resolve_contract_entity(contract.get("source", ""), snapshot_indexes)
        target = _resolve_contract_entity(contract.get("target", ""), snapshot_indexes)
        resolved.append(
            {
                **contract,
                "database_label": database.get("label") if database else None,
                "database_public_id": database.get("notion_public_id") if database else None,
                "database_internal_id": database.get("notion_internal_id") if database else None,
                "source_resolved": source,
                "target_resolved": target,
            }
        )
    return resolved


def _compile_edges(
    agents: list[dict[str, Any]],
    automations: list[dict[str, Any]],
    contracts: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    edges: list[dict[str, Any]] = []
    for agent in agents:
        for trigger in agent.get("triggers", []):
            if trigger.get("type") != "notion.page.updated":
                continue
            signal = ", ".join(prop["name"] for prop in trigger.get("properties", [])) or trigger["type"]
            edges.append(
                {
                    "source": trigger.get("database_key") or trigger.get("database_internal_id"),
                    "signal": signal,
                    "target": agent["key"],
                    "edge_kind": "agent_trigger",
                    "evidence": {"trigger_id": trigger.get("id")},
                }
            )

    for automation in automations:
        for action in automation.get("actions", []):
            target = f"automation:{automation['id']}:{action['type']}"
            signal = ", ".join(prop["name"] for prop in automation.get("trigger_properties", [])) or automation["event_type"]
            edges.append(
                {
                    "source": automation["database_key"],
                    "signal": signal,
                    "target": target,
                    "edge_kind": "native_automation",
                    "evidence": {"automation_id": automation["id"], "action_id": action.get("id")},
                }
            )

    for contract in contracts:
        kinds = [item.get("kind") for item in contract.get("evidence_sources", [])]
        edge_kind = "instruction_contract"
        if "native_automation" in kinds:
            edge_kind = "native_automation"
        elif "agent_trigger" in kinds:
            edge_kind = "agent_trigger"
        elif "repo_enforced" in kinds:
            edge_kind = "repo_enforced"
        edges.append(
            {
                "source": contract["source_resolved"]["key"],
                "signal": ", ".join(contract.get("trigger_fields", [])),
                "target": contract["target_resolved"]["key"],
                "edge_kind": edge_kind,
                "evidence": {"contract": contract["name"]},
            }
        )
    return edges


def compile_snapshot(
    token_v2: str | None = None,
    user_id: str | None = None,
    *,
    registry_path: str = AGENTS_YAML,
    contract_path: str = CONTRACTS_YAML,
) -> dict[str, Any]:
    token_v2, user_id = _get_auth(token_v2, user_id)
    registry = _load_registry(registry_path)
    contracts = load_contracts(contract_path)
    space_id = _detect_space_id(registry, token_v2, user_id)

    live_agents = notion_agent_config.get_all_workspace_agents(space_id, token_v2, user_id)
    live_by_key = {_name_to_key(agent["name"]): agent for agent in live_agents}
    workflow_ids = [agent["notion_internal_id"] for agent in live_agents]
    workflow_records = read_records("workflow", workflow_ids, token_v2, user_id, space_id=space_id)
    published_artifact_ids = [
        ((workflow.get("data", {}) or {}).get("published_artifact_pointer") or {}).get("id")
        for workflow in workflow_records.values()
    ]
    workflow_artifacts = read_records(
        "workflow_artifact",
        published_artifact_ids,
        token_v2,
        user_id,
        space_id=space_id,
    )

    permission_block_ids: set[str] = set()
    instruction_block_ids: set[str] = set()
    collection_ids: set[str] = set()

    for workflow in workflow_records.values():
        data = workflow.get("data", {}) or {}
        instruction = data.get("instructions") or {}
        if isinstance(instruction, dict) and instruction.get("id"):
            instruction_block_ids.add(instruction["id"])
        for trigger in data.get("triggers", []) or []:
            state = trigger.get("state", {}) or {}
            if state.get("collectionId"):
                collection_ids.add(state["collectionId"])
        for module in data.get("modules", []) or []:
            if module.get("type") != "notion":
                continue
            for permission in module.get("permissions", []) or []:
                identifier = permission.get("identifier", {}) or {}
                if identifier.get("type") == "pageOrCollectionViewBlock" and identifier.get("blockId"):
                    permission_block_ids.add(identifier["blockId"])

    block_records = read_records(
        "block",
        list(permission_block_ids | instruction_block_ids),
        token_v2,
        user_id,
        space_id=space_id,
    )

    for block in block_records.values():
        if block.get("collection_id"):
            collection_ids.add(block["collection_id"])

    collection_records = read_records("collection", list(collection_ids), token_v2, user_id, space_id=space_id)

    databases_by_internal: dict[str, dict[str, Any]] = {}
    page_resources: dict[str, dict[str, Any]] = {}
    for block_id, block in block_records.items():
        block_title = _extract_block_title(block)
        collection_id = block.get("collection_id")
        if not collection_id:
            page_resources[block_id] = {
                "notion_public_id": block_id,
                "label": block_title or block_id,
            }
            continue

        collection = collection_records.get(collection_id, {})
        collection_name = ""
        if isinstance(collection.get("name"), list) and collection["name"]:
            collection_name = "".join(item[0] for item in collection["name"] if item)
        label = collection_name or block_title or collection_id
        schema = _collection_schema_summary(collection)
        entry = databases_by_internal.setdefault(
            collection_id,
            {
                "key": _name_to_key(label),
                "label": label,
                "notion_public_id": None,
                "notion_internal_id": collection_id,
                "schema": schema,
                "sources": [],
            },
        )
        entry["label"] = label
        entry["key"] = _name_to_key(label)
        entry["schema"] = schema
        entry["notion_public_id"] = entry.get("notion_public_id") or block_id
        entry["sources"].append("permission_block")

    for collection_id, collection in collection_records.items():
        collection_name = ""
        if isinstance(collection.get("name"), list) and collection["name"]:
            collection_name = "".join(item[0] for item in collection["name"] if item)
        label = collection_name or collection_id
        entry = databases_by_internal.setdefault(
            collection_id,
            {
                "key": _name_to_key(label),
                "label": label,
                "notion_public_id": None,
                "notion_internal_id": collection_id,
                "schema": _collection_schema_summary(collection),
                "sources": [],
            },
        )
        entry["label"] = label
        entry["key"] = _name_to_key(label)
        entry["schema"] = _collection_schema_summary(collection)
        entry["sources"].append("collection_record")

    databases = sorted(databases_by_internal.values(), key=lambda item: item["label"].lower())
    database_by_key = {database["key"]: database for database in databases}
    database_by_public = {
        database["notion_public_id"]: database
        for database in databases
        if database.get("notion_public_id")
    }

    agents: list[dict[str, Any]] = []
    for key in sorted(set(registry) | set(live_by_key)):
        registry_cfg = registry.get(key, {})
        live = live_by_key.get(key)
        workflow = workflow_records.get((live or {}).get("notion_internal_id", ""), {})
        data = workflow.get("data", {}) or {}
        published_artifact_id = ((data.get("published_artifact_pointer") or {}).get("id"))
        artifact = workflow_artifacts.get(published_artifact_id)
        artifact_data = (artifact or {}).get("data", {}) or {}
        artifact_summary = _extract_artifact_summary(artifact)
        instruction = data.get("instructions") or {}
        instruction_id = instruction.get("id") if isinstance(instruction, dict) else None
        instruction_block = block_records.get(instruction_id or "", {})
        permissions = _normalize_permissions_from_modules(data.get("modules", []) or [], database_by_public, page_resources)

        normalized_triggers = [
            _normalize_trigger(trigger, databases_by_internal)
            for trigger in data.get("triggers", []) or []
        ]
        has_enabled_property_trigger = any(
            trigger.get("type") == "notion.page.updated" and trigger.get("enabled", True)
            for trigger in normalized_triggers
        )
        draft_instruction_markdown = None
        published_instruction_markdown = None
        if has_enabled_property_trigger:
            draft_instruction_markdown = _render_block_markdown(
                instruction_id,
                (live or {}).get("space_id") or registry_cfg.get("space_id") or space_id,
                token_v2,
                user_id,
            )
            published_instruction_markdown = _render_block_markdown(
                artifact_summary.get("published_instruction_block_id"),
                (live or {}).get("space_id") or registry_cfg.get("space_id") or space_id,
                token_v2,
                user_id,
            )
        draft_runtime_config = _normalize_runtime_config(
            data,
            databases_by_internal,
            database_by_public,
            page_resources,
        )
        published_runtime_config = None
        if artifact_data:
            published_runtime_config = _normalize_runtime_config(
                artifact_data,
                databases_by_internal,
                database_by_public,
                page_resources,
            )
        agents.append(
            {
                "key": key,
                "label": (live or {}).get("name") or registry_cfg.get("label", key),
                "space_id": (live or {}).get("space_id") or registry_cfg.get("space_id"),
                "notion_public_id": (live or {}).get("notion_public_id") or registry_cfg.get("notion_public_id"),
                "notion_internal_id": (live or {}).get("notion_internal_id") or registry_cfg.get("notion_internal_id"),
                "instruction_block_id": instruction_id,
                "instruction_last_edited_time": _iso_from_millis(instruction_block.get("last_edited_time")),
                "published_artifact_id": published_artifact_id,
                "draft_runtime_actor_id": ((data.get("draft_runtime_actor_pointer") or {}).get("id")),
                "runtime_actor_id": ((data.get("runtime_actor_pointer") or {}).get("id")),
                "workflow_last_edited_time": _iso_from_millis(workflow.get("last_edited_time")),
                "workflow_version": workflow.get("version"),
                "model": _extract_model_type(data),
                **artifact_summary,
                "draft_runtime_config": draft_runtime_config,
                "published_runtime_config": published_runtime_config,
                "draft_instruction_hash": _stable_hash(draft_instruction_markdown),
                "published_instruction_hash": _stable_hash(published_instruction_markdown),
                "permissions": permissions,
                "triggers": normalized_triggers,
                "live_present": live is not None,
                "registry_present": key in registry,
            }
        )

    automation_errors: list[dict[str, str]] = []
    automations: list[dict[str, Any]] = []
    for database in databases:
        public_id = database.get("notion_public_id")
        if not public_id:
            continue
        try:
            raw_automations = notion_blocks.get_db_automations(public_id, token_v2, user_id)
        except Exception as exc:
            automation_errors.append({"database": database["label"], "error": str(exc)})
            continue
        for raw in raw_automations.get("automations", []):
            automations.append(_normalize_automation(database, raw))

    snapshot_indexes = {
        "agent_by_key": {agent["key"]: agent for agent in agents},
        "database_by_key": database_by_key,
        "database_by_public_id": database_by_public,
        "database_by_internal_id": databases_by_internal,
    }
    project_db = snapshot_indexes["database_by_key"].get("lab_projects", {})
    project_policies = _load_project_policies(project_db.get("notion_public_id"))
    snapshot_indexes["project_policy_by_id"] = project_policies
    resolved_contracts = _resolve_contracts(contracts, snapshot_indexes)
    edges = _compile_edges(agents, automations, resolved_contracts)

    return {
        "generated_at": _utc_now().isoformat(),
        "workspace": {"space_id": space_id},
        "databases": databases,
        "agents": sorted(agents, key=lambda item: item["label"].lower()),
        "automations": automations,
        "automation_errors": automation_errors,
        "project_policies": sorted(project_policies.values(), key=lambda item: item["name"].lower()),
        "contracts": resolved_contracts,
        "edges": edges,
        "indexes": snapshot_indexes,
    }


def resolve_resource_identifier(value: str, snapshot: dict[str, Any]) -> dict[str, Any]:
    value = value.strip()
    collection_prefix = "collection://"
    if value.startswith(collection_prefix):
        value = value[len(collection_prefix):]
    try:
        dashed = _to_dashed_uuid(value)
    except ValueError:
        dashed = None

    indexes = snapshot["indexes"]
    if value in indexes["agent_by_key"]:
        agent = indexes["agent_by_key"][value]
        return {
            "resource_type": "agent",
            "label": agent["label"],
            "notion_public_id": agent.get("notion_public_id"),
            "notion_internal_id": agent.get("notion_internal_id"),
            "raw_input": value,
        }
    if dashed and dashed in indexes["database_by_public_id"]:
        database = indexes["database_by_public_id"][dashed]
        return {
            "resource_type": "database",
            "label": database["label"],
            "notion_public_id": database.get("notion_public_id"),
            "notion_internal_id": database.get("notion_internal_id"),
            "raw_input": value,
        }
    if dashed and dashed in indexes["database_by_internal_id"]:
        database = indexes["database_by_internal_id"][dashed]
        return {
            "resource_type": "database",
            "label": database["label"],
            "notion_public_id": database.get("notion_public_id"),
            "notion_internal_id": database.get("notion_internal_id"),
            "raw_input": value,
        }
    for agent in snapshot["agents"]:
        if dashed and dashed in {agent.get("notion_public_id"), agent.get("notion_internal_id")}:
            return {
                "resource_type": "agent",
                "label": agent["label"],
                "notion_public_id": agent.get("notion_public_id"),
                "notion_internal_id": agent.get("notion_internal_id"),
                "raw_input": value,
            }
    return {
        "resource_type": "unknown",
        "label": value,
        "notion_public_id": None,
        "notion_internal_id": dashed,
        "raw_input": value,
    }


def _add_finding(findings: list[dict[str, str]], code: str, severity: str, subject: str, detail: str) -> None:
    findings.append(
        {
            "code": code,
            "severity": severity,
            "subject": subject,
            "detail": detail,
        }
    )


def _permission_satisfies(agent: dict[str, Any], database_key: str, required_access: str) -> bool:
    needed = _ACCESS_STRENGTH.get(required_access, 0)
    if needed == 0:
        return True
    for permission in agent.get("permissions", []):
        if permission.get("resource_key") == database_key and permission.get("access_strength", 0) >= needed:
            return True
    return False


def _find_matching_trigger(agent: dict[str, Any], contract: dict[str, Any]) -> dict[str, Any] | None:
    for trigger in agent.get("triggers", []):
        if trigger.get("type") != "notion.page.updated":
            continue
        if trigger.get("database_key") != contract.get("database"):
            continue
        trigger_names = {item["name"] for item in trigger.get("properties", [])}
        if set(contract.get("trigger_fields", [])) <= trigger_names:
            return trigger
    return None


def _find_matching_automation(snapshot: dict[str, Any], contract: dict[str, Any]) -> dict[str, Any] | None:
    for automation in snapshot.get("automations", []):
        if automation.get("database_key") != contract.get("database"):
            continue
        trigger_names = {item["name"] for item in automation.get("trigger_properties", [])}
        write_names = {
            write["name"]
            for action in automation.get("actions", [])
            for write in action.get("writes", [])
        }
        if set(contract.get("trigger_fields", [])) <= trigger_names and set(contract.get("produced_fields", [])) <= write_names:
            return automation
    return None


def _published_artifact_drift_details(agent: dict[str, Any]) -> list[str]:
    details: list[str] = []
    published_runtime_config = agent.get("published_runtime_config")
    if published_runtime_config:
        draft_json = json.dumps(agent.get("draft_runtime_config"), sort_keys=True)
        published_json = json.dumps(published_runtime_config, sort_keys=True)
        if draft_json != published_json:
            details.append("draft runtime config differs from the published artifact snapshot")

    draft_instruction_hash = agent.get("draft_instruction_hash")
    published_instruction_hash = agent.get("published_instruction_hash")
    if published_instruction_hash and draft_instruction_hash != published_instruction_hash:
        details.append("draft instruction content differs from the published artifact snapshot")
    return details


def _first_relation_id(item: dict[str, Any], field: str) -> str | None:
    values = item.get(field) or []
    if isinstance(values, list) and values:
        return values[0]
    return None


def _summarize_public_page(page: dict[str, Any]) -> dict[str, Any]:
    summary = {
        "id": page.get("id"),
        "_created_time": page.get("created_time"),
        "_last_edited_time": page.get("last_edited_time"),
    }
    for name, prop in (page.get("properties", {}) or {}).items():
        ptype = prop.get("type")
        if ptype == "title":
            summary[name] = "".join(item.get("plain_text", "") for item in prop.get("title", []))
        elif ptype == "rich_text":
            summary[name] = "".join(item.get("plain_text", "") for item in prop.get("rich_text", []))
        elif ptype in {"select", "status"}:
            summary[name] = ((prop.get(ptype) or {}).get("name"))
        elif ptype == "date":
            summary[name] = ((prop.get("date") or {}).get("start"))
        elif ptype == "checkbox":
            summary[name] = bool(prop.get("checkbox"))
        elif ptype == "url":
            summary[name] = prop.get("url")
        elif ptype == "relation":
            summary[name] = [item["id"] for item in prop.get("relation", []) if item.get("id")]
        elif ptype == "number":
            summary[name] = prop.get("number")
    return summary


def fetch_recent_work_items(
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    limit: int = DEFAULT_LOOKBACK_LIMIT,
    *,
    database_id: str | None = None,
) -> tuple[list[dict[str, Any]] | None, str | None]:
    try:
        cfg = get_config()
        client = notion_api.NotionAPIClient(cfg.notion_token)
        work_items_db_id = database_id or cfg.work_items_db_id
        if not work_items_db_id:
            raise ValueError("Work Items database ID is not configured")
        pages = client.query_all(work_items_db_id)
    except Exception as exc:
        return None, str(exc)

    cutoff = _utc_now() - timedelta(days=lookback_days)
    filtered = []
    for page in pages:
        last_seen = _parse_iso(page.get("last_edited_time")) or _parse_iso(page.get("created_time"))
        if last_seen and last_seen >= cutoff:
            filtered.append(page)
    filtered.sort(key=lambda item: item.get("last_edited_time") or item.get("created_time") or "", reverse=True)
    return filtered[:limit], None


def evaluate_drift(
    snapshot: dict[str, Any],
    recent_work_items: list[dict[str, Any]] | None = None,
    *,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    sample_limit: int = DEFAULT_LOOKBACK_LIMIT,
    recent_error: str | None = None,
) -> dict[str, Any]:
    findings: list[dict[str, str]] = []
    indexes = snapshot["indexes"]
    live_keys = {agent["key"] for agent in snapshot["agents"] if agent.get("live_present")}
    registry_keys = {agent["key"] for agent in snapshot["agents"] if agent.get("registry_present")}

    for key in sorted(registry_keys - live_keys):
        _add_finding(findings, "T.1", "MUST-FIX", key, "registered in agents.yaml but missing from live workspace")
    for key in sorted(live_keys - registry_keys):
        _add_finding(findings, "T.1", "MUST-FIX", key, "present in live workspace but missing from agents.yaml")
    for agent in snapshot["agents"]:
        if not (agent.get("live_present") and agent.get("registry_present")):
            continue
        registry_cfg = _load_registry().get(agent["key"], {})
        if registry_cfg.get("notion_public_id") and registry_cfg.get("notion_public_id") != agent.get("notion_public_id"):
            _add_finding(findings, "T.1", "MUST-FIX", agent["label"], "registry public ID does not match live workspace")
        if registry_cfg.get("notion_internal_id") and registry_cfg.get("notion_internal_id") != agent.get("notion_internal_id"):
            _add_finding(findings, "T.1", "MUST-FIX", agent["label"], "registry internal ID does not match live workspace")

    db_key_to_ids: dict[str, set[tuple[str | None, str | None]]] = {}
    public_to_internal: dict[str, set[str]] = {}
    for database in snapshot["databases"]:
        db_key_to_ids.setdefault(database["key"], set()).add(
            (database.get("notion_public_id"), database.get("notion_internal_id"))
        )
        if database.get("notion_public_id") and database.get("notion_internal_id"):
            public_to_internal.setdefault(database["notion_public_id"], set()).add(database["notion_internal_id"])
    for key, id_pairs in db_key_to_ids.items():
        if len(id_pairs) > 1:
            _add_finding(findings, "T.2", "MUST-FIX", key, f"database key resolves to multiple ID pairs: {sorted(id_pairs)}")
    for public_id, internal_ids in public_to_internal.items():
        if len(internal_ids) > 1:
            _add_finding(findings, "T.2", "MUST-FIX", public_id, f"public ID maps to multiple internal IDs: {sorted(internal_ids)}")

    for agent in snapshot["agents"]:
        for trigger in agent.get("triggers", []):
            if trigger.get("type") != "notion.page.updated":
                continue
            if trigger.get("database_key") is None:
                _add_finding(findings, "T.3", "MUST-FIX", agent["label"], f"trigger {trigger.get('id')} references unknown collection {trigger.get('database_internal_id')}")
            unresolved = [item["id"] for item in trigger.get("properties", []) if item["name"] == item["id"]]
            if unresolved:
                _add_finding(findings, "T.3", "MUST-FIX", agent["label"], f"trigger {trigger.get('id')} has unresolved property IDs: {', '.join(unresolved)}")
        has_enabled_property_trigger = any(
            trigger.get("type") == "notion.page.updated" and trigger.get("enabled", True)
            for trigger in agent.get("triggers", [])
        )
        if has_enabled_property_trigger and not agent.get("published_artifact_id"):
            _add_finding(findings, "T.4", "P0", agent["label"], "active property-change trigger has no published artifact")
        elif has_enabled_property_trigger:
            for artifact_detail in _published_artifact_drift_details(agent):
                _add_finding(findings, "T.4", "P0", agent["label"], artifact_detail)
    for contract in snapshot["contracts"]:
        target = indexes["agent_by_key"].get(contract["target_resolved"]["key"])
        database = indexes["database_by_key"].get(contract.get("database"))
        if contract["target_resolved"]["kind"] == "agent" and not target:
            _add_finding(findings, "T.6", "MUST-FIX", contract["name"], f"target agent {contract['target']} is missing")
            continue
        if contract["source_resolved"]["kind"] == "agent" and contract["source_resolved"]["key"] not in indexes["agent_by_key"]:
            _add_finding(findings, "T.6", "MUST-FIX", contract["name"], f"source agent {contract['source']} is missing")
        if not database:
            _add_finding(findings, "T.6", "MUST-FIX", contract["name"], f"database {contract.get('database')} is unresolved")
            continue

        schema_names = set(database["schema"]["property_name_to_type"])
        referenced_fields = set(contract.get("trigger_fields", [])) | set(contract.get("consumed_fields", [])) | set(contract.get("produced_fields", []))
        missing_fields = sorted(field for field in referenced_fields if field not in schema_names)
        if missing_fields:
            _add_finding(findings, "T.6", "MUST-FIX", contract["name"], f"fields missing from {database['label']} schema: {', '.join(missing_fields)}")

        if target and contract.get("required_access") and not _permission_satisfies(target, database["key"], contract["required_access"]):
            _add_finding(findings, "T.5", "P0", contract["name"], f"{target['label']} lacks {contract['required_access']} access to {database['label']}")

        for evidence in contract.get("evidence_sources", []):
            kind = evidence.get("kind")
            if kind == "agent_trigger" and target and _find_matching_trigger(target, contract) is None:
                _add_finding(findings, "T.6", "MUST-FIX", contract["name"], f"no live trigger found for {target['label']} on {database['label']}")
            elif kind == "native_automation" and _find_matching_automation(snapshot, contract) is None:
                _add_finding(findings, "T.6", "MUST-FIX", contract["name"], f"no matching native automation found on {database['label']}")
            elif kind == "repo_enforced" and evidence.get("path") and not os.path.exists(os.path.join(os.path.dirname(ROOT), evidence["path"])):
                _add_finding(findings, "T.6", "MUST-FIX", contract["name"], f"repo evidence path is missing: {evidence['path']}")

    write_targets: dict[tuple[str, str], list[str]] = {}
    for automation in snapshot.get("automations", []):
        for action in automation.get("actions", []):
            for write in action.get("writes", []):
                write_targets.setdefault((automation["database_key"], write["name"]), []).append(f"automation:{automation['id']}")
    for agent in snapshot["agents"]:
        for contract in snapshot["contracts"]:
            if contract["target_resolved"]["key"] != agent["key"]:
                continue
            for produced in contract.get("produced_fields", []):
                write_targets.setdefault((contract.get("database"), produced), []).append(f"contract:{contract['name']}")
    for (database_key, field_name), writers in sorted(write_targets.items()):
        automation_writers = [writer for writer in writers if writer.startswith("automation:")]
        contract_writers = [writer for writer in writers if writer.startswith("contract:")]
        if automation_writers and contract_writers:
            _add_finding(findings, "T.8", "MUST-FIX", f"{database_key}.{field_name}", f"automation and contract paths overlap without a single authoritative write path: {', '.join(writers)}")

    metadata = {
        "lookback_days": lookback_days,
        "sample_limit": sample_limit,
        "recent_query_error": recent_error,
        "t7_evaluated": False,
    }

    if recent_work_items is None:
        return {"findings": findings, "metadata": metadata}

    recent_summaries = [_summarize_public_page(page) for page in recent_work_items]
    recent_by_id = {item["id"]: item for item in recent_summaries}
    project_policy_by_id = snapshot["indexes"].get("project_policy_by_id", {})
    metadata["t7_evaluated"] = True
    metadata["recent_items_seen"] = len(recent_summaries)
    for contract in snapshot["contracts"]:
        if not contract.get("required_artifacts"):
            continue
        database = indexes["database_by_key"].get(contract.get("database"))
        if not database:
            continue
        schema_names = set(database["schema"]["property_name_to_type"])
        if any(artifact not in schema_names for artifact in contract["required_artifacts"]):
            continue

        matches: list[dict[str, Any]] = []
        for item in recent_summaries:
            selector = contract.get("selector", {}) or {}
            selector_ok = True
            for field, expected in selector.items():
                item_value = item.get(field)
                allowed = expected if isinstance(expected, list) else [expected]
                if item_value not in allowed:
                    selector_ok = False
                    break
            if not selector_ok:
                continue
            if any(not item.get(field) for field in contract.get("upstream_complete_fields", [])):
                continue
            if not _contract_rollout_applies(contract, item):
                continue
            matches.append(item)

        if not matches:
            continue
        for item in matches[:sample_limit]:
            missing = [artifact for artifact in contract["required_artifacts"] if not item.get(artifact)]
            if missing:
                subject = item.get("Item Name") or item["id"]
                _add_finding(findings, "T.7", "MUST-FIX", subject, f"{contract['name']} missing downstream artifacts: {', '.join(missing)}")

    fork_counts_by_project: dict[str, int] = {}
    over_budget_projects: set[str] = set()
    for item in recent_summaries:
        if item.get("Disposition") != "Fork" or not item.get("Synthesis Consumed At"):
            continue
        project_id = _first_relation_id(item, "Project")
        if not project_id:
            continue
        fork_counts_by_project[project_id] = fork_counts_by_project.get(project_id, 0) + 1

    for item in recent_summaries:
        if not item.get("Synthesis Consumed At"):
            continue
        subject = item.get("Item Name") or item["id"]
        disposition = item.get("Disposition")
        has_successor = bool(item.get("Superseded By"))
        routing_signal = item.get("Routing Signal")
        if not (has_successor or routing_signal):
            continue

        if not disposition:
            _add_finding(findings, "T.9", "MUST-FIX", subject, "synthesis was consumed and successor/routing state exists, but Disposition is empty")
            continue

        if disposition in {"Repeat", "Fork"} and not has_successor:
            _add_finding(findings, "T.9", "MUST-FIX", subject, f"Disposition '{disposition}' requires Superseded By")
        elif disposition == "Advance" and not has_successor and routing_signal != "ADVANCE":
            _add_finding(findings, "T.9", "MUST-FIX", subject, "Disposition 'Advance' requires Superseded By or Routing Signal = ADVANCE")

        if disposition in {"Archive", "Escalate to Sam"} and has_successor:
            _add_finding(findings, "T.9", "MUST-FIX", subject, f"Disposition '{disposition}' forbids Superseded By")

        if disposition == "Repeat" and has_successor:
            successor_ids = item.get("Superseded By") or []
            known_successors = [recent_by_id[successor_id] for successor_id in successor_ids if successor_id in recent_by_id]
            if known_successors:
                successor_types = {successor.get("Type") for successor in known_successors}
                if item.get("Type") not in successor_types:
                    _add_finding(findings, "T.9", "MUST-FIX", subject, "Disposition 'Repeat' points to a successor with a different Type")

        if disposition == "Fork":
            project_id = _first_relation_id(item, "Project")
            policy = project_policy_by_id.get(project_id or "")
            budget = policy.get("fork_budget") if policy else None
            if budget is not None and project_id not in over_budget_projects and fork_counts_by_project.get(project_id or "", 0) > int(budget):
                over_budget_projects.add(project_id or "")
                project_name = (policy or {}).get("name") or project_id or "unknown project"
                _add_finding(
                    findings,
                    "T.10",
                    "MUST-FIX",
                    project_name,
                    f"fork budget exceeded in lookback window ({fork_counts_by_project.get(project_id or '', 0)}/{int(budget)})",
                )

    return {"findings": findings, "metadata": metadata}


def render_snapshot_summary(snapshot: dict[str, Any]) -> str:
    lines = [
        f"Lab topology snapshot @ {snapshot['generated_at']}",
        f"Workspace: {snapshot['workspace']['space_id']}",
        f"Databases: {len(snapshot['databases'])}",
    ]
    for database in snapshot["databases"]:
        lines.append(
            f"- {database['label']} [{database.get('notion_public_id') or '?'} | {database['notion_internal_id']}]"
        )
    lines.append(f"Agents: {len(snapshot['agents'])}")
    for agent in snapshot["agents"]:
        trigger_count = sum(1 for trigger in agent.get("triggers", []) if trigger.get("type") == "notion.page.updated")
        permission_count = len(agent.get("permissions", []))
        published = "yes" if agent.get("published_artifact_id") else "no"
        lines.append(
            f"- {agent['key']}: triggers={trigger_count} permissions={permission_count} published={published}"
        )
    lines.append(f"Automations: {len(snapshot['automations'])}")
    lines.append(f"Contracts: {len(snapshot['contracts'])}")
    for contract in snapshot["contracts"]:
        lines.append(
            f"- {contract['name']}: {contract['source_resolved']['label']} -> {contract['target_resolved']['label']} on {contract.get('database_label') or contract.get('database')}"
        )
    return "\n".join(lines)


def render_drift_report(report: dict[str, Any]) -> str:
    findings = report.get("findings", [])
    lines = ["== Control Plane Drift =="]
    if not findings:
        lines.append("PASS")
    else:
        for finding in findings:
            lines.append(f"[{finding['severity']}: {finding['code']}] {finding['subject']}: {finding['detail']}")
    metadata = report.get("metadata", {})
    if metadata.get("recent_query_error"):
        lines.append(f"Recent item query unavailable: {metadata['recent_query_error']}")
    elif not metadata.get("t7_evaluated"):
        lines.append("T.7 skipped: recent item query unavailable.")
    return "\n".join(lines)


def trace_work_item(page_id: str, snapshot: dict[str, Any] | None = None) -> str:
    if snapshot is None:
        snapshot = compile_snapshot()

    try:
        page_id = _to_dashed_uuid(page_id)
        cfg = get_config()
        client = notion_api.NotionAPIClient(cfg.notion_token)
        page = client.retrieve_page(page_id)
    except Exception as exc:
        return f"Unable to trace work item {page_id}: {exc}"

    summary = _summarize_public_page(page)
    lines = [f"Trace for {summary.get('Item Name') or page_id}", ""]
    lines.append("Timeline:")
    for field in _TIMELINE_FIELDS:
        if summary.get(field):
            lines.append(f"- {field}: {summary[field]}")
    decision_fields = ("Disposition", "Routing Signal", "Routing Target")
    decision_lines = [f"- {field}: {summary[field]}" for field in decision_fields if summary.get(field)]
    successor_ids = summary.get("Superseded By") or []
    if successor_ids:
        decision_lines.append(f"- Superseded By: {', '.join(successor_ids)}")
    if decision_lines:
        lines.append("")
        lines.append("Decision state:")
        lines.extend(decision_lines)

    matched_contracts: list[dict[str, Any]] = []
    missing_artifacts: list[str] = []
    for contract in snapshot["contracts"]:
        selector = contract.get("selector", {}) or {}
        selector_ok = True
        for field, expected in selector.items():
            allowed = expected if isinstance(expected, list) else [expected]
            if summary.get(field) not in allowed:
                selector_ok = False
                break
        if not selector_ok:
            continue
        if any(not summary.get(field) for field in contract.get("upstream_complete_fields", [])):
            continue
        if not _contract_rollout_applies(contract, summary):
            continue
        matched_contracts.append(contract)
        for artifact in contract.get("required_artifacts", []):
            if not summary.get(artifact):
                missing_artifacts.append(f"{contract['name']}: {artifact}")

    lines.append("")
    lines.append("Triggered edges:")
    if not matched_contracts:
        lines.append("- none")
    else:
        for contract in matched_contracts:
            lines.append(
                f"- {contract['name']}: {contract['source_resolved']['label']} -> {contract['target_resolved']['label']}"
            )

    lines.append("")
    lines.append("Missing expected artifacts:")
    if not missing_artifacts:
        lines.append("- none")
    else:
        for item in missing_artifacts:
            lines.append(f"- {item}")

    return "\n".join(lines)


def write_snapshot(path: str, snapshot: dict[str, Any]) -> None:
    serializable = {key: value for key, value in snapshot.items() if key != "indexes"}
    with open(path, "w") as handle:
        json.dump(serializable, handle, indent=2)
        handle.write("\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Compile and inspect the Lab control-plane topology.")
    parser.add_argument("--output", help="Optional JSON output path.")
    parser.add_argument("--audit", action="store_true", help="Run drift checks after compiling the snapshot.")
    args = parser.parse_args()

    snapshot = compile_snapshot()
    print(render_snapshot_summary(snapshot))
    if args.output:
        write_snapshot(args.output, snapshot)
        print(f"\nWrote snapshot to {args.output}")
    if args.audit:
        work_items_db = snapshot["indexes"]["database_by_key"].get("work_items", {})
        recent_items, recent_error = fetch_recent_work_items(
            database_id=work_items_db.get("notion_public_id")
        )
        report = evaluate_drift(snapshot, recent_items, recent_error=recent_error)
        print("")
        print(render_drift_report(report))


if __name__ == "__main__":
    main()
