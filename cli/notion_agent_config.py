import re
import time
import uuid
from typing import Any
from notion_http import _post, _normalize_record_map, _tx, send_ops

import notion_threads

DEFAULT_AGENT_ICON = "https://www.notion.so/images/customAgentAvatars/triangle-blue.png"

# Pattern to extract {{page:uuid}} mentions from instruction markdown
_MENTION_RE = re.compile(r"\{\{page:([0-9a-f-]{36})\}\}")

def get_user_spaces(token_v2: str) -> list[dict]:
    data = _normalize_record_map(_post("loadUserContent", {}, token_v2))
    record_map = data.get("recordMap", {})
    spaces_map = record_map.get("space", {})
    
    spaces = []
    for space_id, space_rec in spaces_map.items():
        v = space_rec.get("value", {})
        if not v.get("alive", True):
            continue
        spaces.append({
            "id": space_id,
            "name": v.get("name"),
            "domain": v.get("domain"),
        })
    return sorted(spaces, key=lambda s: (s["name"] or "").lower())


def get_all_workspace_agents(space_id: str, token_v2: str,
                              user_id: str | None = None) -> list[dict]:
    bots_data = _normalize_record_map(
        _post("getBots", {"table": "space", "id": space_id, "type": "workflow"},
              token_v2, user_id)
    )
    bot_records = bots_data.get("recordMap", {}).get("bot", {})

    seen: dict[str, dict] = {}
    for bot_data in bot_records.values():
        v = bot_data.get("value", {})
        if not v.get("alive", True):
            continue
        wf_id = v.get("workflow_id", "")
        if not wf_id:
            continue
        version = v.get("version", 0)
        name = v.get("name", "")
        if wf_id not in seen or version > seen[wf_id]["version"]:
            seen[wf_id] = {"name": name, "version": version}

    if not seen:
        return []

    notion_internal_ids = list(seen.keys())

    batch_payload = {
        "requests": [{"id": wid, "table": "workflow"} for wid in notion_internal_ids],
    }
    wf_data = _post("getRecordValues", batch_payload, token_v2, user_id)

    agents = []
    for i, result in enumerate(wf_data.get("results", [])):
        wf = result.get("value")
        if not wf:
            continue
        wf_id = notion_internal_ids[i]
        data = wf.get("data", {})
        name = data.get("name") or seen[wf_id]["name"]
        instructions = data.get("instructions")
        if not instructions:
            continue
        notion_public_id = instructions["id"] if isinstance(instructions, dict) else instructions
        agents.append({
            "name": name,
            "notion_internal_id": wf_id,
            "space_id": wf.get("space_id", space_id),
            "notion_public_id": notion_public_id,
            "triggers": data.get("triggers", []),
        })

    return sorted(agents, key=lambda a: a["name"].lower())


def get_workflow_record(notion_internal_id: str, token_v2: str,
                        user_id: str | None = None) -> dict:
    payload = {
        "requests": [{"id": notion_internal_id, "table": "workflow"}],
    }
    data = _post("getRecordValues", payload, token_v2, user_id)
    results = data.get("results", [])
    if not results or not results[0].get("value"):
        raise RuntimeError(
            f"Workflow {notion_internal_id} not found or inaccessible. "
            f"Response: {data}"
        )
    return results[0]["value"]


MODEL_NAMES = {
    "avocado-froyo-medium": "Opus 4.6",
    "almond-croissant-low": "Sonnet 4.6",
    "oatmeal-cookie": "ChatGPT (o-series)",
    "oval-kumquat-medium": "ChatGPT 5.4",
    "fireworks-minimax-m2.5": "Minimax M2.5",
    "auto": "Auto",
    "unknown": "Auto (default)",
}


def _resolve_page_names(notion_public_ids: list[str], token_v2: str,
                        user_id: str | None = None) -> dict[str, str]:
    if not notion_public_ids:
        return {}
    payload = {
        "requests": [{"id": bid, "table": "block"} for bid in notion_public_ids],
    }
    data = _post("getRecordValues", payload, token_v2, user_id)
    names = {}
    coll_ids_to_resolve: dict[str, str] = {}

    for i, result in enumerate(data.get("results", [])):
        val = result.get("value", {})
        title_prop = val.get("properties", {}).get("title", [])
        if title_prop:
            names[notion_public_ids[i]] = "".join(c[0] for c in title_prop if c)
        else:
            coll_id = val.get("collection_id")
            if coll_id:
                coll_ids_to_resolve[coll_id] = notion_public_ids[i]
            else:
                names[notion_public_ids[i]] = notion_public_ids[i]

    if coll_ids_to_resolve:
        coll_payload = {
            "requests": [{"id": cid, "table": "collection"} for cid in coll_ids_to_resolve],
        }
        coll_data = _post("getRecordValues", coll_payload, token_v2, user_id)
        for i, result in enumerate(coll_data.get("results", [])):
            cid = list(coll_ids_to_resolve.keys())[i]
            bid = coll_ids_to_resolve[cid]
            coll_val = result.get("value", {})
            coll_name = coll_val.get("name", [[""]])[0][0] if coll_val.get("name") else cid
            names[bid] = coll_name

    return names


def get_agent_modules(notion_internal_id: str, token_v2: str,
                      user_id: str | None = None) -> dict:
    wf_record = get_workflow_record(notion_internal_id, token_v2, user_id)
    wf_data = wf_record.get("data", {})
    tools = wf_data.get("tools", [])

    mcp_servers = []
    enabled_mcp_servers = set()
    for tool in tools:
        if tool.get("server_name"):
            mcp_servers.append(tool)
            if tool.get("is_enabled"):
                enabled_mcp_servers.add(tool["server_name"])

    page_ids = []
    modules = wf_data.get("modules", [])
    has_mail = False
    has_calendar = False
    model = wf_data.get("model") or "unknown"
    model_name = MODEL_NAMES.get(model, model)

    for mod in modules:
        mtype = mod.get("type")
        if mtype == "static_notion_pages_context_module":
            page_ids.extend(mod.get("notion_page_ids", []))
        elif mtype == "mail_context_module":
            has_mail = True
        elif mtype == "calendar_context_module":
            has_calendar = True

    page_names = _resolve_page_names(page_ids, token_v2, user_id)

    named_pages = []
    for pid in page_ids:
        title = page_names.get(pid, pid)
        named_pages.append(f"{title} ({pid})")

    return {
        "model": model,
        "model_name": model_name,
        "mcp_servers": mcp_servers,
        "enabled_mcp_servers": list(enabled_mcp_servers),
        "notion_pages": named_pages,
        "has_mail": has_mail,
        "has_calendar": has_calendar,
        "tools": tools,
        "modules": modules,
        "triggers": wf_data.get("triggers", []),
        "agent_name": wf_data.get("name", ""),
    }


def update_agent_modules(notion_internal_id: str, space_id: str,
                         modules: list, token_v2: str,
                         user_id: str | None = None) -> None:
    """Write the full modules list to a workflow record."""
    ops = [{
        "pointer": {"table": "workflow", "id": notion_internal_id, "spaceId": space_id},
        "path": ["data", "modules"],
        "command": "set",
        "args": modules,
    }]
    send_ops(space_id, ops, token_v2, user_id)


def update_agent_model(notion_internal_id: str, space_id: str,
                       model_type: str, token_v2: str,
                       user_id: str | None = None) -> None:
    """Set the AI model for a workflow."""
    ops = [{
        "pointer": {"table": "workflow", "id": notion_internal_id, "spaceId": space_id},
        "path": ["data", "model", "type"],
        "command": "set",
        "args": model_type,
    }]
    send_ops(space_id, ops, token_v2, user_id)


def _get_notion_module(modules: list) -> dict | None:
    """Find the 'notion' module in a workflow's modules list."""
    for mod in modules:
        if mod.get("type") == "notion":
            return mod
    return None


def _get_granted_page_ids(notion_internal_id: str, token_v2: str,
                          user_id: str | None = None) -> set[str]:
    """Return the set of page UUIDs currently granted to an agent.

    Grants are stored as permissions in modules[type=notion].permissions[]
    with identifier.type = 'pageOrCollectionViewBlock'.
    """
    wf_record = get_workflow_record(notion_internal_id, token_v2, user_id)
    modules = wf_record.get("data", {}).get("modules", [])
    notion_mod = _get_notion_module(modules)
    if not notion_mod:
        return set()
    page_ids: set[str] = set()
    for perm in notion_mod.get("permissions", []):
        ident = perm.get("identifier", {})
        if ident.get("type") == "pageOrCollectionViewBlock":
            bid = ident.get("blockId")
            if bid:
                page_ids.add(bid)
    return page_ids


def _make_page_permission(page_id: str, role: str = "reader") -> dict:
    """Build a Notion module permission entry for a page grant."""
    return {
        "type": "notion",
        "actions": [role],
        "identifier": {
            "type": "pageOrCollectionViewBlock",
            "blockId": page_id,
        },
        "moduleType": "notion",
    }


def grant_agent_resource_access(
    notion_internal_id: str, space_id: str,
    notion_public_id: str, role: str,
    token_v2: str, user_id: str | None = None,
) -> dict:
    """Grant an agent access to a Notion page/database.

    Adds a permission entry to modules[type=notion].permissions[] if not
    already present, then publishes the agent.
    """
    wf_record = get_workflow_record(notion_internal_id, token_v2, user_id)
    modules = wf_record.get("data", {}).get("modules", [])
    notion_mod = _get_notion_module(modules)

    if notion_mod is None:
        notion_mod = {"id": None, "name": "Notion", "type": "notion",
                      "version": "1.0.0", "permissions": []}
        modules.append(notion_mod)

    if "permissions" not in notion_mod:
        notion_mod["permissions"] = []

    existing = {p.get("identifier", {}).get("blockId")
                for p in notion_mod["permissions"]
                if p.get("identifier", {}).get("type") == "pageOrCollectionViewBlock"}

    if notion_public_id not in existing:
        notion_mod["permissions"].append(
            _make_page_permission(notion_public_id, role)
        )
        update_agent_modules(notion_internal_id, space_id, modules, token_v2, user_id)

    return publish_agent(notion_internal_id, space_id, token_v2, user_id)


def ensure_mention_access(
    notion_internal_id: str, space_id: str,
    instructions_markdown: str,
    token_v2: str, user_id: str | None = None,
) -> list[str]:
    """Scan instruction markdown for {{page:uuid}} mentions and grant access
    for any pages not already in the agent's modules.

    Returns a list of page IDs that were newly granted.
    """
    mentioned = set(_MENTION_RE.findall(instructions_markdown))
    if not mentioned:
        return []

    granted = _get_granted_page_ids(notion_internal_id, token_v2, user_id)
    missing = mentioned - granted
    if not missing:
        return []

    # Re-fetch to get current modules for mutation
    wf_record = get_workflow_record(notion_internal_id, token_v2, user_id)
    modules = wf_record.get("data", {}).get("modules", [])
    notion_mod = _get_notion_module(modules)

    if notion_mod is None:
        notion_mod = {"id": None, "name": "Notion", "type": "notion",
                      "version": "1.0.0", "permissions": []}
        modules.append(notion_mod)

    if "permissions" not in notion_mod:
        notion_mod["permissions"] = []

    existing = {p.get("identifier", {}).get("blockId")
                for p in notion_mod["permissions"]
                if p.get("identifier", {}).get("type") == "pageOrCollectionViewBlock"}

    for pid in missing:
        if pid not in existing:
            notion_mod["permissions"].append(_make_page_permission(pid))

    update_agent_modules(notion_internal_id, space_id, modules, token_v2, user_id)
    return sorted(missing)


def _new_id() -> str:
    return str(uuid.uuid4())


def create_agent(
    space_id: str, name: str, icon: str | None,
    token_v2: str, user_id: str | None = None,
) -> dict[str, str]:
    """Create a new Notion AI Agent.

    Mirrors the UI's agentActions.createBlankAgent flow:
    1. Create workflow record with name, icon, default modules/triggers
    2. Create instruction page block parented to the workflow
    3. Link instructions to the workflow

    Returns {"notion_internal_id": wf_id, "notion_public_id": instr_id}.
    """
    wf_id = _new_id()
    instr_id = _new_id()
    child_block_id = _new_id()
    notion_module_id = _new_id()
    trigger_id = _new_id()
    now = int(time.time() * 1000)

    if not user_id:
        # Discover user_id from spaces
        spaces = get_user_spaces(token_v2)
        for s in spaces:
            if s["id"] == space_id:
                break
        # Fall back to first available user
        data = _normalize_record_map(_post("loadUserContent", {}, token_v2))
        user_map = data.get("recordMap", {}).get("notion_user", {})
        if user_map:
            user_id = next(iter(user_map))

    user_table = "notion_user"
    icon_url = icon or DEFAULT_AGENT_ICON

    ops = [
        # 1. Create workflow record
        {
            "pointer": {"table": "workflow", "id": wf_id, "spaceId": space_id},
            "path": [],
            "command": "set",
            "args": {
                "id": wf_id,
                "version": 1,
                "parent_id": space_id,
                "parent_table": "space",
                "space_id": space_id,
                "data": {
                    "scripts": [],
                    "modules": [{
                        "id": notion_module_id,
                        "type": "notion",
                        "name": "Notion",
                        "version": "1.0.0",
                        "permissions": [],
                    }],
                    "triggers": [{
                        "id": trigger_id,
                        "moduleId": notion_module_id,
                        "enabled": True,
                        "state": {"type": "notion.agent.mentioned"},
                    }],
                    "name": name,
                    "icon": icon_url,
                },
                "created_by_table": user_table,
                "created_by_id": user_id,
                "created_time": now,
                "last_edited_by_table": user_table,
                "last_edited_by_id": user_id,
                "last_edited_time": now,
                "alive": True,
                "permissions": [{
                    "type": "user_permission",
                    "role": "editor",
                    "user_id": user_id,
                }],
            },
        },
        # 2. Create instruction page block
        {
            "pointer": {"table": "block", "id": instr_id, "spaceId": space_id},
            "path": [],
            "command": "set",
            "args": {
                "id": instr_id,
                "type": "page",
                "properties": {"title": [[f"{name} Instructions"]]},
                "space_id": space_id,
                "created_time": now,
                "created_by_table": user_table,
                "created_by_id": user_id,
            },
        },
        # 3. Create empty child text block
        {
            "pointer": {"table": "block", "id": child_block_id, "spaceId": space_id},
            "path": [],
            "command": "set",
            "args": {
                "id": child_block_id,
                "type": "text",
                "properties": {"title": []},
                "space_id": space_id,
                "created_time": now,
                "created_by_table": user_table,
                "created_by_id": user_id,
            },
        },
        # 4. Add child block to instruction page
        {
            "pointer": {"table": "block", "id": instr_id, "spaceId": space_id},
            "path": ["content"],
            "command": "insertChildrenAfter",
            "args": {"ids": [child_block_id]},
        },
        # 5. Parent instruction page to workflow
        {
            "pointer": {"table": "block", "id": instr_id, "spaceId": space_id},
            "path": [],
            "command": "update",
            "args": {
                "parent_id": wf_id,
                "parent_table": "workflow",
                "alive": True,
            },
        },
        # 6. Link instructions to workflow
        {
            "pointer": {"table": "workflow", "id": wf_id, "spaceId": space_id},
            "command": "set",
            "path": ["data", "instructions"],
            "args": {"table": "block", "id": instr_id, "spaceId": space_id},
        },
    ]

    send_ops(space_id, ops, token_v2, user_id,
             user_action="agentActions.createBlankAgent")

    return {"notion_internal_id": wf_id, "notion_public_id": instr_id}


def add_agent_to_sidebar(
    space_id: str, notion_internal_id: str,
    token_v2: str, user_id: str | None = None,
) -> None:
    """Add an agent to the user's sidebar by updating space_view settings."""
    # Get the space_view_id
    data = _normalize_record_map(_post("loadUserContent", {}, token_v2))
    space_views = data.get("recordMap", {}).get("space_view", {})

    space_view_id = None
    for sv_id, sv_data in space_views.items():
        val = sv_data.get("value", {})
        if val.get("space_id") == space_id:
            space_view_id = sv_id
            break

    if not space_view_id:
        return  # Can't find space_view — agent still works, just not in sidebar

    # Get current sidebar workflow ids
    settings = space_views.get(space_view_id, {}).get("value", {}).get("settings", {})
    current_ids = list(settings.get("sidebar_workflow_ids", []))

    if notion_internal_id in current_ids:
        return  # Already there

    # Prepend new agent
    current_ids.insert(0, notion_internal_id)

    ops = [{
        "pointer": {"id": space_view_id, "table": "space_view", "spaceId": space_id},
        "path": ["settings"],
        "command": "update",
        "args": {"sidebar_workflow_ids": current_ids},
    }]

    send_ops(space_id, ops, token_v2, user_id,
             user_action="sidebarWorkflowsActions.addSidebarWorkflow")


def check_mention_access(
    notion_internal_id: str,
    instructions_markdown: str,
    token_v2: str, user_id: str | None = None,
) -> list[str]:
    """Pre-publish validation: return page UUIDs mentioned in instructions
    that are NOT in the agent's access list."""
    mentioned = set(_MENTION_RE.findall(instructions_markdown))
    if not mentioned:
        return []
    granted = _get_granted_page_ids(notion_internal_id, token_v2, user_id)
    return sorted(mentioned - granted)


def publish_agent(notion_internal_id: str, space_id: str,
                  token_v2: str, user_id: str | None = None,
                  dry_run: bool = False,
                  archive_existing: bool = True) -> dict:
    payload = {"workflowId": notion_internal_id, "spaceId": space_id}

    try:
        result = _post("publishCustomAgentVersion", payload, token_v2, user_id, dry_run)
    except RuntimeError as e:
        err_str = str(e)
        if "incomplete_ancestor_path" in err_str:
            result = {
                "warning": "incomplete_ancestor_path",
                "detail": (
                    "publishCustomAgentVersion returned incomplete_ancestor_path. "
                    "This typically means the agent's instructions reference pages "
                    "(via {{page:uuid}} mentions) that are not granted in the agent's "
                    "Tools & Access settings. Fix: use grant_resource_access to add "
                    "the missing pages, or remove the mentions from instructions. "
                    "Block edits were saved; the published artifact may be stale."
                ),
            }
        else:
            raise

    if dry_run:
        return result

    if "warning" in result:
        return result

    if archive_existing:
        try:
            cleanup = notion_threads.archive_workflow_threads(notion_internal_id, space_id, token_v2, user_id)
            result["archivedThreadCount"] = cleanup["count"]
            result["archivedThreadIds"] = cleanup["threadIds"]
        except Exception as e:
            result["threadCleanupWarning"] = str(e)

    if "workflowArtifactId" not in result:
        raise RuntimeError(f"publishCustomAgentVersion failed. Result: {result}")
    
    return result
