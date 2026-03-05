#!/usr/bin/env python3
"""
mcp_server.py — MCP server for managing Notion AI Agent instructions.

Wraps the existing CLI modules (cookie_extract, notion_client, block_builder)
as MCP tools so any AI client can manage Notion agents headlessly.

Usage:
  python cli/mcp_server.py                       # stdio transport
  claude mcp add notion-agents -- python cli/mcp_server.py
"""

import functools
import json
import os
import re
import sys
import threading
import time

# Allow running from project root or cli/ directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import yaml
from mcp.server.fastmcp import FastMCP

import block_builder
import cookie_extract
import notion_client

AGENTS_YAML = os.path.join(os.path.dirname(os.path.abspath(__file__)), "agents.yaml")

mcp = FastMCP(
    "notion-agents",
    instructions=(
        "Tools for managing Notion AI Agent instructions. "
        "You can list, dump, update, publish, discover, register, and remove agents. "
        "Auth is automatic via Firefox session cookies (token_v2)."
    ),
)


# ── Helpers ──────────────────────────────────────────────────────────────────

_collection_schemas: dict[str, dict[str, str]] = {}  # collection_id -> {prop_id: name}
_collection_lock = threading.Lock()

_auth_cache: tuple[str, str | None] | None = None
_auth_cache_time: float = 0
_auth_db_mtime: float = 0   # mtime of Firefox cookies.sqlite at last read
_auth_lock = threading.Lock()
_AUTH_TTL = 300  # seconds — re-read cookies every 5 minutes

_registry_cache: dict | None = None
_registry_mtime: float = 0
_registry_lock = threading.Lock()


def _load_registry() -> dict:
    """Load agents.yaml with mtime-based caching."""
    global _registry_cache, _registry_mtime
    if not os.path.exists(AGENTS_YAML):
        return {}
    mtime = os.path.getmtime(AGENTS_YAML)
    with _registry_lock:
        if _registry_cache is not None and mtime == _registry_mtime:
            return _registry_cache
        with open(AGENTS_YAML) as f:
            data = yaml.safe_load(f)
        _registry_cache = data if isinstance(data, dict) else {}
        _registry_mtime = mtime
        return _registry_cache


def _save_registry(registry: dict) -> None:
    """Write registry back to agents.yaml and invalidate cache."""
    global _registry_cache, _registry_mtime
    with _registry_lock:
        with open(AGENTS_YAML, "w") as f:
            yaml.dump(registry, f, default_flow_style=False, sort_keys=False)
        _registry_cache = registry
        _registry_mtime = os.path.getmtime(AGENTS_YAML)


def _get_auth(force: bool = False) -> tuple[str, str | None]:
    """Return (token_v2, user_id) with TTL + mtime caching.

    Re-reads Firefox cookies if:
    - force=True (e.g. after a 401)
    - TTL expired
    - Firefox's cookies.sqlite has been modified since last read (user re-logged in)
    """
    global _auth_cache, _auth_cache_time, _auth_db_mtime
    now = time.monotonic()
    with _auth_lock:
        if not force and _auth_cache is not None:
            # Check mtime before trusting TTL
            try:
                db_path = cookie_extract.get_firefox_cookies_db()
                current_mtime = os.path.getmtime(db_path)
            except Exception:
                current_mtime = _auth_db_mtime  # can't stat, use cached

            db_unchanged = (current_mtime == _auth_db_mtime)
            ttl_valid = (now - _auth_cache_time) < _AUTH_TTL
            if db_unchanged and ttl_valid:
                return _auth_cache

        _auth_cache = cookie_extract.get_auth()
        _auth_cache_time = now
        try:
            db_path = cookie_extract.get_firefox_cookies_db()
            _auth_db_mtime = os.path.getmtime(db_path)
        except Exception:
            pass
        return _auth_cache


def _invalidate_auth() -> None:
    """Force next _get_auth() call to re-read Firefox cookies."""
    global _auth_cache
    with _auth_lock:
        _auth_cache = None


def _with_auth_retry(fn):
    """Call fn(token, user_id). On PermissionError (401/403), refresh auth once and retry."""
    token, user_id = _get_auth()
    try:
        return fn(token, user_id)
    except PermissionError:
        _invalidate_auth()
        token, user_id = _get_auth(force=True)
        return fn(token, user_id)


def auth_retry(tool_fn):
    """Decorator: catch PermissionError from any MCP tool, invalidate auth, retry once."""
    @functools.wraps(tool_fn)
    def wrapper(*args, **kwargs):
        try:
            return tool_fn(*args, **kwargs)
        except PermissionError as e:
            _invalidate_auth()
            try:
                return tool_fn(*args, **kwargs)
            except PermissionError:
                raise PermissionError(
                    f"Notion authentication failed after re-reading cookies. "
                    f"Open Firefox and log into Notion, then retry. ({e})"
                )
    return wrapper


def _get_agent_config(name: str) -> dict:
    """Look up an agent in the registry. Raises ValueError if not found."""
    registry = _load_registry()
    if name not in registry:
        available = ", ".join(sorted(registry.keys())) or "(none)"
        raise ValueError(f"Agent '{name}' not found. Available: {available}")
    cfg = registry[name]
    required = {"workflow_id", "space_id", "block_id"}
    missing = required - set(cfg.keys())
    if missing:
        raise ValueError(f"Agent '{name}' is missing fields: {missing}")
    return cfg


_UUID_DASHLESS = re.compile(r'^[0-9a-f]{32}$')
_UUID_DASHED = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')


def _to_dashed_uuid(s: str) -> str:
    """Convert a 32-char hex string to dashed UUID format. Pass through if already dashed."""
    s = s.strip().lower()
    if _UUID_DASHED.match(s):
        return s
    if _UUID_DASHLESS.match(s):
        return f"{s[:8]}-{s[8:12]}-{s[12:16]}-{s[16:20]}-{s[20:]}"
    raise ValueError(f"Not a valid UUID: {s}")


SPACE_ID = "f04bc8a1-18df-42d1-ba9f-961c491cdc1b"


def _name_to_key(name: str) -> str:
    """Convert an agent display name to a registry key (snake_case)."""
    return re.sub(r'[^a-z0-9]+', '_', name.lower()).strip('_')


# ── Tools ────────────────────────────────────────────────────────────────────

@mcp.tool()
@auth_retry
def list_agents() -> str:
    """List all registered Notion AI agents from agents.yaml."""
    registry = _load_registry()
    if not registry:
        return "No agents registered. Use sync_registry to auto-populate from Notion."
    lines = []
    for name, cfg in registry.items():
        wid = cfg.get("workflow_id", "?")
        lines.append(f"- {name}: workflow={wid}")
    return "\n".join(lines)


@mcp.tool()
@auth_retry
def list_workspace_agents() -> str:
    """
    Enumerate all AI agents in the Notion workspace directly from the API.
    Does not require agents.yaml — queries Notion live.
    Returns name, workflow_id, space_id, and block_id for every agent.
    """
    token, user_id = _get_auth()
    agents = notion_client.get_all_workspace_agents(SPACE_ID, token, user_id)
    if not agents:
        return "No agents found in workspace."
    lines = []
    for a in agents:
        lines.append(
            f"{a['name']}\n"
            f"  key:         {_name_to_key(a['name'])}\n"
            f"  workflow_id: {a['workflow_id']}\n"
            f"  space_id:    {a['space_id']}\n"
            f"  block_id:    {a['block_id']}"
        )
    return "\n\n".join(lines)


@mcp.tool()
@auth_retry
def sync_registry() -> str:
    """
    Sync agents.yaml with all agents currently in the Notion workspace.
    Adds new agents. Never removes existing entries (safe to run anytime).
    Returns a summary of what was added vs. already present.
    """
    token, user_id = _get_auth()
    agents = notion_client.get_all_workspace_agents(SPACE_ID, token, user_id)
    if not agents:
        return "No agents found in workspace."

    registry = _load_registry()
    added, skipped = [], []

    for a in agents:
        key = _name_to_key(a["name"])
        if key in registry:
            skipped.append(f"  {key} (already registered)")
        else:
            registry[key] = {
                "workflow_id": a["workflow_id"],
                "space_id": a["space_id"],
                "block_id": a["block_id"],
                "label": a["name"],
            }
            added.append(f"  + {key} ({a['name']})")

    if added:
        _save_registry(registry)

    lines = [f"Workspace has {len(agents)} agents."]
    if added:
        lines.append(f"Added ({len(added)}):")
        lines.extend(added)
    if skipped:
        lines.append(f"Already registered ({len(skipped)}):")
        lines.extend(skipped)
    return "\n".join(lines)


@mcp.tool()
@auth_retry
def dump_agent(agent_name: str) -> str:
    """
    Fetch the live instructions of a Notion AI agent as Markdown.
    Mentions are rendered as {{page:uuid}}, {{user:uuid}}, etc.
    """
    cfg = _get_agent_config(agent_name)
    token, user_id = _get_auth()
    data = notion_client.get_block_tree(
        cfg["block_id"], cfg["space_id"], token, user_id,
    )
    blocks_map = data.get("recordMap", {}).get("block", {})
    if not blocks_map:
        return "(No content found — block may be empty or inaccessible)"
    md = block_builder.blocks_to_markdown(blocks_map, cfg["block_id"])
    return md or "(Empty instructions block)"


@mcp.tool()
@auth_retry
def update_agent(
    agent_name: str,
    instructions_markdown: str,
    publish: bool = True,
) -> str:
    """
    Replace a Notion AI agent's instructions with new Markdown content.
    Optionally publishes the agent afterward (default: True).
    Mentions use {{page:uuid}} syntax.
    """
    cfg = _get_agent_config(agent_name)
    token, user_id = _get_auth()

    new_blocks = block_builder.markdown_to_blocks(instructions_markdown)
    if not new_blocks:
        return "Error: markdown produced no blocks. Check the content."

    stats = notion_client.diff_replace_block_content(
        cfg["block_id"], cfg["space_id"], new_blocks, token, user_id,
    )
    parts = []
    if stats["unchanged"]:
        parts.append(f"{stats['unchanged']} unchanged")
    if stats["updated"]:
        parts.append(f"{stats['updated']} updated")
    if stats["inserted"]:
        parts.append(f"{stats['inserted']} inserted")
    if stats["deleted"]:
        parts.append(f"{stats['deleted']} deleted")
    detail = ", ".join(parts) if parts else "no changes"
    msg = f"Updated {agent_name} ({detail}, {stats['ops']} ops in 1 tx)."

    if publish:
        result = notion_client.publish_agent(
            cfg["workflow_id"], cfg["space_id"], token, user_id,
        )
        version = result.get("version", "?")
        msg += f" Published v{version}."

    return msg


@mcp.tool()
@auth_retry
def publish_agent(agent_name: str) -> str:
    """Publish a Notion AI agent without changing its instructions."""
    cfg = _get_agent_config(agent_name)
    token, user_id = _get_auth()
    result = notion_client.publish_agent(
        cfg["workflow_id"], cfg["space_id"], token, user_id,
    )
    version = result.get("version", "?")
    artifact = result.get("workflowArtifactId", "?")
    return f"Published {agent_name} — version: {version}, artifact: {artifact}"


@mcp.tool()
@auth_retry
def discover_agent(workflow_url_or_id: str) -> str:
    """
    Discover a Notion AI agent's metadata from a URL or workflow ID.

    Accepts:
      - Full URL: https://www.notion.so/agent/315e7cc701d580018dbe0092f3224baa
      - Dashed UUID: 315e7cc7-01d5-8001-8dbe-0092f3224baa
      - Dashless UUID: 315e7cc701d580018dbe0092f3224baa

    Returns the agent's name, workflow_id, space_id, and block_id.
    """
    # Extract UUID from URL or raw input
    url_match = re.search(r'/agent/([0-9a-f-]+)', workflow_url_or_id)
    raw_id = url_match.group(1) if url_match else workflow_url_or_id.strip()
    workflow_id = _to_dashed_uuid(raw_id)

    token, user_id = _get_auth()
    wf = notion_client.get_workflow_record(workflow_id, token, user_id)

    name = wf.get("data", {}).get("name", "(unnamed)")
    space_id = wf.get("space_id", "?")

    # instructions can be a plain UUID string or {"id": "...", "table": "block", ...}
    instructions = wf.get("data", {}).get("instructions", "?")
    block_id = instructions["id"] if isinstance(instructions, dict) else instructions

    lines = [
        f"name: {name}",
        f"workflow_id: {workflow_id}",
        f"space_id: {space_id}",
        f"block_id: {block_id}",
    ]
    return "\n".join(lines)


@mcp.tool()
@auth_retry
def register_agent(
    name: str,
    workflow_id: str,
    space_id: str,
    block_id: str,
    label: str = "",
) -> str:
    """
    Register a Notion AI agent in agents.yaml.
    All three UUIDs (workflow_id, space_id, block_id) are required.
    Use discover_agent first to find them.
    """
    # Validate UUIDs
    workflow_id = _to_dashed_uuid(workflow_id)
    space_id = _to_dashed_uuid(space_id)
    block_id = _to_dashed_uuid(block_id)

    registry = _load_registry()
    if name in registry:
        return f"Agent '{name}' already exists. Remove it first to re-register."

    entry: dict = {
        "workflow_id": workflow_id,
        "space_id": space_id,
        "block_id": block_id,
    }
    if label:
        entry["label"] = label

    registry[name] = entry
    _save_registry(registry)
    return f"Registered agent '{name}' (workflow: {workflow_id})"


@mcp.tool()
@auth_retry
def remove_agent(name: str) -> str:
    """Remove a Notion AI agent from agents.yaml."""
    registry = _load_registry()
    if name not in registry:
        available = ", ".join(sorted(registry.keys())) or "(none)"
        return f"Agent '{name}' not found. Available: {available}"
    del registry[name]
    _save_registry(registry)
    return f"Removed agent '{name}' from registry."


def _resolve_thread_id(thread: str, token: str, user_id: str | None) -> str:
    """
    Resolve a thread UUID from a URL, raw UUID, or title search string.
    Raises ValueError with helpful disambiguation if multiple matches found.
    """
    thread = thread.strip()

    # URL containing ?t= or ?at= parameter
    if "notion.so" in thread or "?" in thread:
        for param in ("t", "at"):
            m = re.search(rf'[?&]{param}=([0-9a-f-]{{32,36}})', thread)
            if m:
                return _to_dashed_uuid(m.group(1))

    # Raw UUID (dashed or dashless)
    try:
        return _to_dashed_uuid(thread)
    except ValueError:
        pass

    # Title search via Notion API
    matches = notion_client.search_threads(thread, SPACE_ID, token, user_id)
    if not matches:
        raise ValueError(
            f"No conversation found for '{thread}'. "
            "Try providing a thread UUID directly."
        )
    if len(matches) > 1:
        options = "\n".join(
            f"  {m['thread_id']}: {m['title']}" for m in matches[:10]
        )
        raise ValueError(
            f"Multiple conversations match '{thread}'. "
            f"Provide a UUID to be specific:\n{options}"
        )
    return matches[0]["thread_id"]


def _conversation_to_markdown(convo: dict) -> str:
    """Format a conversation dict as a Markdown transcript."""
    title = convo.get("title") or "Notion AI Chat"
    model = convo.get("model") or ""
    lines = [
        f"# {title}" + (f" ({model})" if model else ""),
        f"_ID: {convo['id']}_",
        "",
    ]
    for turn in convo.get("turns") or []:
        label = "**Notion AI**" if turn["role"] == "assistant" else "**You**"
        parts = []
        if turn.get("thinking"):
            parts.append(f"<details><summary>Thinking</summary>\n\n{turn['thinking']}\n\n</details>")
        text = turn.get("content") or ""
        if text:
            parts.append(text)
        if turn.get("toolCalls"):
            tc_lines = []
            for tc in turn["toolCalls"]:
                inp = json.dumps(tc.get("input") or {}, ensure_ascii=False)
                tc_line = f"- `{tc['tool']}` {inp}"
                if tc.get("result"):
                    result_str = tc["result"] if isinstance(tc["result"], str) else json.dumps(tc["result"], ensure_ascii=False)
                    # Truncate very long results for readability
                    if len(result_str) > 500:
                        result_str = result_str[:500] + "…"
                    tc_line += f"\n  > {result_str}"
                tc_lines.append(tc_line)
            parts.append("**Tool calls:**\n" + "\n".join(tc_lines))
        body = "\n\n".join(parts) if parts else "(empty turn)"
        lines.append(f"{label}\n\n{body}\n\n---\n")
    if convo.get("toolCalls"):
        lines.append("**Additional tool calls (pre-inference):**")
        for tc in convo["toolCalls"]:
            lines.append(f"- `{tc['tool']}`: {json.dumps(tc.get('input') or {})}")
    return "\n".join(lines).strip()


@mcp.tool()
@auth_retry
def get_conversation(thread: str, format: str = "json") -> str:
    """
    Fetch a Notion AI conversation and return its full transcript.

    thread: Any of:
      - Thread UUID (dashed: 318e7cc7-01d5-... or 32-char dashless)
      - Notion URL containing ?t= or ?at= parameter
      - Title text — searches the workspace and returns the unique match

    format: "json" (default) — full structured data (turns, toolCalls, model, timestamps)
            "md"            — Markdown transcript

    Returns all conversation turns with role, content, thinking (CoT), model,
    and any tool calls (input + result) attached to each turn.
    If multiple conversations match a title search, lists candidates and asks
    you to re-call with a specific UUID.
    """
    if format not in ("json", "md"):
        raise ValueError(f"format must be 'json' or 'md', got '{format}'")

    token, user_id = _get_auth()
    thread_id = _resolve_thread_id(thread, token, user_id)
    convo = notion_client.get_thread_conversation(thread_id, token, user_id)

    if format == "md":
        return _conversation_to_markdown(convo)
    return json.dumps(convo, indent=2, ensure_ascii=False)


def _get_collection_prop_names(collection_id: str) -> dict[str, str]:
    """Fetch and cache a collection's property ID -> name map."""
    with _collection_lock:
        if collection_id in _collection_schemas:
            return _collection_schemas[collection_id]
    token, user_id = _get_auth()
    payload = {"requests": [{"id": collection_id, "table": "collection"}]}
    data = notion_client._post("getRecordValues", payload, token, user_id)
    schema = data.get("results", [{}])[0].get("value", {}).get("schema", {})
    prop_map = {pid: pdef.get("name", pid) for pid, pdef in schema.items()}
    with _collection_lock:
        _collection_schemas[collection_id] = prop_map
    return prop_map


def _format_trigger(t: dict) -> str:
    """Format a single trigger dict as a human-readable line."""
    state = t.get("state", {})
    enabled = "enabled" if t.get("enabled", True) else "disabled"
    ttype = state.get("type", "unknown")

    if ttype == "notion.agent.mentioned":
        return f"@mention [{enabled}]"

    if ttype == "recurrence":
        freq = state.get("frequency", "?")
        hour = state.get("hour", 0)
        minute = state.get("minute", 0)
        tz = state.get("timezone", "UTC")
        time_str = f"{hour:02d}:{minute:02d}"

        if freq == "week":
            days = ", ".join(state.get("weekdays", []))
            return f"Schedule: weekly {days} at {time_str} {tz} [{enabled}]"
        elif freq == "day":
            interval = state.get("interval", 1)
            every = "daily" if interval == 1 else f"every {interval} days"
            return f"Schedule: {every} at {time_str} {tz} [{enabled}]"
        else:
            return f"Schedule: {freq} interval={state.get('interval', 1)} at {time_str} {tz} [{enabled}]"

    if ttype == "notion.page.updated":
        collection = state.get("collectionId", "?")
        prop_ids = state.get("propertyIds", [])
        filters = state.get("propertyFilters", {})
        # Resolve property IDs to names via collection schema
        prop_names = {}
        if collection and collection != "?":
            try:
                prop_names = _get_collection_prop_names(collection)
            except Exception:
                pass  # fall back to raw IDs
        def _pname(pid: str) -> str:
            return prop_names.get(pid, pid)
        # Extract filter conditions for display
        conditions = []
        for f in filters.get("all", []):
            prop_label = _pname(f.get("property", "?"))
            filt = f.get("filter", {})
            op = filt.get("operator", "?")
            raw_val = filt.get("value")
            # value can be: list of dicts, single dict, or absent
            if isinstance(raw_val, list):
                vals = [v.get("value", v) if isinstance(v, dict) else v for v in raw_val]
                conditions.append(f"{prop_label} {op}: {', '.join(str(v) for v in vals)}")
            elif isinstance(raw_val, dict):
                conditions.append(f"{prop_label} {op}: {raw_val.get('value', raw_val)}")
            else:
                conditions.append(f"{prop_label} {op}")
        watched = ", ".join(_pname(p) for p in prop_ids) if prop_ids else "?"
        cond_str = "; ".join(conditions) if conditions else f"watches {watched}"
        ignore_body = state.get("shouldIgnorePageContentUpdates", True)
        body_note = "" if ignore_body else " +body_edits"
        return f"Property change: {cond_str} on {collection}{body_note} [{enabled}]"

    return f"{ttype} [{enabled}]"


def _format_agent_triggers(triggers: list[dict]) -> list[str]:
    """
    Format a trigger list with implicit defaults.

    Notion agents always have two implicit triggers:
      - "New chat" (always on, never stored in data.triggers)
      - "@mention" (stored only when enabled; absent = disabled)

    This function synthesizes the full picture.
    """
    lines = ["  New chat [always on]"]

    has_mention = any(
        t.get("state", {}).get("type") == "notion.agent.mentioned"
        for t in triggers
    )
    if has_mention:
        mention = next(
            t for t in triggers
            if t.get("state", {}).get("type") == "notion.agent.mentioned"
        )
        lines.append(f"  {_format_trigger(mention)}")
    else:
        lines.append("  @mention [disabled]")

    for t in triggers:
        if t.get("state", {}).get("type") != "notion.agent.mentioned":
            lines.append(f"  {_format_trigger(t)}")

    return lines


@mcp.tool()
@auth_retry
def get_agent_triggers(agent: str = "all") -> str:
    """
    Show trigger configuration for Notion AI custom agents.

    agent: A registered agent name, or "all" for every agent in the workspace.

    Returns each agent's triggers: @mention, schedules (recurrence),
    and property-change (notion.page.updated) with filter conditions.

    Includes implicit defaults: "New chat" (always on) and "@mention"
    (shown as disabled when absent from data.triggers).
    """
    token, user_id = _get_auth()

    if agent == "all":
        agents = notion_client.get_all_workspace_agents(SPACE_ID, token, user_id)
        if not agents:
            return "No agents found in workspace."
        lines = []
        for a in agents:
            triggers = a.get("triggers", [])
            trigger_lines = _format_agent_triggers(triggers)
            lines.append(f"{a['name']}:\n" + "\n".join(trigger_lines))
        return "\n\n".join(lines)

    # Single agent from registry
    cfg = _get_agent_config(agent)
    wf = notion_client.get_workflow_record(cfg["workflow_id"], token, user_id)
    triggers = wf.get("data", {}).get("triggers", [])
    name = wf.get("data", {}).get("name", agent)
    trigger_lines = _format_agent_triggers(triggers)
    return f"{name}:\n" + "\n".join(trigger_lines)


@mcp.tool()
@auth_retry
def get_db_automations(db: str) -> str:
    """
    List all native automations configured on a Notion database.

    db: Database page URL (https://www.notion.so/...) or page UUID (dashed or dashless).

    Returns each automation's trigger condition and action(s) in a readable format.
    Uses the internal loadPageChunk API which surfaces the 'automation' and
    'automation_action' record tables alongside block data.
    """
    # Extract UUID from URL or raw input
    url_match = re.search(r'notion\.so/(?:[^/]+/)*([0-9a-f]{32})', db)
    raw_id = url_match.group(1) if url_match else db.strip()
    db_page_id = _to_dashed_uuid(raw_id)

    token, user_id = _get_auth()
    result = notion_client.get_db_automations(db_page_id, token, user_id)
    automations = result.get("automations", [])
    prop_map = result.get("property_map", {})

    def _prop_name(pid: str) -> str:
        return prop_map.get(pid, pid)

    if not automations:
        return f"No automations found on database {db_page_id}."

    lines = [f"Found {len(automations)} automation(s) on {db_page_id}:\n"]
    for i, a in enumerate(automations, 1):
        enabled = a.get("enabled")
        enabled_str = "enabled" if enabled else ("disabled" if enabled is False else "enabled (default)")
        lines.append(f"── Automation {i}: {a['id']}  [{enabled_str}]")

        trigger = a.get("trigger") or {}
        event = trigger.get("event", {})
        if event.get("pagePropertiesEdited"):
            filters = event["pagePropertiesEdited"].get("all", [])
            props = [f"{_prop_name(f['property'])} ({f['filter']['operator']})" for f in filters]
            lines.append(f"   Trigger: pagePropertiesEdited — {', '.join(props)}")
        elif event.get("pagesAdded"):
            lines.append("   Trigger: pagesAdded")
        else:
            lines.append(f"   Trigger: {json.dumps(trigger)}")

        actions = a.get("actions", [])
        lines.append(f"   Actions ({len(actions)}):")
        for act in actions:
            lines.append(f"     [{act['type']}]  id={act['id']}")
            config = act.get("config", {})
            if config.get("values"):
                props_written = [_prop_name(p) for p in config["values"].keys()]
                lines.append(f"       writes properties: {props_written}")
            elif config:
                lines.append(f"       config: {json.dumps(config)[:120]}")
        lines.append("")

    return "\n".join(lines).strip()


@mcp.tool()
@auth_retry
def get_agent_tools(agent_name: str) -> str:
    """
    Show the full tool/module configuration for a Notion AI agent.

    Returns the agent's model, Notion page permissions (with page names),
    MCP server connections (with enabled tools), mail, calendar, and
    any other configured modules.
    """
    cfg = _get_agent_config(agent_name)
    token, user_id = _get_auth()
    result = notion_client.get_agent_modules(cfg["workflow_id"], token, user_id)

    lines = [f"Agent: {cfg.get('label', agent_name)}"]
    model = result.get("model", {})
    lines.append(f"Model: {model.get('display', '?')} ({model.get('type', '?')})")
    lines.append("")

    for mod in result.get("modules", []):
        mtype = mod.get("type", "?")
        name = mod.get("name", mtype)

        if mtype == "notion":
            lines.append(f"[Notion] {name}")
            for perm in mod.get("permissions", []):
                scope = perm.get("scope", "?")
                actions = ", ".join(perm.get("actions", []))
                if scope == "workspacePublic":
                    lines.append(f"  Workspace public pages — {actions}")
                else:
                    page = perm.get("pageName", perm.get("blockId", "?"))
                    lines.append(f"  {page} — {actions}")

        elif mtype == "mcpServer":
            url = mod.get("serverUrl", "?")
            official = mod.get("officialName", "")
            transport = mod.get("preferredTransport", "?")
            auto_write = mod.get("runWriteToolsAutomatically", False)
            enabled = mod.get("enabledToolNames", [])
            total = mod.get("totalTools", 0)
            conn_id = mod.get("connectionId", "")

            label = f"[MCP] {name}"
            if official:
                label += f" ({official})"
            lines.append(label)
            lines.append(f"  URL: {url}")
            lines.append(f"  Transport: {transport}")
            lines.append(f"  Auto-run writes: {auto_write}")
            if conn_id:
                lines.append(f"  Connection ID: {conn_id}")
            lines.append(f"  Enabled: {len(enabled)}/{total} tools")

            # List tools with enabled status
            all_tool_names = {t["name"] for t in mod.get("tools", [])}
            enabled_set = set(enabled)
            for t in mod.get("tools", []):
                status = "ON" if t["name"] in enabled_set else "off"
                lines.append(f"    [{status}] {t['name']}: {t['title']}")

        elif mtype == "mail":
            emails = ", ".join(mod.get("emailAddresses", []))
            scopes = ", ".join(mod.get("scopes", []))
            lines.append(f"[Mail] {name}")
            lines.append(f"  Addresses: {emails}")
            lines.append(f"  Scopes: {scopes}")

        elif mtype == "calendar":
            scopes = ", ".join(mod.get("scopes", []))
            lines.append(f"[Calendar] {name}")
            lines.append(f"  Scopes: {scopes}")

        else:
            lines.append(f"[{mtype}] {name}")

        lines.append("")

    return "\n".join(lines).strip()


@mcp.tool()
@auth_retry
def add_agent_mcp_server(
    agent_name: str,
    server_name: str,
    server_url: str,
    publish: bool = True,
) -> str:
    """
    Add a custom MCP server to a Notion AI agent's tool configuration.

    agent_name: Registered agent name from agents.yaml.
    server_name: Display name for the MCP server (e.g. "my-tools").
    server_url: The MCP server URL (e.g. "https://example.com/mcp").
    publish: Whether to publish the agent afterward (default: True).
    """
    cfg = _get_agent_config(agent_name)
    token, user_id = _get_auth()
    wf = notion_client.get_workflow_record(cfg["workflow_id"], token, user_id)
    modules = wf.get("data", {}).get("modules", [])

    # Check for duplicate
    for m in modules:
        if m.get("type") == "mcpServer" and m.get("state", {}).get("serverUrl") == server_url:
            return f"MCP server at {server_url} is already configured on {agent_name}."

    import uuid as _uuid
    new_module = {
        "id": str(_uuid.uuid4()),
        "name": server_name,
        "type": "mcpServer",
        "version": "1.0.0",
        "state": {
            "serverUrl": server_url,
        },
    }
    modules.append(new_module)
    notion_client.update_agent_modules(
        cfg["workflow_id"], cfg["space_id"], modules, token, user_id,
    )

    msg = f"Added MCP server '{server_name}' ({server_url}) to {agent_name}."
    if publish:
        result = notion_client.publish_agent(
            cfg["workflow_id"], cfg["space_id"], token, user_id,
        )
        msg += f" Published v{result.get('version', '?')}."
    return msg


@mcp.tool()
@auth_retry
def remove_agent_mcp_server(
    agent_name: str,
    server_name: str,
    publish: bool = True,
) -> str:
    """
    Remove an MCP server from a Notion AI agent's tool configuration.

    agent_name: Registered agent name from agents.yaml.
    server_name: The display name of the MCP server to remove.
    publish: Whether to publish the agent afterward (default: True).
    """
    cfg = _get_agent_config(agent_name)
    token, user_id = _get_auth()
    wf = notion_client.get_workflow_record(cfg["workflow_id"], token, user_id)
    modules = wf.get("data", {}).get("modules", [])

    original_count = len(modules)
    modules = [
        m for m in modules
        if not (m.get("type") == "mcpServer" and m.get("name") == server_name)
    ]

    if len(modules) == original_count:
        mcp_names = [m.get("name") for m in modules if m.get("type") == "mcpServer"]
        return f"No MCP server named '{server_name}' found on {agent_name}. Current: {mcp_names}"

    notion_client.update_agent_modules(
        cfg["workflow_id"], cfg["space_id"], modules, token, user_id,
    )

    msg = f"Removed MCP server '{server_name}' from {agent_name}."
    if publish:
        result = notion_client.publish_agent(
            cfg["workflow_id"], cfg["space_id"], token, user_id,
        )
        msg += f" Published v{result.get('version', '?')}."
    return msg


@mcp.tool()
@auth_retry
def set_agent_model(
    agent_name: str,
    model: str,
    publish: bool = True,
) -> str:
    """
    Change a Notion AI agent's model.

    agent_name: Registered agent name from agents.yaml.
    model: Model display name or codename. Accepted values:
      - "opus" / "avocado-froyo-medium" → Opus 4.6
      - "sonnet" / "almond-croissant-low" → Sonnet 4.6
      - "auto" → Auto (Notion picks)
      - Or any raw codename string.
    publish: Whether to publish the agent afterward (default: True).
    """
    # Resolve friendly names to codenames
    aliases = {
        "opus": "avocado-froyo-medium",
        "opus 4.6": "avocado-froyo-medium",
        "sonnet": "almond-croissant-low",
        "sonnet 4.6": "almond-croissant-low",
        "auto": "auto",
    }
    model_type = aliases.get(model.lower().strip(), model.strip())

    cfg = _get_agent_config(agent_name)
    token, user_id = _get_auth()
    notion_client.update_agent_model(
        cfg["workflow_id"], cfg["space_id"], model_type, token, user_id,
    )
    display = notion_client.MODEL_NAMES.get(model_type, model_type)
    msg = f"Set {agent_name} model to {display} ({model_type})."

    if publish:
        result = notion_client.publish_agent(
            cfg["workflow_id"], cfg["space_id"], token, user_id,
        )
        msg += f" Published v{result.get('version', '?')}."
    return msg


def main():
    mcp.run()


if __name__ == "__main__":
    main()
