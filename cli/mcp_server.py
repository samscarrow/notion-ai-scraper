#!/usr/bin/env python3
"""
mcp_server.py — MCP server for managing Notion AI Agent instructions.

Wraps the existing CLI modules (cookie_extract, notion_client, block_builder)
as MCP tools so any AI client can manage Notion agents headlessly.

Usage:
  python cli/mcp_server.py                       # stdio transport
  claude mcp add notion-agents -- python cli/mcp_server.py
"""

import json
import os
import re
import sys

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

def _load_registry() -> dict:
    """Load agents.yaml, returning {} if missing or empty."""
    if not os.path.exists(AGENTS_YAML):
        return {}
    with open(AGENTS_YAML) as f:
        data = yaml.safe_load(f)
    return data if isinstance(data, dict) else {}


def _save_registry(registry: dict) -> None:
    """Write registry back to agents.yaml."""
    with open(AGENTS_YAML, "w") as f:
        yaml.dump(registry, f, default_flow_style=False, sort_keys=False)


def _get_auth() -> tuple[str, str | None]:
    """Return (token_v2, user_id) from Firefox cookies."""
    token = cookie_extract.get_token_v2()
    user_id = cookie_extract.get_user_id()
    return token, user_id


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

    notion_client.replace_block_content(
        cfg["block_id"], cfg["space_id"], new_blocks, token, user_id,
    )
    msg = f"Updated {agent_name} ({len(new_blocks)} blocks)."

    if publish:
        result = notion_client.publish_agent(
            cfg["workflow_id"], cfg["space_id"], token, user_id,
        )
        version = result.get("version", "?")
        msg += f" Published v{version}."

    return msg


@mcp.tool()
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
        text = turn.get("content") or ""
        if turn.get("toolCalls"):
            calls = "\n".join(
                f"- `{tc['tool']}`: {json.dumps(tc.get('input') or {})}"
                for tc in turn["toolCalls"]
            )
            text += ("\n\n" if text else "") + f"**Tool calls:**\n{calls}"
        lines.append(f"{label}\n\n{text}\n\n---\n")
    if convo.get("toolCalls"):
        lines.append("**Additional tool calls (pre-inference):**")
        for tc in convo["toolCalls"]:
            lines.append(f"- `{tc['tool']}`: {json.dumps(tc.get('input') or {})}")
    return "\n".join(lines).strip()


@mcp.tool()
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
        # Extract filter conditions for display
        conditions = []
        for f in filters.get("all", []):
            filt = f.get("filter", {})
            op = filt.get("operator", "?")
            raw_val = filt.get("value")
            # value can be: list of dicts, single dict, or absent
            if isinstance(raw_val, list):
                vals = [v.get("value", v) if isinstance(v, dict) else v for v in raw_val]
                conditions.append(f"{op}: {', '.join(str(v) for v in vals)}")
            elif isinstance(raw_val, dict):
                conditions.append(f"{op}: {raw_val.get('value', raw_val)}")
            else:
                conditions.append(op)
        cond_str = "; ".join(conditions) if conditions else f"propertyIds={prop_ids}"
        return f"Property change: {cond_str} on {collection} [{enabled}]"

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
            props = [f"{f['property']} ({f['filter']['operator']})" for f in filters]
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
                props_written = list(config["values"].keys())
                lines.append(f"       writes properties: {props_written}")
            elif config:
                lines.append(f"       config: {json.dumps(config)[:120]}")
        lines.append("")

    return "\n".join(lines).strip()


def main():
    mcp.run()


if __name__ == "__main__":
    main()
