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
import uuid
import time

# Allow running from project root or cli/ directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import yaml
from mcp.server.fastmcp import FastMCP

import block_builder
import cookie_extract
import dispatch
import notion_api
import notion_client
import config

# Use config instance
CFG = config.get_config()

AGENTS_YAML = os.path.join(os.path.dirname(os.path.abspath(__file__)), "agents.yaml")


mcp = FastMCP(
    "notion-agents",
    host="0.0.0.0",
    port=8090,
    instructions=(
        "PROGRAMMATIC EDITS PAUSED: All automated modifications to Notion Agent "
        "configurations are strictly paused for maintenance. Do NOT use create, "
        "update, or set_config tools until further notice. Read-only discovery "
        "tools remain active. Auth is automatic via Firefox session cookies (token_v2)."
    ),
)


# ── Helpers ──────────────────────────────────────────────────────────────────

_collection_schemas: dict[str, dict[str, str]] = {}  # collection_id -> {prop_id: name}
_collection_lock = threading.Lock()

# Public API schema cache: db_id -> {prop_name: prop_type}
_db_schema_cache: dict[str, dict[str, str]] = {}
_db_schema_cache_time: dict[str, float] = {}
_db_schema_lock = threading.Lock()
_DB_SCHEMA_TTL = 300  # seconds — mirrors _AUTH_TTL pattern

# System audit properties suppressed from default output
_SYSTEM_PROP_TYPES = {"created_time", "last_edited_time", "created_by", "last_edited_by"}

# Relation title cache: page_id -> title string
_relation_title_cache: dict[str, str] = {}
_relation_title_lock = threading.Lock()

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
    """Look up an agent in the registry. Handles both old and new schema keys."""
    registry = _load_registry()
    if name not in registry:
        available = ", ".join(sorted(registry.keys())) or "(none)"
        raise ValueError(f"Agent '{name}' not found. Available: {available}")
    
    raw_cfg = registry[name]
    
    # Map to standardized internal keys
    cfg = {
        "notion_internal_id": raw_cfg.get("notion_internal_id") or raw_cfg.get("notion_internal_id"),
        "notion_public_id": raw_cfg.get("notion_public_id") or raw_cfg.get("notion_public_id"),
        "space_id": raw_cfg.get("space_id"),
        "label": raw_cfg.get("label", name)
    }
    
    required = {"notion_internal_id", "space_id", "notion_public_id"}
    missing = {k for k in required if not cfg[k]}
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


def _name_to_key(name: str) -> str:
    """Convert an agent display name to a registry key (snake_case)."""
    return re.sub(r'[^a-z0-9]+', '_', name.lower()).strip('_')


def _build_update_message(agent_name: str, stats: dict) -> str:
    """Format the human-readable update summary for an instructions write."""
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
    return f"Updated {agent_name} ({detail}, {stats['ops']} ops in 1 tx)."


def _build_thread_cleanup_message(result: dict) -> str:
    """Summarize any stale-thread cleanup performed after publish."""
    parts: list[str] = []
    count = result.get("archivedThreadCount")
    if count is not None:
        noun = "chat" if count == 1 else "chats"
        parts.append(f"Archived {count} stale {noun}.")
    warning = result.get("threadCleanupWarning")
    if warning:
        parts.append(f"Thread cleanup warning: {warning}")
    return " ".join(parts)


def _build_publish_message(agent_name: str, result: dict, *, standalone: bool = False) -> str:
    """Format a publish result, including post-publish stale-thread cleanup."""
    if "warning" in result:
        msg = f"Publish {agent_name}: {result['warning']}."
        detail = result.get("detail")
        if detail:
            msg += f" {detail}"
    else:
        version = result.get("version", "?")
        if standalone:
            artifact = result.get("workflowArtifactId", "?")
            msg = f"Published {agent_name} — version: {version}, artifact: {artifact}."
        else:
            msg = f"Published v{version}."

    cleanup = _build_thread_cleanup_message(result)
    if cleanup:
        msg += f" {cleanup}"
    return msg


def _update_agent_impl(
    agent_name: str,
    instructions_markdown: str,
    publish: bool,
) -> str:
    """Shared implementation for inline and file-based agent instruction updates."""
    cfg = _get_agent_config(agent_name)
    token, user_id = _get_auth()

    new_blocks = block_builder.markdown_to_blocks(instructions_markdown)
    if not new_blocks:
        return "Error: markdown produced no blocks. Check the content."

    stats = notion_client.diff_replace_block_content(
        cfg["notion_public_id"], cfg["space_id"], new_blocks, token, user_id,
    )
    msg = _build_update_message(agent_name, stats)

    if publish:
        result = notion_client.publish_agent(
            cfg["notion_internal_id"], cfg["space_id"], token, user_id,
        )
        msg += f" {_build_publish_message(agent_name, result)}"

    return msg


# ── Tools ────────────────────────────────────────────────────────────────────

@mcp.tool()
@auth_retry
def list_agents() -> str:
    """List all registered Notion AI agents from agents.yaml."""
    registry = _load_registry()
    if not registry:
        return "No agents registered. Use sync_registry to auto-populate from Notion."
    lines = []
    for name, raw_cfg in registry.items():
        wid = raw_cfg.get("notion_internal_id") or raw_cfg.get("notion_internal_id", "?")
        lines.append(f"- {name}: workflow={wid}")
    return "\n".join(lines)


@mcp.tool()
@auth_retry
def list_workspace_agents() -> str:
    """
    Enumerate all AI agents in the Notion workspace directly from the API.
    Does not require agents.yaml — queries Notion live.
    Returns name, notion_internal_id, space_id, and notion_public_id for every agent.
    """
    token, user_id = _get_auth()
    agents = notion_client.get_all_workspace_agents(CFG.space_id, token, user_id)
    if not agents:
        return "No agents found in workspace."
    lines = []
    for a in agents:
        lines.append(
            f"{a['name']}\n"
            f"  key:         {_name_to_key(a['name'])}\n"
            f"  notion_internal_id: {a['notion_internal_id']}\n"
            f"  space_id:    {a['space_id']}\n"
            f"  notion_public_id:    {a['notion_public_id']}"
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
    agents = notion_client.get_all_workspace_agents(CFG.space_id, token, user_id)
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
                "notion_internal_id": a["notion_internal_id"],
                "space_id": a["space_id"],
                "notion_public_id": a["notion_public_id"],
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
        cfg["notion_public_id"], cfg["space_id"], token, user_id,
    )
    blocks_map = data.get("recordMap", {}).get("block", {})
    if not blocks_map:
        return "(No content found — block may be empty or inaccessible)"
    md = block_builder.blocks_to_markdown(blocks_map, cfg["notion_public_id"])
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
    return _update_agent_impl(agent_name, instructions_markdown, publish)


@mcp.tool()
@auth_retry
def update_agent_from_file(
    agent_name: str,
    markdown_path: str,
    publish: bool = True,
) -> str:
    """
    Replace a Notion AI agent's instructions from a local Markdown file path.
    Useful when the caller wants a short tool request instead of inlining the
    full markdown body in the MCP arguments.
    """
    with open(markdown_path, encoding="utf-8") as f:
        instructions_markdown = f.read()
    return _update_agent_impl(agent_name, instructions_markdown, publish)


@mcp.tool()
@auth_retry
def publish_agent(agent_name: str) -> str:
    """Publish a Notion AI agent without changing its instructions."""
    cfg = _get_agent_config(agent_name)
    token, user_id = _get_auth()
    result = notion_client.publish_agent(
        cfg["notion_internal_id"], cfg["space_id"], token, user_id,
    )
    return _build_publish_message(agent_name, result, standalone=True)


@mcp.tool()
@auth_retry
def discover_agent(workflow_url_or_id: str) -> str:
    """
    Discover a Notion AI agent's metadata from a URL or workflow ID.

    Accepts:
      - Full URL: https://www.notion.so/agent/315e7cc701d580018dbe0092f3224baa
      - Dashed UUID: 315e7cc7-01d5-8001-8dbe-0092f3224baa
      - Dashless UUID: 315e7cc701d580018dbe0092f3224baa

    Returns the agent's name, notion_internal_id, space_id, and notion_public_id.
    """
    # Extract UUID from URL or raw input
    url_match = re.search(r'/agent/([0-9a-f-]+)', workflow_url_or_id)
    raw_id = url_match.group(1) if url_match else workflow_url_or_id.strip()
    notion_internal_id = _to_dashed_uuid(raw_id)

    token, user_id = _get_auth()
    wf = notion_client.get_workflow_record(notion_internal_id, token, user_id)

    name = wf.get("data", {}).get("name", "(unnamed)")
    space_id = wf.get("space_id", "?")

    # instructions can be a plain UUID string or {"id": "...", "table": "block", ...}
    instructions = wf.get("data", {}).get("instructions", "?")
    notion_public_id = instructions["id"] if isinstance(instructions, dict) else instructions

    lines = [
        f"name: {name}",
        f"notion_internal_id: {notion_internal_id}",
        f"space_id: {space_id}",
        f"notion_public_id: {notion_public_id}",
    ]
    return "\n".join(lines)


@mcp.tool()
@auth_retry
def register_agent(
    name: str,
    notion_internal_id: str,
    space_id: str,
    notion_public_id: str,
    label: str = "",
) -> str:
    """
    Register a Notion AI agent in agents.yaml.
    All three UUIDs (notion_internal_id, space_id, notion_public_id) are required.
    Use discover_agent first to find them.
    """
    # Validate UUIDs
    notion_internal_id = _to_dashed_uuid(notion_internal_id)
    space_id = _to_dashed_uuid(space_id)
    notion_public_id = _to_dashed_uuid(notion_public_id)

    registry = _load_registry()
    if name in registry:
        return f"Agent '{name}' already exists. Remove it first to re-register."

    entry: dict = {
        "notion_internal_id": notion_internal_id,
        "space_id": space_id,
        "notion_public_id": notion_public_id,
    }
    if label:
        entry["label"] = label

    registry[name] = entry
    _save_registry(registry)
    return f"Registered agent '{name}' (workflow: {notion_internal_id})"


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
    matches = notion_client.search_threads(thread, CFG.space_id, token, user_id)
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


@mcp.tool()
@auth_retry
def chat_with_agent(agent_name: str, message: str, thread_id: str | None = None,
                    new_thread: bool = False, wait: bool = False,
                    timeout: int = 120) -> str:
    """
    Send a message to a Notion AI agent and trigger a response.

    agent_name: Registered agent name from agents.yaml.
    message: The content of the message to send.
    thread_id: Optional UUID of an existing thread to continue.
    new_thread: If true, always create a fresh thread (ignores thread_id).
    wait: If true, poll until the agent responds and return the response text.
    timeout: Max seconds to wait for response (default 120, only used with wait=True).

    If thread_id is omitted and new_thread is false, continues the most recent
    existing thread or creates one if none exist.

    Returns the agent's response (if wait=True) or the message ID + thread ID.
    """
    with open(AGENTS_YAML) as f:
        registry = yaml.safe_load(f)

    cfg = registry.get(agent_name)
    if not cfg:
        raise ValueError(f"Agent '{agent_name}' not found in registry.")

    token, user_id = _get_auth()
    created_new = False

    if new_thread:
        # NOTE: Programmatically created threads do not trigger inference
        # on Notion's backend (returns empty streaming response).
        # For now, require at least one UI-created thread per agent.
        raise ValueError(
            f"Programmatic creation of new threads for agent '{agent_name}' is not yet "
            "supported by Notion's inference backend. Please continue an existing thread "
            "or start a new one in the Notion UI first."
        )

    if not thread_id:
        # Try to find an existing non-trigger thread
        threads = notion_client.list_workflow_threads(
            cfg['notion_internal_id'], cfg['space_id'], token, user_id, limit=5)
        # Filter out trigger-bound threads
        manual_threads = [t for t in threads if not t.get('trigger_id')]
        if manual_threads:
            thread_id = manual_threads[0]['id']
            print(f"Continuing most recent thread: {thread_id}", file=sys.stderr)
        else:
            raise ValueError(
                f"No existing threads found for agent '{agent_name}'. "
                "Please start a chat with this agent in the Notion UI first, "
                "then retry."
            )

    msg_id = notion_client.send_agent_message(
        thread_id=thread_id,
        space_id=cfg['space_id'],
        notion_internal_id=cfg['notion_internal_id'],
        content=message,
        token_v2=token,
        user_id=user_id,
        model=cfg.get('model', 'avocado-froyo-medium'),
    )

    thread_note = " (new thread)" if created_new else ""

    if wait:
        print(f"Waiting for agent response (timeout={timeout}s)...", file=sys.stderr)
        response = notion_client.wait_for_agent_response(
            thread_id, msg_id, token, user_id, timeout=timeout)
        if response:
            return (
                f"**Agent response** (thread: {thread_id}{thread_note}):\n\n{response}"
            )
        return (
            f"Agent did not respond within {timeout}s. Thread: {thread_id}{thread_note}.\n"
            f"Check manually: get_conversation(thread='{thread_id}', format='md')"
        )

    return (
        f"Message sent (ID: {msg_id}) to thread {thread_id}{thread_note}.\n"
        f"The agent inference has been triggered. Wait a few seconds, then call:\n"
        f"get_conversation(thread='{thread_id}', format='md')"
    )


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
        agents = notion_client.get_all_workspace_agents(CFG.space_id, token, user_id)
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
    wf = notion_client.get_workflow_record(cfg["notion_internal_id"], token, user_id)
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
            ppe = event["pagePropertiesEdited"]
            filter_type = ppe.get("type", "all")
            filters = ppe.get("all", []) or ppe.get("some", [])
            filter_parts = []
            for f in filters:
                filt = f.get("filter", {})
                op = filt.get("operator", "?")
                vals = filt.get("value", [])
                val_names = [v.get("value", v.get("id", "?")) for v in vals if isinstance(v, dict)]
                prop_label = _prop_name(f["property"])
                if val_names:
                    filter_parts.append(f"{prop_label} {op} [{', '.join(val_names)}]")
                else:
                    filter_parts.append(f"{prop_label} ({op})")
            qualifier = f" [{filter_type}]" if filter_type != "all" else ""
            lines.append(f"   Trigger: pagePropertiesEdited{qualifier} — {'; '.join(filter_parts)}")
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
                val_parts = []
                for pid, vdef in config["values"].items():
                    pname = _prop_name(pid)
                    raw = vdef.get("value", {}).get("value", "")
                    if isinstance(raw, list) and raw:
                        # Notion rich-text style: [["text"]]
                        display = raw[0][0] if isinstance(raw[0], list) else str(raw[0])
                    else:
                        display = str(raw) if raw else "?"
                    val_parts.append(f"{pname} ← {display}")
                lines.append(f"       sets: {', '.join(val_parts)}")
            elif config:
                lines.append(f"       config: {json.dumps(config)[:120]}")
        lines.append("")

    return "\n".join(lines).strip()


def _get_notion_api_client() -> notion_api.NotionAPIClient:
    """Get a public Notion API client using CFG.notion_token."""
    return notion_api.NotionAPIClient(CFG.notion_token)


def _aggregate_pages(pages: list, schema: dict, show_props: list | None = None) -> str:
    """Compute per-column statistics from a list of Notion pages.

    Returns a markdown stats block. show_props filters which columns to include.
    """
    from collections import Counter

    n = len(pages)

    # Collect raw values per property
    # prop_data: {name: list of raw prop dicts}
    prop_data: dict[str, list] = {}
    for page in pages:
        for name, val in page.get("properties", {}).items():
            if show_props and name not in show_props:
                continue
            prop_data.setdefault(name, []).append(val)

    # Determine column order
    if show_props:
        col_order = [c for c in show_props if c in prop_data]
    else:
        # Sort, suppressing system props
        system_names = {nm for nm, pt in schema.items() if pt in _SYSTEM_PROP_TYPES}
        col_order = sorted(k for k in prop_data if k not in system_names)

    blocks: list[str] = [f"*{n} pages scanned*\n"]

    for name in col_order:
        vals = prop_data.get(name, [])
        ptype = schema.get(name, "")
        total = len(vals)

        if ptype in ("select", "status"):
            counter: Counter = Counter()
            for v in vals:
                raw = v.get(ptype) or {}
                label = raw.get("name") if isinstance(raw, dict) else None
                counter[label or "(empty)"] += 1
            non_empty = total - counter.get("(empty)", 0)
            lines = [f"## {name} ({ptype}) — {total} pages"]
            lines.append("| Value | Count |")
            lines.append("| --- | --- |")
            for label, cnt in counter.most_common():
                lines.append(f"| {label} | {cnt} |")
            blocks.append("\n".join(lines))

        elif ptype == "multi_select":
            counter = Counter()
            empty = 0
            for v in vals:
                opts = v.get("multi_select", [])
                if not opts:
                    empty += 1
                for opt in opts:
                    counter[opt.get("name", "?")] += 1
            lines = [f"## {name} (multi_select) — {total} pages, {empty} empty"]
            lines.append("| Value | Count |")
            lines.append("| --- | --- |")
            for label, cnt in counter.most_common(20):
                lines.append(f"| {label} | {cnt} |")
            blocks.append("\n".join(lines))

        elif ptype == "number":
            nums = [v.get("number") for v in vals if v.get("number") is not None]
            non_empty = len(nums)
            if non_empty == 0:
                blocks.append(f"## {name} (number) — 0 of {total} non-empty")
            else:
                mn = min(nums)
                mx = max(nums)
                mean = sum(nums) / non_empty
                blocks.append(
                    f"## {name} (number) — {non_empty} of {total} non-empty\n"
                    f"min: {mn} / mean: {mean:.4g} / max: {mx}"
                )

        elif ptype == "checkbox":
            true_count = sum(1 for v in vals if v.get("checkbox"))
            false_count = total - true_count
            blocks.append(
                f"## {name} (checkbox) — {total} pages\n"
                f"true: {true_count} / false: {false_count}"
            )

        elif ptype == "date":
            dates = []
            for v in vals:
                d = v.get("date")
                if d and d.get("start"):
                    dates.append(d["start"])
            non_empty = len(dates)
            if non_empty == 0:
                blocks.append(f"## {name} (date) — 0 of {total} non-empty")
            else:
                blocks.append(
                    f"## {name} (date) — {non_empty} of {total} non-empty\n"
                    f"earliest: {min(dates)} / latest: {max(dates)}"
                )

        elif ptype in ("relation", "people"):
            non_empty = sum(1 for v in vals if v.get(ptype))
            blocks.append(f"## {name} ({ptype}) — {non_empty} of {total} non-empty")

        elif ptype in (
            "title", "rich_text", "url", "email", "phone_number",
            "created_by", "last_edited_by", "created_time", "last_edited_time",
            "files", "formula", "rollup", "unique_id", "verification",
        ):
            non_empty = 0
            for v in vals:
                formatted = _format_property_value(v)
                if formatted.strip():
                    non_empty += 1
            blocks.append(f"## {name} ({ptype}) — {non_empty} of {total} non-empty")

        # skip unknown types silently

    return "\n\n".join(blocks)


def _get_db_schema(db_id: str) -> dict[str, str]:
    """Fetch and cache a database's property name -> type map via the public API."""
    now = time.monotonic()
    with _db_schema_lock:
        if db_id in _db_schema_cache and (now - _db_schema_cache_time.get(db_id, 0)) < _DB_SCHEMA_TTL:
            return _db_schema_cache[db_id]
    client = _get_notion_api_client()
    db = client.retrieve_database(db_id)
    schema = {
        name: prop_def["type"]
        for name, prop_def in db.get("properties", {}).items()
    }
    with _db_schema_lock:
        _db_schema_cache[db_id] = schema
        _db_schema_cache_time[db_id] = time.monotonic()
    return schema


# Filter type aliases: maps wrong-but-common filter keys to the correct one.
# e.g. caller uses "select" but the property is actually "status" type.
_FILTER_TYPE_ALIASES: dict[str, set[str]] = {
    "select": {"status", "select"},
    "status": {"status", "select"},
    "multi_select": {"multi_select"},
    "checkbox": {"checkbox"},
    "number": {"number"},
    "date": {"date", "created_time", "last_edited_time"},
    "rich_text": {"rich_text", "title", "url", "email", "phone_number"},
    "title": {"rich_text", "title"},
}


def _fix_filter(
    filter_obj: dict,
    schema: dict[str, str],
    name_map: dict[str, str] | None = None,
) -> dict:
    """Auto-correct a single filter condition to match the actual property type.

    Handles the common mistake of using {"property": "Status", "select": {...}}
    when the property is actually a "status" type (needs {"status": {...}}).
    Compound filters (and/or) are recursed into.
    name_map: optional {lower_name: canonical_name} for case-insensitive matching.
    """
    # Compound filter
    if "and" in filter_obj:
        filter_obj["and"] = [_fix_filter(f, schema, name_map) for f in filter_obj["and"]]
        return filter_obj
    if "or" in filter_obj:
        filter_obj["or"] = [_fix_filter(f, schema, name_map) for f in filter_obj["or"]]
        return filter_obj

    prop_name = filter_obj.get("property")
    # Case-insensitive name correction
    if prop_name and name_map:
        canonical = name_map.get(prop_name.lower())
        if canonical:
            filter_obj["property"] = canonical
            prop_name = canonical

    if not prop_name or prop_name not in schema:
        return filter_obj  # Can't fix what we don't know

    actual_type = schema[prop_name]

    # Find which key the caller used as the filter type
    # (everything except "property" is the filter type key)
    caller_keys = [k for k in filter_obj if k != "property"]
    if len(caller_keys) != 1:
        return filter_obj  # Ambiguous or empty, don't touch

    caller_type = caller_keys[0]
    if caller_type == actual_type:
        return filter_obj  # Already correct

    # Check if the caller's type is a known alias for the actual type
    aliases = _FILTER_TYPE_ALIASES.get(caller_type, set())
    if actual_type in aliases:
        # Swap the key: {"select": {"equals": "X"}} -> {"status": {"equals": "X"}}
        filter_obj[actual_type] = filter_obj.pop(caller_type)

    return filter_obj


def _resolve_relation_titles(
    page_ids: list[str],
    client: "notion_api.NotionAPIClient",
) -> dict[str, str]:
    """Batch-resolve page UUIDs to their title strings. Results are cached."""
    result: dict[str, str] = {}
    to_fetch: list[str] = []
    with _relation_title_lock:
        for pid in page_ids:
            if pid in _relation_title_cache:
                result[pid] = _relation_title_cache[pid]
            else:
                to_fetch.append(pid)

    for pid in to_fetch:
        try:
            page = client.retrieve_page(pid)
            props = page.get("properties", {})
            title = ""
            for prop_val in props.values():
                if prop_val.get("type") == "title":
                    title = "".join(t.get("plain_text", "") for t in prop_val.get("title", []))
                    break
            title = title.strip() or pid
        except Exception:
            title = pid  # degrade gracefully on access error
        result[pid] = title
        with _relation_title_lock:
            _relation_title_cache[pid] = title

    return result


def _format_property_value(prop: dict, client: "notion_api.NotionAPIClient | None" = None) -> str:
    """Extract a readable string from a Notion page property value.

    client: optional API client used to resolve relation page IDs to titles.
    """
    ptype = prop.get("type", "")
    if ptype == "title":
        return "".join(t.get("plain_text", "") for t in prop.get("title", []))
    if ptype == "rich_text":
        return "".join(t.get("plain_text", "") for t in prop.get("rich_text", []))
    if ptype == "select":
        sel = prop.get("select")
        return sel["name"] if sel else ""
    if ptype == "multi_select":
        return ", ".join(o["name"] for o in prop.get("multi_select", []))
    if ptype == "status":
        st = prop.get("status")
        return st["name"] if st else ""
    if ptype == "checkbox":
        return "Yes" if prop.get("checkbox") else "No"
    if ptype == "number":
        val = prop.get("number")
        return str(val) if val is not None else ""
    if ptype == "url":
        return prop.get("url") or ""
    if ptype == "email":
        return prop.get("email") or ""
    if ptype == "phone_number":
        return prop.get("phone_number") or ""
    if ptype == "date":
        d = prop.get("date")
        if not d:
            return ""
        start = d.get("start", "")
        end = d.get("end")
        return f"{start} → {end}" if end else start
    if ptype == "relation":
        rels = prop.get("relation", [])
        if not rels:
            return ""
        ids = [r.get("id", "") for r in rels if r.get("id")]
        if not ids:
            return ""
        if client:
            cap = 10
            fetch_ids = ids[:cap]
            titles = _resolve_relation_titles(fetch_ids, client)
            parts = [titles.get(i, i) for i in fetch_ids]
            if len(ids) > cap:
                parts.append(f"(+{len(ids) - cap} more)")
            return ", ".join(parts)
        return ", ".join(ids)
    if ptype == "people":
        people = prop.get("people", [])
        if not people:
            return ""
        names = []
        for p in people:
            names.append(p.get("name", p.get("id", "?")))
        return ", ".join(names)
    if ptype in ("created_time", "last_edited_time"):
        return prop.get(ptype, "")
    if ptype in ("created_by", "last_edited_by"):
        person = prop.get(ptype, {})
        return person.get("name", person.get("id", ""))
    if ptype == "files":
        files = prop.get("files", [])
        if not files:
            return ""
        return ", ".join(f.get("name", f.get("url", "?")) for f in files if isinstance(f, dict))
    if ptype == "formula":
        f = prop.get("formula", {})
        return str(f.get(f.get("type", ""), ""))
    if ptype == "rollup":
        r = prop.get("rollup", {})
        return str(r.get(r.get("type", ""), ""))
    if ptype == "unique_id":
        uid = prop.get("unique_id", {})
        prefix = uid.get("prefix", "")
        number = uid.get("number", "")
        return f"{prefix}-{number}" if prefix else str(number)
    if ptype == "verification":
        v = prop.get("verification", {})
        return v.get("state", "") if v else ""
    return str(prop)


@mcp.tool()
def describe_database(database_id: str) -> str:
    """
    Show a Notion database's schema: property names, types, and select/status options.

    Call this BEFORE query_database if you don't know the exact property names and
    types. The output tells you which filter type to use for each property.

    database_id: The database page UUID (dashed or dashless).
    """
    db_id = _to_dashed_uuid(database_id)
    client = _get_notion_api_client()
    db = client.retrieve_database(db_id)

    # Extract title
    title_parts = db.get("title", [])
    title = "".join(t.get("plain_text", "") for t in title_parts) or "(untitled)"

    lines = [f"# {title}", f"ID: {db_id}", ""]
    lines.append("| Property | Type | Filter key | Options |")
    lines.append("| --- | --- | --- | --- |")

    for name, prop_def in sorted(db.get("properties", {}).items()):
        ptype = prop_def["type"]
        # The filter key is the same as the type for most properties
        filter_key = ptype
        options = ""

        if ptype == "select":
            opts = prop_def.get("select", {}).get("options", [])
            options = ", ".join(o["name"] for o in opts[:10])
            if len(opts) > 10:
                options += f" (+{len(opts) - 10} more)"
        elif ptype == "status":
            groups = prop_def.get("status", {}).get("groups", [])
            all_opts = []
            for g in groups:
                all_opts.extend(o["name"] for o in g.get("options", []))
            options = ", ".join(all_opts[:10])
        elif ptype == "multi_select":
            opts = prop_def.get("multi_select", {}).get("options", [])
            options = ", ".join(o["name"] for o in opts[:10])
            if len(opts) > 10:
                options += f" (+{len(opts) - 10} more)"

        lines.append(f"| {name} | {ptype} | `{filter_key}` | {options} |")

    # Cache the schema with timestamp
    schema = {name: prop_def["type"] for name, prop_def in db.get("properties", {}).items()}
    with _db_schema_lock:
        _db_schema_cache[db_id] = schema
        _db_schema_cache_time[db_id] = time.monotonic()

    return "\n".join(lines)


@mcp.tool()
def query_database(
    database_id: str,
    filter: str = "",
    sorts: str = "",
    properties: str = "",
    limit: int = 50,
    cursor: str = "",
    aggregate: bool = False,
    max_tokens: int = 0,
    sample: bool = False,
) -> str:
    """
    Query a Notion database by ID and return rows as a formatted table.

    Auto-corrects common filter mistakes (e.g. using "select" filter for a "status"
    property). Property name matching is case-insensitive. System audit columns
    (created_by, created_time, etc.) are hidden by default — list them explicitly
    in 'properties' to show them.

    database_id: The database page UUID (dashed or dashless), NOT the collection://
        data source ID.
    filter: Optional JSON string with a Notion API filter object. The filter type
        key MUST match the property's actual type. Use describe_database to check.
        Example: '{"property": "Status", "status": {"equals": "Active"}}'
    sorts: Optional JSON string with a Notion API sorts array. Property names must
        match the database schema (case-insensitive).
        Example: '[{"property": "Name", "direction": "ascending"}]'
    properties: Comma-separated property names to display. If empty, shows all
        non-system properties. Case-insensitive.
    limit: Max rows to return (default 50, max 100). Ignored when aggregate=True.
    cursor: Pagination cursor from a previous query_database call with more results.
        Ignored when aggregate=True.
    aggregate: If True, fetch up to 200 pages and return per-column statistics
        (value frequencies, numeric ranges, non-empty counts) instead of a row
        table. ~90% token savings vs loading all rows. Use for "what values exist?"
        and "how is data distributed?" questions.
    max_tokens: If > 0, cap output size to approximately this many tokens
        (1 token ≈ 4 chars). Rows are dropped from the end if over budget, with a
        note appended. Has no effect when aggregate=True.
    sample: If True and rows > 10, show first half + last half of results with an
        omission separator, revealing data range without dense middle rows. Has no
        effect when aggregate=True.
    """
    client = _get_notion_api_client()
    db_id = _to_dashed_uuid(database_id)

    filter_obj = json.loads(filter) if filter else None
    sorts_obj = json.loads(sorts) if sorts else None

    # Fetch schema for validation, auto-correction, and case-insensitive matching
    try:
        schema = _get_db_schema(db_id)
    except Exception:
        schema = {}

    # Build case-insensitive name map: lower -> canonical
    prop_name_map = {n.lower(): n for n in schema} if schema else {}

    # Auto-correct filter types and property names against actual schema
    if filter_obj and schema:
        filter_obj = _fix_filter(filter_obj, schema, prop_name_map)

    # Validate and case-correct sort property names
    if sorts_obj and schema:
        for sort_item in sorts_obj:
            prop = sort_item.get("property", "")
            if prop:
                canonical = prop_name_map.get(prop.lower())
                if canonical:
                    sort_item["property"] = canonical
                elif prop not in schema:
                    available = ", ".join(sorted(schema.keys()))
                    return (
                        f"Sort property \"{prop}\" not found in database.\n"
                        f"Available properties: {available}"
                    )

    # Normalize show_props to canonical casing
    show_props: list[str] | None = None
    if properties:
        raw = [p.strip() for p in properties.split(",") if p.strip()]
        show_props = [prop_name_map.get(p.lower(), p) for p in raw]

    # --- Aggregate mode: fetch up to 200 pages, return column statistics ---
    if aggregate:
        _AGG_CAP = 200
        pages = client.query_all(db_id, filter_payload=filter_obj, page_size=100)
        if len(pages) > _AGG_CAP:
            pages = pages[:_AGG_CAP]
        if not pages:
            return "No results."
        return _aggregate_pages(pages, schema, show_props)

    # --- Standard row-table mode ---
    payload: dict = {"page_size": min(limit, 100)}
    if filter_obj:
        payload["filter"] = filter_obj
    if sorts_obj:
        payload["sorts"] = sorts_obj
    if cursor:
        payload["start_cursor"] = cursor.strip()

    result = client._request("POST", f"databases/{db_id}/query", payload)
    pages = result.get("results", [])

    if not pages:
        return "No results."

    # Determine which system columns to suppress when no explicit properties given
    system_names: set[str] = set()
    if not show_props and schema:
        system_names = {name for name, ptype in schema.items() if ptype in _SYSTEM_PROP_TYPES}

    # Build table rows — always include page URL for actionability
    rows = []
    for page in pages:
        props = page.get("properties", {})
        row = {"_url": page.get("url", page.get("id", ""))}
        for name, val in props.items():
            if show_props and name not in show_props:
                continue
            if not show_props and name in system_names:
                continue
            row[name] = _format_property_value(val, client=client)
        rows.append(row)

    if not rows:
        return f"{len(pages)} pages returned but no displayable properties."

    # Get column order: always lead with _url, then requested or all props
    if show_props:
        columns = ["_url"] + [c for c in show_props if c in rows[0]]
    else:
        all_cols = [c for c in rows[0].keys() if c != "_url"]
        columns = ["_url"] + sorted(all_cols)

    # --- Semantic sampling: head + tail with omission row ---
    if sample and len(rows) > 10:
        import math
        head_n = math.ceil(len(rows) / 2)
        tail_n = len(rows) - head_n
        omitted = len(rows) - head_n - tail_n
        omission_row = {c: "..." for c in columns}
        omission_row[columns[0]] = f"*({omitted} rows omitted)*"
        rows = rows[:head_n] + [omission_row] + rows[len(rows) - tail_n:]

    # Format as markdown table
    header = "| " + " | ".join(columns) + " |"
    separator = "| " + " | ".join("---" for _ in columns) + " |"
    lines = [header, separator]
    max_cell = 300
    for row in rows:
        cells = []
        for c in columns:
            val = row.get(c, "").replace("|", "\\|").replace("\n", " ")
            if len(val) > max_cell:
                val = val[: max_cell - 1] + "…"
            cells.append(val)
        lines.append("| " + " | ".join(cells) + " |")

    total_note = ""
    if result.get("has_more") and result.get("next_cursor"):
        next_cur = result["next_cursor"]
        total_note = (
            f"\n\n_Showing {len(pages)} rows. More results exist._\n"
            f'_Next page: call query\\_database with cursor="{next_cur}"_'
        )

    output = "\n".join(lines) + total_note

    # --- Token budget enforcement ---
    if max_tokens > 0:
        budget_chars = max_tokens * 4
        if len(output) > budget_chars:
            # Binary-search: drop rows from the end until we fit
            lo, hi = 0, len(rows)
            while lo < hi:
                mid = (lo + hi + 1) // 2
                candidate_lines = [header, separator]
                for row in rows[:mid]:
                    cells = []
                    for c in columns:
                        val = row.get(c, "").replace("|", "\\|").replace("\n", " ")
                        if len(val) > max_cell:
                            val = val[: max_cell - 1] + "…"
                        cells.append(val)
                    candidate_lines.append("| " + " | ".join(cells) + " |")
                if len("\n".join(candidate_lines)) <= budget_chars:
                    lo = mid
                else:
                    hi = mid - 1
            shown = lo
            truncated_lines = [header, separator]
            for row in rows[:shown]:
                cells = []
                for c in columns:
                    val = row.get(c, "").replace("|", "\\|").replace("\n", " ")
                    if len(val) > max_cell:
                        val = val[: max_cell - 1] + "…"
                    cells.append(val)
                truncated_lines.append("| " + " | ".join(cells) + " |")
            budget_note = (
                f"\n\n> Showing {shown} of {len(rows)} rows "
                f"(token budget: {max_tokens} tokens — "
                f"use aggregate=True or narrow with properties= for full coverage)"
            )
            output = "\n".join(truncated_lines) + budget_note

    return output


@mcp.tool()
def count_database(
    database_id: str,
    filter: str = "",
    exact: bool = False,
) -> str:
    """
    Count rows in a Notion database, optionally matching a filter.

    Use this for "how many?" and "does X exist?" questions instead of fetching
    rows with query_database.

    database_id: The database page UUID (dashed or dashless).
    filter: Optional JSON filter (same format as query_database).
    exact: If False (default), returns a fast existence check using 1 API call —
        answers "0 rows", "1 row", or "at least 2 rows". If True, pages through
        the full database to return an exact count (may make multiple API calls).
    """
    client = _get_notion_api_client()
    db_id = _to_dashed_uuid(database_id)

    filter_obj = json.loads(filter) if filter else None

    # Apply same schema validation and auto-correction as query_database
    try:
        schema = _get_db_schema(db_id)
        prop_name_map = {n.lower(): n for n in schema}
    except Exception:
        schema = {}
        prop_name_map = {}

    if filter_obj and schema:
        filter_obj = _fix_filter(filter_obj, schema, prop_name_map)

    if not exact:
        # Fast path: 1 API call, answers existence only
        result = client.query_database(db_id, filter_payload=filter_obj, page_size=1)
        count = len(result.get("results", []))
        if count == 0:
            return "0 rows match."
        if result.get("has_more"):
            return "At least 2 rows match. Use exact=True for a precise count."
        return "1 row matches."

    # Exact count: page through the full result set
    pages = client.query_all(db_id, filter_payload=filter_obj)
    n = len(pages)
    return f"{n} {'row' if n == 1 else 'rows'} match."


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
    result = notion_client.get_agent_modules(cfg["notion_internal_id"], token, user_id)

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
    wf = notion_client.get_workflow_record(cfg["notion_internal_id"], token, user_id)
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
        cfg["notion_internal_id"], cfg["space_id"], modules, token, user_id,
    )

    msg = f"Added MCP server '{server_name}' ({server_url}) to {agent_name}."
    if publish:
        result = notion_client.publish_agent(
            cfg["notion_internal_id"], cfg["space_id"], token, user_id,
        )
        msg += f" {_build_publish_message(agent_name, result)}"
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
    wf = notion_client.get_workflow_record(cfg["notion_internal_id"], token, user_id)
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
        cfg["notion_internal_id"], cfg["space_id"], modules, token, user_id,
    )

    msg = f"Removed MCP server '{server_name}' from {agent_name}."
    if publish:
        result = notion_client.publish_agent(
            cfg["notion_internal_id"], cfg["space_id"], token, user_id,
        )
        msg += f" {_build_publish_message(agent_name, result)}"
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
        "chatgpt": "oval-kumquat-medium",
        "chatgpt 5.4": "oval-kumquat-medium",
        "gpt 5.4": "oval-kumquat-medium",
        "auto": "auto",
    }
    model_type = aliases.get(model.lower().strip(), model.strip())

    cfg = _get_agent_config(agent_name)
    token, user_id = _get_auth()
    notion_client.update_agent_model(
        cfg["notion_internal_id"], cfg["space_id"], model_type, token, user_id,
    )
    display = notion_client.MODEL_NAMES.get(model_type, model_type)
    msg = f"Set {agent_name} model to {display} ({model_type})."

    if publish:
        result = notion_client.publish_agent(
            cfg["notion_internal_id"], cfg["space_id"], token, user_id,
        )
        msg += f" {_build_publish_message(agent_name, result)}"
    return msg


@mcp.tool()
@auth_retry
def create_agent(
    name: str,
    icon: str | None = None,
    space_id: str | None = None,
    register: bool = True,
) -> str:
    """
    Create a new Notion AI Agent from scratch.

    name: Display name for the agent.
    icon: Optional URL to an icon image.
    space_id: Target Notion space ID. If omitted, auto-discovers if the user has only one space.
    register: If True, adds the agent to agents.yaml automatically.
    """
    token, user_id = _get_auth()

    if not space_id:
        spaces = notion_client.get_user_spaces(token)
        if len(spaces) == 1:
            space_id = spaces[0]["id"]
        elif not spaces:
            raise ValueError("No Notion spaces found for this user.")
        else:
            space_list = ", ".join(f"{s['name']} ({s['id']})" for s in spaces)
            raise ValueError(f"Multiple spaces found. Provide space_id. Available: {space_list}")

    result = notion_client.create_agent(space_id, name, icon, token, user_id)
    wf_id = result["notion_internal_id"]
    notion_public_id = result["notion_public_id"]

    # Always add to sidebar
    notion_client.add_agent_to_sidebar(space_id, wf_id, token, user_id)

    # Initial publish (v1)
    notion_client.publish_agent(wf_id, space_id, token, user_id, archive_existing=False)

    msg = f"Created agent '{name}' (workflow: {wf_id}, instructions: {notion_public_id})."

    if register:
        key = _name_to_key(name)
        registry = _load_registry()
        registry[key] = {
            "notion_internal_id": wf_id,
            "space_id": space_id,
            "notion_public_id": notion_public_id,
            "label": name,
        }
        _save_registry(registry)
        msg += f" Registered as '{key}' in agents.yaml."

    return msg


@mcp.tool()
@auth_retry
def get_agent_config_raw(agent_name: str) -> str:
    """
    Fetch the raw workflow record for an agent.
    Useful for cloning tool/module configurations.
    """
    cfg = _get_agent_config(agent_name)
    token, user_id = _get_auth()
    wf = notion_client.get_workflow_record(cfg["notion_internal_id"], token, user_id)
    return json.dumps(wf, indent=2, ensure_ascii=False)


@mcp.tool()
@auth_retry
def set_agent_modules(
    agent_name: str,
    modules_json: str,
    publish: bool = True,
) -> str:
    """
    Update an agent's modules (tools and permissions) in bulk.

    agent_name: Registered agent name.
    modules_json: JSON string containing the modules array (from get_agent_config_raw).
    publish: Whether to publish immediately.
    """
    modules = json.loads(modules_json)
    if not isinstance(modules, list):
        raise ValueError("modules_json must be a list of modules.")

    cfg = _get_agent_config(agent_name)
    token, user_id = _get_auth()

    notion_client.update_agent_modules(
        cfg["notion_internal_id"], cfg["space_id"], modules, token, user_id,
    )

    msg = f"Bulk-updated modules for {agent_name}."
    if publish:
        result = notion_client.publish_agent(
            cfg["notion_internal_id"], cfg["space_id"], token, user_id,
        )
        msg += f" {_build_publish_message(agent_name, result)}"
    return msg


@mcp.tool()
@auth_retry
def set_agent_config_raw(
    agent_name: str,
    config_json: str,
    publish: bool = True,
    freshen_triggers: bool = True,
) -> str:
    """
    Update an agent's core configuration (modules, triggers, metadata) in bulk.

    agent_name: Registered agent name.
    config_json: JSON string containing the 'data' payload (from get_agent_config_raw).
    publish: Whether to publish immediately.
    freshen_triggers: If True (default), generate new random IDs for triggers.
                      Highly recommended when cloning from another agent to avoid conflicts.
    """
    new_data = json.loads(config_json)
    if not isinstance(new_data, dict):
        raise ValueError("config_json must be a dictionary.")

    # If the user passed the full workflow record, extract the .data part
    if "data" in new_data and "id" in new_data:
        new_data = new_data["data"]

    if freshen_triggers and "triggers" in new_data:
        for t in new_data["triggers"]:
            t["id"] = str(uuid.uuid4())
            if t.get("state", {}).get("type") == "recurrence":
                t["state"]["scheduleId"] = str(uuid.uuid4())

    cfg = _get_agent_config(agent_name)
    token, user_id = _get_auth()

    ops = [{
        "pointer": {"table": "workflow", "id": cfg["notion_internal_id"], "spaceId": cfg["space_id"]},
        "path": ["data"],
        "command": "update",
        "args": new_data
    }]
    notion_client.send_ops(cfg["space_id"], ops, token, user_id)

    msg = f"Bulk-updated configuration for {agent_name}."
    if publish:
        result = notion_client.publish_agent(
            cfg["notion_internal_id"], cfg["space_id"], token, user_id,
        )
        msg += f" {_build_publish_message(agent_name, result)}"
    return msg


@mcp.tool()
@auth_retry
def grant_resource_access(
    agent_name: str,
    notion_public_id: str,
    role: str = "editor",
) -> str:
    """
    Authoritatively grant an agent access to a Notion page or database.
    Performs the full handshake required for backend provisioning.

    agent_name: Registered agent name.
    notion_public_id: The UUID of the page or database.
    role: 'editor' (read/write) or 'reader' (read-only).
    """
    cfg = _get_agent_config(agent_name)
    token, user_id = _get_auth()

    result = notion_client.grant_agent_resource_access(
        cfg["notion_internal_id"], cfg["space_id"], notion_public_id, role,
        token, user_id,
    )

    msg = f"Granted {role} access to {notion_public_id} for {agent_name}."
    msg += f" {_build_publish_message(agent_name, result)}"
    return msg


# ── Dispatch tools ───────────────────────────────────────────────────────────


@mcp.tool()
def get_dispatchable_items() -> str:
    """
    Find Work Items ready for dispatch by an execution plane.

    Returns items where Dispatch Requested Received At is set, Dispatch Requested
    Consumed At is empty, and Status is Not Started or Prompt Requested.
    """
    client = _get_notion_api_client()
    items = dispatch.get_dispatchable_items(client)

    if not items:
        return "No dispatchable items found."

    lines = [f"**{len(items)} dispatchable item(s):**\n"]
    lines.append("| Item Name | Dispatch Via | Lane | Environment | Branch | Project | Type |")
    lines.append("| --- | --- | --- | --- | --- | --- | --- |")
    for item in items:
        lines.append(
            f"| [{item['name']}](https://www.notion.so/{item['id'].replace('-', '')}) "
            f"| {item.get('dispatch_via') or '—'} "
            f"| {item.get('execution_lane') or '—'} "
            f"| {item.get('environment') or '—'} "
            f"| {item.get('branch') or '—'} "
            f"| {item.get('project_name') or '—'} "
            f"| {item.get('type') or '—'} |"
        )

    return "\n".join(lines)


@mcp.tool()
def build_dispatch_packet(work_item_id: str) -> str:
    """
    Build and validate a dispatch packet for a Work Item.

    Reads the Work Item, resolves inheritance from its Project, applies the
    Dispatch Via -> Execution Lane default mapping, and runs V1-V12 validation.

    work_item_id: UUID of the Work Item page.

    Returns a validated dispatch packet (JSON) ready for an execution plane,
    or a list of validation errors if the item is not dispatchable.
    """
    client = _get_notion_api_client()
    result = dispatch.build_dispatch_packet(work_item_id, client)

    if result["errors"]:
        error_list = "\n".join(f"- {e}" for e in result["errors"])
        return f"**Validation failed** ({len(result['errors'])} error(s)):\n\n{error_list}"

    packet = result["packet"]
    audit_note = ""
    if result.get("_production_audit"):
        audit_note = "\n\n**Note:** Production environment — elevated access logged."

    return f"**Dispatch packet built successfully.**\n\n```json\n{json.dumps(packet, indent=2)}\n```{audit_note}"


@mcp.tool()
def stamp_dispatch_consumed(work_item_id: str, run_id: str) -> str:
    """
    Mark a Work Item as consumed by an execution plane.

    Sets Dispatch Requested Consumed At=now(), Status=In Progress, and writes
    the run_id for idempotency tracking.
    Also creates an audit log entry.

    work_item_id: UUID of the Work Item page.
    run_id: The run_id from the dispatch packet (from build_dispatch_packet).
    """
    client = _get_notion_api_client()
    result = dispatch.stamp_dispatch_consumed(work_item_id, run_id, client)

    return (
        f"**Dispatch consumed.**\n\n"
        f"- Work Item: `{result['work_item_id']}`\n"
        f"- Run ID: `{result['run_id']}`\n"
        f"- Consumed At: `{result['consumed_at']}`\n"
        f"- Status: In Progress"
    )


def main():
    transport = sys.argv[1] if len(sys.argv) > 1 else "stdio"
    mcp.run(transport=transport)


if __name__ == "__main__":
    main()
