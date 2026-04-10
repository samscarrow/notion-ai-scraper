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
import notion_api
import notion_client
import config
import lab_topology
from utils import _to_dashed_uuid, _name_to_key

# Use config instance
CFG = config.get_config()

AGENTS_YAML = os.path.join(os.path.dirname(os.path.abspath(__file__)), "agents.yaml")
_SAFE_TOOL_WAIT_SECONDS = 90


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

_auth_cache: tuple[str, str | None] | None = None
_auth_cache_time: float = 0
_auth_db_mtime: float = 0   # mtime of Firefox cookies.sqlite at last read
_auth_lock = threading.Lock()
_AUTH_TTL = 300  # seconds — re-read cookies every 5 minutes
_TOKEN_FILE = os.path.expanduser("~/.notion-token-v2")
_KEEPALIVE_INTERVAL = 3600  # seconds — ping Notion hourly to keep session alive
_AUTH_SOURCE: str = "none"  # tracks where the current token came from

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


def _resolve_space_id(token: str, user_id: str | None) -> str:
    """Resolve the active workspace the same way live topology compilation does."""
    registry = _load_registry()
    for cfg in registry.values():
        if cfg.get("space_id"):
            return cfg["space_id"]

    spaces = notion_client.get_user_spaces(token)
    if len(spaces) == 1:
        return spaces[0]["id"]

    raise ValueError("Unable to determine Notion space ID for workspace agent operations.")


TEMPLATE_DATA_JSON = os.path.expanduser(
    "~/projects/agent-env/home/.agents/template-data.json"
)


def _sync_template_data(registry: dict) -> str | None:
    """Sync the agents section of agent-env's template-data.json from the registry.

    Additive-only: adds new agents, never removes existing ones.
    Returns a status message or None if no changes.
    """
    if not os.path.exists(TEMPLATE_DATA_JSON):
        return None

    try:
        with open(TEMPLATE_DATA_JSON, "r") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return None

    agents_section = data.setdefault("agents", {})
    added = 0

    for key, cfg in registry.items():
        if key not in agents_section:
            agents_section[key] = {
                "label": cfg.get("label", key),
                "key": key,
                "notion_internal_id": cfg.get("notion_internal_id", ""),
                "notion_public_id": cfg.get("notion_public_id", ""),
                "role": "Active Agent",
            }
            added += 1

    if added == 0:
        return None

    with open(TEMPLATE_DATA_JSON, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.write("\n")

    return f"Synced {added} new agent(s) to template-data.json."


def _read_token_file() -> tuple[str, str | None] | None:
    """Read token_v2 (and optional user_id) from ~/.notion-token-v2.

    File format: first line is token_v2, optional second line is user_id.
    """
    try:
        with open(_TOKEN_FILE) as f:
            lines = f.read().strip().splitlines()
        if not lines or not lines[0].strip():
            return None
        token = lines[0].strip()
        user_id = lines[1].strip() if len(lines) > 1 and lines[1].strip() else None
        return token, user_id
    except (OSError, IndexError):
        return None


def _write_token_file(token: str, user_id: str | None) -> None:
    """Persist token_v2 (and user_id) to ~/.notion-token-v2 for reuse across restarts."""
    try:
        content = token
        if user_id:
            content += f"\n{user_id}"
        content += "\n"
        fd = os.open(_TOKEN_FILE, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        try:
            os.write(fd, content.encode())
        finally:
            os.close(fd)
    except OSError:
        pass  # best-effort


def _get_auth(force: bool = False) -> tuple[str, str | None]:
    """Return (token_v2, user_id) with cascading sources and caching.

    Auth cascade (first match wins):
    1. NOTION_TOKEN_V2 env var (+ optional NOTION_USER_ID)
    2. ~/.notion-token-v2 file
    3. Firefox cookies.sqlite

    On successful Firefox extraction, persists to token file for next time.
    Re-reads on force=True, TTL expiry, or Firefox DB mtime change.
    """
    global _auth_cache, _auth_cache_time, _auth_db_mtime, _AUTH_SOURCE
    now = time.monotonic()
    with _auth_lock:
        if not force and _auth_cache is not None:
            # For env var source, trust the cache (env doesn't change)
            if _AUTH_SOURCE == "env":
                return _auth_cache
            # For file/firefox, check staleness
            ttl_valid = (now - _auth_cache_time) < _AUTH_TTL
            if _AUTH_SOURCE == "file" and ttl_valid:
                return _auth_cache
            if _AUTH_SOURCE == "firefox":
                try:
                    db_path = cookie_extract.get_firefox_cookies_db()
                    current_mtime = os.path.getmtime(db_path)
                except Exception:
                    current_mtime = _auth_db_mtime
                if current_mtime == _auth_db_mtime and ttl_valid:
                    return _auth_cache

        # Source 1: environment variable
        env_token = os.environ.get("NOTION_TOKEN_V2")
        if env_token:
            _auth_cache = (env_token, os.environ.get("NOTION_USER_ID"))
            _auth_cache_time = now
            _AUTH_SOURCE = "env"
            return _auth_cache

        # Source 2: token file
        file_auth = _read_token_file()
        if file_auth and not force:
            _auth_cache = file_auth
            _auth_cache_time = now
            _AUTH_SOURCE = "file"
            return _auth_cache

        # Source 3: Firefox cookies
        try:
            _auth_cache = cookie_extract.get_auth()
            _auth_cache_time = now
            _AUTH_SOURCE = "firefox"
            try:
                db_path = cookie_extract.get_firefox_cookies_db()
                _auth_db_mtime = os.path.getmtime(db_path)
            except Exception:
                pass
            # Persist to file for next time
            _write_token_file(_auth_cache[0], _auth_cache[1])
            return _auth_cache
        except (ValueError, FileNotFoundError):
            # Firefox extraction failed — retry token file even on force
            if file_auth:
                _auth_cache = file_auth
                _auth_cache_time = now
                _AUTH_SOURCE = "file"
                return _auth_cache
            raise


def _invalidate_auth() -> None:
    """Force next _get_auth() call to re-read from sources."""
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
                    f"Notion auth failed after retry. Sources tried: "
                    f"NOTION_TOKEN_V2 env, {_TOKEN_FILE}, Firefox cookies. "
                    f"Set NOTION_TOKEN_V2 or refresh Firefox session. ({e})"
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

    # Auto-grant access for any {{page:uuid}} mentions in instructions
    newly_granted = notion_client.ensure_mention_access(
        cfg["notion_internal_id"], cfg["space_id"],
        instructions_markdown, token, user_id,
    )
    if newly_granted:
        count = len(newly_granted)
        noun = "page" if count == 1 else "pages"
        msg += f" Auto-granted access to {count} mentioned {noun}."

    if publish:
        # Pre-publish validation: check for any remaining unresolvable mentions
        still_missing = notion_client.check_mention_access(
            cfg["notion_internal_id"], instructions_markdown, token, user_id,
        )
        if still_missing:
            msg += (
                f" WARNING: {len(still_missing)} mentioned page(s) still lack access"
                f" grants: {', '.join(still_missing)}. Publish may fail with"
                f" incomplete_ancestor_path."
            )

        result = notion_client.publish_agent(
            cfg["notion_internal_id"], cfg["space_id"], token, user_id,
        )
        msg += f" {_build_publish_message(agent_name, result)}"

    return msg


# ── Tools ────────────────────────────────────────────────────────────────────

@mcp.tool()
@auth_retry
def list_agents(live: bool = False) -> str:
    """
    List Notion AI agents.

    live: If False (default), reads from agents.yaml registry.
          If True, queries the Notion workspace directly for all agents
          (does not require agents.yaml, returns full UUIDs).
    """
    if live:
        token, user_id = _get_auth()
        space_id = _resolve_space_id(token, user_id)
        agents = notion_client.get_all_workspace_agents(space_id, token, user_id)
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

    registry = _load_registry()
    if not registry:
        return "No agents registered. Use sync_registry to auto-populate from Notion."
    lines = []
    for name, raw_cfg in registry.items():
        wid = raw_cfg.get("notion_internal_id", "?")
        lines.append(f"- {name}: workflow={wid}")
    return "\n".join(lines)


@mcp.tool()
@auth_retry
def sync_registry() -> str:
    """
    Sync agents.yaml with all agents currently in the Notion workspace.
    Adds new agents. Never removes existing entries (safe to run anytime).
    Returns a summary of what was added vs. already present.
    """
    token, user_id = _get_auth()
    space_id = _resolve_space_id(token, user_id)
    agents = notion_client.get_all_workspace_agents(space_id, token, user_id)
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

    # Also sync to agent-env template-data.json so generate.py can rebuild SKILL.md
    resources_synced = _sync_template_data(registry)

    lines = [f"Workspace has {len(agents)} agents."]
    if added:
        lines.append(f"Added ({len(added)}):")
        lines.extend(added)
    if skipped:
        lines.append(f"Already registered ({len(skipped)}):")
        lines.extend(skipped)
    if resources_synced:
        lines.append(resources_synced)
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
    instructions_markdown: str | None = None,
    publish: bool = True,
) -> str:
    """
    Replace a Notion AI agent's instructions with new Markdown content,
    then publish. Mentions use {{page:uuid}} syntax.

    If instructions_markdown is None (or omitted), the agent's existing
    instructions are left unchanged and only a publish is performed.
    """
    if instructions_markdown is None:
        cfg = _get_agent_config(agent_name)
        token, user_id = _get_auth()
        result = notion_client.publish_agent(
            cfg["notion_internal_id"], cfg["space_id"], token, user_id,
        )
        return _build_publish_message(agent_name, result, standalone=True)
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
def manage_registry(
    action: str,
    name: str,
    notion_internal_id: str = "",
    space_id: str = "",
    notion_public_id: str = "",
    label: str = "",
) -> str:
    """
    Add or remove a Notion AI agent from agents.yaml.

    action: "register" — add an agent (requires notion_internal_id, space_id, notion_public_id).
            "remove"   — remove an agent by name (only name is required).

    Use discover_agent first to find the three UUIDs needed for register.
    """
    if action == "register":
        if not (notion_internal_id and space_id and notion_public_id):
            raise ValueError("register requires notion_internal_id, space_id, and notion_public_id.")
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

    if action == "remove":
        registry = _load_registry()
        if name not in registry:
            available = ", ".join(sorted(registry.keys())) or "(none)"
            return f"Agent '{name}' not found. Available: {available}"
        del registry[name]
        _save_registry(registry)
        return f"Removed agent '{name}' from registry."

    raise ValueError(f"Unknown action '{action}'. Use 'register' or 'remove'.")


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
def get_conversation(thread: str, format: str = "json",
                     since_msg_id: str | None = None,
                     space_id: str | None = None) -> str:
    """
    Fetch a Notion AI conversation and return its full transcript.

    thread: Any of:
      - Thread UUID (dashed: 318e7cc7-01d5-... or 32-char dashless)
      - Notion URL containing ?t= or ?at= parameter
      - Title text — searches the workspace and returns the unique match

    format: "json" (default) — full structured data (turns, toolCalls, model, timestamps)
            "md"            — Markdown transcript

    since_msg_id: If provided, only returns turns that appear AFTER this message ID
                  in the conversation. Useful for polling for new turns without
                  re-reading the full transcript.

    Returns all conversation turns with role, content, thinking (CoT), model,
    and any tool calls (input + result) attached to each turn.
    If multiple conversations match a title search, lists candidates and asks
    you to re-call with a specific UUID.
    """
    if format not in ("json", "md"):
        raise ValueError(f"format must be 'json' or 'md', got '{format}'")

    token, user_id = _get_auth()
    thread_id = _resolve_thread_id(thread, token, user_id)
    convo = notion_client.get_thread_conversation(
        thread_id, token, user_id, space_id=space_id or CFG.space_id
    )

    if since_msg_id:
        turns = convo.get("turns") or []
        after = False
        filtered = []
        for turn in turns:
            if after:
                filtered.append(turn)
            elif (turn.get("msgId") or turn.get("id")) == since_msg_id:
                after = True
        convo = {**convo, "turns": filtered}

    if format == "md":
        return _conversation_to_markdown(convo)
    return json.dumps(convo, indent=2, ensure_ascii=False)


def _build_agent_tracking_payload(thread_id: str, msg_id: str) -> dict:
    return {
        "check_agent_response": {
            "thread_id": thread_id,
            "after_msg_id": msg_id,
            "space_id": CFG.space_id,
        },
        "get_conversation": {
            "thread": thread_id,
            "since_msg_id": msg_id,
            "space_id": CFG.space_id,
        },
    }


def _compute_effective_wait_timeout(timeout: int) -> int:
    """Cap blocking waits so the MCP transport can still return a payload."""
    if timeout <= 0:
        return 0
    return min(timeout, _SAFE_TOOL_WAIT_SECONDS)


@mcp.tool()
@auth_retry
def check_agent_response(thread_id: str, after_msg_id: str,
                         format: str = "text",
                         space_id: str | None = None,
                         timeout: int = 0) -> str:
    """
    Non-blocking check for an agent response after a sent message.

    Makes a single conversation fetch and returns the latest assistant turn
    that appears after after_msg_id, including partial content while inference
    is still pending.

    thread_id: UUID of the thread (returned by chat_with_agent).
    after_msg_id: The message ID of the user turn (returned by chat_with_agent).
    format: "text" (default) — plain response content
            "json" — full turn object(s) as JSON
    timeout: If > 0, poll internally for up to this many seconds before returning.
             Eliminates the need for the caller to loop with sleep. Default: 0 (single check).

    Returns one of:
      - {"status": "pending", "content": "...|null"} — inference still running
      - {"status": "complete", "content": "...", "turns": [...]} — response ready

    For multi-step agents, content is the final assistant turn text.
    """
    if format not in ("text", "json"):
        raise ValueError(f"format must be 'text' or 'json', got '{format}'")

    token, user_id = _get_auth()
    sid = space_id or CFG.space_id

    if timeout > 0:
        state = notion_client.wait_for_agent_response_state(
            thread_id, after_msg_id, token, user_id,
            timeout=timeout, space_id=sid,
        )
    else:
        state = notion_client.get_agent_response_state(
            thread_id, after_msg_id, token, user_id, space_id=sid,
        )

    result = {
        "status": state["status"],
        "thread_id": thread_id,
        "after_msg_id": after_msg_id,
        "content": state["content"],
    }
    if format == "json":
        result["turns"] = state["turns"]
    return json.dumps(result, ensure_ascii=False)


@mcp.tool()
@auth_retry
def chat_with_agent(agent_name: str, message: str, thread_id: str | None = None,
                    new_thread: bool = False, wait: bool = False,
                    timeout: int = 600) -> str:
    """
    Send a message to a Notion AI agent and trigger a response.

    agent_name: Registered agent name from agents.yaml.
    message: The content of the message to send.
    thread_id: Optional UUID of an existing thread to continue.
    new_thread: If true, always create a fresh thread (ignores thread_id).
    wait: If true, poll briefly and return either completion or a tracked pending state.
    timeout: Requested max wait in seconds (default 600). Blocking wait is capped to
             a transport-safe budget so the MCP call can still return a payload.

    If thread_id is omitted and new_thread is false, continues the most recent
    existing thread or creates one if none exist.

    Returns a JSON status envelope containing thread/message IDs, content, and
    tracking handles for follow-up polling.
    """
    registry = _load_registry()
    cfg = registry.get(agent_name)
    if not cfg:
        raise ValueError(f"Agent '{agent_name}' not found in registry.")

    token, user_id = _get_auth()
    created_new = False

    if new_thread:
        thread_id = notion_client.create_workflow_thread(
            cfg['notion_internal_id'], cfg['space_id'], token, user_id,
        )
        created_new = True
        print(f"Created new thread: {thread_id}", file=sys.stderr)

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
            # No threads at all — create one
            thread_id = notion_client.create_workflow_thread(
                cfg['notion_internal_id'], cfg['space_id'], token, user_id,
            )
            created_new = True
            print(f"Created first thread: {thread_id}", file=sys.stderr)

    # Read model from the live workflow record (what set_agent_model sets),
    # not from agents.yaml which never has a model field.
    wf = notion_client.get_workflow_record(cfg['notion_internal_id'], token, user_id)
    model_type = (wf.get('data', {}).get('model') or {}).get('type') or 'auto'

    msg_id = notion_client.send_agent_message(
        thread_id=thread_id,
        space_id=cfg['space_id'],
        notion_internal_id=cfg['notion_internal_id'],
        content=message,
        token_v2=token,
        user_id=user_id,
        model=model_type,
    )

    tracking = _build_agent_tracking_payload(thread_id, msg_id)

    if wait:
        effective_timeout = _compute_effective_wait_timeout(timeout)
        print(
            f"Waiting for agent response (requested={timeout}s, effective={effective_timeout}s)...",
            file=sys.stderr,
        )
        state = notion_client.wait_for_agent_response_state(
            thread_id, msg_id, token, user_id,
            timeout=effective_timeout,
            space_id=CFG.space_id,
        )
        payload = {
            "status": state["status"],
            "agent_name": agent_name,
            "thread_id": thread_id,
            "message_id": msg_id,
            "thread_created": created_new,
            "requested_timeout_seconds": timeout,
            "effective_timeout_seconds": effective_timeout,
            "content": state["content"],
            "tracking": tracking,
        }
        if state["status"] == "complete":
            return json.dumps(payload, ensure_ascii=False)
        if effective_timeout < timeout:
            payload["note"] = (
                f"No completed response yet after the transport-safe wait budget of "
                f"{effective_timeout}s (requested {timeout}s). "
                f"Call check_agent_response(timeout=120) to poll internally without sleep loops."
            )
        else:
            payload["note"] = (
                f"No completed response yet after {timeout}s. "
                f"Call check_agent_response(timeout=120) to poll internally without sleep loops."
            )
        return json.dumps(payload, ensure_ascii=False)

    return json.dumps(
        {
            "status": "queued",
            "agent_name": agent_name,
            "thread_id": thread_id,
            "message_id": msg_id,
            "thread_created": created_new,
            "tracking": tracking,
            "note": "Message sent. Call check_agent_response(timeout=120) to wait for completion — it polls internally so you don't need a sleep loop.",
        },
        ensure_ascii=False,
    )


@mcp.tool()
def start_agent_run(
    agent_name: str,
    message: str,
    thread_id: str | None = None,
    new_thread: bool = True,
) -> str:
    """
    Non-blocking entry point for slow agents.

    Dispatches a message via chat_with_agent(wait=False) and returns immediately
    with queued status plus tracking handles (thread_id, message_id) that can be
    passed to check_agent_response for polling.

    Prefer this over chat_with_agent(wait=True) when the agent is known to take
    longer than the MCP transport budget, so the tool call itself never blocks.
    """
    return chat_with_agent(
        agent_name=agent_name,
        message=message,
        thread_id=thread_id,
        new_thread=new_thread,
        wait=False,
    )


def _get_collection_prop_names(collection_id: str) -> dict[str, str]:
    """Fetch and cache a collection's property ID -> name map."""
    with _collection_lock:
        if collection_id in _collection_schemas:
            return _collection_schemas[collection_id]
    token, user_id = _get_auth()
    data = notion_client.read_records("collection", [collection_id], token, user_id)
    schema = data.get(collection_id, {}).get("schema", {})
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
def get_triggers(
    agent: str = "all",
    scope: str = "agent",
    db: str | None = None,
) -> str:
    """
    Show trigger and automation configuration.

    scope: "agent" (default) — show Notion AI agent triggers (@mention, schedules,
               property-change). agent="all" returns every agent; agent="<name>"
               returns a single registered agent.
           "db" — show native Notion database automations. Requires db parameter.

    db: Database page URL or UUID (required when scope="db").

    Includes implicit defaults: "New chat" (always on) and "@mention"
    (shown as disabled when absent).
    """
    token, user_id = _get_auth()

    if scope == "db":
        if not db:
            raise ValueError("scope='db' requires the db parameter (URL or UUID).")
        url_match = re.search(r'notion\.so/(?:[^/]+/)*([0-9a-f]{32})', db)
        raw_id = url_match.group(1) if url_match else db.strip()
        db_page_id = _to_dashed_uuid(raw_id)

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
                            display = raw[0][0] if isinstance(raw[0], list) else str(raw[0])
                        else:
                            display = str(raw) if raw else "?"
                        val_parts.append(f"{pname} ← {display}")
                    lines.append(f"       sets: {', '.join(val_parts)}")
                elif config:
                    lines.append(f"       config: {json.dumps(config)[:120]}")
            lines.append("")

        return "\n".join(lines).strip()

    # scope == "agent"
    if agent == "all":
        space_id = _resolve_space_id(token, user_id)
        agents = notion_client.get_all_workspace_agents(space_id, token, user_id)
        if not agents:
            return "No agents found in workspace."
        lines = []
        for a in agents:
            triggers = a.get("triggers", [])
            trigger_lines = _format_agent_triggers(triggers)
            lines.append(f"{a['name']}:\n" + "\n".join(trigger_lines))
        return "\n\n".join(lines)

    cfg = _get_agent_config(agent)
    wf = notion_client.get_workflow_record(cfg["notion_internal_id"], token, user_id)
    triggers = wf.get("data", {}).get("triggers", [])
    name = wf.get("data", {}).get("name", agent)
    trigger_lines = _format_agent_triggers(triggers)
    return f"{name}:\n" + "\n".join(trigger_lines)


@mcp.tool()
@auth_retry
def get_lab_topology(audit: bool = False) -> str:
    """
    Compile the live Lab topology and return a summary.

    audit: If False (default), returns a compact topology summary
           (agents, databases, triggers, permissions).
           If True, runs drift checks and returns trigger/permission/
           publish-state/contract findings.
    """
    token, user_id = _get_auth()
    snapshot = lab_topology.compile_snapshot(token, user_id)
    if audit:
        work_items_db = snapshot["indexes"]["database_by_key"].get("work_items", {})
        recent_items, recent_error = lab_topology.fetch_recent_work_items(
            database_id=work_items_db.get("notion_public_id")
        )
        report = lab_topology.evaluate_drift(
            snapshot,
            recent_work_items=recent_items,
            recent_error=recent_error,
        )
        return lab_topology.render_drift_report(report)
    return lab_topology.render_snapshot_summary(snapshot)



import database_tools

@mcp.tool()
def describe_database(database_id: str) -> str:
    """
    Show a Notion database's schema: property names, types, and select/status options.

    Call this BEFORE query_database if you don't know the exact property names and
    types. The output tells you which filter type to use for each property.

    database_id: Database UUID or a page UUID inside the database (dashed or
        dashless). If a page ID is given, the parent database is resolved
        automatically.
    """
    return database_tools.describe_database(database_id)

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

    database_id: The UUID of the database itself (dashed or dashless), NOT a row's
        page ID or a collection:// data source ID.
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
    return database_tools.query_database(
        database_id, filter, sorts, properties, limit,
        cursor, aggregate, max_tokens, sample
    )

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
    return database_tools.count_database(database_id, filter, exact)
@mcp.tool()
@auth_retry
def configure_agent_mcp(
    action: str,
    agent_name: str,
    server_name: str,
    server_url: str = "",
    publish: bool = True,
) -> str:
    """
    Add or remove a custom MCP server from a Notion AI agent's tool configuration.

    action: "add"    — add a new MCP server (requires server_name and server_url).
            "remove" — remove an existing MCP server by name (only server_name required).

    agent_name: Registered agent name from agents.yaml.
    server_name: Display name for the MCP server (e.g. "my-tools").
    server_url: The MCP server URL (required for action="add").
    publish: Whether to publish the agent afterward (default: True).
    """
    cfg = _get_agent_config(agent_name)
    token, user_id = _get_auth()
    wf = notion_client.get_workflow_record(cfg["notion_internal_id"], token, user_id)
    modules = wf.get("data", {}).get("modules", [])

    if action == "add":
        if not server_url:
            raise ValueError("action='add' requires server_url.")
        for m in modules:
            if m.get("type") == "mcpServer" and m.get("state", {}).get("serverUrl") == server_url:
                return f"MCP server at {server_url} is already configured on {agent_name}."
        new_module = {
            "id": str(uuid.uuid4()),
            "name": server_name,
            "type": "mcpServer",
            "version": "1.0.0",
            "state": {"serverUrl": server_url},
        }
        modules.append(new_module)
        notion_client.update_agent_modules(
            cfg["notion_internal_id"], cfg["space_id"], modules, token, user_id,
        )
        msg = f"Added MCP server '{server_name}' ({server_url}) to {agent_name}."

    elif action == "remove":
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

    else:
        raise ValueError(f"Unknown action '{action}'. Use 'add' or 'remove'.")

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
      - "haiku" / "anthropic-haiku-4.5" → Haiku 4.5
      - "gpt 5.2" / "oatmeal-cookie" → GPT-5.2
      - "gpt 5.4" / "oval-kumquat-medium" → GPT-5.4
      - "gpt 5.4 mini" / "gpt 5.4 nano" / "otaheite-apple-medium" → GPT-5.4 mini/nano
      - "gemini" / "gingerbread" → Gemini 3 Flash
      - "minimax" / "fireworks-minimax-m2.5" → Minimax M2.5
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
        "haiku": "anthropic-haiku-4.5",
        "haiku 4.5": "anthropic-haiku-4.5",
        "chatgpt": "oval-kumquat-medium",
        "chatgpt 5.4": "oval-kumquat-medium",
        "gpt 5.4": "oval-kumquat-medium",
        "gpt 5.4 mini": "otaheite-apple-medium",
        "chatgpt 5.4 mini": "otaheite-apple-medium",
        "gpt 5.4 nano": "otaheite-apple-medium",
        "chatgpt 5.4 nano": "otaheite-apple-medium",
        "gpt 5.2": "oatmeal-cookie",
        "chatgpt 5.2": "oatmeal-cookie",
        "gemini": "gingerbread",
        "gemini 3 flash": "gingerbread",
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
def get_agent_config_raw(agent_name: str, section: str = "all") -> str:
    """
    Fetch the raw workflow record for an agent.

    section: "all"     (default) — full workflow JSON (model, triggers, modules, data).
             "tools"   — formatted human-readable summary of model, MCP servers,
                         Notion permissions, mail, and calendar modules.
             "modules" — just the raw modules array as JSON.
    """
    cfg = _get_agent_config(agent_name)
    token, user_id = _get_auth()
    wf = notion_client.get_workflow_record(cfg["notion_internal_id"], token, user_id)

    if section == "modules":
        modules = wf.get("data", {}).get("modules", [])
        return json.dumps(modules, indent=2, ensure_ascii=False)

    if section == "tools":
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
                label = f"[MCP] {name}" + (f" ({official})" if official else "")
                lines.append(label)
                lines.append(f"  URL: {url}  Transport: {transport}  Auto-run writes: {auto_write}")
                if conn_id:
                    lines.append(f"  Connection ID: {conn_id}")
                lines.append(f"  Enabled: {len(enabled)}/{total} tools")
                enabled_set = set(enabled)
                for t in mod.get("tools", []):
                    status = "ON" if t["name"] in enabled_set else "off"
                    lines.append(f"    [{status}] {t['name']}: {t['title']}")
            elif mtype == "mail":
                emails = ", ".join(mod.get("emailAddresses", []))
                scopes = ", ".join(mod.get("scopes", []))
                lines.append(f"[Mail] {name}  Addresses: {emails}  Scopes: {scopes}")
            elif mtype == "calendar":
                scopes = ", ".join(mod.get("scopes", []))
                lines.append(f"[Calendar] {name}  Scopes: {scopes}")
            else:
                lines.append(f"[{mtype}] {name}")
            lines.append("")
        return "\n".join(lines).strip()

    return json.dumps(wf, indent=2, ensure_ascii=False)


@mcp.tool()
@auth_retry
def set_agent_config_raw(
    agent_name: str,
    config_json: str,
    publish: bool = True,
    freshen_triggers: bool = True,
    scope: str = "full",
) -> str:
    """
    Update an agent's configuration in bulk.

    agent_name: Registered agent name.
    config_json: JSON string — content depends on scope (see below).
    publish: Whether to publish immediately.
    freshen_triggers: If True (default), generate new random IDs for triggers.
                      Recommended when cloning from another agent.
    scope: "full"    (default) — config_json is the 'data' payload from get_agent_config_raw.
           "modules" — config_json is a modules array (from get_agent_config_raw section="modules").
    """
    cfg = _get_agent_config(agent_name)
    token, user_id = _get_auth()

    if scope == "modules":
        modules = json.loads(config_json)
        if not isinstance(modules, list):
            raise ValueError("scope='modules' requires config_json to be a JSON array.")
        notion_client.update_agent_modules(
            cfg["notion_internal_id"], cfg["space_id"], modules, token, user_id,
        )
        msg = f"Bulk-updated modules for {agent_name}."
    else:
        new_data = json.loads(config_json)
        if not isinstance(new_data, dict):
            raise ValueError("config_json must be a dictionary.")
        if "data" in new_data and "id" in new_data:
            new_data = new_data["data"]
        if freshen_triggers and "triggers" in new_data:
            for t in new_data["triggers"]:
                t["id"] = str(uuid.uuid4())
                if t.get("state", {}).get("type") == "recurrence":
                    t["state"]["scheduleId"] = str(uuid.uuid4())
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


# ── Dispatch tools (conditional) ─────────────────────────────────────────────
# Registered only when Lab-specific config is present (work_items, audit_log,
# lab_control database IDs). Sam's env populates these via op run --env-file;
# a generic user gets the 25 agent-management tools without dispatch.

if CFG and CFG.has_lab_config:
    import dispatch_tools
    dispatch_tools.register(mcp, CFG)


def _sync_mirrors_background():
    """Auto-sync agent instruction mirrors on startup (background thread)."""
    try:
        from agent_mirror import dump_as_manifest
        mirrors_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mirrors")
        os.makedirs(mirrors_dir, exist_ok=True)
        agents_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "agents.yaml")
        with open(agents_path, "r") as f:
            registry = yaml.safe_load(f) or {}
        for agent_name in registry:
            try:
                yaml_content, instructions = dump_as_manifest(agent_name)
                with open(os.path.join(mirrors_dir, f"{agent_name}.yaml"), "w") as f:
                    f.write(yaml_content)
                md_path = os.path.join(mirrors_dir, f"{agent_name}.md")
                existing_has_content = os.path.exists(md_path) and os.path.getsize(md_path) > 0
                if instructions or not existing_has_content:
                    with open(md_path, "w") as f:
                        f.write(instructions or "")
            except Exception:
                pass
    except Exception:
        pass


def _keepalive_loop() -> None:
    """Background thread: periodically ping Notion to keep token_v2 session alive."""
    import logging
    log = logging.getLogger("notion-agents.keepalive")
    while True:
        time.sleep(_KEEPALIVE_INTERVAL)
        try:
            token, user_id = _get_auth()
            # Lightweight call — loadUserContent returns spaces/settings
            notion_client._post("loadUserContent", {}, token, user_id)
            log.debug("keep-alive ping OK (source=%s)", _AUTH_SOURCE)
        except Exception as exc:
            log.warning("keep-alive ping failed: %s", exc)


def main():
    threading.Thread(target=_sync_mirrors_background, daemon=True).start()
    threading.Thread(target=_keepalive_loop, daemon=True, name="notion-keepalive").start()
    transport = sys.argv[1] if len(sys.argv) > 1 else "stdio"
    mcp.run(transport=transport)


if __name__ == "__main__":
    main()
