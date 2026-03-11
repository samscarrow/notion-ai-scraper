# notion-forge

Firefox extension + Tampermonkey script + Python CLI + MCP server for capturing and managing Notion AI chat conversations and agent instructions.

## Environment

- Python venv: `cli/.venv` — use this for all CLI/MCP work; system Python lacks `mcp` and `pyyaml`
- Node: ES modules — use `node --input-type=module -e "import ..."` for inline scripts

## Configuration
The project uses a centralized configuration pattern in `cli/config.py`. All hardcoded IDs are stored there as defaults and can be overridden via environment variables or a `.env` file in the project root.

### Core Environment Variables
- `NOTION_TOKEN`: Notion integration token (required for public API tools like `lab_auditor.py`).
- `NOTION_SPACE_ID`: The target Notion Space UUID.
- `WORK_ITEMS_DB_ID`: The Work Items database UUID.
- `LAB_PROJECTS_DB_ID`: The Lab Projects database UUID.
- `AUDIT_LOG_DB_ID`: The Lab Audit Log database UUID.

### Tool-Specific Configuration
- `LIBRARIAN_WORKFLOW_ID`: The Agent workflow ID for the Lab Librarian.
- `LIBRARIAN_BOT_RUNTIME`: The Bot ID for the Librarian's runtime permission.
- `LIBRARIAN_BOT_DRAFT`: The Bot ID for the Librarian's draft permission.

## Testing

```bash
# Block builder round-trip (live data)
node --input-type=module -e "import {blocksToMarkdown,markdownToBlocks} from './agent-manager/block-builder.js'; ..."

# MCP server — FastMCP blocks on stdin, always pipe with timeout
printf 'JSON\nJSON\n' | timeout 30 cli/.venv/bin/python cli/mcp_server.py 2>/dev/null

# Fetch live Notion blocks
cli/.venv/bin/python -c "import sys; sys.path.insert(0,'cli'); import notion_client, cookie_extract; ..."
```

## MCP Server

- Entry: `cli/mcp_server.py`, registered in `.mcp.json`
- Server name: `notion-agents`
- Tools: `list_agents`, `list_workspace_agents`, `sync_registry`, `dump_agent`, `update_agent`, `publish_agent`, `discover_agent`, `register_agent`, `remove_agent`, `get_agent_tools`, `add_agent_mcp_server`, `remove_agent_mcp_server`, `set_agent_model`, `chat_with_agent`, `get_conversation`, `describe_database`, `query_database`, `get_agent_triggers`, `get_db_automations`, `grant_resource_access`, `get_dispatchable_items`, `build_dispatch_packet`, `stamp_dispatch_consumed`
- `describe_database(database_id)` returns the schema (property names, types, select/status options). **Always call this before `query_database` if you don't know the exact property names and types.** The filter type key in `query_database` must match the property's actual type (e.g. `status` not `select` for status-type properties). `query_database` auto-corrects common mismatches, but `describe_database` prevents them entirely.
- `chat_with_agent(agent_name, message, wait=True)` sends a message and returns the agent's response. Requires at least one UI-created thread per agent (programmatic thread creation is not yet supported by Notion's inference backend).
- `sync_registry` auto-populates `cli/agents.yaml` from the live workspace (additive-only, safe to re-run)
- See `~/.agents/skills/notion-agent-mcp/SKILL.md` for full API reference

## ID Duality & Tool Compatibility

Notion databases have two distinct UUIDs. Using the wrong one will result in a 404.

| ID Type | Example Name | Tooling |
|---|---|---|
| **notion_public_id** | `page_id` | Public API (`retrieve-a-database`, `query-database`, `update-page-v2`) |
| **notion_internal_id** | `collection_id` | Internal Tools (`triggers`, `query-data-source`, `view`) |

## Key files

| File | Purpose |
|---|---|
| `cli/mcp_server.py` | MCP server (27 tools) |
| `cli/notion_client.py` | Internal Notion API client |
| `cli/block_builder.py` | Markdown ↔ Notion blocks (Python) |
| `cli/cookie_extract.py` | Firefox `token_v2` auth |
| `cli/agents.yaml` | Agent registry (12 agents) |
| `agent-manager/block-builder.js` | Markdown ↔ Notion blocks (JS, used by extension) |
| `background/service-worker.js` | Extension: chat interception + agent write API |
| `popup/popup.js` | Extension: UI thin client |
| `cli/dispatch.py` | Dispatch adapter (v1.1 contract) |
| `cli/contracts/` | JSON schemas + configs for dispatch contract |
| `cli/test_dispatch.py` | Dispatch adapter unit tests |
| `.mcp.json` | MCP server registration |
