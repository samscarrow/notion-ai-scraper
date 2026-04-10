"""
notion_client.py — HTTP client for Notion's internal /api/v3/ endpoints.
This module has been refactored into smaller, domain-specific modules.
It now acts as a facade, exporting all functions for backward compatibility.
"""

from notion_http import (
    BASE_URL,
    MAX_RETRIES,
    BACKOFF_BASE,
    _make_headers,
    _post,
    _tx,
    _block_pointer,
    send_ops,
    _record_value,
    _normalize_record_map,
    read_records,
)

from notion_blocks import (
    get_block_children,
    get_block_tree,
    get_db_automations,
    _ops_delete_block,
    _ops_insert_block,
    _ops_update_block,
    delete_block,
    insert_block,
    _title_text,
    _block_fingerprint,
    _api_block_fingerprint,
    _collect_delete_tree_ops,
    diff_replace_block_content,
    replace_block_content,
)

from notion_agent_config import (
    get_user_spaces,
    get_all_workspace_agents,
    get_workflow_record,
    MODEL_NAMES,
    _resolve_page_names,
    get_agent_modules,
    update_agent_modules,
    update_agent_model,
    grant_agent_resource_access,
    ensure_mention_access,
    check_mention_access,
    create_agent,
    add_agent_to_sidebar,
    publish_agent,
)

from notion_threads import (
    _extract_rich_text,
    _clean_text,
    _extract_inference_turn,
    get_thread_conversation,
    get_agent_response_state,
    search_threads,
    list_workflow_threads,
    archive_threads,
    archive_workflow_threads,
    unarchive_threads,
    unarchive_selected_workflow_threads,
    create_workflow_thread,
    send_agent_message,
    wait_for_agent_response,
    wait_for_agent_response_state,
    get_snapshots_list,
    get_snapshot_contents,
    snapshot_contents_to_blocks_map,
    restore_snapshot,
)

__all__ = [
    "BASE_URL", "MAX_RETRIES", "BACKOFF_BASE", "_make_headers", "_post", "_tx",
    "_block_pointer", "send_ops", "_record_value", "_normalize_record_map",
    
    "get_block_children", "get_block_tree", "get_db_automations",
    "_ops_delete_block", "_ops_insert_block", "_ops_update_block",
    "delete_block", "insert_block", "_title_text", "_block_fingerprint",
    "_api_block_fingerprint", "_collect_delete_tree_ops",
    "diff_replace_block_content", "replace_block_content",
    
    "get_user_spaces", "get_all_workspace_agents", "get_workflow_record",
    "MODEL_NAMES", "_resolve_page_names", "get_agent_modules",
    "update_agent_modules", "update_agent_model",
    "grant_agent_resource_access", "ensure_mention_access", "check_mention_access",
    "create_agent", "add_agent_to_sidebar",
    "publish_agent",
    
    "_extract_rich_text", "_clean_text", "_extract_inference_turn",
    "get_thread_conversation", "get_agent_response_state", "search_threads", "list_workflow_threads",
    "archive_threads", "archive_workflow_threads", "unarchive_threads",
    "unarchive_selected_workflow_threads", "create_workflow_thread",
    "send_agent_message", "wait_for_agent_response", "wait_for_agent_response_state",
    "get_snapshots_list", "get_snapshot_contents", "snapshot_contents_to_blocks_map",
    "restore_snapshot",
]
