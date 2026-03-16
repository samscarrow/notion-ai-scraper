import time
import uuid
from typing import Any

from notion_http import _post, _normalize_record_map, _tx, _block_pointer, send_ops

def get_block_children(notion_public_id: str, space_id: str,
                       token_v2: str, user_id: str | None = None) -> list[str]:
    """Return ordered list of child block IDs for a given block."""
    payload = {
        "pageId": notion_public_id,
        "limit": 100,
        "cursor": {"stack": []},
        "chunkNumber": 0,
        "verticalColumns": False,
    }
    data = _normalize_record_map(
        _post("loadPageChunk", payload, token_v2, user_id)
    )

    record_map = data.get("recordMap", {})
    blocks = record_map.get("block", {})

    parent = blocks.get(notion_public_id, {}).get("value", {})
    return parent.get("content", [])


def get_block_tree(notion_public_id: str, space_id: str,
                   token_v2: str, user_id: str | None = None) -> dict:
    """Return the full recordMap for a block and all its descendants, paginating as needed."""
    cursor = {"stack": []}
    merged_blocks: dict = {}
    first_response: dict | None = None

    while True:
        payload = {
            "pageId": notion_public_id,
            "limit": 500,
            "cursor": cursor,
            "chunkNumber": 0,
            "verticalColumns": False,
        }
        data = _normalize_record_map(
            _post("loadPageChunk", payload, token_v2, user_id)
        )
        if first_response is None:
            first_response = data
        merged_blocks.update(data.get("recordMap", {}).get("block", {}))

        next_cursor = data.get("cursor")
        if not next_cursor or not next_cursor.get("stack"):
            break
        cursor = next_cursor

    first_response.setdefault("recordMap", {})["block"] = merged_blocks
    return first_response


def get_db_automations(db_page_id: str, token_v2: str,
                       user_id: str | None = None) -> dict:
    """
    Return all native automations and their actions for a Notion database page.
    """
    payload = {
        "page": {"id": db_page_id},
        "limit": 100,
        "cursor": {"stack": []},
        "chunkNumber": 0,
        "verticalColumns": False,
    }
    data = _normalize_record_map(
        _post("loadPageChunk", payload, token_v2, user_id)
    )
    record_map = data.get("recordMap", {})

    raw_automations = record_map.get("automation", {})
    raw_actions = record_map.get("automation_action", {})

    # Build property ID → name map from collection schema
    prop_map: dict[str, str] = {}
    for coll_rec in record_map.get("collection", {}).values():
        schema = coll_rec.get("value", {}).get("schema", {})
        for pid, pdef in schema.items():
            prop_map[pid] = pdef.get("name", pid)

    result = []
    for aid, arec in raw_automations.items():
        av = arec.get("value", {})
        # Collect actions belonging to this automation
        actions = [
            {
                "id": av2.get("id"),
                "type": av2.get("type"),
                "config": av2.get("config", {}),
            }
            for av2 in (v.get("value", {}) for v in raw_actions.values())
            if av2.get("parent_id") == aid
        ]
        result.append({
            "id": aid,
            "enabled": av.get("enabled"),
            "trigger": av.get("trigger"),
            "actions": actions,
        })

    return {"automations": result, "property_map": prop_map}


def _ops_delete_block(notion_public_id: str, parent_id: str, space_id: str) -> list[dict]:
    """Return ops to soft-delete a block and remove from parent content."""
    return [
        {
            "pointer": _block_pointer(notion_public_id, space_id),
            "path": [],
            "command": "update",
            "args": {"alive": False},
        },
        {
            "pointer": _block_pointer(parent_id, space_id),
            "path": ["content"],
            "command": "listRemove",
            "args": {"id": notion_public_id},
        },
    ]


def _ops_insert_block(block: dict, parent_id: str, after_id: str | None,
                      space_id: str) -> tuple[list[dict], str]:
    """Return (ops, new_notion_public_id) to insert a block. Handles children recursively."""
    children = block.pop("children", None)
    notion_public_id = str(uuid.uuid4())
    now = int(time.time() * 1000)

    block_value = {
        "id": notion_public_id,
        "parent_id": parent_id,
        "parent_table": "block",
        "alive": True,
        "created_time": now,
        "last_edited_time": now,
        "space_id": space_id,
        **block,
    }

    list_after_args: dict[str, Any] = {"id": notion_public_id}
    if after_id:
        list_after_args["after"] = after_id

    ops = [
        {
            "pointer": _block_pointer(notion_public_id, space_id),
            "path": [],
            "command": "set",
            "args": block_value,
        },
        {
            "pointer": _block_pointer(parent_id, space_id),
            "path": ["content"],
            "command": "listAfter",
            "args": list_after_args,
        },
    ]

    if children:
        child_after = None
        for child_block in children:
            child_ops, child_id = _ops_insert_block(
                child_block, notion_public_id, child_after, space_id,
            )
            ops.extend(child_ops)
            child_after = child_id

    return ops, notion_public_id


def _ops_update_block(notion_public_id: str, space_id: str,
                      properties: dict, format_: dict | None = None) -> list[dict]:
    """Return ops to update a block's properties (and optionally format) in place."""
    ops = [
        {
            "pointer": _block_pointer(notion_public_id, space_id),
            "path": ["properties"],
            "command": "set",
            "args": properties,
        },
    ]
    if format_ is not None:
        ops.append({
            "pointer": _block_pointer(notion_public_id, space_id),
            "path": ["format"],
            "command": "set",
            "args": format_,
        })
    return ops


def delete_block(notion_public_id: str, parent_id: str, space_id: str,
                 token_v2: str, user_id: str | None = None,
                 dry_run: bool = False) -> None:
    """Soft-delete a block and remove it from its parent's content list."""
    send_ops(space_id, _ops_delete_block(notion_public_id, parent_id, space_id),
             token_v2, user_id, dry_run)


def insert_block(block: dict, parent_id: str, after_id: str | None,
                 space_id: str, token_v2: str, user_id: str | None = None,
                 dry_run: bool = False) -> str:
    """Insert a new block into parent_id after after_id (or at start if None).
    Returns the new block's ID."""
    ops, notion_public_id = _ops_insert_block(block, parent_id, after_id, space_id)
    send_ops(space_id, ops, token_v2, user_id, dry_run)
    return notion_public_id


def _title_text(block: dict) -> str:
    """Extract plain text from a block's title property."""
    title = block.get("properties", {}).get("title", [])
    if not title:
        return ""
    return "".join(chunk[0] for chunk in title if chunk)


def _block_fingerprint(block: dict) -> tuple:
    """Canonical fingerprint for comparing blocks regardless of source."""
    btype = block.get("type", "text")
    props = block.get("properties", {})
    title = _title_text(block)
    lang = ""
    if "language" in props:
        lang_val = props["language"]
        lang = lang_val[0][0] if lang_val and lang_val[0] else ""
    fmt = block.get("format", {}).get("page_icon", "")

    child_fps = ()
    children = block.get("children")
    if children:
        child_fps = tuple(_block_fingerprint(c) for c in children)

    return (btype, title, lang, fmt, child_fps)


def _api_block_fingerprint(block: dict, blocks_map: dict) -> tuple:
    """Fingerprint an API block record, recursing into children via blocks_map."""
    btype = block.get("type", "text")
    props = block.get("properties", {})
    title = _title_text(block)
    lang = ""
    if "language" in props:
        lang_val = props["language"]
        lang = lang_val[0][0] if lang_val and lang_val[0] else ""
    fmt = block.get("format", {}).get("page_icon", "")

    child_fps = ()
    child_ids = block.get("content", [])
    if child_ids:
        child_fps = tuple(
            _api_block_fingerprint(
                blocks_map.get(cid, {}).get("value", {}), blocks_map
            )
            for cid in child_ids
            if blocks_map.get(cid, {}).get("value", {}).get("alive", True)
        )

    return (btype, title, lang, fmt, child_fps)


def _collect_delete_tree_ops(notion_public_id: str, parent_id: str, space_id: str,
                             blocks_map: dict) -> list[dict]:
    """Collect ops to delete a block and all its descendants."""
    ops = []
    block = blocks_map.get(notion_public_id, {}).get("value", {})
    for child_id in block.get("content", []):
        ops.extend(_collect_delete_tree_ops(child_id, notion_public_id, space_id, blocks_map))
    ops.extend(_ops_delete_block(notion_public_id, parent_id, space_id))
    return ops


def diff_replace_block_content(
    parent_id: str, space_id: str,
    new_blocks: list[dict],
    token_v2: str, user_id: str | None = None,
    dry_run: bool = False,
) -> dict:
    """Replace block content with minimal operations using a structural diff."""
    tree = get_block_tree(parent_id, space_id, token_v2, user_id)
    blocks_map = tree.get("recordMap", {}).get("block", {})

    parent_block = blocks_map.get(parent_id, {}).get("value", {})
    existing_ids = parent_block.get("content", [])

    existing_fps = []
    for bid in existing_ids:
        bdata = blocks_map.get(bid, {}).get("value", {})
        if not bdata or not bdata.get("alive", True):
            continue
        existing_fps.append((bid, _api_block_fingerprint(bdata, blocks_map)))

    new_fps = [_block_fingerprint(b) for b in new_blocks]

    prefix_len = 0
    for i in range(min(len(existing_fps), len(new_fps))):
        if existing_fps[i][1] == new_fps[i]:
            prefix_len = i + 1
        else:
            break

    suffix_len = 0
    max_suffix = min(len(existing_fps) - prefix_len, len(new_fps) - prefix_len)
    for i in range(1, max_suffix + 1):
        if existing_fps[-i][1] == new_fps[-i]:
            suffix_len = i
        else:
            break

    old_start = prefix_len
    old_end = len(existing_fps) - suffix_len
    new_start = prefix_len
    new_end = len(new_fps) - suffix_len

    n_unchanged = prefix_len + suffix_len
    n_deleted = 0
    n_inserted = 0
    n_updated = 0

    if old_start >= old_end and new_start >= new_end:
        return {
            "unchanged": n_unchanged, "deleted": 0, "inserted": 0,
            "updated": 0, "ops": 0, "api_calls_saved": len(existing_fps),
        }

    ops: list[dict] = []

    old_zone = existing_fps[old_start:old_end]
    new_zone = new_blocks[new_start:new_end]

    i_old, i_new = 0, 0
    consumed_old: set[int] = set()

    while i_old < len(old_zone) and i_new < len(new_zone):
        old_bid, old_fp = old_zone[i_old]
        new_fp = new_fps[new_start + i_new]
        new_block = new_zone[i_new]

        if old_fp == new_fp:
            n_unchanged += 1
            consumed_old.add(i_old)
            i_old += 1
            i_new += 1
        elif old_fp[0] == new_fp[0]:
            new_props = new_block.get("properties", {})
            new_fmt = new_block.get("format")
            ops.extend(_ops_update_block(old_bid, space_id, new_props, new_fmt))

            new_children = new_block.get("children")
            old_block = blocks_map.get(old_bid, {}).get("value", {})
            old_child_ids = old_block.get("content", [])

            if new_children or old_child_ids:
                for cid in old_child_ids:
                    ops.extend(_collect_delete_tree_ops(cid, old_bid, space_id, blocks_map))
                if new_children:
                    child_after = None
                    for child_block in new_children:
                        child_ops, child_id = _ops_insert_block(
                            child_block, old_bid, child_after, space_id,
                        )
                        ops.extend(child_ops)
                        child_after = child_id

            n_updated += 1
            consumed_old.add(i_old)
            i_old += 1
            i_new += 1
        else:
            ops.extend(_collect_delete_tree_ops(old_bid, parent_id, space_id, blocks_map))
            consumed_old.add(i_old)
            n_deleted += 1
            i_old += 1

    while i_old < len(old_zone):
        old_bid, _ = old_zone[i_old]
        if i_old not in consumed_old:
            ops.extend(_collect_delete_tree_ops(old_bid, parent_id, space_id, blocks_map))
            n_deleted += 1
        i_old += 1

    if i_new < len(new_zone):
        if consumed_old:
            max_consumed = max(consumed_old)
            after_id = old_zone[max_consumed][0]
            while max_consumed in consumed_old and max_consumed >= 0:
                old_bid_check = old_zone[max_consumed][0]
                was_deleted = any(
                    op.get("args", {}).get("alive") is False
                    and op.get("pointer", {}).get("id") == old_bid_check
                    for op in ops
                )
                if not was_deleted:
                    after_id = old_bid_check
                    break
                max_consumed -= 1
            else:
                after_id = existing_fps[prefix_len - 1][0] if prefix_len > 0 else None
        else:
            after_id = existing_fps[prefix_len - 1][0] if prefix_len > 0 else None

        while i_new < len(new_zone):
            insert_ops, new_id = _ops_insert_block(
                new_zone[i_new], parent_id, after_id, space_id,
            )
            ops.extend(insert_ops)
            after_id = new_id
            n_inserted += 1
            i_new += 1

    send_ops(space_id, ops, token_v2, user_id, dry_run)
    old_approach_calls = len(existing_ids) + len(new_blocks)

    return {
        "unchanged": n_unchanged,
        "deleted": n_deleted,
        "inserted": n_inserted,
        "updated": n_updated,
        "ops": len(ops),
        "api_calls_saved": max(0, old_approach_calls - 1),
    }


def replace_block_content(parent_id: str, space_id: str,
                          new_blocks: list[dict],
                          token_v2: str, user_id: str | None = None,
                          dry_run: bool = False) -> None:
    """Replace all children of parent_id with new_blocks.
    Uses batched transaction — single API call regardless of block count."""
    existing = get_block_children(parent_id, space_id, token_v2, user_id)

    ops: list[dict] = []
    for child_id in existing:
        ops.extend(_ops_delete_block(child_id, parent_id, space_id))

    after_id = None
    for block in new_blocks:
        insert_ops, after_id = _ops_insert_block(block, parent_id, after_id, space_id)
        ops.extend(insert_ops)

    send_ops(space_id, ops, token_v2, user_id, user_action="agentPersistenceActions.addPage", dry_run=dry_run)
