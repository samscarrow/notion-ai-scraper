import copy
import difflib
import time
import uuid
import base64
from typing import Any

from notion_http import _post, _normalize_record_map, _tx, _block_pointer, send_ops


def _copied_from_block_id(block: dict) -> str | None:
    return (((block.get("format") or {}).get("copied_from_pointer") or {}).get("id"))


def _is_copied_shell_page(root_id: str, blocks: dict) -> bool:
    root = (blocks.get(root_id) or {}).get("value", {})
    if root.get("type") != "page":
        return False
    if not _copied_from_block_id(root):
        return False

    child_ids = root.get("content", []) or []
    if len(child_ids) != 1 or len(blocks) != 2:
        return False

    child = (blocks.get(child_ids[0]) or {}).get("value", {})
    if not child.get("alive", True):
        return False
    if child.get("content"):
        return False
    if child.get("properties"):
        return False
    return bool(_copied_from_block_id(child))


def _alias_root_id(tree: dict, requested_root_id: str, source_root_id: str) -> dict:
    record_map = tree.setdefault("recordMap", {})
    blocks = record_map.setdefault("block", {})
    source_entry = blocks.get(source_root_id)
    if not source_entry:
        return tree
    blocks[requested_root_id] = source_entry
    return tree


def resolve_render_root_id(requested_root_id: str, blocks: dict) -> str:
    """Return the root block ID whose content should be rendered.

    Published instruction pages are often copy wrappers that point back to the
    editable source page via format.copied_from_pointer. For semantic render and
    diff purposes we want the source content, not the wrapper shell or copied
    child graph.
    """
    current_id = requested_root_id
    seen: set[str] = set()
    while current_id and current_id not in seen:
        seen.add(current_id)
        root = (blocks.get(current_id) or {}).get("value", {})
        source_id = _copied_from_block_id(root)
        if not source_id:
            return current_id
        current_id = source_id
    return current_id or requested_root_id

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
    if _is_copied_shell_page(notion_public_id, merged_blocks):
        source_id = _copied_from_block_id((merged_blocks.get(notion_public_id) or {}).get("value", {}))
        if source_id and source_id != notion_public_id:
            source_tree = get_block_tree(source_id, space_id, token_v2, user_id)
            return _alias_root_id(source_tree, notion_public_id, source_id)
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
    block = copy.deepcopy(block)
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
    """Return merge-style ops to update a block in place.

    Use `update` rather than `set` for nested `properties` / `format` payloads.
    The live Notion editor and older in-product agent edit paths both avoid
    whole-dict replacement for block text edits; they mutate specific fields
    while preserving surrounding CRDT / property structure.
    """
    ops = [
        {
            "pointer": _block_pointer(notion_public_id, space_id),
            "path": ["properties"],
            "command": "update",
            "args": properties,
        },
    ]
    if format_ is not None:
        ops.append({
            "pointer": _block_pointer(notion_public_id, space_id),
            "path": ["format"],
            "command": "update",
            "args": format_,
        })
    return ops


def _new_text_item_id() -> list[Any]:
    """Generate a Notion-style CRDT text item ID."""
    raw = base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip("=")
    return [raw[:12], 1]


def _extract_crdt_title_state(block: dict) -> dict[str, Any] | None:
    """Extract the minimum CRDT state needed for whole-title replacement.

    We only support the safe case where the root CRDT title graph exposes a
    visible text span that matches the current rendered plain text exactly.
    When the title graph is more fragmented or ambiguous, callers should fall
    back to a merge-style `properties` update.
    """
    crdt_title = (block.get("crdt_data") or {}).get("title")
    if not isinstance(crdt_title, dict):
        return None

    nodes = crdt_title.get("n") or {}
    root_key = crdt_title.get("r")
    root = nodes.get(root_key) or {}
    root_state = root.get("s") or {}
    text_instance_id = root_state.get("x")
    if not isinstance(text_instance_id, str) or not text_instance_id:
        return None

    current_text = _title_text(block)
    if not current_text:
        return {
            "text_instance_id": text_instance_id,
            "plain_text": "",
            "start_id": None,
            "length": 0,
            "runs": [],
        }

    items = root_state.get("i") or []
    if not isinstance(items, list):
        return None

    visible_nodes = [
        item for item in items
        if isinstance(item, dict)
        and item.get("t") == "t"
        and isinstance(item.get("i"), list)
        and isinstance(item.get("c"), str)
        and isinstance(item.get("l"), int)
        and item.get("l", 0) > 0
    ]
    if not visible_nodes:
        return None

    reconstructed = "".join(item.get("c", "") for item in visible_nodes)
    if reconstructed != current_text:
        return None

    runs: list[dict[str, Any]] = []
    cursor = 0
    for item in visible_nodes:
        start_id = item.get("i")
        if not isinstance(start_id, list) or len(start_id) != 2:
            return None
        content = item.get("c", "")
        length = item.get("l", 0)
        runs.append({
            "start_id": start_id,
            "content": content,
            "length": length,
            "start_offset": cursor,
            "end_offset": cursor + length,
        })
        cursor += length

    start_id = runs[0]["start_id"]
    if not isinstance(start_id, list) or len(start_id) != 2:
        return None

    return {
        "text_instance_id": text_instance_id,
        "plain_text": current_text,
        "start_id": start_id,
        "length": len(current_text),
        "runs": runs,
    }


def _ops_touch_block(notion_public_id: str, space_id: str,
                     user_id: str | None = None,
                     now_ms: int | None = None) -> list[dict]:
    now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    args: dict[str, Any] = {"last_edited_time": now_ms}
    if user_id:
        args["last_edited_by_id"] = user_id
        args["last_edited_by_table"] = "notion_user"
    return [{
        "pointer": _block_pointer(notion_public_id, space_id),
        "path": [],
        "command": "update",
        "args": args,
    }]


def _ops_replace_title_text_via_crdt(
    notion_public_id: str,
    space_id: str,
    old_block: dict,
    new_text: str,
) -> list[dict]:
    """Emit UI-like CRDT text ops for a title patch when safe."""
    state = _extract_crdt_title_state(old_block)
    if state is None:
        return []

    text_instance_id = state["text_instance_id"]
    old_text = state["plain_text"]
    runs = state["runs"]

    def _item_id_at_offset(offset: int) -> list[Any] | None:
        for run in runs:
            if run["start_offset"] <= offset < run["end_offset"]:
                base = run["start_id"]
                if not isinstance(base, list) or len(base) != 2 or not isinstance(base[1], int):
                    return None
                delta = offset - run["start_offset"]
                return [base[0], base[1] + delta]
        return None

    def _origin_id_before_offset(offset: int) -> str | list[Any] | None:
        if offset <= 0:
            return "start"
        return _item_id_at_offset(offset - 1)

    prefix = 0
    max_prefix = min(len(old_text), len(new_text))
    while prefix < max_prefix and old_text[prefix] == new_text[prefix]:
        prefix += 1

    suffix = 0
    max_suffix = min(len(old_text) - prefix, len(new_text) - prefix)
    while suffix < max_suffix and old_text[len(old_text) - 1 - suffix] == new_text[len(new_text) - 1 - suffix]:
        suffix += 1

    delete_len = len(old_text) - prefix - suffix
    insert_text = new_text[prefix: len(new_text) - suffix if suffix else len(new_text)]

    ops: list[dict] = []
    if delete_len:
        start_id = _item_id_at_offset(prefix)
        if start_id is None:
            return []
        ops.append({
            "command": "deleteText",
            "pointer": _block_pointer(notion_public_id, space_id),
            "path": [],
            "opVersion": 2,
            "args": {
                "type": "deleteText",
                "textInstanceId": text_instance_id,
                "searchLabel": "",
                "idRanges": [[start_id, delete_len]],
            },
        })

    if insert_text:
        origin_id = _origin_id_before_offset(prefix)
        if origin_id is None:
            return []
        ops.append({
            "command": "insertText",
            "pointer": _block_pointer(notion_public_id, space_id),
            "path": [],
            "opVersion": 2,
            "args": {
                "type": "insertText",
                "textInstanceId": text_instance_id,
                "searchLabel": "",
                "id": _new_text_item_id(),
                "originId": origin_id,
                "content": insert_text,
            },
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


def _diff_block_children(
    parent_id: str,
    existing_ids: list[str],
    new_blocks: list[dict],
    blocks_map: dict,
    space_id: str,
    user_id: str | None,
) -> tuple[list[dict], dict[str, int]]:
    """Diff a parent's children while preserving existing block IDs when possible."""
    existing_fps: list[tuple[str, tuple]] = []
    for bid in existing_ids:
        bdata = blocks_map.get(bid, {}).get("value", {})
        if not bdata or not bdata.get("alive", True):
            continue
        existing_fps.append((bid, _api_block_fingerprint(bdata, blocks_map)))

    new_fps = [_block_fingerprint(b) for b in new_blocks]
    ops, stats, _ = _diff_block_children_zone(
        parent_id,
        existing_fps,
        new_blocks,
        new_fps,
        blocks_map,
        space_id,
        user_id,
        None,
    )
    return ops, stats


def _diff_block_children_zone(
    parent_id: str,
    existing_zone: list[tuple[str, tuple]],
    new_blocks: list[dict],
    new_fps: list[tuple],
    blocks_map: dict,
    space_id: str,
    user_id: str | None,
    after_id: str | None,
) -> tuple[list[dict], dict[str, int], str | None]:
    """Diff one sibling zone using exact-match alignment before fallback updates."""
    stats = {
        "unchanged": 0,
        "deleted": 0,
        "inserted": 0,
        "updated": 0,
    }
    ops: list[dict] = []
    matcher = difflib.SequenceMatcher(
        a=[fp for _, fp in existing_zone],
        b=new_fps,
        autojunk=False,
    )

    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == "equal":
            stats["unchanged"] += (i2 - i1)
            if i2 > i1:
                after_id = existing_zone[i2 - 1][0]
            continue

        zone_ops, zone_stats, after_id = _diff_block_children_replace_zone(
            parent_id,
            existing_zone[i1:i2],
            new_blocks[j1:j2],
            new_fps[j1:j2],
            blocks_map,
            space_id,
            user_id,
            after_id,
        )
        ops.extend(zone_ops)
        for key, value in zone_stats.items():
            stats[key] += value

    return ops, stats, after_id


def _diff_block_children_replace_zone(
    parent_id: str,
    existing_zone: list[tuple[str, tuple]],
    new_blocks: list[dict],
    new_fps: list[tuple],
    blocks_map: dict,
    space_id: str,
    user_id: str | None,
    after_id: str | None,
) -> tuple[list[dict], dict[str, int], str | None]:
    """Handle a mixed replace zone with a conservative identity-preserving fallback."""
    stats = {
        "unchanged": 0,
        "deleted": 0,
        "inserted": 0,
        "updated": 0,
    }
    ops: list[dict] = []
    i_old = 0
    i_new = 0

    while i_old < len(existing_zone) and i_new < len(new_blocks):
        old_bid, old_fp = existing_zone[i_old]
        new_fp = new_fps[i_new]
        new_block = new_blocks[i_new]

        if old_fp == new_fp:
            stats["unchanged"] += 1
            after_id = old_bid
            i_old += 1
            i_new += 1
            continue

        old_block = blocks_map.get(old_bid, {}).get("value", {})
        if old_fp[0] == new_fp[0]:
            new_props = new_block.get("properties", {})
            new_fmt = new_block.get("format")
            old_plain = _title_text(old_block)
            new_plain = _title_text(new_block)
            if old_plain != new_plain:
                ops.extend(_ops_replace_title_text_via_crdt(old_bid, space_id, old_block, new_plain))
            ops.extend(_ops_update_block(old_bid, space_id, new_props, new_fmt))
            ops.extend(_ops_touch_block(old_bid, space_id, user_id))

            child_ops, child_stats = _diff_block_children(
                old_bid,
                old_block.get("content", []),
                new_block.get("children") or [],
                blocks_map,
                space_id,
                user_id,
            )
            ops.extend(child_ops)
            for key, value in child_stats.items():
                stats[key] += value

            stats["updated"] += 1
            after_id = old_bid
            i_old += 1
            i_new += 1
            continue

        remaining_old_types = [fp[0] for _, fp in existing_zone[i_old + 1:]]
        if new_fp[0] in remaining_old_types:
            ops.extend(_collect_delete_tree_ops(old_bid, parent_id, space_id, blocks_map))
            stats["deleted"] += 1
            i_old += 1
            continue

        insert_ops, new_id = _ops_insert_block(new_block, parent_id, after_id, space_id)
        ops.extend(insert_ops)
        stats["inserted"] += 1
        after_id = new_id
        i_new += 1

    while i_old < len(existing_zone):
        old_bid, _ = existing_zone[i_old]
        ops.extend(_collect_delete_tree_ops(old_bid, parent_id, space_id, blocks_map))
        stats["deleted"] += 1
        i_old += 1

    while i_new < len(new_blocks):
        insert_ops, new_id = _ops_insert_block(new_blocks[i_new], parent_id, after_id, space_id)
        ops.extend(insert_ops)
        stats["inserted"] += 1
        after_id = new_id
        i_new += 1

    return ops, stats, after_id


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
    ops, stats = _diff_block_children(
        parent_id,
        parent_block.get("content", []),
        new_blocks,
        blocks_map,
        space_id,
        user_id,
    )

    if not ops:
        existing_count = len([
            bid for bid in parent_block.get("content", [])
            if blocks_map.get(bid, {}).get("value", {}).get("alive", True)
        ])
        return {
            "unchanged": stats["unchanged"],
            "deleted": 0,
            "inserted": 0,
            "updated": 0,
            "ops": 0,
            "api_calls_saved": existing_count,
        }

    touched_parent = any(
        op.get("pointer", {}).get("id") != parent_id
        for op in ops
    )
    if touched_parent:
        ops.extend(_ops_touch_block(parent_id, space_id, user_id))

    send_ops(space_id, ops, token_v2, user_id, dry_run)
    old_approach_calls = len(parent_block.get("content", [])) + len(new_blocks)

    return {
        "unchanged": stats["unchanged"],
        "deleted": stats["deleted"],
        "inserted": stats["inserted"],
        "updated": stats["updated"],
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
