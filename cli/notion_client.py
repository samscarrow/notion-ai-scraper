"""
notion_client.py — HTTP client for Notion's internal /api/v3/ endpoints.

Implements:
  - loadPageChunk         → read child block IDs of an instructions page
  - saveTransactionsFanout → write block content (delete old, insert new)
  - publishCustomAgentVersion → deploy agent after instruction update

Transaction envelope format from live capture (2026-03-03) and
notion-enhancer/api/notion.mjs (MIT):
https://github.com/notion-enhancer/api/blob/dev/notion.mjs
"""

import json
import time
import urllib.error
import urllib.request
import uuid
from typing import Any

BASE_URL = "https://www.notion.so/api/v3"
MAX_RETRIES = 3
BACKOFF_BASE = 1  # seconds


def _make_headers(token_v2: str, user_id: str | None = None) -> dict:
    headers = {
        "Content-Type": "application/json",
        "Cookie": f"token_v2={token_v2}",
        "Notion-Audit-Log-Platform": "web",
    }
    if user_id:
        headers["x-notion-active-user-header"] = user_id
    return headers


def _post(endpoint: str, payload: dict, token_v2: str, user_id: str | None = None,
          dry_run: bool = False) -> dict:
    """POST to a Notion internal endpoint with retry on 5xx."""
    url = f"{BASE_URL}/{endpoint}"
    body = json.dumps(payload).encode()

    if dry_run:
        print(f"[DRY RUN] POST {url}")
        print(json.dumps(payload, indent=2))
        return {}

    headers = _make_headers(token_v2, user_id)
    last_err = None

    for attempt in range(MAX_RETRIES):
        try:
            req = urllib.request.Request(url, data=body, headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read())
                return data
        except urllib.error.HTTPError as e:
            status = e.code
            body_text = e.read().decode(errors="replace")

            # Token expired — caller should refresh and retry
            if status in (401, 403):
                raise PermissionError(
                    f"Notion returned {status}. token_v2 may be expired. "
                    f"Response: {body_text}"
                ) from e

            # Retryable server errors
            if status >= 500:
                last_err = e
                wait = BACKOFF_BASE * (2 ** attempt)
                print(f"  [{attempt+1}/{MAX_RETRIES}] {status} error, retrying in {wait}s...")
                time.sleep(wait)
                continue

            # Non-retryable client error
            raise RuntimeError(
                f"Notion API error {status}: {body_text}"
            ) from e

        except urllib.error.URLError as e:
            last_err = e
            wait = BACKOFF_BASE * (2 ** attempt)
            print(f"  [{attempt+1}/{MAX_RETRIES}] Network error, retrying in {wait}s: {e}")
            time.sleep(wait)

    raise RuntimeError(f"Failed after {MAX_RETRIES} attempts: {last_err}")


# ── Discover ──────────────────────────────────────────────────────────────────

def get_all_workspace_agents(space_id: str, token_v2: str,
                              user_id: str | None = None) -> list[dict]:
    """
    Enumerate all AI agents (workflows) in a Notion workspace.

    Uses two calls:
      1. POST /api/v3/getBots — returns bot records keyed by bot_id, each with workflow_id
      2. POST /api/v3/getRecordValues (batched) — fetches all workflow records for name + block_id

    Returns a list of dicts:
      {name, workflow_id, space_id, block_id}
    """
    # Step 1: getBots — list all workflow-type bots in the space
    bots_data = _post("getBots", {"table": "space", "id": space_id, "type": "workflow"},
                      token_v2, user_id)
    bot_records = bots_data.get("recordMap", {}).get("bot", {})

    # Collect unique workflow_ids, keeping highest version per workflow
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

    workflow_ids = list(seen.keys())

    # Step 2: batch getRecordValues — fetch all workflow records for block_id
    batch_payload = {
        "requests": [{"id": wid, "table": "workflow"} for wid in workflow_ids],
    }
    wf_data = _post("getRecordValues", batch_payload, token_v2, user_id)

    agents = []
    for i, result in enumerate(wf_data.get("results", [])):
        wf = result.get("value")
        if not wf:
            continue
        wf_id = workflow_ids[i]
        data = wf.get("data", {})
        name = data.get("name") or seen[wf_id]["name"]
        instructions = data.get("instructions")
        if not instructions:
            continue
        block_id = instructions["id"] if isinstance(instructions, dict) else instructions
        agents.append({
            "name": name,
            "workflow_id": wf_id,
            "space_id": wf.get("space_id", space_id),
            "block_id": block_id,
            "triggers": data.get("triggers", []),
        })

    return sorted(agents, key=lambda a: a["name"].lower())


def get_workflow_record(workflow_id: str, token_v2: str,
                        user_id: str | None = None) -> dict:
    """
    Fetch a workflow record via getRecordValues (table: "workflow").
    Returns the workflow's value dict with keys like name, data, space_id, etc.
    """
    payload = {
        "requests": [{"id": workflow_id, "table": "workflow"}],
    }
    data = _post("getRecordValues", payload, token_v2, user_id)
    results = data.get("results", [])
    if not results or not results[0].get("value"):
        raise RuntimeError(
            f"Workflow {workflow_id} not found or inaccessible. "
            f"Response: {data}"
        )
    return results[0]["value"]


# ── Read ──────────────────────────────────────────────────────────────────────

def get_block_children(block_id: str, space_id: str,
                       token_v2: str, user_id: str | None = None) -> list[str]:
    """Return ordered list of child block IDs for a given block."""
    payload = {
        "pageId": block_id,
        "limit": 100,
        "cursor": {"stack": []},
        "chunkNumber": 0,
        "verticalColumns": False,
    }
    data = _post("loadPageChunk", payload, token_v2, user_id)

    record_map = data.get("recordMap", {})
    blocks = record_map.get("block", {})

    parent = blocks.get(block_id, {}).get("value", {})
    return parent.get("content", [])


def get_block_tree(block_id: str, space_id: str,
                   token_v2: str, user_id: str | None = None) -> dict:
    """Return the full recordMap for a block and all its descendants."""
    payload = {
        "pageId": block_id,
        "limit": 500,
        "cursor": {"stack": []},
        "chunkNumber": 0,
        "verticalColumns": False,
    }
    return _post("loadPageChunk", payload, token_v2, user_id)


def get_db_automations(db_page_id: str, token_v2: str,
                       user_id: str | None = None) -> dict:
    """
    Return all native automations and their actions for a Notion database page.

    Uses loadPageChunk which includes 'automation' and 'automation_action' tables
    in the recordMap alongside block data.

    Returns:
        {
          "automations": [
            {
              "id": str,
              "enabled": bool | None,
              "trigger": dict,
              "actions": [{"id": str, "type": str, "config": dict}, ...]
            },
            ...
          ]
        }
    """
    payload = {
        "pageId": db_page_id,
        "limit": 100,
        "cursor": {"stack": []},
        "chunkNumber": 0,
        "verticalColumns": False,
    }
    data = _post("loadPageChunk", payload, token_v2, user_id)
    record_map = data.get("recordMap", {})

    raw_automations = record_map.get("automation", {})
    raw_actions = record_map.get("automation_action", {})

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

    return {"automations": result}


# ── Write ─────────────────────────────────────────────────────────────────────

def _tx(space_id: str, operations: list[dict]) -> dict:
    """Wrap operations in the modern saveTransactionsFanout envelope."""
    return {
        "requestId": str(uuid.uuid4()),
        "transactions": [{
            "id": str(uuid.uuid4()),
            "spaceId": space_id,
            "debug": {"userAction": "cli.update_agent"},
            "operations": operations,
        }],
    }


def _block_pointer(block_id: str, space_id: str) -> dict:
    return {"table": "block", "id": block_id, "spaceId": space_id}


def delete_block(block_id: str, parent_id: str, space_id: str,
                 token_v2: str, user_id: str | None = None,
                 dry_run: bool = False) -> None:
    """Soft-delete a block and remove it from its parent's content list."""
    ops = [
        {
            "pointer": _block_pointer(block_id, space_id),
            "path": [],
            "command": "update",
            "args": {"alive": False},
        },
        {
            "pointer": _block_pointer(parent_id, space_id),
            "path": ["content"],
            "command": "listRemove",
            "args": {"id": block_id},
        },
    ]
    _post("saveTransactionsFanout", _tx(space_id, ops), token_v2, user_id, dry_run)


def insert_block(block: dict, parent_id: str, after_id: str | None,
                 space_id: str, token_v2: str, user_id: str | None = None,
                 dry_run: bool = False) -> str:
    """
    Insert a new block into parent_id after after_id (or at start if None).
    If the block has a 'children' key, inserts them recursively.
    Returns the new block's ID.
    """
    children = block.pop("children", None)
    block_id = str(uuid.uuid4())
    now = int(time.time() * 1000)

    block_value = {
        "id": block_id,
        "parent_id": parent_id,
        "parent_table": "block",
        "alive": True,
        "created_time": now,
        "last_edited_time": now,
        "space_id": space_id,
        **block,
    }

    list_after_args: dict[str, Any] = {"id": block_id}
    if after_id:
        list_after_args["after"] = after_id

    ops = [
        {
            "pointer": _block_pointer(block_id, space_id),
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
    _post("saveTransactionsFanout", _tx(space_id, ops), token_v2, user_id, dry_run)

    # Recursively insert children
    if children:
        child_after = None
        for child_block in children:
            child_after = insert_block(
                child_block, block_id, child_after,
                space_id, token_v2, user_id, dry_run,
            )

    return block_id


def replace_block_content(parent_id: str, space_id: str,
                          new_blocks: list[dict],
                          token_v2: str, user_id: str | None = None,
                          dry_run: bool = False) -> None:
    """
    Replace all children of parent_id with new_blocks.
    Deletes existing children first, then inserts new ones in order.
    """
    existing = get_block_children(parent_id, space_id, token_v2, user_id)

    print(f"  Deleting {len(existing)} existing block(s)...")
    for child_id in existing:
        delete_block(child_id, parent_id, space_id, token_v2, user_id, dry_run)

    print(f"  Inserting {len(new_blocks)} new block(s)...")
    after_id = None
    for block in new_blocks:
        after_id = insert_block(block, parent_id, after_id, space_id,
                                token_v2, user_id, dry_run)


# ── Conversations ─────────────────────────────────────────────────────────────

def _extract_rich_text(value) -> str | None:
    """Port of extractRichText from service-worker.js."""
    if not isinstance(value, list):
        return str(value).strip() if value else None
    parts = []
    for chunk in value:
        if not isinstance(chunk, list):
            parts.append(str(chunk) if chunk else "")
            continue
        text = chunk[0] if chunk else ""
        ann = chunk[1] if len(chunk) > 1 else None
        if text == "\u2023" and isinstance(ann, list):  # ‣ mention
            for a in ann:
                if isinstance(a, list) and len(a) >= 2:
                    parts.append(f"[{a[0]}:{a[1]}]")
                    break
        else:
            parts.append(text or "")
    return "".join(parts).strip() or None


def _clean_text(text: str) -> str:
    """Port of cleanText from service-worker.js."""
    import re as _re
    text = _re.sub(r'<lang[^>]*/>', '', text)
    text = _re.sub(r'<edit_reference[^>]*>[\s\S]*?</edit_reference>', '', text)
    return text.strip()


def _extract_inference_turn(step: dict) -> dict | None:
    """Port of extractInferenceTurn from service-worker.js."""
    resp, think = [], None
    for v in step.get("value") or []:
        if v.get("type") == "text":
            c = _clean_text(v.get("content") or "")
            if c:
                resp.append(c)
        elif v.get("type") == "thinking":
            think = (v.get("content") or "").strip() or None
    if not resp:
        return None
    turn: dict = {"role": "assistant", "content": "\n".join(resp)}
    if think:
        turn["thinking"] = think
    if step.get("model"):
        turn["model"] = step["model"]
    return turn


def get_thread_conversation(thread_id: str, token_v2: str,
                             user_id: str | None = None) -> dict:
    """
    Fetch a Notion AI thread and all its messages, returning a parsed
    conversation dict matching the extension's export shape.

    Steps:
      1. getRecordValues(table="thread") → message order + title
      2. getRecordValues(table="thread_message") batched → parse turns + tool calls
    """
    # Step 1: thread record
    thread_resp = _post(
        "getRecordValues",
        {"requests": [{"id": thread_id, "table": "thread"}]},
        token_v2, user_id,
    )
    results = thread_resp.get("results", [])
    if not results or not results[0].get("value"):
        raise ValueError(f"Thread '{thread_id}' not found or inaccessible.")
    thread = results[0]["value"]

    message_ids: list[str] = thread.get("messages") or []
    title: str | None = thread.get("data", {}).get("title") or None
    space_id: str = thread.get("space_id", "")

    if not message_ids:
        return {
            "id": f"thread-{thread_id.replace('-', '')}",
            "threadId": thread_id, "spaceId": space_id, "title": title,
            "turns": [], "toolCalls": [],
            "createdAt": thread.get("created_time"),
            "updatedAt": thread.get("last_edited_time"),
        }

    # Step 2: batch fetch messages (in order)
    msg_resp = _post(
        "getRecordValues",
        {"requests": [{"id": mid, "table": "thread_message"} for mid in message_ids]},
        token_v2, user_id,
    )

    turns: list[dict] = []
    orphan_tool_calls: list[dict] = []

    for i, result in enumerate(msg_resp.get("results", [])):
        msg = result.get("value")
        if not msg:
            continue
        mid = message_ids[i]
        step = msg.get("step") or {}
        ts = msg.get("created_time")

        if step.get("type") == "agent-inference":
            turn = _extract_inference_turn(step)
            if turn:
                turn["msgId"] = mid
                if ts:
                    turn["timestamp"] = ts
                turns.append(turn)

        elif step.get("type") in ("user", "human"):
            content = _extract_rich_text(step.get("value"))
            if content:
                turns.append({"role": "user", "content": content,
                              "msgId": mid, "timestamp": ts})

        elif (step.get("type") == "agent-tool-result"
              and step.get("state") == "applied"
              and step.get("toolName")):
            tool_call = {
                "tool": step["toolName"],
                "input": step.get("input") or {},
                "result": step.get("result"),
            }
            parent_idx = next(
                (j for j, t in enumerate(turns) if t.get("msgId") == step.get("agentStepId")),
                -1,
            )
            if parent_idx >= 0:
                turns[parent_idx].setdefault("toolCalls", []).append(tool_call)
            else:
                orphan_tool_calls.append(tool_call)

    # Derive model from first assistant turn that has one
    model = next((t.get("model") for t in turns if t.get("model")), None)

    return {
        "id": f"thread-{thread_id.replace('-', '')}",
        "threadId": thread_id,
        "spaceId": space_id,
        "title": title,
        "model": model,
        "turns": turns,
        "toolCalls": orphan_tool_calls,
        "createdAt": thread.get("created_time"),
        "updatedAt": thread.get("last_edited_time"),
    }


def search_threads(query: str, space_id: str, token_v2: str,
                   user_id: str | None = None) -> list[dict]:
    """
    Search for Notion AI threads by title using the internal search endpoint.
    Returns list of {thread_id, title, created_time} dicts.
    """
    payload = {
        "type": "BlocksInSpace",
        "query": query,
        "spaceId": space_id,
        "filters": {
            "isDeletedOnly": False,
            "excludeTemplates": False,
            "isNavigableOnly": False,
            "requireEditPermissions": False,
        },
        "sort": "Relevance",
        "limit": 20,
    }
    data = _post("search", payload, token_v2, user_id)
    record_map = data.get("recordMap", {})
    thread_rm = record_map.get("thread", {})
    matches = []
    for result in data.get("results", []):
        if result.get("table") == "thread":
            tid = result.get("id", "")
            rec = (thread_rm.get(tid) or {}).get("value", {})
            matches.append({
                "thread_id": tid,
                "title": rec.get("data", {}).get("title") or "(no title)",
                "created_time": rec.get("created_time"),
            })
    return matches


# ── Publish ───────────────────────────────────────────────────────────────────

def publish_agent(workflow_id: str, space_id: str,
                  token_v2: str, user_id: str | None = None,
                  dry_run: bool = False) -> dict:
    """
    Publish a Notion AI Agent workflow.
    Returns {workflowArtifactId, version} on success.

    Endpoint discovered via browser network intercept (2026-03-03).
    No public documentation exists for this endpoint.
    """
    payload = {"workflowId": workflow_id, "spaceId": space_id}
    result = _post("publishCustomAgentVersion", payload, token_v2, user_id, dry_run)

    if not dry_run and "workflowArtifactId" not in result:
        raise RuntimeError(
            f"Unexpected publish response (missing workflowArtifactId): {result}"
        )

    return result
