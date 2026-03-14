import json
import time
import uuid
from datetime import datetime
from typing import Any

from notion_http import _post, _normalize_record_map, _tx, send_ops, _record_value
import notion_agent_config

def _extract_rich_text(value) -> str | None:
    if not isinstance(value, list):
        return str(value).strip() if value else None
    parts = []
    for chunk in value:
        if not isinstance(chunk, list):
            parts.append(str(chunk) if chunk else "")
            continue
        text = chunk[0] if chunk else ""
        ann = chunk[1] if len(chunk) > 1 else None
        if text == "\u2023" and isinstance(ann, list):
            for a in ann:
                if isinstance(a, list) and len(a) >= 2:
                    parts.append(f"[{a[0]}:{a[1]}]")
                    break
        else:
            parts.append(text or "")
    return "".join(parts).strip() or None


def _clean_text(text: str) -> str:
    import re as _re
    text = _re.sub(r'<lang[^>]*/>', '', text)
    text = _re.sub(r'<edit_reference[^>]*>[\s\S]*?</edit_reference>', '', text)
    return text.strip()


def _extract_inference_turn(step: dict) -> dict | None:
    resp, think, tool_calls = [], None, []
    for v in step.get("value") or []:
        if v.get("type") == "text":
            c = _clean_text(v.get("content") or "")
            if c:
                resp.append(c)
        elif v.get("type") == "thinking":
            t = (v.get("content") or "").strip()
            if t:
                think = t
        elif v.get("type") == "tool_use":
            tc: dict = {"tool": v.get("name") or "unknown_tool"}
            if v.get("id"):
                tc["toolCallId"] = v["id"]
            raw_content = v.get("content")
            if raw_content:
                try:
                    tc["input"] = json.loads(raw_content) if isinstance(raw_content, str) else raw_content
                except (json.JSONDecodeError, TypeError):
                    tc["input"] = raw_content
            tool_calls.append(tc)
    if not resp and not tool_calls:
        return None
    content = "\n".join(resp) if resp else ""
    turn: dict = {"role": "assistant", "content": content}
    if think:
        turn["thinking"] = think
    if tool_calls:
        turn["toolCalls"] = tool_calls
    if step.get("model"):
        turn["model"] = step["model"]
    return turn


def get_thread_conversation(thread_id: str, token_v2: str,
                             user_id: str | None = None) -> dict:
    thread_resp = _post(
        "getRecordValues",
        {"requests": [{"id": thread_id, "table": "thread"}]},
        token_v2, user_id,
    )
    results = thread_resp.get("results", [])
    if not results:
        raise ValueError(f"Thread '{thread_id}' not found.")
    rec = results[0]
    if not rec.get("value"):
        if rec.get("role"):
            raise ValueError(
                f"Thread '{thread_id}' exists but its content has been deleted or purged by Notion."
            )
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
            "updatedAt": thread.get("updated_time"),
            "createdById": thread.get("created_by_id"),
            "updatedById": thread.get("updated_by_id"),
        }

    msg_resp = _post(
        "getRecordValues",
        {"requests": [{"id": mid, "table": "thread_message"} for mid in message_ids]},
        token_v2, user_id,
        space_id=space_id,
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
        author = msg.get("created_by_id")

        if step.get("type") == "agent-inference":
            turn = _extract_inference_turn(step)
            if turn:
                turn["msgId"] = mid
                if ts:
                    turn["timestamp"] = ts
                if author:
                    turn["createdById"] = author
                turns.append(turn)

        elif step.get("type") in ("user", "human"):
            content = _extract_rich_text(step.get("value"))
            if content:
                turn_data = {"role": "user", "content": content,
                             "msgId": mid, "timestamp": ts}
                if author:
                    turn_data["createdById"] = author
                turns.append(turn_data)

        elif (step.get("type") == "agent-tool-result"
              and step.get("state") == "applied"
              and step.get("toolName")):
            result_data = step.get("result")
            tool_call_id = step.get("toolCallId")
            agent_step_id = step.get("agentStepId")

            merged = False
            if agent_step_id and tool_call_id:
                parent_idx = next(
                    (j for j, t in enumerate(turns) if t.get("msgId") == agent_step_id),
                    -1,
                )
                if parent_idx >= 0:
                    for tc in turns[parent_idx].get("toolCalls", []):
                        if tc.get("toolCallId") == tool_call_id:
                            tc["result"] = result_data
                            if not tc.get("input") and step.get("input"):
                                tc["input"] = step["input"]
                            merged = True
                            break

            if not merged:
                tool_call = {
                    "tool": step["toolName"],
                    "input": step.get("input") or {},
                    "result": result_data,
                }
                if tool_call_id:
                    tool_call["toolCallId"] = tool_call_id
                parent_idx = next(
                    (j for j, t in enumerate(turns) if t.get("msgId") == agent_step_id),
                    -1,
                ) if agent_step_id else -1
                if parent_idx >= 0:
                    turns[parent_idx].setdefault("toolCalls", []).append(tool_call)
                else:
                    orphan_tool_calls.append(tool_call)

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
        "updatedAt": thread.get("updated_time"),
        "createdById": thread.get("created_by_id"),
        "updatedById": thread.get("updated_by_id"),
    }


def search_threads(query: str, space_id: str, token_v2: str,
                   user_id: str | None = None) -> list[dict]:
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
    data = _normalize_record_map(_post("search", payload, token_v2, user_id))
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


def list_workflow_threads(notion_internal_id: str, space_id: str,
                          token_v2: str, user_id: str | None = None,
                          limit: int = 100) -> list[dict]:
    threads: list[dict] = []
    seen_ids: set[str] = set()
    seen_cursors: set[str] = set()
    cursor: str | None = None

    while True:
        payload = {
            "workflowId": notion_internal_id,
            "spaceId": space_id,
            "limit": limit,
        }
        if user_id:
            payload["userId"] = user_id
        if cursor:
            payload["cursor"] = cursor

        data = _normalize_record_map(
            _post("getInferenceTranscriptsForWorkflow", payload, token_v2, user_id)
        )
        transcripts = data.get("transcripts") or []
        transcript_by_id = {
            item.get("id"): item for item in transcripts
            if isinstance(item, dict) and item.get("id")
        }
        record_threads = (data.get("recordMap") or {}).get("thread") or {}

        raw_ids = data.get("threadIds") or list(transcript_by_id.keys())
        for thread_id in raw_ids:
            if not thread_id or thread_id in seen_ids:
                continue

            transcript = dict(transcript_by_id.get(thread_id) or {})
            record = _record_value(record_threads.get(thread_id))
            if record and record.get("alive") is False:
                continue

            record_data = record.get("data") or {}
            meta = {
                "id": thread_id,
                "title": transcript.get("title") or record_data.get("title"),
                "created_at": transcript.get("created_at") or record.get("created_time"),
                "updated_at": transcript.get("updated_at") or record.get("updated_time"),
                "created_by_display_name": transcript.get("created_by_display_name"),
                "trigger_id": transcript.get("trigger_id") or record_data.get("trigger_id"),
                "run_id": transcript.get("run_id") or record_data.get("run_id"),
                "type": transcript.get("type") or record.get("type") or "workflow",
            }
            threads.append({k: v for k, v in meta.items() if v is not None})
            seen_ids.add(thread_id)

        next_cursor = data.get("nextCursor")
        if not next_cursor or next_cursor in seen_cursors:
            break
        seen_cursors.add(next_cursor)
        cursor = next_cursor

    return threads


def archive_threads(thread_ids: list[str], space_id: str,
                    token_v2: str, user_id: str | None = None,
                    dry_run: bool = False) -> list[str]:
    seen: set[str] = set()
    ordered_ids = [
        thread_id for thread_id in thread_ids
        if thread_id and not (thread_id in seen or seen.add(thread_id))
    ]
    if not ordered_ids:
        return []

    ops = [{
        "pointer": {"table": "thread", "id": thread_id, "spaceId": space_id},
        "path": [],
        "command": "update",
        "args": {
            "alive": False,
            "current_inference_id": None,
            "current_inference_lease_expiration": None,
        },
    } for thread_id in ordered_ids]

    payload = _tx(
        space_id,
        ops,
        user_action="assistantChatHistoryItem.deleteInferenceChatTranscript",
        unretryable_error_behavior="continue",
    )
    _post("saveTransactionsFanout", payload, token_v2, user_id, dry_run)
    return ordered_ids


def archive_workflow_threads(notion_internal_id: str, space_id: str,
                             token_v2: str, user_id: str | None = None,
                             limit: int = 100) -> dict:
    threads = list_workflow_threads(notion_internal_id, space_id, token_v2, user_id, limit=limit)
    manual_ids = [
        thread["id"] for thread in threads
        if thread.get("id") and not thread.get("trigger_id")
    ]
    archived_ids = archive_threads(manual_ids, space_id, token_v2, user_id)
    return {
        "count": len(archived_ids),
        "threadIds": archived_ids,
        "threads": threads,
        "skippedTriggerThreads": len(threads) - len(manual_ids),
    }


def create_workflow_thread(notion_internal_id: str, space_id: str,
                           token_v2: str, user_id: str | None = None,
                           title: str = "New conversation") -> str:
    """Create a new chat thread for a workflow agent.

    Mirrors the UI's WorkflowActions.addTranscriptToNewThread flow:
    creates the thread record plus config, context, and title messages
    so the Notion inference backend can process responses.
    """
    thread_id = str(uuid.uuid4())
    config_msg_id = str(uuid.uuid4())
    context_msg_id = str(uuid.uuid4())
    title_msg_id = str(uuid.uuid4())
    now_ms = int(time.time() * 1000)

    wf = notion_agent_config.get_workflow_record(notion_internal_id, token_v2, user_id)
    wf_data = wf.get("data") or {}

    thread_data: dict[str, Any] = {
        "workflow_id": notion_internal_id,
    }
    artifact_ptr = wf_data.get("published_artifact_pointer")
    if artifact_ptr:
        thread_data["workflow_artifact_pointer"] = artifact_ptr

    # Discover user info for context message
    user_name = "User"
    user_email = ""
    space_name = ""
    space_view_id = ""
    try:
        user_content = _normalize_record_map(_post("loadUserContent", {}, token_v2))
        user_map = user_content.get("recordMap", {}).get("notion_user", {})
        if user_id and user_id in user_map:
            u = user_map[user_id].get("value", {})
            user_name = u.get("name", "User")
            user_email = u.get("email", "")
        elif user_map:
            first_id = next(iter(user_map))
            u = user_map[first_id].get("value", {})
            user_name = u.get("name", "User")
            user_email = u.get("email", "")
            if not user_id:
                user_id = first_id

        space_map = user_content.get("recordMap", {}).get("space", {})
        if space_id in space_map:
            space_name = space_map[space_id].get("value", {}).get("name", "")

        sv_map = user_content.get("recordMap", {}).get("space_view", {})
        for sv_id, sv_data in sv_map.items():
            if sv_data.get("value", {}).get("space_id") == space_id:
                space_view_id = sv_id
                break
    except Exception:
        pass

    now_dt = datetime.fromtimestamp(now_ms / 1000).astimezone()
    offset_str = now_dt.strftime("%z")
    offset_formatted = f"{offset_str[:3]}:{offset_str[3:]}"
    now_iso = now_dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + offset_formatted

    # Resolve available connectors from modules
    available_connectors = []
    for mod in wf_data.get("modules", []):
        mtype = mod.get("type", "")
        if mtype == "notion":
            continue
        if mtype == "mail_context_module":
            available_connectors.append("notion-mail")
        elif mtype == "calendar_context_module":
            available_connectors.append("notion-calendar")

    def _msg_pointer(msg_id: str) -> dict:
        return {"table": "thread_message", "id": msg_id, "spaceId": space_id}

    ops: list[dict] = [
        # 1. Create thread record
        {
            "pointer": {"table": "thread", "id": thread_id, "spaceId": space_id},
            "path": [],
            "command": "set",
            "args": {
                "id": thread_id,
                "version": 1,
                "parent_id": notion_internal_id,
                "parent_table": "workflow",
                "space_id": space_id,
                "created_time": now_ms,
                "created_by_id": user_id,
                "created_by_table": "notion_user",
                "messages": [],
                "data": thread_data,
                "alive": True,
                "type": "workflow",
            },
        },
        # 2. Config message — feature flags required by inference backend
        {
            "pointer": _msg_pointer(config_msg_id),
            "path": [],
            "command": "set",
            "args": {
                "id": config_msg_id,
                "version": 1,
                "step": {
                    "id": config_msg_id,
                    "type": "config",
                    "value": {
                        "type": "workflow",
                        "workflowId": notion_internal_id,
                        "isCustomAgent": True,
                        "isCustomAgentBuilder": False,
                        "useCustomAgentDraft": True,
                        "use_draft_actor_pointer": False,
                        "enableAgentAutomations": True,
                        "enableAgentIntegrations": True,
                        "enableCustomAgents": True,
                        "enableAgentDiffs": True,
                        "enableAgentCreateDbTemplate": True,
                        "enableCsvAttachmentSupport": True,
                        "enableScriptAgent": True,
                        "enableScriptAgentSlack": True,
                        "enableScriptAgentCalendar": True,
                        "enableScriptAgentCustomAgentTools": True,
                        "enableScriptAgentCustomToolCalling": True,
                        "enableCreateAndRunThread": True,
                        "enableQueryMail": True,
                        "enableMailExplicitToolCalls": True,
                        "enableUpdatePageAutofixer": True,
                        "enableUpdatePageOrderUpdates": True,
                        "enableAgentSupportPropertyReorder": True,
                        "enableAgentCardCustomization": True,
                        "useRulePrioritization": True,
                        "useWebSearch": True,
                        "availableConnectors": available_connectors,
                        "customConnectorNames": [],
                        "searchScopes": [{"type": "everything"}],
                        "modelFromUser": False,
                    },
                },
                "parent_id": thread_id,
                "parent_table": "thread",
                "space_id": space_id,
                "created_time": now_ms,
                "created_by_id": user_id,
                "created_by_table": "notion_user",
            },
        },
        # 3. Context message — user/space info
        {
            "pointer": _msg_pointer(context_msg_id),
            "path": [],
            "command": "set",
            "args": {
                "id": context_msg_id,
                "version": 1,
                "step": {
                    "id": context_msg_id,
                    "type": "context",
                    "value": {
                        "timezone": "America/New_York",
                        "userName": user_name,
                        "userId": user_id,
                        "userEmail": user_email,
                        "spaceName": space_name,
                        "spaceId": space_id,
                        "spaceViewId": space_view_id,
                        "currentDatetime": now_iso,
                        "surface": "workflows",
                        "workflowId": notion_internal_id,
                    },
                },
                "parent_id": thread_id,
                "parent_table": "thread",
                "space_id": space_id,
                "created_time": now_ms,
                "created_by_id": user_id,
                "created_by_table": "notion_user",
            },
        },
        # 4. Title message
        {
            "pointer": _msg_pointer(title_msg_id),
            "path": [],
            "command": "set",
            "args": {
                "id": title_msg_id,
                "version": 1,
                "step": {
                    "id": title_msg_id,
                    "type": "title",
                    "value": title,
                },
                "parent_id": thread_id,
                "parent_table": "thread",
                "space_id": space_id,
                "created_time": now_ms,
                "created_by_id": user_id,
                "created_by_table": "notion_user",
            },
        },
        # 5. Link messages to thread
        {
            "pointer": {"table": "thread", "id": thread_id, "spaceId": space_id},
            "path": ["messages"],
            "command": "listAfterMulti",
            "args": {
                "ids": [config_msg_id, context_msg_id, title_msg_id],
            },
        },
    ]

    # Also set thread title in data
    ops.append({
        "pointer": {"table": "thread", "id": thread_id, "spaceId": space_id},
        "path": ["data"],
        "command": "update",
        "args": {"title": title},
    })

    send_ops(space_id, ops, token_v2, user_id,
             user_action="WorkflowActions.addTranscriptToNewThread")
    return thread_id


def send_agent_message(thread_id: str, space_id: str, notion_internal_id: str, content: str,
                       token_v2: str, user_id: str | None = None,
                       model: str = "avocado-froyo-medium",
                       dry_run: bool = False) -> str:
    msg_id = str(uuid.uuid4())
    trace_id = str(uuid.uuid4())
    now_ms = int(time.time() * 1000)

    now_dt = datetime.fromtimestamp(now_ms/1000).astimezone()
    offset_str = now_dt.strftime("%z")
    offset_formatted = f"{offset_str[:3]}:{offset_str[3:]}"
    now_iso = now_dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + offset_formatted

    msg_args = {
        "id": msg_id,
        "version": 1,
        "step": {
            "id": msg_id,
            "type": "user",
            "value": [[content]],
            "userId": user_id,
            "createdAt": now_iso
        },
        "parent_id": thread_id,
        "parent_table": "thread",
        "space_id": space_id,
        "created_time": now_ms,
        "created_by_id": user_id,
        "created_by_table": "notion_user"
    }

    ops = [
        {"pointer": {"table": "thread_message", "id": msg_id, "spaceId": space_id}, "command": "set", "path": [], "args": msg_args},
        {"pointer": {"table": "thread", "id": thread_id, "spaceId": space_id}, "command": "listAfterMulti", "path": ["messages"], "args": {"ids": [msg_id]}},
        {"pointer": {"table": "thread", "id": thread_id, "spaceId": space_id}, "command": "update", "path": [], "args": {"updated_time": now_ms, "updated_by_id": user_id, "updated_by_table": "notion_user"}}
    ]

    send_ops(space_id, ops, token_v2, user_id, user_action="WorkflowActions.addStepsToExistingThreadAndRun", dry_run=dry_run)

    if dry_run:
        return msg_id

    inference_payload = {
        "traceId": trace_id,
        "spaceId": space_id,
        "threadId": thread_id,
        "createThread": False,
        "generateTitle": True,
        "threadType": "workflow",
        "isPartialTranscript": True,
        "asPatchResponse": True,
        "saveAllThreadOperations": True,
        "setUnreadState": True,
        "isUserInAnySalesAssistedSpace": False,
        "isSpaceSalesAssisted": False,
        "debugOverrides": {"emitAgentSearchExtractedResults": True, "cachedInferences": {}, "annotationInferences": {}, "emitInferences": False},
        "transcript": [
            {
                "id": str(uuid.uuid4()),
                "type": "config",
                "value": {
                    "type": "workflow",
                    "enableAgentAutomations": True,
                    "enableAgentIntegrations": True,
                    "enableCustomAgents": True,
                    "enableExperimentalIntegrations": False,
                    "enableAgentViewNotificationsTool": False,
                    "enableAgentDiffs": True,
                    "enableAgentCreateDbTemplate": True,
                    "enableCsvAttachmentSupport": True,
                    "enableDatabaseAgents": False,
                    "enableAgentThreadTools": False,
                    "enableRunAgentTool": False,
                    "enableAgentDashboards": False,
                    "enableAgentCardCustomization": True,
                    "enableSystemPromptAsPage": False,
                    "enableUserSessionContext": False,
                    "enableScriptAgentAdvanced": False,
                    "enableScriptAgent": True,
                    "enableScriptAgentSearchConnectorsInCustomAgent": False,
                    "enableScriptAgentGoogleDriveInCustomAgent": False,
                    "enableScriptAgentSlack": True,
                    "enableScriptAgentMcpServers": False,
                    "enableScriptAgentMail": False,
                    "enableScriptAgentCalendar": True,
                    "enableScriptAgentCustomAgentTools": True,
                    "enableScriptAgentCustomToolCalling": False,
                    "enableCreateAndRunThread": True,
                    "enableSpeculativeSearch": False,
                    "enableQueryCalendar": False,
                    "enableQueryMail": True,
                    "enableMailExplicitToolCalls": True,
                    "enableAgentVerification": False,
                    "useRulePrioritization": True,
                    "workflowId": notion_internal_id,
                    "availableConnectors": ["notion-mail", "notion-calendar", "github", "linear"],
                    "searchScopes": [{"type": "everything"}],
                    "useSearchToolV2": False,
                    "useWebSearch": True,
                    "useReadOnlyMode": False,
                    "writerMode": False,
                    "model": model,
                    "modelFromUser": False,
                    "isCustomAgent": True,
                    "isCustomAgentBuilder": True,
                    "useCustomAgentDraft": True,
                    "use_draft_actor_pointer": False,
                    "enableUpdatePageAutofixer": True,
                    "enableMarkdownVNext": False,
                    "enableUpdatePageOrderUpdates": True,
                    "enableAgentSupportPropertyReorder": True,
                    "useServerUndo": True,
                    "databaseAgentConfigMode": False,
                    "isOnboardingAgent": False
                }
            },
            {
                "id": str(uuid.uuid4()),
                "type": "context",
                "value": {
                    "userId": user_id,
                    "spaceId": space_id,
                    "surface": "workflows",
                    "timezone": "America/New_York",
                    "userName": "Sam Scarrow",
                    "spaceName": "Sam Scarrow's Notion",
                    "userEmail": "sscarrow@gmail.com",
                    "workflowId": notion_internal_id,
                    "currentDatetime": now_iso,
                }
            },
            {
                "id": msg_id,
                "type": "user",
                "value": [[content]],
                "userId": user_id,
                "createdAt": now_iso
            }
        ],
    }
    
    try:
        _post("runInferenceTranscript", inference_payload, token_v2, user_id)
    except json.JSONDecodeError:
        pass

    return msg_id


def wait_for_agent_response(thread_id: str, after_msg_id: str,
                             token_v2: str, user_id: str | None = None,
                             timeout: int = 120, poll_interval: int = 3) -> str | None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        time.sleep(poll_interval)
        try:
            conv = get_thread_conversation(thread_id, token_v2, user_id)
        except Exception:
            continue
        turns = conv.get("turns") or conv.get("messages") or []
        if not turns:
            continue
        found_user = False
        for turn in turns:
            turn_id = turn.get("msgId") or turn.get("id") or turn.get("messageId")
            if turn_id == after_msg_id:
                found_user = True
                continue
            if found_user and turn.get("role") in ("assistant", "agent"):
                content = turn.get("content") or turn.get("text") or ""
                if isinstance(content, list):
                    content = "\n".join(str(c) for c in content)
                if content.strip():
                    return content.strip()
    return None
