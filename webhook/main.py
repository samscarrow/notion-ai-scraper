import os
import hmac
import hashlib
import json
import time
import requests
import re
import logging
from datetime import datetime, timezone
from pathlib import Path
from fastapi import FastAPI, Request, HTTPException, Header, BackgroundTasks

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

from lib.validate import validate_return_payload
from lib.redact import redact, check_residual_secrets
from lib.dispatch import resolve_verdict_mapping

# Configuration from Environment
NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
PROJECTS_DATABASE_ID = os.environ.get("NOTION_PROJECTS_DATABASE_ID") or os.environ.get("NOTION_DATABASE_ID")
WORK_ITEMS_DATABASE_ID = os.environ.get("NOTION_WORK_ITEMS_DATABASE_ID", "daeb64d4-e5a8-4a7b-b0dc-7555cbc3def6")
AUDIT_LOG_DATABASE_ID = os.environ.get("NOTION_AUDIT_LOG_DATABASE_ID", "4621be9a-0709-443e-bee6-7e6166f76fae")
GITHUB_WEBHOOK_SECRET = os.environ.get("GITHUB_WEBHOOK_SECRET")
RETURN_TOKEN = os.environ.get("OPENCLAW_RETURN_TOKEN", "")
NOTION_WEBHOOK_SECRET = os.environ.get("NOTION_WEBHOOK_SECRET", "")

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}

RE_ISSUE_LINK = re.compile(r"(?:close|fixes|resolves|closes|fixed|resolved)\s+(?:#(\d+)|(https://github\.com/[\w\-\.]+/[\w\-\.]+/issues/(\d+)))", re.IGNORECASE)

# v1.1 contract config (lib/ and config/ are co-located siblings under /app in the container)
_CONFIG_DIR = Path(__file__).resolve().parent / "config"


def _load_verdict_mapping() -> dict:
    with open(_CONFIG_DIR / "verdict_mapping.json") as f:
        return json.load(f)

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _notion_request(method: str, url: str, **kwargs) -> requests.Response:
    """Notion API call with 3 retries and exponential backoff."""
    kwargs.setdefault("headers", HEADERS)
    kwargs.setdefault("timeout", 30)
    last_exc = None
    for attempt in range(3):
        try:
            resp = requests.request(method, url, **kwargs)
            resp.raise_for_status()
            return resp
        except (requests.Timeout, requests.ConnectionError) as e:
            last_exc = e
            wait = 2 ** attempt
            logger.warning("Notion request failed (attempt %d/3), retrying in %ds: %s", attempt + 1, wait, e)
            time.sleep(wait)
        except requests.HTTPError as e:
            # Don't retry 4xx — they won't succeed
            if e.response is not None and e.response.status_code < 500:
                raise
            last_exc = e
            wait = 2 ** attempt
            logger.warning("Notion HTTP error (attempt %d/3), retrying in %ds: %s", attempt + 1, wait, e)
            time.sleep(wait)
    raise RuntimeError(f"Notion request failed after 3 attempts") from last_exc

def verify_signature(payload_body: bytes, signature_header: str | None):
    if not GITHUB_WEBHOOK_SECRET: return
    if not signature_header: raise HTTPException(status_code=401, detail="X-Hub-Signature-256 missing")
    expected = "sha256=" + hmac.new(GITHUB_WEBHOOK_SECRET.encode(), msg=payload_body, digestmod=hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, signature_header): raise HTTPException(status_code=401, detail="Invalid signature")

def _find_page_by_url(database_id: str, property_name: str, url: str) -> dict | None:
    query_url = f"https://api.notion.com/v1/databases/{database_id}/query"
    payload = {"filter": {"property": property_name, "url": {"equals": url}}}
    resp = _notion_request("POST", query_url, json=payload)
    results = resp.json().get("results", [])
    return results[0] if results else None

def _create_stub_work_item(issue_url: str, title: str, body: str):
    """Create a new Work Item in Notion from a GitHub Issue."""
    payload = {
        "parent": {"database_id": WORK_ITEMS_DATABASE_ID},
        "properties": {
            "Item Name": {"title": [{"text": {"content": title}}]},
            "GitHub Issue URL": {"url": issue_url},
            "Status": {"status": {"name": "Not Started"}},
            "Objective": {"rich_text": [{"text": {"content": body[:2000]}}]}
        }
    }
    resp = _notion_request("POST", "https://api.notion.com/v1/pages", json=payload)
    return resp.json()["id"]

def _get_status_name(work_item: dict) -> str | None:
    """Extract current status name from a Work Item API result."""
    try:
        return work_item["properties"]["Status"]["status"]["name"]
    except (KeyError, TypeError):
        return None

def _update_work_item_complete(page_id: str, summary: str, from_status: str | None = None):
    """Update Work Item status and signal the Librarian directly.

    Bypasses the Intake Clerk — sets Librarian Request Received At alongside
    Return Received At so the Librarian fires without an intermediate agent hop.
    """
    update_url = f"https://api.notion.com/v1/pages/{page_id}"
    ts = now_iso()

    payload = {
        "properties": {
            "Status": {"status": {"name": "Done"}},
            "Return Received At": {"date": {"start": ts}},
            "Return Consumed At": {"date": {"start": ts}},
            "Librarian Request Received At": {"date": {"start": ts}},
            "Outcome": {"rich_text": [{"text": {"content": summary[:2000]}}]},
        }
    }
    _notion_request("PATCH", update_url, json=payload)

    # Append summary block
    children_url = f"https://api.notion.com/v1/blocks/{page_id}/children"
    children_payload = {
        "children": [
            {"object": "block", "type": "heading_3", "heading_3": {"rich_text": [{"text": {"content": "GitHub Return Summary"}}]}},
            {"object": "block", "type": "paragraph", "paragraph": {"rich_text": [{"text": {"content": summary}}]}}
        ]
    }
    _notion_request("PATCH", children_url, json=children_payload)

    # Log to Audit Log
    if AUDIT_LOG_DATABASE_ID:
        try:
            transition = f"{from_status}→Done" if from_status else "→Done"
            audit_props = {
                "Transition": {"title": [{"text": {"content": transition}}]},
                "Work Item": {"relation": [{"id": page_id}]},
                "Agent": {"select": {"name": "Webhook Bridge"}},
                "To Status": {"select": {"name": "Done"}},
                "Consumption Timestamp": {"date": {"start": ts}},
            }
            if from_status:
                audit_props["From Status"] = {"select": {"name": from_status}}
            _notion_request("POST", "https://api.notion.com/v1/pages", json={
                "parent": {"database_id": AUDIT_LOG_DATABASE_ID},
                "properties": audit_props,
            })
        except Exception as e:
            logger.error("Audit log write failed for db %s: %s", AUDIT_LOG_DATABASE_ID, e)

def _execute_return_protocol(project_id: str, nexus_url: str):
    """Clear project lock via Return Protocol Agent trigger."""
    update_url = f"https://api.notion.com/v1/pages/{project_id}"
    update_payload = {
        "properties": {
            "Active GitHub Issue": {"url": None},
            "Next Action": {"rich_text": [{"text": {"content": f"Factory implementation merged for {nexus_url}. Check Librarian synthesis."}}]}
        }
    }
    _notion_request("PATCH", update_url, json=update_payload)

def _handle_issue_closed(issue_url: str, issue_number: int, sender: str):
    """Background task: process a closed issue and update Notion."""
    summary = f"Issue #{issue_number} closed by {sender}."
    try:
        work_item = _find_page_by_url(WORK_ITEMS_DATABASE_ID, "GitHub Issue URL", issue_url)
        if not work_item:
            logger.info("No Work Item found for %s — skipping", issue_url)
            return
        page_id = work_item["id"]
        from_status = _get_status_name(work_item)
        if from_status == "Awaiting Intake":
            logger.info("Work Item %s already Awaiting Intake — PR merge handled it, skipping issue close", issue_url)
            return
        _update_work_item_complete(page_id, summary, from_status=from_status)
        project_rel = work_item["properties"].get("Project", {}).get("relation", [])
        if project_rel:
            _execute_return_protocol(project_rel[0]["id"], issue_url)
        logger.info("Return complete for %s (from_status=%s)", issue_url, from_status)
    except Exception as e:
        logger.error("_handle_issue_closed failed for %s: %s", issue_url, e)

def _handle_issue_reopened(issue_url: str, issue_number: int):
    """Background task: reset Work Item to In Progress when a GH issue is reopened."""
    try:
        work_item = _find_page_by_url(WORK_ITEMS_DATABASE_ID, "GitHub Issue URL", issue_url)
        if not work_item:
            logger.info("No Work Item found for reopened issue %s — skipping", issue_url)
            return
        page_id = work_item["id"]
        ts = now_iso()
        _notion_request("PATCH", f"https://api.notion.com/v1/pages/{page_id}", json={
            "properties": {
                "Status": {"status": {"name": "In Progress"}},
                "Return Received At": {"date": None},
                "Return Consumed At": {"date": None},
            }
        })
        if AUDIT_LOG_DATABASE_ID:
            try:
                from_status = _get_status_name(work_item)
                transition = f"{from_status}→InProgress" if from_status else "→InProgress"
                _notion_request("POST", "https://api.notion.com/v1/pages", json={
                    "parent": {"database_id": AUDIT_LOG_DATABASE_ID},
                    "properties": {
                        "Transition": {"title": [{"text": {"content": transition}}]},
                        "Work Item": {"relation": [{"id": page_id}]},
                        "Agent": {"select": {"name": "Webhook Bridge"}},
                        "To Status": {"select": {"name": "In Progress"}},
                        "Consumption Timestamp": {"date": {"start": ts}},
                        **({"From Status": {"select": {"name": from_status}}} if from_status else {}),
                    },
                })
            except Exception as e:
                logger.error("Audit log write failed for reopened issue %s: %s", issue_url, e)
        logger.info("Reset Work Item to In Progress for reopened issue %s", issue_url)
    except Exception as e:
        logger.error("_handle_issue_reopened failed for %s: %s", issue_url, e)

def _handle_pr_merged(pr_url: str, pr_body: str, repo_url: str):
    """Background task: process a merged PR and update linked Notion Work Items."""
    linked_issue_urls = []
    for m in RE_ISSUE_LINK.findall(pr_body):
        issue_num, full_url, url_num = m
        if full_url:
            linked_issue_urls.append(full_url)
        elif issue_num:
            linked_issue_urls.append(f"{repo_url}/issues/{issue_num}")

    if not linked_issue_urls:
        logger.info("PR %s merged but no linked issues found", pr_url)
        return

    for nexus_url in linked_issue_urls:
        try:
            work_item = _find_page_by_url(WORK_ITEMS_DATABASE_ID, "GitHub Issue URL", nexus_url)
            project_id = None
            if work_item:
                from_status = _get_status_name(work_item)
                _update_work_item_complete(work_item["id"], f"Completed by Merge of PR: {pr_url}", from_status=from_status)
                project_rel = work_item["properties"].get("Project", {}).get("relation", [])
                if project_rel:
                    project_id = project_rel[0]["id"]
            if not project_id and PROJECTS_DATABASE_ID:
                project_page = _find_page_by_url(PROJECTS_DATABASE_ID, "Active GitHub Issue", nexus_url)
                if project_page:
                    project_id = project_page["id"]
            if project_id:
                _execute_return_protocol(project_id, nexus_url)
            logger.info("PR merge return complete for %s", nexus_url)
        except Exception as e:
            logger.error("_handle_pr_merged failed for %s: %s", nexus_url, e)

@app.post("/webhook")
@app.post("/github-return")
async def github_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    x_github_event: str = Header(None),
    x_hub_signature_256: str = Header(None)
):
    payload_body = await request.body()
    verify_signature(payload_body, x_hub_signature_256)
    payload = await request.json()
    event = x_github_event or request.headers.get("X-GitHub-Event", "")
    action = payload.get("action", "")

    logger.info("Received event=%s action=%s", event, action)

    # 1. HANDLE ISSUES (Opened -> Stub, Closed -> Return)
    if event == "issues":
        issue = payload["issue"]
        url = issue["html_url"]

        if action == "opened":
            logger.info("Issue opened %s — no stub created (Lab is Notion-first)", url)
            return {"status": "ignored", "reason": "stubs_disabled"}

        if action == "closed":
            background_tasks.add_task(
                _handle_issue_closed, url, issue["number"], payload["sender"]["login"]
            )
            return {"status": "accepted", "action": "return_queued"}

        if action == "reopened":
            background_tasks.add_task(_handle_issue_reopened, url, issue["number"])
            return {"status": "accepted", "action": "reopen_queued"}

    # 2. HANDLE MERGED PRs
    if event == "pull_request" and action == "closed":
        pr = payload["pull_request"]
        if not pr.get("merged"):
            return {"status": "ignored", "reason": "PR closed without merge"}
        background_tasks.add_task(
            _handle_pr_merged,
            pr["html_url"],
            pr.get("body") or "",
            payload["repository"]["html_url"],
        )
        return {"status": "accepted", "action": "pr_merge_queued"}

    return {"status": "ignored", "reason": "event_not_handled"}


# ──────────────────────────────────────────────────────────
# v1.1 Contract: Return Ingestion Endpoint
# Accepts OpenClaw final return payloads and applies the
# Intake Clerk flow: idempotency → capture → signal.
# ──────────────────────────────────────────────────────────

def _verify_return_token(token: str | None):
    """Verify the return endpoint auth token."""
    if not RETURN_TOKEN:
        return  # No token configured — open (dev mode)
    if not token or token != RETURN_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid return token")


def _find_work_item_by_name(item_name: str) -> dict | None:
    """Find a Work Item by its Item Name (title property)."""
    query_url = f"https://api.notion.com/v1/databases/{WORK_ITEMS_DATABASE_ID}/query"
    payload = {"filter": {"property": "Item Name", "title": {"equals": item_name}}}
    resp = _notion_request("POST", query_url, json=payload)
    results = resp.json().get("results", [])
    return results[0] if results else None


def _check_idempotency(page_id: str, run_id: str) -> bool:
    """Check if this run_id has already been ingested by scanning page content."""
    blocks_url = f"https://api.notion.com/v1/blocks/{page_id}/children?page_size=100"
    try:
        resp = _notion_request("GET", blocks_url)
        for block in resp.json().get("results", []):
            if block.get("type") == "heading_3":
                texts = block.get("heading_3", {}).get("rich_text", [])
                for t in texts:
                    if run_id in t.get("text", {}).get("content", ""):
                        return True
    except Exception as e:
        logger.warning("Idempotency check failed for %s: %s", page_id, e)
    return False


def _ingest_final_return(payload: dict):
    """Process a v1.1 final return payload — the Intake Clerk flow."""
    item_name = payload["item_name"]
    run_id = payload["run_id"]
    status = payload["status"]
    ts = now_iso()

    # Find the Work Item
    work_item = _find_work_item_by_name(item_name)
    if not work_item:
        logger.error("No Work Item found for item_name=%s (run_id=%s)", item_name, run_id)
        return {"ingested": False, "reason": "work_item_not_found"}

    page_id = work_item["id"]

    # Idempotency gate: check if this run_id is already ingested
    if _check_idempotency(page_id, run_id):
        logger.info("Idempotency gate: run_id=%s already ingested for %s", run_id, item_name)
        return {"ingested": False, "reason": "duplicate_run_id"}

    # Determine Work Item type for verdict mapping
    wi_type = None
    try:
        wi_type = work_item["properties"]["Type"]["select"]["name"]
    except (KeyError, TypeError):
        wi_type = "Other"

    # Build page content blocks
    content_blocks = []

    if status == "ok":
        # Apply verdict mapping
        verdict = payload.get("verdict", "INCONCLUSIVE")
        mapping = resolve_verdict_mapping(verdict, wi_type)

        props_update = {
            "Status": {"status": {"name": mapping.get("status", "Done")}},
            "Return Received At": {"date": {"start": ts}},
            "Return Consumed At": {"date": {"start": ts}},
        }
        mapped_verdict = mapping.get("verdict")
        if mapped_verdict:
            props_update["Verdict"] = {"select": {"name": mapped_verdict}}

        outcome_text = payload.get("summary", "")
        if mapping.get("warning"):
            outcome_text = f"[WARNING: {mapping['warning']}] {outcome_text}"
        if outcome_text:
            props_update["Outcome"] = {"rich_text": [{"text": {"content": outcome_text[:2000]}}]}

        # Extract metrics if present
        metrics = payload.get("metrics")
        if metrics and isinstance(metrics, dict):
            metrics_text = json.dumps(metrics, indent=2)
            props_update["Metrics"] = {"rich_text": [{"text": {"content": metrics_text[:2000]}}]}

        # Signal Librarian
        props_update["Librarian Request Received At"] = {"date": {"start": ts}}

        _notion_request("PATCH", f"https://api.notion.com/v1/pages/{page_id}", json={"properties": props_update})

        # Append raw_output and summary to page body
        content_blocks.append({
            "object": "block", "type": "heading_3",
            "heading_3": {"rich_text": [{"text": {"content": f"Execution Result (run_id: {run_id})"}}]}
        })
        content_blocks.append({
            "object": "block", "type": "paragraph",
            "paragraph": {"rich_text": [{"text": {"content": f"Lane: {payload.get('lane', '?')} | Model: {payload.get('model', '?')} | Duration: {payload.get('duration_ms', 0)}ms | Verdict: {verdict}"}}]}
        })

        # Raw output (truncated to Notion's block limit)
        raw_output = payload.get("raw_output", "")
        if raw_output:
            chunks = [raw_output[i:i+2000] for i in range(0, min(len(raw_output), 10000), 2000)]
            content_blocks.append({
                "object": "block", "type": "toggle",
                "toggle": {
                    "rich_text": [{"text": {"content": "Raw Output"}}],
                    "children": [
                        {"object": "block", "type": "code", "code": {"rich_text": [{"text": {"content": chunk}}], "language": "plain text"}}
                        for chunk in chunks
                    ]
                }
            })

    else:
        # Error/gated/timeout path: do NOT advance status
        error_msg = payload.get("error", "Unknown error")
        content_blocks.append({
            "object": "block", "type": "heading_3",
            "heading_3": {"rich_text": [{"text": {"content": f"Execution Error (run_id: {run_id})"}}]}
        })
        content_blocks.append({
            "object": "block", "type": "callout",
            "callout": {
                "icon": {"emoji": "⚠️"},
                "rich_text": [{"text": {"content": f"Status: {status} | Lane: {payload.get('lane', '?')} | Error: {error_msg}"}}]
            }
        })

    # Append artifact section if present
    artifacts = payload.get("artifacts") or []
    files_changed = payload.get("files_changed") or []
    commit_sha = payload.get("commit_sha")
    pr_url = payload.get("pr_url")

    if artifacts or files_changed or commit_sha or pr_url:
        artifact_lines = []
        if commit_sha:
            artifact_lines.append(f"Commit: {commit_sha}")
        if pr_url:
            artifact_lines.append(f"PR: {pr_url}")
        for a in artifacts:
            artifact_lines.append(f"[{a.get('type', 'file')}] {a.get('path_or_url', '')} — {a.get('description', '')}")
        if files_changed:
            artifact_lines.append(f"Files changed: {', '.join(files_changed[:20])}")

        content_blocks.append({
            "object": "block", "type": "heading_3",
            "heading_3": {"rich_text": [{"text": {"content": "Artifacts"}}]}
        })
        content_blocks.append({
            "object": "block", "type": "paragraph",
            "paragraph": {"rich_text": [{"text": {"content": "\n".join(artifact_lines)[:2000]}}]}
        })

    # Write content blocks to page
    if content_blocks:
        _notion_request("PATCH", f"https://api.notion.com/v1/blocks/{page_id}/children", json={"children": content_blocks})

    # Audit log entry
    if AUDIT_LOG_DATABASE_ID:
        try:
            from_status = _get_status_name(work_item)
            to_status = "Done" if status == "ok" else from_status or "In Progress"
            _notion_request("POST", "https://api.notion.com/v1/pages", json={
                "parent": {"database_id": AUDIT_LOG_DATABASE_ID},
                "properties": {
                    "Transition": {"title": [{"text": {"content": f"v1.1 Return: {item_name} ({status})"}}]},
                    "Work Item": {"relation": [{"id": page_id}]},
                    "Agent": {"select": {"name": "Intake Clerk (v1.1)"}},
                    "To Status": {"select": {"name": to_status}},
                    "Consumption Timestamp": {"date": {"start": ts}},
                    **({"From Status": {"select": {"name": from_status}}} if from_status else {}),
                },
            })
        except Exception as e:
            logger.error("Audit log write failed for %s: %s", item_name, e)

    logger.info("Ingested v1.1 return: item=%s run_id=%s status=%s", item_name, run_id, status)
    return {"ingested": True, "item_name": item_name, "run_id": run_id, "status": status}


@app.post("/return")
async def openclaw_return(
    request: Request,
    background_tasks: BackgroundTasks,
    x_openclaw_token: str = Header(None),
):
    """v1.1 Return ingestion endpoint.

    Accepts a final return payload from OpenClaw and processes it
    through the Intake Clerk flow.
    """
    _verify_return_token(x_openclaw_token)
    payload = await request.json()

    # Validate against contract schema
    err = validate_return_payload(payload)
    if err:
        raise HTTPException(status_code=422, detail=err.to_dict())

    # Check for residual secrets in raw_output (audit safety net)
    residual = check_residual_secrets(payload.get("raw_output", ""))
    if residual:
        logger.warning("Residual secrets detected in return for %s: %s", payload.get("item_name"), residual)
        payload["raw_output"] = redact(payload["raw_output"])

    background_tasks.add_task(_ingest_final_return, payload)
    return {"status": "accepted", "run_id": payload.get("run_id")}


@app.post("/progress")
async def openclaw_progress(
    request: Request,
    x_openclaw_token: str = Header(None),
):
    """v1.1 Progress event endpoint.

    Accepts started/heartbeat/checkpoint events. Non-final events
    are informational only — they do not advance Work Item status.
    """
    _verify_return_token(x_openclaw_token)
    payload = await request.json()
    event_type = payload.get("event_type", "")

    if event_type == "final":
        raise HTTPException(status_code=400, detail="Final events must use /return endpoint")

    logger.info(
        "Progress event: item=%s run_id=%s type=%s msg=%s",
        payload.get("item_name"), payload.get("run_id"),
        event_type, payload.get("message", "")[:100],
    )
    return {"status": "ack", "event_type": event_type}


# ──────────────────────────────────────────────────────────
# Notion Webhook → Dispatch Poller Trigger
# Receives page.properties_updated events and triggers the
# dispatch-poller agent via the OpenClaw gateway. The poller
# handles packet building, consumption, and execution.
# ──────────────────────────────────────────────────────────

OPENCLAW_GATEWAY_URL = os.environ.get("OPENCLAW_GATEWAY_URL", "http://openclaw:18789")
OPENCLAW_GATEWAY_TOKEN = os.environ.get("OPENCLAW_GATEWAY_TOKEN", "")


def _verify_notion_signature(payload_bytes: bytes, signature: str | None) -> bool:
    """Verify X-Notion-Signature (sha256=<hex>). Passes if secret not configured."""
    if not NOTION_WEBHOOK_SECRET:
        return True
    if not signature:
        return False
    expected = "sha256=" + hmac.new(NOTION_WEBHOOK_SECRET.encode(), msg=payload_bytes, digestmod=hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)


def _trigger_dispatch_poller(reason: str):
    """Trigger the dispatch-poller via OpenClaw gateway webhook."""
    try:
        resp = requests.post(
            f"{OPENCLAW_GATEWAY_URL}/hooks/agent",
            json={
                "message": f"Dispatch trigger: {reason}. Run your heartbeat now.",
                "agentId": "dispatch-poller",
                "name": "notion-webhook",
                "wakeMode": "now",
            },
            headers={"Content-Type": "application/json", "x-openclaw-token": OPENCLAW_GATEWAY_TOKEN},
            timeout=10,
        )
        logger.info("Triggered dispatch-poller (%s): %s", resp.status_code, reason)
    except Exception as e:
        logger.error("Failed to trigger dispatch-poller: %s", e)


@app.post("/notion-dispatch")
async def notion_dispatch_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    x_notion_signature: str = Header(None),
):
    """Notion webhook — signal relay to the dispatch-poller.

    Flow 1 (verification): Notion sends {"verification_token": "..."} during setup.
    Flow 2 (event): page.properties_updated → trigger dispatch-poller heartbeat.
    """
    payload_bytes = await request.body()

    try:
        payload = json.loads(payload_bytes)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    # ── Subscription verification (one-time setup) ───────────────────
    if "verification_token" in payload and "type" not in payload:
        token = payload["verification_token"]
        logger.info("Notion webhook verification token: %s", token)
        return {"status": "verification_received", "token": token}

    # ── Signature verification on real events ────────────────────────
    if not _verify_notion_signature(payload_bytes, x_notion_signature):
        raise HTTPException(status_code=401, detail="Invalid Notion signature")

    # ── Filter event type ────────────────────────────────────────────
    event_type = payload.get("type", "")
    if event_type and event_type != "page.properties_updated":
        return {"status": "ignored", "reason": f"event_type={event_type}"}

    # ── Trigger the dispatch-poller ──────────────────────────────────
    page_id = (payload.get("entity") or {}).get("id", "unknown")
    background_tasks.add_task(_trigger_dispatch_poller, f"page {page_id}")
    return {"status": "triggered", "page_id": page_id}


@app.get("/health")
async def health(): return {"ok": True}


@app.api_route("/github", methods=["GET", "POST", "PUT", "PATCH"])
async def unknown_github_route(request: Request):
    """Catch requests to /github and log headers for provenance analysis."""
    headers = {k: v for k, v in request.headers.items()
               if k.lower().startswith("x-github") or k.lower() in ("user-agent", "x-forwarded-for")}
    logger.warning("Unknown /github request: method=%s headers=%s", request.method, headers)
    return {"status": "not_found", "hint": "Use /webhook for GitHub webhooks"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
