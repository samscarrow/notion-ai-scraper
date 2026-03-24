#!/usr/bin/env python3
"""
webhook_receiver.py — FastAPI receiver for GitHub and Notion webhooks.

Routes:
  POST /github-return    — GitHub issues/PR closed → update Notion Work Item
  POST /notion-dispatch  — Notion automation (Dispatch Requested checked) → dispatch to OpenClaw

Installation:
  pip install fastapi uvicorn httpx
  (httpx required for outbound OpenClaw call)

Usage:
  uvicorn cli.webhook_receiver:app --host 0.0.0.0 --port 8000

Environment variables:
  GITHUB_WEBHOOK_SECRET   — GitHub webhook HMAC secret
  NOTION_WEBHOOK_SECRET   — Notion automation webhook HMAC secret
  NOTION_TOKEN            — Notion integration token (for dispatch.py)
  OPENCLAW_HOOK_URL       — Full URL of OpenClaw /hooks/agent endpoint
  OPENCLAW_HOOK_TOKEN     — Bearer token for OpenClaw hook auth (optional)
"""

import os
import hmac
import hashlib
import json
import uuid
import logging
from fastapi import FastAPI, Request, HTTPException, Header
from . import github_return
from . import notion_api
from . import dispatch

logger = logging.getLogger(__name__)

app = FastAPI()

# ── Secrets ──────────────────────────────────────────────────────────────────

GITHUB_WEBHOOK_SECRET = os.environ.get("GITHUB_WEBHOOK_SECRET")
NOTION_WEBHOOK_SECRET = os.environ.get("NOTION_WEBHOOK_SECRET")
OPENCLAW_HOOK_URL = os.environ.get("OPENCLAW_HOOK_URL")
OPENCLAW_HOOK_TOKEN = os.environ.get("OPENCLAW_HOOK_TOKEN")


def _verify_hmac(payload: bytes, signature: str | None, secret: str | None) -> bool:
    """Verify sha256=<hex> HMAC signature. Passes if secret not configured."""
    if not secret:
        return True
    if not signature:
        return False
    mac = hmac.new(secret.encode(), msg=payload, digestmod=hashlib.sha256)
    expected = "sha256=" + mac.hexdigest()
    return hmac.compare_digest(expected, signature)


# ── Legacy alias kept for backward compatibility ──────────────────────────────

def verify_signature(payload: bytes, signature: str):
    """Verify that the webhook comes from GitHub."""
    return _verify_hmac(payload, signature, GITHUB_WEBHOOK_SECRET)

@app.post("/github-return")
async def github_webhook(
    request: Request,
    x_github_event: str = Header(None),
    x_hub_signature_256: str = Header(None)
):
    payload_bytes = await request.body()
    
    if not verify_signature(payload_bytes, x_hub_signature_256):
        raise HTTPException(status_code=401, detail="Invalid signature")

    payload = json.loads(payload_bytes)
    
    # Handle Issue Closed
    if x_github_event == "issues" and payload.get("action") == "closed":
        issue = payload["issue"]
        url = issue["html_url"]
        summary = f"Issue #{issue['number']} closed by {payload['sender']['login']}."
        return _handle_issue_closed(url, summary)

    # Handle PR Merged (closed with merged=true)
    if x_github_event == "pull_request" and payload.get("action") == "closed":
        pr = payload["pull_request"]
        if pr.get("merged"):
            url = pr["html_url"]
            summary = f"PR #{pr['number']} merged by {payload['sender']['login']}."
            return process_return(url, summary)

    # Handle issue comment → Prompt Notes (convention: ## Dispatch Prompt)
    if x_github_event == "issue_comment" and payload.get("action") == "created":
        comment = payload.get("comment", {})
        body = comment.get("body", "")
        if body.lstrip().startswith("## Dispatch Prompt"):
            issue = payload["issue"]
            url = issue["html_url"]
            return _handle_prompt_comment(url, body)

    return {"status": "ignored", "reason": "event_not_handled"}

def _handle_issue_closed(url: str, summary: str):
    """Issue-closed path with dedup guard.

    If the PR merge handler already ran first it will have set Status to
    'Awaiting Intake' while preserving GitHub Issue URL. In that case the Work
    Item won't be found (URL cleared) OR it will be found but already in
    the target state — either way skip to avoid a double audit-log entry
    and a redundant Notion write.
    """
    try:
        token = os.environ.get("NOTION_TOKEN")
        if not token:
            raise RuntimeError("NOTION_TOKEN environment variable required")
        client = notion_api.NotionAPIClient(token)

        work_item = github_return.find_work_item_by_url(client, url)
        if not work_item:
            return {"status": "error", "reason": "work_item_not_found", "url": url}

        current_status = (
            work_item.get("properties", {})
            .get("Status", {})
            .get("status", {})
            .get("name")
        )
        if current_status == "Awaiting Intake":
            logger.info("Skipping issue_closed for %s — already Awaiting Intake (PR merge handled it)", url)
            return {"status": "skipped", "reason": "already_awaiting_intake", "work_item_id": work_item["id"]}

        github_return.perform_return(client, work_item["id"], summary)
        return {"status": "success", "work_item_id": work_item["id"]}
    except Exception as e:
        logger.error("Error in _handle_issue_closed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


PROMPT_MARKER = "## Dispatch Prompt"


def _handle_prompt_comment(issue_url: str, comment_body: str):
    """Copy a '## Dispatch Prompt' comment into the Work Item's Prompt Notes.

    Latest matching comment wins — each delivery overwrites the previous value.
    Only comments whose body starts with '## Dispatch Prompt' are picked up;
    regular discussion comments are ignored.
    """
    try:
        token = os.environ.get("NOTION_TOKEN")
        if not token:
            raise RuntimeError("NOTION_TOKEN environment variable required")
        client = notion_api.NotionAPIClient(token)

        work_item = github_return.find_work_item_by_url(client, issue_url)
        if not work_item:
            return {"status": "error", "reason": "work_item_not_found", "url": issue_url}

        # Notion rich_text has a 2000-char limit per segment
        prompt_text = comment_body
        segments = []
        while prompt_text:
            segments.append({"type": "text", "text": {"content": prompt_text[:2000]}})
            prompt_text = prompt_text[2000:]

        client.update_page(work_item["id"], properties={
            "Prompt Notes": {"rich_text": segments},
        })

        logger.info("Wrote Prompt Notes for %s from issue %s", work_item["id"], issue_url)
        return {"status": "prompt_written", "work_item_id": work_item["id"]}
    except Exception as e:
        logger.error("Error in _handle_prompt_comment: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


def process_return(url: str, summary: str):
    """Bridge to the return logic."""
    try:
        token = os.environ.get("NOTION_TOKEN")
        if not token:
            raise RuntimeError("NOTION_TOKEN environment variable required")
        client = notion_api.NotionAPIClient(token)
        
        work_item = github_return.find_work_item_by_url(client, url)
        if not work_item:
            return {"status": "error", "reason": "work_item_not_found", "url": url}
        
        github_return.perform_return(client, work_item["id"], summary)
        return {"status": "success", "work_item_id": work_item["id"]}
    except Exception as e:
        print(f"Error processing return: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ── Notion dispatch webhook ──────────────────────────────────────────────────

@app.post("/notion-dispatch")
async def notion_dispatch_webhook(
    request: Request,
    x_notion_signature: str = Header(None),
):
    """Receive Notion automation webhook when 'Dispatch Requested' is checked.

    Expected automation body template (configure in Notion UI):
        {"page_id": "{{page.id}}"}

    On success: builds+validates dispatch packet, stamps consumed, forwards to OpenClaw.
    Returns 200 even on validation failure so Notion does not retry a bad Work Item.
    """
    payload_bytes = await request.body()

    if not _verify_hmac(payload_bytes, x_notion_signature, NOTION_WEBHOOK_SECRET):
        raise HTTPException(status_code=401, detail="Invalid Notion signature")

    try:
        payload = json.loads(payload_bytes)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    # Accept {"page_id": "..."} (configured template) or raw Notion event format
    raw_id = payload.get("page_id") or (payload.get("entity") or {}).get("id")
    if not raw_id:
        return {"status": "ignored", "reason": "no_page_id"}

    # Normalize to hyphenated UUID
    try:
        page_id = str(uuid.UUID(str(raw_id).replace("-", "")))
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid page_id: {raw_id!r}")

    # Build and validate dispatch packet (V1-V12)
    try:
        result = dispatch.build_dispatch_packet(page_id)
    except Exception as e:
        logger.error("build_dispatch_packet(%s) failed: %s", page_id, e)
        raise HTTPException(status_code=500, detail=str(e))

    if result["errors"]:
        # Return 200 — Notion must not retry a Work Item that fails validation
        logger.warning("Dispatch validation failed for %s: %s", page_id, result["errors"])
        return {"status": "validation_failed", "page_id": page_id, "errors": result["errors"]}

    packet = result["packet"]
    run_id = packet["run_id"]

    # Stamp consumed: set In Progress + run_id + consumed_at
    try:
        dispatch.stamp_dispatch_consumed(page_id, run_id)
    except Exception as e:
        logger.error("stamp_dispatch_consumed(%s, %s) failed: %s", page_id, run_id, e)
        raise HTTPException(status_code=500, detail=str(e))

    # Forward packet to OpenClaw to spawn the lane agent
    openclaw_result: str | int = "skipped (OPENCLAW_HOOK_URL not configured)"
    if OPENCLAW_HOOK_URL:
        try:
            import httpx
            headers = {"Content-Type": "application/json"}
            if OPENCLAW_HOOK_TOKEN:
                headers["Authorization"] = f"Bearer {OPENCLAW_HOOK_TOKEN}"
            resp = httpx.post(
                OPENCLAW_HOOK_URL,
                json={"packet": packet},
                headers=headers,
                timeout=10.0,
            )
            openclaw_result = resp.status_code
            if resp.status_code >= 400:
                logger.warning(
                    "OpenClaw hook returned %s for run_id=%s: %s",
                    resp.status_code, run_id, resp.text[:500],
                )
        except Exception as e:
            # Stamp already written — log and return degraded success rather than 500
            logger.error("OpenClaw hook call failed for run_id=%s: %s", run_id, e)
            openclaw_result = f"error: {e}"

    logger.info(
        "Dispatched %s (run_id=%s, lane=%s, openclaw=%s)",
        page_id, run_id, packet.get("execution_lane"), openclaw_result,
    )
    return {
        "status": "dispatched",
        "page_id": page_id,
        "run_id": run_id,
        "lane": packet.get("execution_lane"),
        "work_item_name": packet.get("work_item_name"),
        "openclaw": openclaw_result,
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
