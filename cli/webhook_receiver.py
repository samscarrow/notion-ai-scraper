#!/usr/bin/env python3
"""
webhook_receiver.py — Minimalist FastAPI receiver for GitHub webhooks.

Receives 'issues.closed' or 'pull_request.closed' events.
Uses 'cli/github_return.py' logic to update Notion.

Installation:
  pip install fastapi uvicorn cryptography
  (cryptography is for webhook signature verification)

Usage:
  uvicorn cli.webhook_receiver:app --host 0.0.0.0 --port 8000
"""

import os
import hmac
import hashlib
import json
from fastapi import FastAPI, Request, HTTPException, Header
from . import github_return
from . import notion_api

app = FastAPI()

# GitHub Webhook Secret (Set in Repo Settings -> Webhooks)
GITHUB_WEBHOOK_SECRET = os.environ.get("GITHUB_WEBHOOK_SECRET")

def verify_signature(payload: bytes, signature: str):
    """Verify that the webhook comes from GitHub."""
    if not GITHUB_WEBHOOK_SECRET:
        return True # Skip verification if no secret is set
    
    mac = hmac.new(GITHUB_WEBHOOK_SECRET.encode(), msg=payload, digestmod=hashlib.sha256)
    expected_signature = "sha256=" + mac.hexdigest()
    return hmac.compare_digest(expected_signature, signature)

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
        return process_return(url, summary)

    # Handle PR Merged (closed with merged=true)
    if x_github_event == "pull_request" and payload.get("action") == "closed":
        pr = payload["pull_request"]
        if pr.get("merged"):
            url = pr["html_url"]
            summary = f"PR #{pr['number']} merged by {payload['sender']['login']}."
            return process_return(url, summary)

    return {"status": "ignored", "reason": "event_not_handled"}

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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
