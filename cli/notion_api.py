"""
notion_api.py — Minimal public Notion API client with retries.

This is separate from notion_client.py because the existing module targets
Notion's private `/api/v3` endpoints for workflow content, while cycle_bridge
needs the official `/v1` API for database-style page creation.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from typing import Any

BASE_URL = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"
MAX_RETRIES = 3
BACKOFF_BASE = 1
RICH_TEXT_LIMIT = 1900
APPEND_BLOCK_LIMIT = 100


def split_rich_text(text: str, limit: int = RICH_TEXT_LIMIT) -> list[str]:
    if not text:
        return [""]
    return [text[i : i + limit] for i in range(0, len(text), limit)]


def paragraph_block(text: str) -> dict[str, Any]:
    return {
        "object": "block",
        "type": "paragraph",
        "paragraph": {
            "rich_text": [{"type": "text", "text": {"content": chunk}} for chunk in split_rich_text(text)]
        },
    }


def heading_block(kind: str, text: str) -> dict[str, Any]:
    return {
        "object": "block",
        "type": kind,
        kind: {
            "rich_text": [{"type": "text", "text": {"content": chunk}} for chunk in split_rich_text(text)]
        },
    }


def code_block(text: str, language: str = "plain text") -> dict[str, Any]:
    return {
        "object": "block",
        "type": "code",
        "code": {
            "language": language,
            "rich_text": [{"type": "text", "text": {"content": chunk}} for chunk in split_rich_text(text)],
        },
    }


class NotionAPIClient:
    def __init__(self, token: str) -> None:
        self.token = token

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "Notion-Version": NOTION_VERSION,
        }

    def _request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        body = json.dumps(payload).encode() if payload is not None else None
        last_err: Exception | None = None

        for attempt in range(MAX_RETRIES):
            try:
                req = urllib.request.Request(
                    f"{BASE_URL}/{path}",
                    data=body,
                    headers=self._headers(),
                    method=method,
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    data = resp.read()
                    return json.loads(data) if data else {}
            except urllib.error.HTTPError as exc:
                status = exc.code
                body_text = exc.read().decode(errors="replace")
                if status >= 500:
                    last_err = exc
                    time.sleep(BACKOFF_BASE * (2**attempt))
                    continue
                raise RuntimeError(f"Notion API error {status}: {body_text}") from exc
            except urllib.error.URLError as exc:
                last_err = exc
                time.sleep(BACKOFF_BASE * (2**attempt))

        raise RuntimeError(f"Notion API request failed after {MAX_RETRIES} attempts: {last_err}")

    def create_page(self, parent: dict[str, Any], properties: dict[str, Any]) -> dict[str, Any]:
        return self._request(
            "POST",
            "pages",
            {"parent": parent, "properties": properties},
        )

    def append_block_children(self, block_id: str, children: list[dict[str, Any]]) -> None:
        for start in range(0, len(children), APPEND_BLOCK_LIMIT):
            chunk = children[start : start + APPEND_BLOCK_LIMIT]
            self._request(
                "PATCH",
                f"blocks/{block_id}/children",
                {"children": chunk},
            )

    def query_data_source(
        self,
        data_source_id: str,
        filter_payload: dict[str, Any] | None = None,
        start_cursor: str | None = None,
        page_size: int = 100,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {"page_size": page_size}
        if filter_payload is not None:
            payload["filter"] = filter_payload
        if start_cursor is not None:
            payload["start_cursor"] = start_cursor
        return self._request("POST", f"data_sources/{data_source_id}/query", payload)

    def query_all(
        self,
        data_source_id: str,
        filter_payload: dict[str, Any] | None = None,
        page_size: int = 100,
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        cursor: str | None = None
        while True:
            page = self.query_data_source(
                data_source_id,
                filter_payload=filter_payload,
                start_cursor=cursor,
                page_size=page_size,
            )
            results.extend(page.get("results", []))
            if not page.get("has_more"):
                return results
            cursor = page.get("next_cursor")
