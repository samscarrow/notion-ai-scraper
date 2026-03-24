"""
client.py — Claude.ai Projects API client.

Uses the internal web API with Firefox session cookie auth.
"""

import json
from urllib.request import Request, urlopen
from urllib.error import HTTPError

BASE = "https://claude.ai/api"


class ClaudeProjectClient:
    def __init__(self, cookie_header: str, org_id: str):
        self.cookie_header = cookie_header
        self.org_id = org_id

    def _request(self, method: str, path: str, body: dict | None = None) -> dict | list | None:
        url = f"{BASE}/organizations/{self.org_id}/{path}"
        data = json.dumps(body).encode() if body else None
        req = Request(url, data=data, method=method)
        req.add_header("Cookie", self.cookie_header)
        req.add_header("Content-Type", "application/json")
        req.add_header("anthropic-client-platform", "web_claude_ai")
        req.add_header("User-Agent", "Mozilla/5.0 (X11; Linux x86_64; rv:147.0) Gecko/20100101 Firefox/147.0")

        try:
            with urlopen(req) as resp:
                raw = resp.read()
                if not raw:
                    return None
                return json.loads(raw)
        except HTTPError as e:
            body_text = e.read().decode(errors="replace")
            raise RuntimeError(f"{method} {url} → {e.code}: {body_text}") from e

    # -- Projects --

    def get_project(self, project_id: str) -> dict:
        return self._request("GET", f"projects/{project_id}")

    def update_project(self, project_id: str, **fields) -> dict:
        """Update project fields. Valid keys: prompt_template, name, description."""
        return self._request("PUT", f"projects/{project_id}", fields)

    def list_projects(self, limit: int = 30) -> list[dict]:
        return self._request(
            "GET",
            f"projects?include_harmony_projects=true&limit={limit}&order_by=latest_chat",
        )

    # -- Docs (knowledge files) --

    def list_docs(self, project_id: str) -> list[dict]:
        return self._request("GET", f"projects/{project_id}/docs")

    def upload_doc(self, project_id: str, file_name: str, content: str) -> dict:
        return self._request(
            "POST",
            f"projects/{project_id}/docs",
            {"file_name": file_name, "content": content},
        )

    def delete_doc(self, project_id: str, doc_uuid: str) -> None:
        self._request("DELETE", f"projects/{project_id}/docs/{doc_uuid}")

    # -- Memory --

    def get_memory(self, project_id: str) -> dict:
        return self._request("GET", f"projects/{project_id}/memory")
