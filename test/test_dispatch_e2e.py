"""
test_dispatch_e2e.py — End-to-end dispatch pipeline tests.

Exercises the full Notion -> dispatch -> return -> Notion loop using a
shared-state mock, then validates the auditor sees a clean state after
simulated automations.

Run: cli/.venv/bin/python -m pytest test/test_dispatch_e2e.py -v
"""

import copy
import os
import sys
import uuid
from collections import Counter
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "cli"))

import dispatch
import lab_auditor
import notion_api

# ── Config stub ───────────────────────────────────────────────────────────────

WORK_ITEMS_DB = "e2e-work-items-db"
AUDIT_LOG_DB = "e2e-audit-log-db"
PROJECTS_DB = "e2e-projects-db"


def _mock_config():
    cfg = MagicMock()
    cfg.work_items_db_id = WORK_ITEMS_DB
    cfg.audit_log_db_id = AUDIT_LOG_DB
    return cfg


# ── NotionStateMock ──────────────────────────────────────────────────────────


class NotionStateMock:
    """Shared-state mock with NotionAPIClient method signatures.

    Mutations from update_page persist and are visible to subsequent
    retrieve_page calls, enabling full pipeline testing.
    """

    def __init__(self):
        self.pages: dict[str, dict] = {}
        self.blocks: dict[str, list[dict]] = {}
        self.audit_log: list[dict] = []
        self._call_log: list[tuple] = []
        self._page_parent_db: dict[str, str] = {}

    def add_page(self, page_id: str, page: dict, database_id: str) -> None:
        self.pages[page_id] = page
        self._page_parent_db[page_id] = database_id

    def retrieve_page(self, page_id: str) -> dict:
        self._call_log.append(("retrieve_page", page_id))
        return copy.deepcopy(self.pages[page_id])

    def update_page(self, page_id: str, properties: dict) -> dict:
        self._call_log.append(("update_page", page_id, list(properties.keys())))
        for key, value in properties.items():
            self.pages[page_id]["properties"][key] = copy.deepcopy(value)
        self.pages[page_id]["last_edited_time"] = datetime.now(timezone.utc).isoformat()
        return self.pages[page_id]

    def create_page(self, parent: dict, properties: dict) -> dict:
        self._call_log.append(("create_page", parent))
        db_id = parent.get("database_id", "")
        page_id = str(uuid.uuid4())
        page = {
            "id": page_id,
            "created_time": datetime.now(timezone.utc).isoformat(),
            "last_edited_time": datetime.now(timezone.utc).isoformat(),
            "properties": copy.deepcopy(properties),
        }
        if db_id == AUDIT_LOG_DB:
            self.audit_log.append(page)
        else:
            self.pages[page_id] = page
            self._page_parent_db[page_id] = db_id
        return page

    def append_block_children(self, page_id: str, children: list[dict]) -> None:
        self._call_log.append(("append_block_children", page_id))
        self.blocks.setdefault(page_id, []).extend(copy.deepcopy(children))

    def list_block_children(self, page_id: str, page_size: int = 100) -> list[dict]:
        self._call_log.append(("list_block_children", page_id))
        return list(self.blocks.get(page_id, []))

    def query_all(self, database_id: str, filter_payload: dict | None = None) -> list[dict]:
        self._call_log.append(("query_all", database_id))
        candidates = [
            copy.deepcopy(p)
            for pid, p in self.pages.items()
            if self._page_parent_db.get(pid) == database_id
        ]
        if filter_payload is None:
            return candidates
        return [p for p in candidates if _evaluate_filter(p["properties"], filter_payload)]


# ── Filter evaluator ─────────────────────────────────────────────────────────


def _evaluate_filter(props: dict, clause: dict) -> bool:
    """Minimal filter evaluator for the get_dispatchable_items compound filter."""
    if "and" in clause:
        return all(_evaluate_filter(props, c) for c in clause["and"])
    if "or" in clause:
        return any(_evaluate_filter(props, c) for c in clause["or"])

    prop_name = clause.get("property")
    if not prop_name:
        return True
    prop = props.get(prop_name, {}) or {}

    if "date" in clause:
        date_val = (prop.get("date") or {}).get("start") if prop.get("date") else None
        if clause["date"].get("is_not_empty"):
            return date_val is not None
        if clause["date"].get("is_empty"):
            return date_val is None

    if "status" in clause:
        status_val = (prop.get("status") or {}).get("name")
        if "equals" in clause["status"]:
            return status_val == clause["status"]["equals"]

    return True


# ── WorkItem factory ─────────────────────────────────────────────────────────


def _make_work_item(
    page_id: str | None = None,
    name: str = "TEST-E2E-1",
    objective: str = "Test objective",
    kill_condition: str = "Test kill condition",
    dispatch_via: str = "Claude Code",
    item_type: str = "Gauntlet",
    branch: str = "main",
    environment: str | None = None,
    execution_lane: str | None = None,
    status: str = "Not Started",
    dispatch_received: str | None = "2026-03-10T00:00:00Z",
    dispatch_consumed: str | None = None,
    retry_count: int | None = None,
    blocked_reason: str = "",
    escalation_level: str | None = None,
    execution_budget: int | float | None = None,
    concurrency_group: str = "",
    project_ids: list[str] | None = None,
    dispatch_mode: str | None = None,
    repo_ready: bool = True,
    dispatch_block: str | None = None,
    return_received: str | None = None,
) -> tuple[str, dict]:
    """Build a dispatchable work item page dict."""
    page_id = page_id or str(uuid.uuid4())
    now_str = datetime.now(timezone.utc).isoformat()

    props = {
        "Item Name": {"type": "title", "title": [{"plain_text": name}]},
        "Objective": {"type": "rich_text", "rich_text": [{"plain_text": objective}]},
        "Kill/Stop Condition": {"type": "rich_text", "rich_text": [{"plain_text": kill_condition}]},
        "Dispatch Via": {"type": "select", "select": {"name": dispatch_via} if dispatch_via else None},
        "Execution Lane": {"type": "select", "select": {"name": execution_lane} if execution_lane else None},
        "Environment": {"type": "select", "select": {"name": environment} if environment else None},
        "Branch": {"type": "rich_text", "rich_text": [{"plain_text": branch}] if branch else []},
        "Type": {"type": "select", "select": {"name": item_type} if item_type else None},
        "Status": {"type": "status", "status": {"name": status}},
        "Dispatch Requested Received At": {
            "type": "date",
            "date": {"start": dispatch_received} if dispatch_received else None,
        },
        "Dispatch Requested Consumed At": {
            "type": "date",
            "date": {"start": dispatch_consumed} if dispatch_consumed else None,
        },
        "Librarian Request Received At": {"type": "date", "date": None},
        "Librarian Request Consumed At": {"type": "date", "date": None},
        "Synthesis Complete": {"type": "checkbox", "checkbox": False},
        "GitHub Issue URL": {"type": "url", "url": None},
        "Project": {"type": "relation", "relation": []},
        "Prompt Notes": {"type": "rich_text", "rich_text": []},
        "Outcome": {"type": "rich_text", "rich_text": []},
        "Verdict": {"type": "select", "select": None},
        "Retry Count": {"type": "number", "number": retry_count},
        "Blocked Reason": {"type": "rich_text", "rich_text": [{"plain_text": blocked_reason}]} if blocked_reason else {"type": "rich_text", "rich_text": []},
        "Escalation Level": {"type": "select", "select": {"name": escalation_level} if escalation_level else None},
        "Execution Budget": {"type": "number", "number": execution_budget},
        "Concurrency Group": {"type": "rich_text", "rich_text": [{"plain_text": concurrency_group}]} if concurrency_group else {"type": "rich_text", "rich_text": []},
        "Synthesis Consumed At": {"type": "date", "date": None},
        "Dispatch Mode": {"type": "select", "select": {"name": dispatch_mode} if dispatch_mode else None},
        "Repo Ready": {"type": "checkbox", "checkbox": repo_ready},
        "Dispatch Block": {"type": "select", "select": {"name": dispatch_block} if dispatch_block else None},
        "Return Received At": {
            "type": "date",
            "date": {"start": return_received} if return_received else None,
        },
    }
    if project_ids:
        props["Project"] = {"type": "relation", "relation": [{"id": project_id} for project_id in project_ids]}

    page = {
        "id": page_id,
        "created_time": "2026-03-10T00:00:00.000Z",
        "last_edited_time": now_str,
        "properties": props,
    }
    return page_id, page


def _make_project(
    page_id: str | None = None,
    *,
    name: str = "Test Project",
    focus: bool = False,
    max_active_items: int | None = None,
    min_terminal_value: str | None = None,
    fork_budget: int | float | None = None,
) -> tuple[str, dict]:
    page_id = page_id or str(uuid.uuid4())
    now_str = datetime.now(timezone.utc).isoformat()
    page = {
        "id": page_id,
        "created_time": now_str,
        "last_edited_time": now_str,
        "properties": {
            "Project Name": {"type": "title", "title": [{"plain_text": name}]},
            "Focus": {"type": "checkbox", "checkbox": focus},
            "Max Active Items": {"type": "number", "number": max_active_items},
            "Min Terminal Value": {"type": "select", "select": {"name": min_terminal_value} if min_terminal_value else None},
            "Fork Budget": {"type": "number", "number": fork_budget},
        },
    }
    return page_id, page


# ── Property extraction helpers ──────────────────────────────────────────────


def _extract_status(page: dict) -> str | None:
    return ((page["properties"].get("Status", {}) or {}).get("status") or {}).get("name")


def _extract_select(page: dict, prop: str) -> str | None:
    return ((page["properties"].get(prop, {}) or {}).get("select") or {}).get("name")


def _extract_date(page: dict, prop: str) -> str | None:
    return ((page["properties"].get(prop, {}) or {}).get("date") or {}).get("start")


def _extract_rich_text(page: dict, prop: str) -> str:
    rich_text = (page["properties"].get(prop, {}) or {}).get("rich_text") or []
    return "".join(
        (chunk.get("plain_text") or chunk.get("text", {}).get("content") or "")
        for chunk in rich_text
    )


# ── Automation simulator ─────────────────────────────────────────────────────


def _simulate_automations(mock: NotionStateMock, page_id: str) -> None:
    """Simulate Notion automations that fire after pipeline completion.

    E.1: DRRA + DRCA both set -> automation clears DRRA.
    E.7: LRRA set but LRCA empty -> Librarian sets LRCA.
    """
    props = mock.pages[page_id]["properties"]

    drra = (props.get("Dispatch Requested Received At", {}).get("date") or {}).get("start")
    drca = (props.get("Dispatch Requested Consumed At", {}).get("date") or {}).get("start")
    if drra and drca:
        props["Dispatch Requested Received At"] = {"type": "date", "date": None}

    lrra = (props.get("Librarian Request Received At", {}).get("date") or {}).get("start")
    lrca = (props.get("Librarian Request Consumed At", {}).get("date") or {}).get("start")
    if lrra and not lrca:
        props["Librarian Request Consumed At"] = {"type": "date", "date": {"start": notion_api.now_iso()}}


# ── Full-loop helper ─────────────────────────────────────────────────────────


def _run_full_loop(mock, page_id, *, status="ok", verdict="PASS", error=None):
    """Execute get -> build -> stamp -> return and return all intermediate results."""
    items = dispatch.get_dispatchable_items(mock)

    result = dispatch.build_dispatch_packet(page_id, mock)
    assert result["packet"] is not None, f"build_dispatch_packet failed: {result['errors']}"
    packet = result["packet"]

    dispatch.stamp_dispatch_consumed(page_id, packet["run_id"], mock)

    # After stamp, item should no longer be dispatchable
    post_stamp_items = dispatch.get_dispatchable_items(mock)

    return_result = dispatch.handle_final_return(
        work_item_id=page_id,
        run_id=packet["run_id"],
        status=status,
        summary="Test summary",
        raw_output="Test output",
        duration_ms=30000,
        model="avocado-froyo-medium",
        lane=packet["execution_lane"],
        verdict=verdict if status == "ok" else None,
        error=error if status != "ok" else None,
        client=mock,
    )

    return {
        "items": items,
        "packet": packet,
        "post_stamp_items": post_stamp_items,
        "return_result": return_result,
    }


# ── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _patch_config():
    with patch("dispatch.get_config", return_value=_mock_config()):
        yield


def _fresh_mock(page_id, page):
    mock = NotionStateMock()
    mock.add_page(page_id, page, WORK_ITEMS_DB)
    return mock


# ── E2E Tests ────────────────────────────────────────────────────────────────


class TestOpenClawE2E:
    """Full 4-function chain: get -> build -> accept -> return."""

    def test_gauntlet_pass(self):
        page_id, page = _make_work_item(item_type="Gauntlet")
        mock = _fresh_mock(page_id, page)
        r = _run_full_loop(mock, page_id, verdict="PASS")

        assert len(r["items"]) == 1
        assert r["items"][0]["id"] == page_id
        assert len(r["post_stamp_items"]) == 0
        assert r["return_result"]["ingested"] is True
        assert _extract_status(mock.pages[page_id]) == "Done"
        assert _extract_select(mock.pages[page_id], "Verdict") == "Passed"
        assert _extract_date(mock.pages[page_id], "Librarian Request Received At") is None
        assert len(mock.audit_log) == 2
        assert len(mock.blocks.get(page_id, [])) > 0

    def test_gauntlet_fail(self):
        page_id, page = _make_work_item(item_type="Gauntlet")
        mock = _fresh_mock(page_id, page)
        r = _run_full_loop(mock, page_id, verdict="FAIL")

        assert r["return_result"]["ingested"] is True
        assert _extract_status(mock.pages[page_id]) == "Kill Condition Met"
        assert _extract_select(mock.pages[page_id], "Verdict") == "Killed"
        assert len(mock.audit_log) == 2

    def test_gauntlet_inconclusive(self):
        page_id, page = _make_work_item(item_type="Gauntlet")
        mock = _fresh_mock(page_id, page)
        r = _run_full_loop(mock, page_id, verdict="INCONCLUSIVE")

        assert r["return_result"]["ingested"] is True
        assert _extract_status(mock.pages[page_id]) == "Done"
        assert _extract_select(mock.pages[page_id], "Verdict") == "Inconclusive"
        assert len(mock.audit_log) == 2

    def test_gauntlet_observations_fallback(self):
        page_id, page = _make_work_item(item_type="Gauntlet")
        mock = _fresh_mock(page_id, page)
        r = _run_full_loop(mock, page_id, verdict="OBSERVATIONS")

        assert r["return_result"]["ingested"] is True
        assert _extract_status(mock.pages[page_id]) == "Done"
        assert _extract_select(mock.pages[page_id], "Verdict") == "Inconclusive"
        # Outcome should include the warning
        outcome_rt = mock.pages[page_id]["properties"].get("Outcome", {}).get("rich_text", [])
        outcome_text = "".join(t.get("text", {}).get("content", "") for t in outcome_rt)
        assert "WARNING" in outcome_text

    def test_non_gauntlet_pass(self):
        page_id, page = _make_work_item(item_type="Other", kill_condition="")
        mock = _fresh_mock(page_id, page)
        r = _run_full_loop(mock, page_id, verdict="PASS")

        assert r["return_result"]["ingested"] is True
        assert _extract_status(mock.pages[page_id]) == "Done"
        assert _extract_select(mock.pages[page_id], "Verdict") == "Passed"
        assert len(mock.audit_log) == 2

    def test_non_gauntlet_observations(self):
        page_id, page = _make_work_item(item_type="Other", kill_condition="")
        mock = _fresh_mock(page_id, page)
        r = _run_full_loop(mock, page_id, verdict="OBSERVATIONS")

        assert r["return_result"]["ingested"] is True
        assert _extract_status(mock.pages[page_id]) == "Done"
        assert _extract_select(mock.pages[page_id], "Verdict") is None

    def test_error_return(self):
        page_id, page = _make_work_item(item_type="Gauntlet")
        mock = _fresh_mock(page_id, page)
        r = _run_full_loop(mock, page_id, status="error", error="Execution failed")

        assert r["return_result"]["ingested"] is True
        assert _extract_status(mock.pages[page_id]) == "Blocked"
        assert _extract_select(mock.pages[page_id], "Verdict") is None
        assert len(mock.audit_log) == 2
        assert mock.pages[page_id]["properties"]["Retry Count"]["number"] == 1

    def test_third_failed_return_escalates_to_needs_sam(self):
        page_id, page = _make_work_item(item_type="Gauntlet", retry_count=2)
        mock = _fresh_mock(page_id, page)
        r = _run_full_loop(mock, page_id, status="error", error="Execution failed")

        assert r["return_result"]["ingested"] is True
        assert mock.pages[page_id]["properties"]["Retry Count"]["number"] == 3
        assert _extract_select(mock.pages[page_id], "Escalation Level") == "Needs Sam"

    def test_timeout_return(self):
        page_id, page = _make_work_item(item_type="Gauntlet")
        mock = _fresh_mock(page_id, page)
        r = _run_full_loop(mock, page_id, status="timeout", error="Timed out after 300s")

        assert r["return_result"]["ingested"] is True
        assert _extract_status(mock.pages[page_id]) == "Blocked"
        assert len(mock.audit_log) == 2

    def test_idempotency_rejects_duplicate(self):
        page_id, page = _make_work_item(item_type="Gauntlet")
        mock = _fresh_mock(page_id, page)
        r = _run_full_loop(mock, page_id, verdict="PASS")
        run_id = r["packet"]["run_id"]

        # Second return with same run_id should be rejected
        second = dispatch.handle_final_return(
            work_item_id=page_id,
            run_id=run_id,
            status="ok",
            summary="Duplicate",
            raw_output="Duplicate output",
            duration_ms=30000,
            model="avocado-froyo-medium",
            lane="dev",
            verdict="PASS",
            client=mock,
        )
        assert second["ingested"] is False
        assert second.get("reason") == "duplicate_run_id"

    def test_stamp_dispatch_consumed_clears_blocked_reason(self):
        page_id, page = _make_work_item(blocked_reason="Capped")
        mock = _fresh_mock(page_id, page)
        packet = dispatch.build_dispatch_packet(page_id, mock)
        assert packet["packet"] is None

        mock.pages[page_id]["properties"]["Blocked Reason"] = {"type": "rich_text", "rich_text": []}
        packet = dispatch.build_dispatch_packet(page_id, mock)
        dispatch.stamp_dispatch_consumed(page_id, packet["packet"]["run_id"], mock)

        assert mock.pages[page_id]["properties"]["Blocked Reason"]["rich_text"] == []

    def test_fail_dispatch_preflight_records_reason_without_false_in_progress(self):
        page_id, page = _make_work_item()
        mock = _fresh_mock(page_id, page)
        packet = dispatch.build_dispatch_packet(page_id, mock)["packet"]

        result = dispatch.fail_dispatch_preflight(page_id, packet["run_id"], "Repo not present on nix", mock)

        assert result["status"] == "recorded"
        assert _extract_status(mock.pages[page_id]) == "Not Started"
        assert _extract_date(mock.pages[page_id], "Dispatch Requested Consumed At") is None
        assert _extract_rich_text(mock.pages[page_id], "Blocked Reason") == "Repo not present on nix"
        assert len(mock.audit_log) == 1

    def test_fail_dispatch_preflight_reverts_false_accept(self):
        page_id, page = _make_work_item(status="Prompt Drafted")
        page["properties"]["Prompt Request Consumed At"] = {
            "type": "date",
            "date": {"start": "2026-03-10T00:05:00Z"},
        }
        mock = _fresh_mock(page_id, page)
        packet = dispatch.build_dispatch_packet(page_id, mock)["packet"]

        dispatch.accept_dispatch_start(page_id, packet["run_id"], mock)
        result = dispatch.fail_dispatch_preflight(page_id, packet["run_id"], "Sandbox preparation failed", mock)

        assert result["status"] == "reverted"
        assert _extract_status(mock.pages[page_id]) == "Prompt Drafted"
        assert _extract_date(mock.pages[page_id], "Dispatch Requested Consumed At") is None
        assert mock.pages[page_id]["properties"]["run_id"]["rich_text"] == []
        assert _extract_rich_text(mock.pages[page_id], "Blocked Reason") == "Sandbox preparation failed"
        assert len(mock.audit_log) == 2

    def test_focus_projects_define_portfolio_candidate_set(self):
        focused_project_id, focused_project = _make_project(name="Focused", focus=True)
        held_project_id, held_project = _make_project(name="Held", focus=False)
        held_item_id, held_item = _make_work_item(name="HELD-1", project_ids=[held_project_id])
        focus_item_id, focus_item = _make_work_item(name="FOCUS-1", project_ids=[focused_project_id])

        mock = NotionStateMock()
        mock.add_page(focused_project_id, focused_project, PROJECTS_DB)
        mock.add_page(held_project_id, held_project, PROJECTS_DB)
        mock.add_page(held_item_id, held_item, WORK_ITEMS_DB)
        mock.add_page(focus_item_id, focus_item, WORK_ITEMS_DB)

        items = dispatch.get_dispatchable_items(mock)
        assert [item["name"] for item in items] == ["FOCUS-1"]

        held_result = dispatch.build_dispatch_packet(held_item_id, mock)
        assert any("V21" in error for error in held_result["errors"])

        focus_result = dispatch.build_dispatch_packet(focus_item_id, mock)
        assert focus_result["errors"] == []
        assert focus_result["packet"]["project_focus"] is True


# ── V20 WIP-cap tests ────────────────────────────────────────────────────────


class TestV20ProjectWipCap:
    """V20: in-flight items count against WIP; terminal-verdict items do not."""

    def _make_in_flight_item(self, project_id: str, name: str = "IN-FLIGHT") -> tuple[str, dict]:
        """Consumed but not yet returned — should count as WIP."""
        return _make_work_item(
            name=name,
            status="In Progress",
            dispatch_consumed="2026-03-10T00:05:00Z",
            return_received=None,
            project_ids=[project_id],
        )

    def _make_returned_item(self, project_id: str, name: str = "RETURNED") -> tuple[str, dict]:
        """Consumed AND returned — terminal verdict, must NOT count as WIP."""
        return _make_work_item(
            name=name,
            status="Done",
            dispatch_consumed="2026-03-10T00:05:00Z",
            return_received="2026-03-10T01:00:00Z",
            project_ids=[project_id],
        )

    def test_v20_fires_when_in_flight_items_hit_cap(self):
        proj_id, proj = _make_project(name="Capped", max_active_items=1)
        # One in-flight item already consuming the cap
        existing_id, existing = self._make_in_flight_item(proj_id, "EXISTING-1")
        # New candidate wants to dispatch into the same project
        candidate_id, candidate = _make_work_item(
            name="CANDIDATE-1", project_ids=[proj_id], repo_ready=True
        )

        mock = NotionStateMock()
        mock.add_page(proj_id, proj, PROJECTS_DB)
        mock.add_page(existing_id, existing, WORK_ITEMS_DB)
        mock.add_page(candidate_id, candidate, WORK_ITEMS_DB)

        result = dispatch.build_dispatch_packet(candidate_id, mock)
        assert any("V20" in e for e in result["errors"]), result["errors"]

    def test_v20_does_not_fire_when_returned_item_clears_wip(self):
        proj_id, proj = _make_project(name="Capped", max_active_items=1)
        # Prior item has received a terminal return — should NOT count
        returned_id, returned = self._make_returned_item(proj_id, "RETURNED-1")
        candidate_id, candidate = _make_work_item(
            name="CANDIDATE-2", project_ids=[proj_id], repo_ready=True
        )

        mock = NotionStateMock()
        mock.add_page(proj_id, proj, PROJECTS_DB)
        mock.add_page(returned_id, returned, WORK_ITEMS_DB)
        mock.add_page(candidate_id, candidate, WORK_ITEMS_DB)

        result = dispatch.build_dispatch_packet(candidate_id, mock)
        assert not any("V20" in e for e in result["errors"]), result["errors"]

    def test_v20_redispatch_clears_return_timestamps_and_counts_as_wip(self):
        """After a human unblocks and re-dispatches a previously-returned item,
        accept_dispatch_start must clear Return Received At so the re-run counts
        toward WIP again.  Without this, the re-dispatched item would be
        invisible to V20 and could exceed the project cap."""
        proj_id, proj = _make_project(name="Capped", max_active_items=1)
        # Prior run: item returned (Blocked), now unblocked by human to
        # Prompt Drafted, ready for re-dispatch
        retried_id, retried = _make_work_item(
            name="RETRIED-1",
            status="Prompt Drafted",
            dispatch_consumed="2026-03-10T00:05:00Z",
            return_received="2026-03-10T01:00:00Z",
            project_ids=[proj_id],
        )
        retried["properties"]["Return Consumed At"] = {
            "type": "date",
            "date": {"start": "2026-03-10T01:00:00Z"},
        }

        mock = NotionStateMock()
        mock.add_page(proj_id, proj, PROJECTS_DB)
        mock.add_page(retried_id, retried, WORK_ITEMS_DB)

        # Simulate re-dispatch: stamp Lab Dispatch Requested At fresh
        retried["properties"]["Lab Dispatch Requested At"] = {
            "type": "date",
            "date": {"start": "2026-03-11T00:00:00Z"},
        }
        retried["properties"]["Dispatch Requested Consumed At"] = {
            "type": "date",
            "date": None,
        }

        dispatch.accept_dispatch_start(retried_id, "run-retry-1", mock)

        props = mock.pages[retried_id]["properties"]
        assert (props["Return Received At"].get("date") or {}).get("start") is None
        assert (props["Return Consumed At"].get("date") or {}).get("start") is None

        # Now a second item in the same project should be blocked by V20
        candidate_id, candidate = _make_work_item(
            name="CANDIDATE-4", project_ids=[proj_id], repo_ready=True
        )
        mock.add_page(candidate_id, candidate, WORK_ITEMS_DB)

        result = dispatch.build_dispatch_packet(candidate_id, mock)
        assert any("V20" in e for e in result["errors"]), result["errors"]

    def test_v20_status_lag_does_not_count_returned_item(self):
        """Return Received At is set but Status still reads 'In Progress' (lag window).

        The old approach (filtering by terminal Status) would count this item.
        The new approach (filtering by Return Received At is empty) correctly excludes it.
        """
        proj_id, proj = _make_project(name="Capped", max_active_items=1)
        # Status hasn't been updated yet but return has been received
        lagged_id, lagged = _make_work_item(
            name="LAGGED-1",
            status="In Progress",  # Status update hasn't landed yet
            dispatch_consumed="2026-03-10T00:05:00Z",
            return_received="2026-03-10T01:00:00Z",  # Return IS recorded
            project_ids=[proj_id],
        )
        candidate_id, candidate = _make_work_item(
            name="CANDIDATE-3", project_ids=[proj_id], repo_ready=True
        )

        mock = NotionStateMock()
        mock.add_page(proj_id, proj, PROJECTS_DB)
        mock.add_page(lagged_id, lagged, WORK_ITEMS_DB)
        mock.add_page(candidate_id, candidate, WORK_ITEMS_DB)

        result = dispatch.build_dispatch_packet(candidate_id, mock)
        assert not any("V20" in e for e in result["errors"]), result["errors"]


# ── Auditor-after-loop tests ────────────────────────────────────────────────


class TestAuditorAfterLoop:
    """Run full loop + automations, then verify auditor reports 0 blocking violations."""

    def _run_and_audit(self, mock, page_id, **loop_kwargs):
        _run_full_loop(mock, page_id, **loop_kwargs)
        _simulate_automations(mock, page_id)

        work_items = list(mock.pages.values())
        audit_counts = Counter()
        for entry in mock.audit_log:
            for rel in entry.get("properties", {}).get("Work Item", {}).get("relation", []):
                audit_counts[rel["id"]] += 1

        violations, _ = lab_auditor.check_lab_loop(mock, work_items, {}, audit_counts)
        return violations

    def test_gauntlet_pass_auditor_clean(self):
        page_id, page = _make_work_item(item_type="Gauntlet")
        mock = _fresh_mock(page_id, page)
        violations = self._run_and_audit(mock, page_id, verdict="PASS")
        assert lab_auditor._blocking_violation_count(violations) == 0

    def test_non_gauntlet_pass_auditor_clean(self):
        page_id, page = _make_work_item(item_type="Other", kill_condition="")
        mock = _fresh_mock(page_id, page)
        violations = self._run_and_audit(mock, page_id, verdict="PASS")
        assert lab_auditor._blocking_violation_count(violations) == 0

    def test_error_return_auditor_clean(self):
        page_id, page = _make_work_item(item_type="Gauntlet")
        mock = _fresh_mock(page_id, page)
        violations = self._run_and_audit(mock, page_id, status="error", error="Execution failed")
        assert lab_auditor._blocking_violation_count(violations) == 0
