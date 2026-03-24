"""
test_dispatch.py — Unit tests for the dispatch adapter.

Tests validation rules V1-V12, inheritance resolution, and Dispatch Via defaults.
Run: cd cli && .venv/bin/python -m pytest test_dispatch.py -v
"""

import json
import os
import sys
import uuid
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import dispatch


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _clear_lab_control_cache():
    """Keep gate checks deterministic across tests."""
    dispatch._lab_control_cache.clear()
    yield
    dispatch._lab_control_cache.clear()

def _make_props(
    name: str = "TEST-GAUNTLET-1",
    objective: str = "Test objective",
    kill_condition: str = "Test kill condition",
    dispatch_via: str = "Claude Code",
    execution_lane: str | None = None,
    environment: str | None = None,
    branch: str = "",
    item_type: str = "Gauntlet",
    received_at: str | None = "2026-01-01T00:00:00Z",
    consumed_at: str | None = None,
    run_id: str = "",
    github_url: str = "",
    project_ids: list[str] | None = None,
    blocked_reason: str = "",
    retry_count: int | None = None,
    execution_budget: int | float | None = None,
    concurrency_group: str = "",
    escalation_level: str | None = None,
    status: str = "Not Started",
    dispatch_mode: str | None = None,
    repo_ready: bool = False,
    dispatch_block: str | None = None,
) -> dict:
    """Build a mock Notion page properties dict."""
    props = {
        "Item Name": {"type": "title", "title": [{"plain_text": name}]},
        "Objective": {"type": "rich_text", "rich_text": [{"plain_text": objective}]},
        "Kill/Stop Condition": {"type": "rich_text", "rich_text": [{"plain_text": kill_condition}]},
        "Dispatch Via": {"type": "select", "select": {"name": dispatch_via} if dispatch_via else None},
        "Dispatch Requested Received At": {
            "type": "date",
            "date": {"start": received_at} if received_at else None,
        },
        "Dispatch Requested Consumed At": {
            "type": "date",
            "date": {"start": consumed_at} if consumed_at else None,
        },
        "Prompt Notes": {"type": "rich_text", "rich_text": []},
        "GitHub Issue URL": {"type": "url", "url": github_url or None},
        "Status": {"type": "status", "status": {"name": status}},
        "Type": {"type": "select", "select": {"name": item_type} if item_type else None},
        "Project": {
            "type": "relation",
            "relation": [{"id": pid} for pid in (project_ids or [])],
        },
        "Blocked Reason": {"type": "rich_text", "rich_text": [{"plain_text": blocked_reason}]} if blocked_reason else {"type": "rich_text", "rich_text": []},
        "Retry Count": {"type": "number", "number": retry_count},
        "Execution Budget": {"type": "number", "number": execution_budget},
        "Concurrency Group": {"type": "rich_text", "rich_text": [{"plain_text": concurrency_group}]} if concurrency_group else {"type": "rich_text", "rich_text": []},
        "Escalation Level": {"type": "select", "select": {"name": escalation_level} if escalation_level else None},
        "Dispatch Mode": {"type": "select", "select": {"name": dispatch_mode} if dispatch_mode else None},
        "Repo Ready": {"type": "checkbox", "checkbox": repo_ready},
        "Dispatch Block": {"type": "select", "select": {"name": dispatch_block} if dispatch_block else None},
    }
    if execution_lane:
        props["Execution Lane"] = {"type": "select", "select": {"name": execution_lane}}
    else:
        props["Execution Lane"] = {"type": "select", "select": None}

    if environment:
        props["Environment"] = {"type": "select", "select": {"name": environment}}
    else:
        props["Environment"] = {"type": "select", "select": None}

    if branch:
        props["Branch"] = {"type": "rich_text", "rich_text": [{"plain_text": branch}]}
    else:
        props["Branch"] = {"type": "rich_text", "rich_text": []}

    if run_id:
        props["run_id"] = {"type": "rich_text", "rich_text": [{"plain_text": run_id}]}

    return props


def _project_page(
    name: str = "Test Project",
    max_active_items: int | None = None,
    *,
    focus: bool = False,
    min_terminal_value: str | None = None,
    fork_budget: int | float | None = None,
    github_url: str | None = None,
) -> dict:
    return {
        "id": str(uuid.uuid4()),
        "properties": {
            "Project Name": {"type": "title", "title": [{"plain_text": name}]},
            "Max Active Items": {"type": "number", "number": max_active_items},
            "Focus": {"type": "checkbox", "checkbox": focus},
            "Min Terminal Value": {"type": "select", "select": {"name": min_terminal_value} if min_terminal_value else None},
            "Fork Budget": {"type": "number", "number": fork_budget},
            "GitHub URL": {"type": "url", "url": github_url},
        },
    }


def _mock_client(
    props: dict,
    work_item_id: str | None = None,
    *,
    project_pages: dict[str, dict] | None = None,
    query_results: list[list[dict]] | None = None,
) -> MagicMock:
    """Create a mock NotionAPIClient that returns given properties."""
    client = MagicMock()
    wid = work_item_id or str(uuid.uuid4())
    pages = {wid: {"id": wid, "properties": props}}
    pages.update(project_pages or {})

    def _retrieve(page_id: str):
        return pages[page_id]

    client.retrieve_page.side_effect = _retrieve
    if query_results is None:
        client.query_all.return_value = []
    else:
        queued = list(query_results)

        def _query_all(*args, **kwargs):
            filter_payload = kwargs.get("filter_payload") or {}
            title_filter = (filter_payload.get("title") or {}).get("equals")
            if title_filter in {"Pre-Flight Mode", "Max Cascade Depth"}:
                return []
            return queued.pop(0) if queued else []

        client.query_all.side_effect = _query_all
    return client


# ── Contract loading tests ───────────────────────────────────────────────────

def test_contracts_loaded():
    """Contract configs load without error."""
    assert "lanes" in dispatch.LANE_CAPABILITIES
    assert len(dispatch.VALID_LANES) == 13
    assert len(dispatch.VALID_ENVIRONMENTS) == 4
    assert len(dispatch.DISPATCH_VIA_DEFAULTS) == 8


def test_lane_capabilities_structure():
    """Each lane has required capability fields."""
    for lane, caps in dispatch.LANE_CAPABILITIES["lanes"].items():
        assert "can_code" in caps, f"{lane} missing can_code"
        assert "can_browse" in caps, f"{lane} missing can_browse"
        assert "can_deploy" in caps, f"{lane} missing can_deploy"
        assert "max_timeout_s" in caps, f"{lane} missing max_timeout_s"
        assert isinstance(caps["max_timeout_s"], int), f"{lane} max_timeout_s not int"


def test_apply_redaction_tolerates_missing_replacement_template(monkeypatch):
    monkeypatch.setattr(
        dispatch,
        "REDACTION_CONFIG",
        {
            "patterns": [
                {"label": "API key", "regex": r"sk-[A-Za-z0-9]{20,}"},
            ]
        },
    )
    redacted = dispatch._apply_redaction("Token sk-abcdefghijklmnopqrstuvwxyz1234")
    assert "[REDACTED:API key]" in redacted
    assert "sk-" not in redacted


# ── Dispatch Via default mapping ─────────────────────────────────────────────

def test_dispatch_via_defaults():
    """Dispatch Via values map to expected default lanes."""
    assert dispatch.DISPATCH_VIA_DEFAULTS["Claude Code"] == "dev"
    assert dispatch.DISPATCH_VIA_DEFAULTS["Cursor"] == "coder"
    assert dispatch.DISPATCH_VIA_DEFAULTS["Antigravity"] == "thinker"
    assert dispatch.DISPATCH_VIA_DEFAULTS["Manual"] is None


# ── Validation rule tests ────────────────────────────────────────────────────

class TestValidation:
    """Test each V1-V12 validation rule independently."""

    def _build(self, **overrides) -> dict:
        """Build a packet and return the result dict."""
        wid = overrides.pop("work_item_id", str(uuid.uuid4()))
        props = _make_props(**overrides)
        client = _mock_client(props, wid)
        return dispatch.build_dispatch_packet(wid, client)

    def test_v1_invalid_uuid(self):
        """V1: Invalid work_item_id."""
        props = _make_props()
        client = _mock_client(props, "not-a-uuid")
        # The retrieve_page will be called with "not-a-uuid"
        result = dispatch.build_dispatch_packet("not-a-uuid", client)
        assert any("V1" in e for e in result["errors"])

    def test_v2_empty_dispatch_via(self):
        """V2: Empty dispatch_via."""
        result = self._build(dispatch_via=None)
        assert any("V2" in e for e in result["errors"])

    def test_v2_unknown_dispatch_via(self):
        """V2: Unknown dispatch_via value."""
        result = self._build(dispatch_via="UnknownProvider")
        assert any("V2" in e for e in result["errors"])

    def test_v3_unresolvable_lane(self):
        """V3: Manual dispatch with no explicit lane."""
        result = self._build(dispatch_via="Manual", execution_lane=None)
        assert any("V3" in e for e in result["errors"])

    def test_v3_invalid_lane(self):
        """V3: Explicitly set invalid lane."""
        result = self._build(execution_lane="nonexistent-lane")
        assert any("V3" in e for e in result["errors"])

    def test_v4_invalid_environment(self):
        """V4: Invalid environment value."""
        result = self._build(environment="invalid-env")
        assert any("V4" in e for e in result["errors"])

    def test_v5_lane_environment_incompatible(self):
        """V5: Lane not allowed in production."""
        result = self._build(execution_lane="dev", environment="production")
        assert any("V5" in e for e in result["errors"])

    def test_v5_lane_environment_compatible(self):
        """V5: sentinel-deploy is allowed in production."""
        result = self._build(execution_lane="sentinel-deploy", environment="production")
        assert not any("V5" in e for e in result.get("errors", []))

    def test_v6_empty_objective(self):
        """V6: Empty objective."""
        result = self._build(objective="")
        assert any("V6" in e for e in result["errors"])

    def test_v7_gauntlet_no_kill_condition(self):
        """V7: Gauntlet without kill condition."""
        result = self._build(item_type="Gauntlet", kill_condition="")
        assert any("V7" in e for e in result["errors"])

    def test_v7_non_gauntlet_no_kill_condition_ok(self):
        """V7: Non-Gauntlet without kill condition is fine."""
        result = self._build(item_type="Other", kill_condition="")
        assert not any("V7" in e for e in result.get("errors", []))

    def test_v8_existing_run_id(self):
        """V8: Work item already has a run_id."""
        result = self._build(run_id="existing-run-id")
        assert any("V8" in e for e in result["errors"])

    def test_v9_dispatch_received_at_not_set(self):
        """V9: Dispatch Requested Received At is empty."""
        result = self._build(received_at=None)
        assert any("V9" in e for e in result["errors"])

    def test_v10_already_consumed(self):
        """V10: Already consumed."""
        result = self._build(consumed_at="2026-03-10T00:00:00Z")
        assert any("V10" in e for e in result["errors"])

    def test_v15_blocked_reason_blocks_dispatch(self):
        result = self._build(blocked_reason="Waiting on Sam")
        assert any("V18" in e for e in result["errors"])

    def test_v15_incubate_mode_blocks_dispatch(self):
        result = self._build(dispatch_mode="incubate", repo_ready=True)
        assert any("V15" in e for e in result["errors"])

    def test_v16_dispatch_block_blocks_dispatch(self):
        result = self._build(dispatch_block="pre_repo_incubation", repo_ready=True)
        assert any("V16" in e for e in result["errors"])

    def test_v17_repo_not_ready_blocks_dispatch(self):
        result = self._build()
        assert any("V17" in e for e in result["errors"])

    def test_v19_escalation_level_blocks_dispatch(self):
        result = self._build(escalation_level="Needs Sam")
        assert any("V19" in e for e in result["errors"])

    def test_v20_project_cap_blocks_dispatch(self):
        wid = str(uuid.uuid4())
        project_id = str(uuid.uuid4())
        props = _make_props(project_ids=[project_id], repo_ready=True)
        project_page = _project_page(max_active_items=1)
        active_page = {"id": str(uuid.uuid4()), "properties": _make_props(project_ids=[project_id], consumed_at="2026-03-10T00:00:00Z", status="In Progress", repo_ready=True)}
        client = _mock_client(
            props,
            wid,
            project_pages={project_id: project_page},
            query_results=[[active_page]],
        )
        result = dispatch.build_dispatch_packet(wid, client)
        assert any("V20" in e for e in result["errors"])

    def test_v21_non_focus_project_blocked_when_focus_candidates_exist(self):
        wid = str(uuid.uuid4())
        focused_project_id = str(uuid.uuid4())
        non_focus_project_id = str(uuid.uuid4())
        props = _make_props(project_ids=[non_focus_project_id], repo_ready=True)
        focused_page = {
            "id": str(uuid.uuid4()),
            "properties": _make_props(name="FOCUS-READY", project_ids=[focused_project_id], repo_ready=True),
        }
        non_focus_page = {"id": wid, "properties": props}
        project_pages = {
            focused_project_id: _project_page(name="Focused", focus=True),
            non_focus_project_id: _project_page(name="Held", focus=False),
        }
        client = _mock_client(
            props,
            wid,
            project_pages=project_pages,
            query_results=[[], [focused_page, non_focus_page], []],
        )

        result = dispatch.build_dispatch_packet(wid, client)
        assert any("V21" in e for e in result["errors"])


class TestHappyPath:
    """Test successful packet building."""

    def test_basic_packet(self):
        """Build a valid packet with all required fields."""
        wid = str(uuid.uuid4())
        props = _make_props(
            dispatch_via="Claude Code",
            objective="Run the tests",
            kill_condition="Any test fails",
            repo_ready=True,
        )
        client = _mock_client(props, wid)
        result = dispatch.build_dispatch_packet(wid, client)

        assert result["errors"] == []
        packet = result["packet"]
        assert packet["version"] == "1.1"
        assert packet["work_item_id"] == wid
        assert packet["execution_lane"] == "dev"  # default from Claude Code
        assert packet["environment"] == "dev"  # default
        assert packet["constraints"]["can_code"] is True
        assert packet["constraints"]["can_browse"] is True
        assert packet["retry_count"] == 0
        assert packet["escalation_level"] == "Normal"
        assert uuid.UUID(packet["run_id"])  # valid UUID

    def test_packet_includes_queue_metadata(self):
        wid = str(uuid.uuid4())
        props = _make_props(
            execution_budget=45,
            concurrency_group="notion-forge-main",
            retry_count=2,
            escalation_level="Normal",
            repo_ready=True,
        )
        project_id = str(uuid.uuid4())
        props["Project"] = {"type": "relation", "relation": [{"id": project_id}]}
        client = _mock_client(
            props,
            wid,
            project_pages={
                project_id: _project_page(
                    name="notion-forge",
                    focus=True,
                    min_terminal_value="Meaningful",
                    fork_budget=2,
                    github_url="https://github.com/git-scarrow/notion-forge",
                )
            },
            query_results=[[], [{"id": wid, "properties": props}], []],
        )
        result = dispatch.build_dispatch_packet(wid, client)

        assert result["errors"] == []
        packet = result["packet"]
        assert packet["execution_budget"] == 45
        assert packet["concurrency_group"] == "notion-forge-main"
        assert packet["retry_count"] == 2
        assert packet["escalation_level"] == "Normal"
        assert packet["dispatch_mode"] == "execute"
        assert packet["dispatch_block"] == "none"
        assert packet["repo_ready"] is True
        assert packet["project_focus"] is True
        assert packet["project_min_terminal_value"] == "Meaningful"
        assert packet["project_fork_budget"] == 2
        assert packet["repo_url"] == "https://github.com/git-scarrow/notion-forge"
        assert packet["portfolio_focus_active"] is True

    def test_explicit_lane_overrides_default(self):
        """Explicit Execution Lane overrides Dispatch Via default."""
        wid = str(uuid.uuid4())
        props = _make_props(
            dispatch_via="Claude Code",  # default -> dev
            execution_lane="thinker",     # explicit override
            repo_ready=True,
        )
        client = _mock_client(props, wid)
        result = dispatch.build_dispatch_packet(wid, client)

        assert result["errors"] == []
        assert result["packet"]["execution_lane"] == "thinker"
        assert result["packet"]["constraints"]["can_code"] is False  # thinker can't code

    def test_branch_defaults_to_main(self):
        """Branch defaults to 'main' when not set and env is not sandbox."""
        wid = str(uuid.uuid4())
        props = _make_props(branch="")
        props["Repo Ready"] = {"type": "checkbox", "checkbox": True}
        client = _mock_client(props, wid)
        result = dispatch.build_dispatch_packet(wid, client)

        assert result["errors"] == []
        assert result["packet"]["branch"] == "main"


# ── Schema validation tests ──────────────────────────────────────────────────

def test_dispatch_packet_schema_valid():
    """A valid packet passes JSON Schema validation."""
    try:
        import jsonschema
    except ImportError:
        pytest.skip("jsonschema not installed")

    schema_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "contracts", "dispatch_packet.schema.json",
    )
    with open(schema_path) as f:
        schema = json.load(f)

    wid = str(uuid.uuid4())
    props = _make_props()
    client = _mock_client(props, wid)
    result = dispatch.build_dispatch_packet(wid, client)

    assert result["errors"] == []
    jsonschema.validate(result["packet"], schema)


def test_final_return_schema_structure():
    """Final return schema loads and has expected required fields."""
    schema_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "contracts", "final_return.schema.json",
    )
    with open(schema_path) as f:
        schema = json.load(f)

    assert "run_id" in schema["required"]
    assert "status" in schema["required"]
    assert "verdict" not in schema["required"]  # only required when status=ok (via if/then)


# ── get_dispatchable_items tests ─────────────────────────────────────────────

def test_get_dispatchable_items_empty():
    """Returns empty list when no items match."""
    client = MagicMock()
    client.query_all.side_effect = [[], []]

    with patch("dispatch.get_config") as mock_cfg:
        mock_cfg.return_value = MagicMock(work_items_db_id="fake-db-id")
        items = dispatch.get_dispatchable_items(client)

    assert items == []


def test_get_dispatchable_items_parses_results():
    """Parses Notion page properties into dispatch item dicts."""
    page = {
        "id": str(uuid.uuid4()),
        "properties": _make_props(name="TEST-1", dispatch_via="Cursor", repo_ready=True),
    }
    client = MagicMock()
    client.query_all.side_effect = [[page], []]
    client.retrieve_page.return_value = _project_page()

    with patch("dispatch.get_config") as mock_cfg:
        mock_cfg.return_value = MagicMock(work_items_db_id="fake-db-id")
        items = dispatch.get_dispatchable_items(client)

    assert len(items) == 1
    assert items[0]["name"] == "TEST-1"
    assert items[0]["dispatch_via"] == "Cursor"
    assert items[0]["retry_count"] == 0
    assert items[0]["escalation_level"] == "Normal"
    assert items[0]["project_focus"] is False


def test_get_dispatchable_items_excludes_blocked_and_escalated_rows():
    blocked_page = {
        "id": str(uuid.uuid4()),
        "properties": _make_props(name="BLOCKED-1", blocked_reason="Waiting"),
    }
    escalated_page = {
        "id": str(uuid.uuid4()),
        "properties": _make_props(name="ESC-1", escalation_level="Needs Sam"),
    }
    client = MagicMock()
    client.query_all.side_effect = [[blocked_page, escalated_page], []]

    with patch("dispatch.get_config") as mock_cfg:
        mock_cfg.return_value = MagicMock(work_items_db_id="fake-db-id")
        items = dispatch.get_dispatchable_items(client)

    assert items == []


def test_get_dispatchable_items_excludes_incubate_and_repo_not_ready_rows():
    incubate_page = {
        "id": str(uuid.uuid4()),
        "properties": _make_props(name="INC-1", dispatch_mode="incubate", repo_ready=True),
    }
    not_ready_page = {
        "id": str(uuid.uuid4()),
        "properties": _make_props(name="EXEC-1", repo_ready=False),
    }
    client = MagicMock()
    client.query_all.side_effect = [[incubate_page, not_ready_page], []]

    with patch("dispatch.get_config") as mock_cfg:
        mock_cfg.return_value = MagicMock(work_items_db_id="fake-db-id")
        items = dispatch.get_dispatchable_items(client)

    assert items == []


def test_get_dispatchable_items_enforces_project_cap_and_orders_by_age_then_retry():
    project_id = str(uuid.uuid4())
    older = {
        "id": str(uuid.uuid4()),
            "properties": _make_props(
                name="OLDER-1",
                received_at="2026-01-01T00:00:00Z",
                project_ids=[project_id],
                retry_count=2,
                repo_ready=True,
            ),
        }
    newer_low_retry = {
        "id": str(uuid.uuid4()),
            "properties": _make_props(
                name="NEWER-1",
                received_at="2026-01-02T00:00:00Z",
                retry_count=0,
                repo_ready=True,
            ),
        }
    active_page = {
        "id": str(uuid.uuid4()),
            "properties": _make_props(
                name="ACTIVE-1",
                project_ids=[project_id],
                consumed_at="2026-01-01T01:00:00Z",
                status="In Progress",
                repo_ready=True,
            ),
        }
    project_page = _project_page(name="Capped Project", max_active_items=1)
    client = MagicMock()
    client.query_all.side_effect = [[older, newer_low_retry], [active_page]]
    client.retrieve_page.side_effect = lambda page_id: project_page

    with patch("dispatch.get_config") as mock_cfg:
        mock_cfg.return_value = MagicMock(work_items_db_id="fake-db-id")
        items = dispatch.get_dispatchable_items(client)

    assert [item["name"] for item in items] == ["NEWER-1"]


def test_get_dispatchable_items_prefers_focus_projects_when_available():
    focus_project_id = str(uuid.uuid4())
    other_project_id = str(uuid.uuid4())
    focus_page = {
        "id": str(uuid.uuid4()),
        "properties": _make_props(name="FOCUS-1", project_ids=[focus_project_id], repo_ready=True),
    }
    non_focus_page = {
        "id": str(uuid.uuid4()),
        "properties": _make_props(name="OTHER-1", project_ids=[other_project_id], repo_ready=True),
    }
    client = MagicMock()
    client.query_all.side_effect = [[focus_page, non_focus_page], []]
    project_pages = {
        focus_project_id: _project_page(name="Focused", focus=True),
        other_project_id: _project_page(name="Other"),
    }
    client.retrieve_page.side_effect = lambda page_id: project_pages[page_id]

    with patch("dispatch.get_config") as mock_cfg:
        mock_cfg.return_value = MagicMock(work_items_db_id="fake-db-id")
        items = dispatch.get_dispatchable_items(client)

    assert [item["name"] for item in items] == ["FOCUS-1"]
