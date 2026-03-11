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
        "Status": {"type": "status", "status": {"name": "Not Started"}},
        "Type": {"type": "select", "select": {"name": item_type} if item_type else None},
        "Project": {
            "type": "relation",
            "relation": [{"id": pid} for pid in (project_ids or [])],
        },
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


def _mock_client(props: dict, work_item_id: str | None = None) -> MagicMock:
    """Create a mock NotionAPIClient that returns given properties."""
    client = MagicMock()
    wid = work_item_id or str(uuid.uuid4())
    client.retrieve_page.return_value = {"id": wid, "properties": props}
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


class TestHappyPath:
    """Test successful packet building."""

    def test_basic_packet(self):
        """Build a valid packet with all required fields."""
        wid = str(uuid.uuid4())
        props = _make_props(
            dispatch_via="Claude Code",
            objective="Run the tests",
            kill_condition="Any test fails",
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
        assert uuid.UUID(packet["run_id"])  # valid UUID

    def test_explicit_lane_overrides_default(self):
        """Explicit Execution Lane overrides Dispatch Via default."""
        wid = str(uuid.uuid4())
        props = _make_props(
            dispatch_via="Claude Code",  # default -> dev
            execution_lane="thinker",     # explicit override
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
    client.query_all.return_value = []

    with patch("dispatch.get_config") as mock_cfg:
        mock_cfg.return_value = MagicMock(work_items_db_id="fake-db-id")
        items = dispatch.get_dispatchable_items(client)

    assert items == []


def test_get_dispatchable_items_parses_results():
    """Parses Notion page properties into dispatch item dicts."""
    page = {
        "id": str(uuid.uuid4()),
        "properties": _make_props(name="TEST-1", dispatch_via="Cursor"),
    }
    client = MagicMock()
    client.query_all.return_value = [page]

    with patch("dispatch.get_config") as mock_cfg:
        mock_cfg.return_value = MagicMock(work_items_db_id="fake-db-id")
        items = dispatch.get_dispatchable_items(client)

    assert len(items) == 1
    assert items[0]["name"] == "TEST-1"
    assert items[0]["dispatch_via"] == "Cursor"
