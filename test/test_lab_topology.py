from __future__ import annotations

import os
import sys
from types import SimpleNamespace
from unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "cli"))

import lab_topology


def _make_snapshot() -> dict:
    database = {
        "key": "work_items",
        "label": "Work Items",
        "notion_public_id": "daeb64d4-e5a8-4a7b-b0dc-7555cbc3def6",
        "notion_internal_id": "94e7ae5f-19c8-4008-b9cd-66afc18ce087",
        "schema": {
            "property_id_to_name": {
                "drra": "Dispatch Requested Received At",
                "drca": "Dispatch Requested Consumed At",
                "pn": "Prompt Notes",
                "sc": "Synthesis Completed At",
                "scoa": "Synthesis Consumed At",
                "disp": "Disposition",
                "rs": "Routing Signal",
                "sb": "Superseded By",
                "proj": "Project",
                "type": "Type",
            },
            "property_name_to_type": {
                "Dispatch Requested Received At": "date",
                "Dispatch Requested Consumed At": "date",
                "Prompt Notes": "rich_text",
                "Synthesis Completed At": "date",
                "Synthesis Consumed At": "date",
                "Disposition": "select",
                "Routing Signal": "select",
                "Superseded By": "relation",
                "Project": "relation",
                "Type": "select",
            },
            "property_name_to_id": {
                "Dispatch Requested Received At": "drra",
                "Dispatch Requested Consumed At": "drca",
                "Prompt Notes": "pn",
                "Synthesis Completed At": "sc",
                "Synthesis Consumed At": "scoa",
                "Disposition": "disp",
                "Routing Signal": "rs",
                "Superseded By": "sb",
                "Project": "proj",
                "Type": "type",
            },
            "options": {},
        },
        "sources": ["test"],
    }
    project_policy = {
        "id": "project-1",
        "name": "Work Items Project",
        "focus": True,
        "max_active_items": 2,
        "min_terminal_value": "Any",
        "fork_budget": 1,
    }
    agent = {
        "key": "lab_dispatcher",
        "label": "Lab Dispatcher",
        "notion_public_id": "31be7cc7-01d5-8042-98e4-dc87af5761d4",
        "notion_internal_id": "31be7cc7-01d5-80eb-b535-009278533411",
        "published_artifact_id": "artifact-1",
        "published_artifact_publish_time": "2026-03-19T00:00:00+00:00",
        "published_artifact_publish_version": 12,
        "published_artifact_workflow_version": 12,
        "published_instruction_block_id": "published-block-1",
        "draft_runtime_config": {
            "name": "Lab Dispatcher",
            "description": None,
            "model": "unknown",
            "triggers": [
                {
                    "enabled": True,
                    "type": "notion.page.updated",
                    "database_key": "work_items",
                    "properties": ["Dispatch Requested Received At"],
                    "conditions": [],
                }
            ],
            "permissions": [
                {
                    "resource_type": "database",
                    "resource_key": "work_items",
                    "access": "read_and_write",
                }
            ],
            "mcp_servers": [],
        },
        "published_runtime_config": {
            "name": "Lab Dispatcher",
            "description": None,
            "model": "unknown",
            "triggers": [
                {
                    "enabled": True,
                    "type": "notion.page.updated",
                    "database_key": "work_items",
                    "properties": ["Dispatch Requested Received At"],
                    "conditions": [],
                }
            ],
            "permissions": [
                {
                    "resource_type": "database",
                    "resource_key": "work_items",
                    "access": "read_and_write",
                }
            ],
            "mcp_servers": [],
        },
        "draft_instruction_hash": "draft-hash",
        "published_instruction_hash": "draft-hash",
        "instruction_last_edited_time": "2026-03-19T00:00:00+00:00",
        "workflow_last_edited_time": "2026-03-19T00:00:00+00:00",
        "workflow_version": 12,
        "permissions": [
            {
                "resource_key": "work_items",
                "resource_label": "Work Items",
                "actions": ["read_and_write"],
                "access": "read_and_write",
                "access_strength": 2,
                "resource_type": "database",
            }
        ],
        "triggers": [
            {
                "id": "trigger-1",
                "enabled": True,
                "type": "notion.page.updated",
                "database_key": "work_items",
                "database_label": "Work Items",
                "database_internal_id": database["notion_internal_id"],
                "properties": [
                    {"id": "drra", "name": "Dispatch Requested Received At", "type": "date"},
                ],
                "conditions": [],
            }
        ],
        "live_present": True,
        "registry_present": True,
    }
    contract = {
        "name": "dispatch_request_to_dispatcher",
        "source": "work_items",
        "target": "lab_dispatcher",
        "database": "work_items",
        "database_label": "Work Items",
        "database_public_id": database["notion_public_id"],
        "database_internal_id": database["notion_internal_id"],
        "trigger_fields": ["Dispatch Requested Received At"],
        "consumed_fields": ["Dispatch Requested Consumed At"],
        "produced_fields": ["Prompt Notes"],
        "required_artifacts": ["Prompt Notes"],
        "selector": {},
        "upstream_complete_fields": ["Dispatch Requested Received At"],
        "required_access": "read_and_write",
        "evidence_sources": [{"kind": "agent_trigger", "agent": "lab_dispatcher"}],
        "source_resolved": {"kind": "database", "key": "work_items", "label": "Work Items"},
        "target_resolved": {"kind": "agent", "key": "lab_dispatcher", "label": "Lab Dispatcher"},
    }
    snapshot = {
        "generated_at": "2026-03-19T00:00:00+00:00",
        "workspace": {"space_id": "space-1"},
        "databases": [database],
        "agents": [agent],
        "automations": [],
        "project_policies": [project_policy],
        "contracts": [contract],
    }
    snapshot["indexes"] = {
        "agent_by_key": {"lab_dispatcher": agent},
        "database_by_key": {"work_items": database},
        "database_by_public_id": {database["notion_public_id"]: database},
        "database_by_internal_id": {database["notion_internal_id"]: database},
        "project_policy_by_id": {project_policy["id"]: project_policy},
    }
    return snapshot


def _make_recent_page(
    *,
    prompt_notes: str = "",
    disposition: str | None = None,
    routing_signal: str | None = None,
    superseded_by: list[str] | None = None,
    item_type: str | None = None,
    project_ids: list[str] | None = None,
) -> dict:
    return {
        "id": "page-1",
        "created_time": "2026-03-19T00:00:00Z",
        "last_edited_time": "2026-03-19T00:00:00Z",
        "properties": {
            "Item Name": {"type": "title", "title": [{"plain_text": "TEST-1"}]},
            "Dispatch Requested Received At": {
                "type": "date",
                "date": {"start": "2026-03-19T00:00:00Z"},
            },
            "Prompt Notes": {
                "type": "rich_text",
                "rich_text": [{"plain_text": prompt_notes}] if prompt_notes else [],
            },
            "Synthesis Completed At": {
                "type": "date",
                "date": None,
            },
            "Synthesis Consumed At": {
                "type": "date",
                "date": None,
            },
            "Disposition": {
                "type": "select",
                "select": {"name": disposition} if disposition else None,
            },
            "Routing Signal": {
                "type": "select",
                "select": {"name": routing_signal} if routing_signal else None,
            },
            "Superseded By": {
                "type": "relation",
                "relation": [{"id": successor_id} for successor_id in (superseded_by or [])],
            },
            "Project": {
                "type": "relation",
                "relation": [{"id": project_id} for project_id in (project_ids or ["project-1"])],
            },
            "Type": {
                "type": "select",
                "select": {"name": item_type} if item_type else None,
            },
        },
    }


def test_resolve_resource_identifier_handles_public_internal_and_collection():
    snapshot = _make_snapshot()

    public_result = lab_topology.resolve_resource_identifier(
        "daeb64d4-e5a8-4a7b-b0dc-7555cbc3def6",
        snapshot,
    )
    internal_result = lab_topology.resolve_resource_identifier(
        "94e7ae5f-19c8-4008-b9cd-66afc18ce087",
        snapshot,
    )
    collection_result = lab_topology.resolve_resource_identifier(
        "collection://94e7ae5f-19c8-4008-b9cd-66afc18ce087",
        snapshot,
    )

    assert public_result["resource_type"] == "database"
    assert internal_result["resource_type"] == "database"
    assert collection_result["resource_type"] == "database"
    assert public_result["label"] == "Work Items"


def test_normalize_trigger_resolves_property_names():
    snapshot = _make_snapshot()
    database = snapshot["indexes"]["database_by_internal_id"]["94e7ae5f-19c8-4008-b9cd-66afc18ce087"]
    trigger = {
        "id": "trigger-2",
        "enabled": True,
        "state": {
            "type": "notion.page.updated",
            "collectionId": "94e7ae5f-19c8-4008-b9cd-66afc18ce087",
            "propertyIds": ["drra"],
            "propertyFilters": {
                "all": [
                    {
                        "property": "drra",
                        "filter": {"operator": "changes_to_any"},
                    }
                ]
            },
        },
    }

    normalized = lab_topology._normalize_trigger(
        trigger,
        {"94e7ae5f-19c8-4008-b9cd-66afc18ce087": database},
    )

    assert normalized["database_key"] == "work_items"
    assert normalized["properties"][0]["name"] == "Dispatch Requested Received At"
    assert normalized["conditions"][0]["property_name"] == "Dispatch Requested Received At"


def test_render_block_markdown_follows_multi_hop_copied_chain():
    published_tree = {
        "recordMap": {
            "block": {
                "published": {
                    "value": {
                        "id": "published",
                        "type": "page",
                        "format": {"copied_from_pointer": {"id": "draft"}},
                    }
                }
            }
        }
    }
    draft_tree = {
        "recordMap": {
            "block": {
                "draft": {
                    "value": {
                        "id": "draft",
                        "type": "page",
                        "format": {"copied_from_pointer": {"id": "source"}},
                    }
                }
            }
        }
    }
    source_tree = {
        "recordMap": {
            "block": {
                "source": {
                    "value": {
                        "id": "source",
                        "type": "page",
                    }
                }
            }
        }
    }

    with mock.patch.object(
        lab_topology.notion_blocks,
        "get_block_tree",
        side_effect=[published_tree, draft_tree, source_tree],
    ), mock.patch.object(
        lab_topology.block_builder,
        "blocks_to_markdown",
        side_effect=lambda blocks_map, root_id: f"root={root_id}",
    ):
        rendered = lab_topology._render_block_markdown("published", "space-1", "token", "user-1")

    assert rendered == "root=source"


def test_fetch_recent_work_items_uses_explicit_database_id_when_config_is_empty():
    recent_page = {
        "id": "page-1",
        "created_time": "2026-03-20T00:00:00Z",
        "last_edited_time": "2026-03-20T00:00:00Z",
        "properties": {},
    }
    fake_client = mock.Mock()
    fake_client.query_all.return_value = [recent_page]

    with mock.patch.object(
        lab_topology,
        "get_config",
        return_value=SimpleNamespace(notion_token="token", work_items_db_id=""),
    ), mock.patch.object(
        lab_topology.notion_api,
        "NotionAPIClient",
        return_value=fake_client,
    ):
        pages, error = lab_topology.fetch_recent_work_items(database_id="daeb64d4-e5a8-4a7b-b0dc-7555cbc3def6")

    assert error is None
    assert pages == [recent_page]
    fake_client.query_all.assert_called_once_with("daeb64d4-e5a8-4a7b-b0dc-7555cbc3def6")


def test_evaluate_drift_flags_missing_published_artifact():
    snapshot = _make_snapshot()
    snapshot["agents"][0]["published_artifact_id"] = None

    report = lab_topology.evaluate_drift(snapshot, recent_work_items=[])

    assert any(
        finding["code"] == "T.4" and finding["severity"] == "P0"
        for finding in report["findings"]
    )


def test_evaluate_drift_flags_missing_contract_trigger():
    snapshot = _make_snapshot()
    snapshot["agents"][0]["triggers"] = []

    report = lab_topology.evaluate_drift(snapshot, recent_work_items=[])

    assert any(
        finding["code"] == "T.6" and finding["severity"] == "MUST-FIX"
        for finding in report["findings"]
    )


def test_evaluate_drift_flags_semantic_runtime_config_drift():
    snapshot = _make_snapshot()
    snapshot["agents"][0]["draft_runtime_config"]["model"] = "oval-kumquat-medium"

    report = lab_topology.evaluate_drift(snapshot, recent_work_items=[])

    assert any(
        finding["code"] == "T.4" and "runtime config differs" in finding["detail"]
        for finding in report["findings"]
    )


def test_evaluate_drift_flags_instruction_content_drift():
    snapshot = _make_snapshot()
    snapshot["agents"][0]["draft_instruction_hash"] = "new-hash"

    report = lab_topology.evaluate_drift(snapshot, recent_work_items=[])

    assert any(
        finding["code"] == "T.4" and "instruction content differs" in finding["detail"]
        for finding in report["findings"]
    )


def test_evaluate_drift_does_not_overcall_when_artifact_payload_is_unreadable():
    snapshot = _make_snapshot()
    snapshot["agents"][0]["published_runtime_config"] = None
    snapshot["agents"][0]["published_instruction_hash"] = None
    snapshot["agents"][0]["workflow_version"] = 99

    report = lab_topology.evaluate_drift(snapshot, recent_work_items=[])

    assert not any(finding["code"] == "T.4" for finding in report["findings"])


def test_evaluate_drift_flags_missing_permission():
    snapshot = _make_snapshot()
    snapshot["agents"][0]["permissions"] = []

    report = lab_topology.evaluate_drift(snapshot, recent_work_items=[])

    assert any(
        finding["code"] == "T.5" and finding["severity"] == "P0"
        for finding in report["findings"]
    )


def test_evaluate_drift_flags_t7_missing_downstream_artifact():
    snapshot = _make_snapshot()
    report = lab_topology.evaluate_drift(snapshot, recent_work_items=[_make_recent_page(prompt_notes="")])

    assert any(
        finding["code"] == "T.7" and "Prompt Notes" in finding["detail"]
        for finding in report["findings"]
    )


def test_evaluate_drift_passes_when_required_artifact_is_present():
    snapshot = _make_snapshot()
    report = lab_topology.evaluate_drift(snapshot, recent_work_items=[_make_recent_page(prompt_notes="ready")])

    assert not any(finding["code"] == "T.7" for finding in report["findings"])


def test_evaluate_drift_flags_missing_synthesis_consumed_at():
    snapshot = _make_snapshot()
    snapshot["contracts"].append(
        {
            "name": "synthesis_completed_to_research_designer",
            "source": "work_items",
            "target": "lab_research_designer",
            "database": "work_items",
            "database_label": "Work Items",
            "database_public_id": snapshot["databases"][0]["notion_public_id"],
            "database_internal_id": snapshot["databases"][0]["notion_internal_id"],
            "trigger_fields": ["Synthesis Completed At"],
            "consumed_fields": [],
            "produced_fields": ["Synthesis Consumed At"],
            "required_artifacts": ["Synthesis Consumed At"],
            "selector": {},
            "upstream_complete_fields": ["Synthesis Completed At"],
            "required_access": "read_and_write",
            "evidence_sources": [],
            "source_resolved": {"kind": "database", "key": "work_items", "label": "Work Items"},
            "target_resolved": {"kind": "agent", "key": "lab_dispatcher", "label": "Lab Dispatcher"},
        }
    )
    page = _make_recent_page()
    page["properties"]["Synthesis Completed At"] = {
        "type": "date",
        "date": {"start": "2026-03-19T00:00:00Z"},
    }

    report = lab_topology.evaluate_drift(snapshot, recent_work_items=[page])

    assert any(
        finding["code"] == "T.7" and "Synthesis Consumed At" in finding["detail"]
        for finding in report["findings"]
    )


def test_evaluate_drift_skips_t7_before_contract_rollout():
    snapshot = _make_snapshot()
    snapshot["contracts"][0]["rollout_started_at"] = "2026-03-21T00:00:00+00:00"
    page = _make_recent_page(prompt_notes="")
    page["properties"]["Dispatch Requested Received At"] = {
        "type": "date",
        "date": {"start": "2026-03-19T00:00:00Z"},
    }

    report = lab_topology.evaluate_drift(snapshot, recent_work_items=[page])

    assert not any(finding["code"] == "T.7" for finding in report["findings"])


def test_evaluate_drift_handles_naive_page_timestamps_for_rollout_filtering():
    snapshot = _make_snapshot()
    snapshot["contracts"][0]["rollout_started_at"] = "2026-03-21T00:00:00+00:00"
    page = _make_recent_page(prompt_notes="")
    page["created_time"] = "2026-03-19T00:00:00"
    page["last_edited_time"] = "2026-03-19T01:00:00"
    page["properties"]["Dispatch Requested Received At"] = {
        "type": "date",
        "date": {"start": "2026-03-19T00:00:00"},
    }

    report = lab_topology.evaluate_drift(snapshot, recent_work_items=[page])

    assert not any(finding["code"] == "T.7" for finding in report["findings"])


def test_evaluate_drift_flags_missing_disposition_when_successor_exists():
    snapshot = _make_snapshot()
    page = _make_recent_page(superseded_by=["successor-1"])
    page["properties"]["Synthesis Consumed At"] = {
        "type": "date",
        "date": {"start": "2026-03-19T00:00:00Z"},
    }

    report = lab_topology.evaluate_drift(snapshot, recent_work_items=[page])

    assert any(
        finding["code"] == "T.9" and "Disposition is empty" in finding["detail"]
        for finding in report["findings"]
    )


def test_evaluate_drift_flags_repeat_with_different_successor_type():
    snapshot = _make_snapshot()
    parent = _make_recent_page(
        disposition="Repeat",
        superseded_by=["successor-1"],
        item_type="Design Spec",
    )
    parent["properties"]["Synthesis Consumed At"] = {
        "type": "date",
        "date": {"start": "2026-03-19T00:00:00Z"},
    }
    successor = _make_recent_page(item_type="Experiment")
    successor["id"] = "successor-1"
    successor["properties"]["Item Name"] = {
        "type": "title",
        "title": [{"plain_text": "SUCCESSOR-1"}],
    }

    report = lab_topology.evaluate_drift(snapshot, recent_work_items=[parent, successor])

    assert any(
        finding["code"] == "T.9" and "different Type" in finding["detail"]
        for finding in report["findings"]
    )


def test_evaluate_drift_flags_project_fork_budget_exceeded():
    snapshot = _make_snapshot()
    first = _make_recent_page(
        disposition="Fork",
        superseded_by=["successor-1"],
        project_ids=["project-1"],
    )
    second = _make_recent_page(
        disposition="Fork",
        superseded_by=["successor-2"],
        project_ids=["project-1"],
    )
    first["id"] = "fork-1"
    second["id"] = "fork-2"
    first["properties"]["Item Name"] = {"type": "title", "title": [{"plain_text": "FORK-1"}]}
    second["properties"]["Item Name"] = {"type": "title", "title": [{"plain_text": "FORK-2"}]}
    for page in (first, second):
        page["properties"]["Synthesis Consumed At"] = {
            "type": "date",
            "date": {"start": "2026-03-19T00:00:00Z"},
        }

    report = lab_topology.evaluate_drift(snapshot, recent_work_items=[first, second])

    assert any(
        finding["code"] == "T.10" and "fork budget exceeded" in finding["detail"]
        for finding in report["findings"]
    )
