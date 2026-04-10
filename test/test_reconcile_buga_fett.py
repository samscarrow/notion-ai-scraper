import os
import sys

ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, os.path.join(ROOT, "cli"))

import reconcile_buga_fett


def _title_prop(text: str) -> dict:
    return {"title": [{"plain_text": text}]}


def _rich_prop(text: str = "") -> dict:
    return {"rich_text": [{"plain_text": text}]} if text else {"rich_text": []}


def _project_page() -> dict:
    return {
        "id": "project-1",
        "properties": {
            "Project Name": _title_prop("bountyswarm (buga-fett)"),
            "Work Items": {"relation": []},
        },
    }


def _work_item(name: str, page_id: str, *, outcome: str = "", findings: str = "") -> dict:
    return {
        "id": page_id,
        "page": {
            "id": page_id,
            "properties": {
                "Item Name": _title_prop(name),
                "Outcome": _rich_prop(outcome),
                "Findings": _rich_prop(findings),
            },
        },
        "name": name,
        "status": "Not Started",
        "outcome": outcome,
        "findings": findings,
    }


def test_terminal_report_backfills_full_state_and_audit_rows(monkeypatch) -> None:
    monkeypatch.setattr(reconcile_buga_fett, "_ahead_count", lambda repo_path: 2)
    monkeypatch.setattr(reconcile_buga_fett, "_matching_commit", lambda repo_path, patterns: None)

    patches = reconcile_buga_fett.build_buga_fett_plan(
        _project_page(),
        [
            _work_item("Anduril #3627204 — validate or close", "wi-anduril"),
            _work_item("Palantir #3627744 — monitor triage", "wi-palantir"),
        ],
        {
            "3627204": {
                "report_id": "3627204",
                "program": "Anduril",
                "outcome": "duplicate",
                "timestamp": "2026-03-27T16:30:07.394274+00:00",
                "notes": "Marked duplicate.",
            },
            "3627744": {
                "report_id": "3627744",
                "program": "Palantir",
                "outcome": "likely_informative",
                "timestamp": "2026-03-27T15:11:50.036026+00:00",
                "notes": "Class downgraded.",
            },
        },
        repo_path="/tmp/buga-fett",
        audit_transitions={},
    )

    anduril_patch = next(patch for patch in patches if patch.target == "work_item" and patch.page_id == "wi-anduril")
    assert anduril_patch.properties["Status"]["status"]["name"] == "Done"
    assert anduril_patch.properties["Verdict"]["select"]["name"] == "Killed"
    assert anduril_patch.properties["Return Received At"]["date"]["start"] == "2026-03-27T16:30:07.394274+00:00"
    assert anduril_patch.properties["Return Consumed At"]["date"]["start"] == "2026-03-27T16:30:07.394274+00:00"
    assert anduril_patch.properties["Librarian Request Received At"]["date"]["start"] == "2026-03-27T16:30:07.394274+00:00"
    assert anduril_patch.properties["Librarian Request Consumed At"]["date"]["start"] == "2026-03-27T16:30:07.394274+00:00"
    assert anduril_patch.properties["Synthesis Complete"]["checkbox"] is True

    audit_patches = [patch for patch in patches if patch.target == "audit_log" and patch.page_id == "wi-anduril"]
    assert len(audit_patches) == 2
    transitions = {patch.properties["Transition"]["title"][0]["text"]["content"] for patch in audit_patches}
    assert transitions == {"InProgress→Awaiting Intake", "Awaiting Intake→Done"}


def test_nonterminal_report_only_moves_to_in_progress(monkeypatch) -> None:
    monkeypatch.setattr(reconcile_buga_fett, "_ahead_count", lambda repo_path: 2)
    monkeypatch.setattr(reconcile_buga_fett, "_matching_commit", lambda repo_path, patterns: None)

    patches = reconcile_buga_fett.build_buga_fett_plan(
        _project_page(),
        [_work_item("Palantir #3627744 — monitor triage", "wi-palantir")],
        {
            "3627744": {
                "report_id": "3627744",
                "program": "Palantir",
                "outcome": "likely_informative",
                "timestamp": "2026-03-27T15:11:50.036026+00:00",
                "notes": "Class downgraded.",
            }
        },
        repo_path="/tmp/buga-fett",
        audit_transitions={},
    )

    palantir_patch = next(patch for patch in patches if patch.target == "work_item")
    assert palantir_patch.properties["Status"]["status"]["name"] == "In Progress"
    assert "Synthesis Complete" not in palantir_patch.properties
    assert all(patch.target != "audit_log" for patch in patches)


def test_commit_backfill_adds_missing_audit_rows(monkeypatch) -> None:
    monkeypatch.setattr(reconcile_buga_fett, "_ahead_count", lambda repo_path: 2)

    def fake_matching_commit(repo_path: str, patterns: list[str]) -> dict | None:
        if "finding class conversion gate" in patterns[0]:
            return {
                "sha": "b1d6f45abc",
                "committed_at": "2026-03-27T14:00:00+00:00",
                "subject": "governance: finding class conversion gate + fix broken passive checks",
            }
        return None

    monkeypatch.setattr(reconcile_buga_fett, "_matching_commit", fake_matching_commit)

    patches = reconcile_buga_fett.build_buga_fett_plan(
        _project_page(),
        [_work_item("Finding class conversion validation — pre-scan gate", "wi-gate")],
        {},
        repo_path="/tmp/buga-fett",
        audit_transitions={"wi-gate": {"InProgress→Awaiting Intake"}},
    )

    gate_patch = next(patch for patch in patches if patch.target == "work_item")
    assert gate_patch.properties["Status"]["status"]["name"] == "Done"
    assert gate_patch.properties["Verdict"]["select"]["name"] == "Passed"
    assert gate_patch.properties["Synthesis Complete"]["checkbox"] is True

    audit_patches = [patch for patch in patches if patch.target == "audit_log"]
    assert len(audit_patches) == 1
    assert audit_patches[0].properties["Transition"]["title"][0]["text"]["content"] == "Awaiting Intake→Done"
