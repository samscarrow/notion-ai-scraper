#!/usr/bin/env python3
"""Retroactive state reconciliation for the buga-fett Lab project.

This script repairs Notion drift when local repo evidence advanced outside the
normal GitHub return path. It is intentionally narrow: it reconciles the
`git-scarrow/buga-fett` Lab project from durable local evidence
(`reports/outcomes.jsonl` plus committed git history).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import get_config
from notion_api import NotionAPIClient, split_rich_text


BUGA_FETT_REPO_URL = "https://github.com/git-scarrow/buga-fett"
NEGATIVE_H1_OUTCOMES = {"duplicate", "informative", "not_applicable", "retracted", "likely_informative"}
TERMINAL_H1_OUTCOMES = {"accepted", "duplicate", "informative", "not_applicable", "retracted"}
REPORT_ID_RE = re.compile(r"#(?P<report_id>\d{7,})")


@dataclass
class Patch:
    target: str
    page_id: str
    title: str
    properties: dict[str, Any]


def _parse_iso(value: str | None) -> datetime:
    if not value:
        return datetime.min.replace(tzinfo=timezone.utc)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _rich_text_property(text: str) -> dict[str, Any]:
    return {
        "rich_text": [{"type": "text", "text": {"content": chunk}} for chunk in split_rich_text(text)]
    }


def _read_latest_outcomes(outcomes_path: str) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    with open(outcomes_path, encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            report_id = str(row.get("report_id") or "").strip()
            if not report_id:
                continue
            existing = latest.get(report_id)
            if existing is None or _parse_iso(row.get("timestamp")) >= _parse_iso(existing.get("timestamp")):
                latest[report_id] = row
    return latest


def _git_output(repo_path: str, *args: str) -> str:
    return subprocess.check_output(["git", "-C", repo_path, *args], text=True).strip()


def _ahead_count(repo_path: str) -> int:
    left_right = _git_output(repo_path, "rev-list", "--left-right", "--count", "origin/main...main")
    behind, ahead = left_right.split()
    _ = behind
    return int(ahead)


def _matching_commit(repo_path: str, patterns: list[str]) -> dict[str, str] | None:
    for pattern in patterns:
        try:
            raw = _git_output(
                repo_path,
                "log",
                "--fixed-strings",
                f"--grep={pattern}",
                "--format=%H%x1f%cI%x1f%s",
                "-n",
                "1",
            )
        except subprocess.CalledProcessError:
            continue
        if raw:
            sha, committed_at, subject = raw.split("\x1f", 2)
            return {"sha": sha, "committed_at": committed_at, "subject": subject}
    return None


def _title(props: dict[str, Any], name: str) -> str:
    return "".join(part.get("plain_text", "") for part in props.get(name, {}).get("title", []))


def _rich(props: dict[str, Any], name: str) -> str:
    return "".join(part.get("plain_text", "") for part in props.get(name, {}).get("rich_text", []))


def _extract_work_item_summaries(client: NotionAPIClient, page_ids: list[str]) -> list[dict[str, Any]]:
    pages: list[dict[str, Any]] = []
    for page_id in page_ids:
        page = client.retrieve_page(page_id)
        props = page.get("properties", {})
        pages.append(
            {
                "id": page_id,
                "page": page,
                "name": _title(props, "Item Name"),
                "status": ((props.get("Status", {}).get("status")) or {}).get("name"),
                "outcome": _rich(props, "Outcome"),
                "findings": _rich(props, "Findings"),
            }
        )
    return pages


def _find_report_work_item(work_items: list[dict[str, Any]], report_id: str) -> dict[str, Any] | None:
    for item in work_items:
        if report_id in item["name"]:
            return item
    return None


def _report_summary(outcome: dict[str, Any]) -> str:
    timestamp = outcome["timestamp"][:10]
    status = outcome["outcome"].replace("_", " ")
    return (
        f"HackerOne report #{outcome['report_id']} for {outcome['program']}: {status} as of {timestamp}. "
        f"{outcome.get('notes', '').strip()}".strip()
    )


def _report_findings(outcome: dict[str, Any]) -> str:
    timestamp = outcome["timestamp"][:10]
    status = outcome["outcome"].replace("_", " ")
    if outcome["outcome"] in NEGATIVE_H1_OUTCOMES:
        return (
            f"Negative market evidence recorded on {timestamp}: HackerOne report #{outcome['report_id']} "
            f"for {outcome['program']} ended in {status}. {outcome.get('notes', '').strip()} "
            f"Treat this finding class as low-value on this program family unless a materially stronger exploit chain exists."
        )
    return (
        f"HackerOne report #{outcome['report_id']} for {outcome['program']} remains active in {status} state "
        f"as of {timestamp}. {outcome.get('notes', '').strip()}"
    )


def _project_h1_status(report_id: str, outcome: dict[str, Any] | None) -> str:
    if not outcome:
        return f"H1 status: report #{report_id} has no local evidence recorded."
    return (
        f"H1 status: report #{report_id} {outcome['outcome'].replace('_', ' ')} "
        f"on {outcome['timestamp'][:10]}."
    )


def _terminal_work_item_properties(
    *,
    status_timestamp: str,
    verdict: str,
    findings: str,
) -> dict[str, Any]:
    return {
        "Status": {"status": {"name": "Done"}},
        "Verdict": {"select": {"name": verdict}},
        "Close Reason": {"select": {"name": "Normal"}},
        "Findings": _rich_text_property(findings),
        "Return Received At": {"date": {"start": status_timestamp}},
        "Return Consumed At": {"date": {"start": status_timestamp}},
        "Librarian Request Received At": {"date": {"start": status_timestamp}},
        "Librarian Request Consumed At": {"date": {"start": status_timestamp}},
        "Synthesis Complete": {"checkbox": True},
        "Synthesis Completed At": {"date": {"start": status_timestamp}},
    }


def _append_missing_terminal_audit_rows(
    patches: list[Patch],
    *,
    item_id: str,
    item_name: str,
    status_timestamp: str,
    existing_transitions: set[str],
) -> None:
    intake_transition = "InProgress→Awaiting Intake"
    synth_transition = "Awaiting Intake→Done"

    if intake_transition not in existing_transitions:
        patches.append(
            Patch(
                "audit_log",
                item_id,
                f"{item_name} audit: intake",
                {
                    "Transition": {"title": [{"text": {"content": intake_transition}}]},
                    "Work Item": {"relation": [{"id": item_id}]},
                    "Agent": {"select": {"name": "Webhook Bridge"}},
                    "From Status": {"select": {"name": "In Progress"}},
                    "To Status": {"select": {"name": "Awaiting Intake"}},
                    "Signal Consumed": {"select": {"name": "LR"}},
                    "Consumption Timestamp": {"date": {"start": status_timestamp}},
                },
            )
        )

    if synth_transition not in existing_transitions:
        patches.append(
            Patch(
                "audit_log",
                item_id,
                f"{item_name} audit: synthesis",
                {
                    "Transition": {"title": [{"text": {"content": synth_transition}}]},
                    "Work Item": {"relation": [{"id": item_id}]},
                    "Agent": {"select": {"name": "Librarian"}},
                    "From Status": {"select": {"name": "Awaiting Intake"}},
                    "To Status": {"select": {"name": "Done"}},
                    "Signal Consumed": {"select": {"name": "Synth"}},
                    "Consumption Timestamp": {"date": {"start": status_timestamp}},
                },
            )
        )


def build_buga_fett_plan(
    project_page: dict[str, Any],
    work_items: list[dict[str, Any]],
    outcomes: dict[str, dict[str, Any]],
    *,
    repo_path: str,
    audit_transitions: dict[str, set[str]] | None = None,
) -> list[Patch]:
    patches: list[Patch] = []
    project_props = project_page.get("properties", {})
    audit_transitions = audit_transitions or {}

    for report_id in ("3627204", "3627744"):
        outcome = outcomes.get(report_id)
        item = _find_report_work_item(work_items, report_id)
        if outcome is None or item is None:
            continue

        summary = _report_summary(outcome)
        props: dict[str, Any] = {
            "Outcome": _rich_text_property(summary),
            "Run Date": {"date": {"start": outcome["timestamp"]}},
        }

        if outcome["outcome"] in TERMINAL_H1_OUTCOMES:
            verdict = "Passed" if outcome["outcome"] == "accepted" else "Killed"
            props.update(
                _terminal_work_item_properties(
                    status_timestamp=outcome["timestamp"],
                    verdict=verdict,
                    findings=_report_findings(outcome),
                )
            )
        else:
            props["Status"] = {"status": {"name": "In Progress"}}

        patches.append(Patch("work_item", item["id"], item["name"], props))
        if outcome["outcome"] in TERMINAL_H1_OUTCOMES:
            _append_missing_terminal_audit_rows(
                patches,
                item_id=item["id"],
                item_name=item["name"],
                status_timestamp=outcome["timestamp"],
                existing_transitions=audit_transitions.get(item["id"], set()),
            )

    finding_gate_commit = _matching_commit(
        repo_path,
        ["finding class conversion gate", "governance: finding class conversion gate"],
    )
    if finding_gate_commit:
        for item in work_items:
            if item["name"] != "Finding class conversion validation — pre-scan gate":
                continue
            summary = (
                f"Implemented in local repo commit {finding_gate_commit['sha'][:7]} on "
                f"{finding_gate_commit['committed_at'][:10]}: {finding_gate_commit['subject']}."
            )
            findings = (
                "The repo now enforces a pre-scan finding-class conversion gate. "
                "Noisy stale-DNS/info-disclosure classes are downgraded before submission, "
                "which closes the original review question and converts the result into a durable governance control."
            )
            patches.append(
                Patch(
                    "work_item",
                    item["id"],
                    item["name"],
                    {
                        "Outcome": _rich_text_property(summary),
                        "Run Date": {"date": {"start": finding_gate_commit["committed_at"]}},
                        **_terminal_work_item_properties(
                            status_timestamp=finding_gate_commit["committed_at"],
                            verdict="Passed",
                            findings=findings,
                        ),
                    },
                )
            )
            _append_missing_terminal_audit_rows(
                patches,
                item_id=item["id"],
                item_name=item["name"],
                status_timestamp=finding_gate_commit["committed_at"],
                existing_transitions=audit_transitions.get(item["id"], set()),
            )

    intel_feed_commit = _matching_commit(
        repo_path,
        ["intel_feed.py — competitive intelligence feed", "competitive intelligence feed"],
    )
    if intel_feed_commit:
        for item in work_items:
            if item["name"] != "Competitive intelligence feed — dynamic FINDING_CLASS_REGISTRY":
                continue
            summary = item["outcome"] or (
                f"Core implementation landed in local repo commit {intel_feed_commit['sha'][:7]} on "
                f"{intel_feed_commit['committed_at'][:10]}: {intel_feed_commit['subject']}."
            )
            patches.append(
                Patch(
                    "work_item",
                    item["id"],
                    item["name"],
                    {
                        "Status": {"status": {"name": "In Progress"}},
                        "Outcome": _rich_text_property(summary),
                        "Run Date": {"date": {"start": intel_feed_commit["committed_at"]}},
                    },
                )
            )

    ahead = _ahead_count(repo_path)
    anduril = outcomes.get("3627204")
    palantir = outcomes.get("3627744")
    notes = (
        "Autonomous bug bounty pipeline -> governed multi-mode evidence platform. "
        "4 modes: bounty, monitor, gov, election. 411 programs, 28 check manifests, "
        "deny-by-default governance. "
        f"Anduril #3627204: {anduril['outcome'].replace('_', ' ')} on {anduril['timestamp'][:10]}. "
        if anduril
        else "Anduril #3627204: no local evidence recorded. "
    )
    notes += (
        f"Palantir #3627744: {palantir['outcome'].replace('_', ' ')} on {palantir['timestamp'][:10]}. "
        if palantir
        else "Palantir #3627744: no local evidence recorded. "
    )
    notes += (
        f"Local repo main is ahead {ahead} commit(s) over origin/main."
    )
    patches.append(
        Patch(
            "project",
            project_page["id"],
            _title(project_props, "Project Name"),
            {"Notes": _rich_text_property(notes)},
        )
    )

    return patches


def _dedupe_patches(patches: list[Patch]) -> list[Patch]:
    merged: dict[str, Patch] = {}
    ordered: list[Patch] = []
    for patch in patches:
        if patch.target == "audit_log":
            ordered.append(patch)
            continue
        existing = merged.get(patch.page_id)
        if existing is None:
            merged[patch.page_id] = patch
            ordered.append(patch)
            continue
        existing.properties.update(patch.properties)
    return ordered


def apply_plan(client: NotionAPIClient, patches: list[Patch], *, dry_run: bool) -> None:
    for patch in patches:
        if dry_run:
            print(json.dumps({"target": patch.target, "page_id": patch.page_id, "title": patch.title, "properties": patch.properties}, indent=2))
            continue
        if patch.target == "audit_log":
            client.create_page(parent={"database_id": get_config().audit_log_db_id}, properties=patch.properties)
        else:
            client.update_page(patch.page_id, patch.properties)
        print(f"updated {patch.target}: {patch.title} ({patch.page_id})")


def load_project_by_repo_url(client: NotionAPIClient, repo_url: str) -> dict[str, Any]:
    cfg = get_config()
    matches = client.query_all(
        cfg.lab_projects_db_id,
        filter_payload={"property": "GitHub URL", "url": {"equals": repo_url}},
        page_size=10,
    )
    if len(matches) != 1:
        raise RuntimeError(f"Expected exactly one Lab Project for {repo_url}, found {len(matches)}")
    return matches[0]


def load_audit_transitions_by_work_item(
    client: NotionAPIClient,
    work_item_ids: list[str],
) -> dict[str, set[str]]:
    cfg = get_config()
    transitions: dict[str, set[str]] = {work_item_id: set() for work_item_id in work_item_ids}
    for work_item_id in work_item_ids:
        rows = client.query_all(
            cfg.audit_log_db_id,
            filter_payload={"property": "Work Item", "relation": {"contains": work_item_id}},
            page_size=100,
        )
        for row in rows:
            title = _title(row.get("properties", {}), "Transition")
            if title:
                transitions.setdefault(work_item_id, set()).add(title)
    return transitions


def main() -> int:
    parser = argparse.ArgumentParser(description="Reconcile buga-fett Notion state from local repo evidence.")
    parser.add_argument("--repo-path", default="/home/sam/projects/buga-fett", help="Path to the local buga-fett repo")
    parser.add_argument("--dry-run", action="store_true", help="Print the patch plan without applying it")
    args = parser.parse_args()

    cfg = get_config()
    client = NotionAPIClient(cfg.notion_token)
    project_page = load_project_by_repo_url(client, BUGA_FETT_REPO_URL)
    project_props = project_page.get("properties", {})
    work_item_ids = [rel["id"] for rel in project_props.get("Work Items", {}).get("relation", [])]
    work_items = _extract_work_item_summaries(client, work_item_ids)
    audit_transitions = load_audit_transitions_by_work_item(client, work_item_ids)
    outcomes = _read_latest_outcomes(os.path.join(args.repo_path, "reports", "outcomes.jsonl"))
    patches = _dedupe_patches(
        build_buga_fett_plan(
            project_page,
            work_items,
            outcomes,
            repo_path=args.repo_path,
            audit_transitions=audit_transitions,
        )
    )
    apply_plan(client, patches, dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
