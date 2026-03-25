#!/usr/bin/env python3
"""
lab_auditor.py — Live Lab audit runner.

Runs the objective, automatable portions of the Lab Auditor spec against the
live Notion workspace using the public API.
"""

from __future__ import annotations

import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

try:
    from . import config, lab_topology, notion_api
except ImportError:
    import config, lab_topology, notion_api

try:
    from .dispatch import REDACTION_CONFIG, ENV_RESTRICTIONS
except ImportError:
    from dispatch import REDACTION_CONFIG, ENV_RESTRICTIONS


CFG = config.get_config()
MODEL_EPOCH = datetime(2026, 3, 6, tzinfo=timezone.utc)
TERMINAL_STATUSES = {"Done", "Passed", "Kill Condition Met", "Inconclusive", "Closed", "Blocked"}
TERMINAL_BODY_SCAN_STATUSES = {"Done", "Passed", "Kill Condition Met", "Inconclusive"}
PROMPT_ACTIVE_STATUSES = {"Queued", "Dispatched", "Generating", "Dispatch requested"}
PROMPT_TERMINAL_STATUSES = {"Delivered", "Revised", "Skipped"}
MAX_BODY_SCAN_ITEMS = 10
# Derived from shared contracts
SAFE_PROD_LANES = set(ENV_RESTRICTIONS["environments"]["production"]["allowed_lanes"])
REDACTION_PATTERNS = [
    (p["label"], re.compile(p["regex"], re.IGNORECASE if "postgres" in p["regex"].lower() else 0))
    for p in REDACTION_CONFIG["patterns"]
]


@dataclass
class Violation:
    code: str
    severity: str
    subject: str
    detail: str


BLOCKING_SEVERITIES = {"MUST-FIX", "P0"}


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _join_plain_text(items: list[dict[str, Any]]) -> str:
    parts: list[str] = []
    for item in items or []:
        text = item.get("plain_text")
        if text is None:
            text = (item.get("text") or {}).get("content", "")
        parts.append(text)
    return "".join(parts).strip()


def _get_title(props: dict[str, Any], key: str) -> str:
    return _join_plain_text(props.get(key, {}).get("title", [])) or "Untitled"


def _get_rich_text(props: dict[str, Any], key: str) -> str:
    return _join_plain_text(props.get(key, {}).get("rich_text", []))


def _get_status(props: dict[str, Any], key: str = "Status") -> str | None:
    return ((props.get(key, {}) or {}).get("status") or {}).get("name")


def _get_select(props: dict[str, Any], key: str) -> str | None:
    return ((props.get(key, {}) or {}).get("select") or {}).get("name")


def _get_checkbox(props: dict[str, Any], key: str) -> bool:
    return bool((props.get(key, {}) or {}).get("checkbox", False))


def _get_url(props: dict[str, Any], key: str) -> str:
    return (props.get(key, {}) or {}).get("url") or ""


def _get_date_start(props: dict[str, Any], key: str) -> str | None:
    return ((props.get(key, {}) or {}).get("date") or {}).get("start")


def _get_relation_ids(props: dict[str, Any], key: str) -> list[str]:
    return [rel["id"] for rel in (props.get(key, {}) or {}).get("relation", []) if rel.get("id")]


def _get_people_names(props: dict[str, Any], key: str) -> list[str]:
    names: list[str] = []
    for person in (props.get(key, {}) or {}).get("people", []):
        if person.get("name"):
            names.append(person["name"])
        elif person.get("id"):
            names.append(person["id"])
    return names


def _property_timestamp(item: dict[str, Any], property_name: str, fallback_field: str | None = None) -> datetime | None:
    props = item.get("properties", {})
    prop = props.get(property_name, {})
    ptype = prop.get("type")
    if ptype in {"created_time", "last_edited_time"}:
        return _parse_dt(prop.get(ptype))
    if ptype == "date":
        return _parse_dt((prop.get("date") or {}).get("start"))
    if fallback_field:
        return _parse_dt(item.get(fallback_field))
    return None


def _extract_block_text(value: Any) -> list[str]:
    parts: list[str] = []
    if isinstance(value, dict):
        if "plain_text" in value and isinstance(value["plain_text"], str):
            parts.append(value["plain_text"])
        text_obj = value.get("text")
        if isinstance(text_obj, dict) and isinstance(text_obj.get("content"), str):
            parts.append(text_obj["content"])
        for key, nested in value.items():
            if key in {"plain_text", "text"}:
                continue
            parts.extend(_extract_block_text(nested))
    elif isinstance(value, list):
        for nested in value:
            parts.extend(_extract_block_text(nested))
    return parts


def _fetch_page_text(client: notion_api.NotionAPIClient, block_id: str) -> str:
    texts: list[str] = []
    stack = [block_id]

    while stack:
        current = stack.pop()
        for block in client.list_block_children(current):
            block_type = block.get("type")
            if block_type:
                texts.extend(_extract_block_text(block.get(block_type, {})))
            if block.get("has_children"):
                stack.append(block["id"])

    return "\n".join(part for part in texts if part).strip()


def _record(violations: list[Violation], code: str, severity: str, subject: str, detail: str) -> None:
    violations.append(Violation(code=code, severity=severity, subject=subject, detail=detail))


def _print_section(title: str, violations: list[Violation]) -> None:
    print(f"\n== {title} ==")
    if not violations:
        print("PASS")
        return
    for violation in violations:
        print(f"[{violation.severity}: {violation.code}] {violation.subject}: {violation.detail}")


def _blocking_violation_count(violations: list[Violation]) -> int:
    return sum(1 for violation in violations if violation.severity in BLOCKING_SEVERITIES)


def check_prompt_engineering_invariants(prompt_requests: list[dict[str, Any]]) -> list[Violation]:
    violations: list[Violation] = []
    active_pairs: dict[tuple[str, str], list[str]] = defaultdict(list)

    for page in prompt_requests:
        props = page.get("properties", {})
        name = _get_title(props, "Request Name")
        status = _get_status(props)
        dispatch_prompt = _get_rich_text(props, "Dispatch Prompt")
        work_items = _get_relation_ids(props, "Work Item")
        providers = _get_select(props, "Target Provider") or "Unknown"
        requested_by = [person.lower() for person in _get_people_names(props, "Requested By")]

        if status in PROMPT_ACTIVE_STATUSES and not dispatch_prompt:
            _record(
                violations,
                "A.1",
                "MUST-FIX",
                name,
                "prompt request is active but Dispatch Prompt is empty",
            )

        if status in PROMPT_ACTIVE_STATUSES and any("bot-" in person or person.endswith("bot") for person in requested_by):
            _record(
                violations,
                "A.2",
                "MUST-FIX",
                name,
                f"active prompt request was bot-ingested via Requested By={', '.join(requested_by)}",
            )

        if status not in PROMPT_TERMINAL_STATUSES and work_items:
            active_pairs[(work_items[0], providers)].append(name)

    for (work_item_id, provider), names in active_pairs.items():
        if len(names) > 1:
            _record(
                violations,
                "A.3",
                "MUST-FIX",
                work_item_id,
                f"duplicate active prompt dispatches for provider {provider}: {', '.join(names)}",
            )

    return violations


def build_project_issue_index(projects: list[dict[str, Any]]) -> dict[str, str]:
    index: dict[str, str] = {}
    for project in projects:
        props = project.get("properties", {})
        active_issue = _get_url(props, "Active GitHub Issue")
        if active_issue:
            index[project["id"]] = active_issue
    return index


def build_audit_log_counts(audit_logs: list[dict[str, Any]]) -> Counter[str]:
    counts: Counter[str] = Counter()
    for page in audit_logs:
        for work_item_id in _get_relation_ids(page.get("properties", {}), "Work Item"):
            counts[work_item_id] += 1
    return counts


def check_lab_loop(
    client: notion_api.NotionAPIClient,
    work_items: list[dict[str, Any]],
    project_issue_index: dict[str, str],
    audit_log_counts: Counter[str],
) -> tuple[list[Violation], dict[str, int]]:
    violations: list[Violation] = []
    counters: Counter[str] = Counter()
    now = datetime.now(timezone.utc)

    body_scan_candidates: list[dict[str, Any]] = []

    for page in work_items:
        props = page.get("properties", {})
        name = _get_title(props, "Item Name")
        status = _get_status(props)
        verdict = _get_select(props, "Verdict")
        close_reason = _get_select(props, "Close Reason")
        created = _property_timestamp(page, "Created Time", fallback_field="created_time")
        last_edited = _property_timestamp(page, "Last Edited Time", fallback_field="last_edited_time")
        is_post_epoch = bool(created and created >= MODEL_EPOCH)

        drra = _get_date_start(props, "Dispatch Requested Received At")
        drca = _get_date_start(props, "Dispatch Requested Consumed At")
        lrra = _get_date_start(props, "Librarian Request Received At")
        lrca = _get_date_start(props, "Librarian Request Consumed At")
        github_issue_url = _get_url(props, "GitHub Issue URL")
        synthesis_complete = _get_checkbox(props, "Synthesis Complete")
        environment = _get_select(props, "Environment")
        execution_lane = _get_select(props, "Execution Lane") or ""
        outcome = _get_rich_text(props, "Outcome")
        project_ids = _get_relation_ids(props, "Project")

        if is_post_epoch:
            if drra and drca:
                _record(violations, "E.1", "MUST-FIX", name, "Dispatch Requested Received At is still set after Dispatch Requested Consumed At was written")
                counters["e1"] += 1
            if lrra and lrca:
                _record(violations, "E.1", "INFO", name, "Librarian request received and consumed timestamps are both set")
                counters["e1_info"] += 1

        if status == "Closed" and close_reason == "Normal" and not verdict:
            _record(violations, "E.8A", "MUST-FIX", name, "Closed/Normal item is missing Verdict")
            counters["e8a"] += 1

        if is_post_epoch:
            if status == "Done" and drra and not drca:
                _record(violations, "E.3", "MUST-FIX", name, "Status is Done but dispatch was never consumed (Dispatch Requested Received At set, Consumed At empty)")
                counters["e3"] += 1
            if status == "Not Started" and synthesis_complete:
                _record(violations, "E.3", "MUST-FIX", name, "Status is Not Started while Synthesis Complete is true")
                counters["e3"] += 1
            if status == "Prompt Drafted" and not github_issue_url:
                _record(violations, "E.3", "MUST-FIX", name, "Prompt Drafted item is missing GitHub Issue URL")
                counters["e3"] += 1

        if last_edited:
            if status == "Prompt Drafted" and last_edited < now - timedelta(hours=24):
                _record(violations, "E.4", "MUST-FIX", name, "Prompt Drafted for more than 24 hours")
                counters["e4"] += 1
            if status == "In Progress" and last_edited < now - timedelta(days=7):
                _record(violations, "E.4", "MUST-FIX", name, "In Progress for more than 7 days")
                counters["e4"] += 1
            if status in {"Done", "Passed", "Kill Condition Met"} and not synthesis_complete and last_edited < now - timedelta(days=3):
                _record(violations, "E.4", "NICE-TO-HAVE", name, f"{status} without synthesis for more than 3 days")
                counters["e4_nice"] += 1

        if is_post_epoch and status in {"Done", "Inconclusive", "Kill Condition Met"} and audit_log_counts[page["id"]] < 2:
            _record(
                violations,
                "E.5",
                "MUST-FIX",
                name,
                f"terminal post-epoch item has only {audit_log_counts[page['id']]} audit log entries",
            )
            counters["e5"] += 1

        if is_post_epoch:
            if drra and not drca:
                _record(violations, "E.7", "P0", name, "Dispatch Requested Received At is set but Dispatch Requested Consumed At is empty (stalled dispatch)")
                counters["e7"] += 1
            if lrra and not lrca:
                _record(violations, "E.7", "P0", name, "Librarian Request Received At exists but Librarian Request Consumed At is empty")
                counters["e7"] += 1

        if environment == "production" and execution_lane and execution_lane not in SAFE_PROD_LANES:
            _record(
                violations,
                "E.8",
                "MUST-FIX",
                name,
                f"production item is assigned to unsafe execution lane {execution_lane}",
            )
            counters["e8"] += 1

        if status in TERMINAL_BODY_SCAN_STATUSES and project_ids:
            for project_id in project_ids:
                if project_issue_index.get(project_id):
                    _record(
                        violations,
                        "E.2",
                        "MUST-FIX",
                        name,
                        f"linked project still has Active GitHub Issue {project_issue_index[project_id]}",
                    )
                    counters["e2"] += 1

        if status in TERMINAL_BODY_SCAN_STATUSES and outcome and len(body_scan_candidates) < MAX_BODY_SCAN_ITEMS:
            body_scan_candidates.append(page)

    for page in body_scan_candidates:
        props = page.get("properties", {})
        name = _get_title(props, "Item Name")
        execution_lane = _get_select(props, "Execution Lane") or "unknown"
        body_text = _fetch_page_text(client, page["id"])
        if not body_text:
            continue

        for label, pattern in REDACTION_PATTERNS:
            if pattern.search(body_text):
                _record(
                    violations,
                    "E.9",
                    "MUST-FIX",
                    name,
                    f"page body leaked {label}; execution lane={execution_lane}",
                )
                counters["e9"] += 1
                break

        status = _get_status(props)
        if status in TERMINAL_BODY_SCAN_STATUSES and "### Execution Error" in body_text:
            _record(
                violations,
                "E.10",
                "MUST-FIX",
                name,
                f"terminal item contains execution error section; execution lane={execution_lane}",
            )
            counters["e10"] += 1

    return violations, dict(counters)


def check_evidence_dossier(
    client: notion_api.NotionAPIClient,
) -> tuple[list[Violation], dict[str, int]]:
    """E.11: Publication gate — no Load-bearing + Unchecked claims in Evidence Dossier."""
    violations: list[Violation] = []
    counters: Counter[str] = Counter()

    if not CFG.evidence_dossier_db_id:
        return violations, dict(counters)

    unchecked_load_bearing = client.query_all(
        CFG.evidence_dossier_db_id,
        filter_payload={
            "and": [
                {"property": "Fragility", "select": {"equals": "Load-bearing"}},
                {"property": "Verification Status", "select": {"equals": "Unchecked"}},
            ]
        },
    )

    for page in unchecked_load_bearing:
        props = page.get("properties", {})
        claim = _get_title(props, "Claim")
        section = _get_select(props, "Section") or "?"
        claim_type = _get_select(props, "Claim Type") or "unknown"
        _record(
            violations,
            "E.11",
            "MUST-FIX",
            f"§{section}: {claim[:80]}",
            f"load-bearing {claim_type} is unchecked — publication gate blocked",
        )
        counters["e11"] += 1

    return violations, dict(counters)


def summarize_counts(prompt_violations: list[Violation], loop_counts: dict[str, int]) -> None:
    print("\n== Lab-Loop-v1 Scorecard ==")
    print(f"Prompt invariants: {len(prompt_violations)}")
    print(f"Signal integrity (E.1): {loop_counts.get('e1', 0)}")
    print(f"Signal integrity info (E.1 LR): {loop_counts.get('e1_info', 0)}")
    print(f"Dangling pointers (E.2): {loop_counts.get('e2', 0)}")
    print(f"Impossible states (E.3): {loop_counts.get('e3', 0)}")
    print(f"Liveness stalls (E.4 active): {loop_counts.get('e4', 0)}")
    print(f"Liveness stale unsynthesized (E.4 nice-to-have): {loop_counts.get('e4_nice', 0)}")
    print(f"Audit log coverage (E.5): {loop_counts.get('e5', 0)}")
    print(f"Unconsumed signals (E.7): {loop_counts.get('e7', 0)}")
    print(f"Environment/lane violations (E.8): {loop_counts.get('e8', 0)}")
    print(f"Closed-without-verdict (E.8A): {loop_counts.get('e8a', 0)}")
    print(f"Redaction incidents (E.9): {loop_counts.get('e9', 0)}")
    print(f"Status advancement violations (E.10): {loop_counts.get('e10', 0)}")
    print(f"Publication gate (E.11): {loop_counts.get('e11', 0)}")
    print(f"Control-plane drift (T.*): {loop_counts.get('t', 0)}")


def check_control_plane_drift(work_items: list[dict[str, Any]]) -> tuple[list[Violation], dict[str, int]]:
    violations: list[Violation] = []
    try:
        snapshot = lab_topology.compile_snapshot()
        recent = sorted(
            work_items,
            key=lambda item: item.get("last_edited_time") or item.get("created_time") or "",
            reverse=True,
        )[:lab_topology.DEFAULT_LOOKBACK_LIMIT]
        report = lab_topology.evaluate_drift(snapshot, recent_work_items=recent)
    except Exception as exc:
        _record(
            violations,
            "T.0",
            "MUST-FIX",
            "Control Plane",
            f"unable to compile live topology: {exc}",
        )
        return violations, {"t": len(violations)}

    for finding in report.get("findings", []):
        _record(
            violations,
            finding["code"],
            finding["severity"],
            finding["subject"],
            finding["detail"],
        )

    recent_error = report.get("metadata", {}).get("recent_query_error")
    if recent_error:
        _record(
            violations,
            "T.7",
            "INFO",
            "Control Plane",
            f"recent Work Item query unavailable during drift audit: {recent_error}",
        )

    return violations, {"t": len(violations)}


def check_invariants(client: notion_api.NotionAPIClient) -> int:
    print("--- Lab Audit: Commencing State-Trace Verification ---")

    work_items = client.query_all(CFG.work_items_db_id)
    projects = client.query_all(CFG.lab_projects_db_id)
    prompt_requests = client.query_all(CFG.prompt_engineering_db_id)
    audit_logs = client.query_all(CFG.audit_log_db_id)

    prompt_violations = check_prompt_engineering_invariants(prompt_requests)
    project_issue_index = build_project_issue_index(projects)
    audit_log_counts = build_audit_log_counts(audit_logs)
    loop_violations, loop_counts = check_lab_loop(client, work_items, project_issue_index, audit_log_counts)
    dossier_violations, dossier_counts = check_evidence_dossier(client)
    control_plane_violations, control_plane_counts = check_control_plane_drift(work_items)
    loop_counts.update(dossier_counts)
    loop_counts.update(control_plane_counts)

    _print_section("Prompt Engineering", prompt_violations)
    _print_section("Lab-Loop-v1", loop_violations)
    _print_section("Evidence Dossier", dossier_violations)
    _print_section("Control Plane", control_plane_violations)
    summarize_counts(prompt_violations, loop_counts)

    total_blocking = (
        _blocking_violation_count(prompt_violations)
        + _blocking_violation_count(loop_violations)
        + _blocking_violation_count(dossier_violations)
        + _blocking_violation_count(control_plane_violations)
    )
    if total_blocking == 0:
        print("\n--- Audit Result: Lab is MATHEMATICALLY CONSISTENT ---")
    else:
        print(f"\n--- Audit Result: {total_blocking} Blocking Invariant Violations Found ---")

    return total_blocking


def main() -> None:
    client = notion_api.NotionAPIClient(CFG.notion_token)
    violations = check_invariants(client)
    if violations > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
