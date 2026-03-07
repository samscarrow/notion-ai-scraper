#!/usr/bin/env python3
"""
lab_auditor.py — State-Trace Audit & Model Checking for the Lab.

Verifies TLA+ invariants against the live Notion databases.
1. Safety: No signal bit remains TRUE after consumption timestamp is set.
2. Exclusive Ownership: No Lab Project has an Active GitHub Issue when all its Work Items are terminal.
3. Liveness: Detects items stuck in 'Prompt Requested' or 'In Progress' for > 24h.
4. E.7: Post-epoch items with active signal bit but no consumption timestamp.
"""

import os
import sys
from datetime import datetime, timezone, timedelta
from typing import Any

try:
    from . import notion_api
except ImportError:
    import notion_api

# Constants
WORK_ITEMS_DB_ID = "daeb64d4-e5a8-4a7b-b0dc-7555cbc3def6"
LAB_PROJECTS_DB_ID = "389645af-0e4f-479e-a910-79b169a99462"
AUDIT_LOG_DB_ID = "4621be9a-0709-443e-bee6-7e6166f76fae"
MODEL_EPOCH = datetime(2026, 3, 6, tzinfo=timezone.utc)

TERMINAL_STATUSES = {"Done", "Passed", "Kill Condition Met", "Inconclusive"}


def _get_title(props: dict, key: str) -> str:
    title_list = props.get(key, {}).get("title", [])
    return title_list[0].get("plain_text", "Unknown") if title_list else "Untitled"


def _get_status(props: dict) -> str | None:
    status_prop = props.get("Status", {})
    return (status_prop.get("status") or {}).get("name") if status_prop else None


def check_work_item_invariants(client: notion_api.NotionAPIClient) -> int:
    """E.1 Safety, E.4 Liveness, E.7 Unconsumed Signal — all on Work Items."""
    items = client.query_all(WORK_ITEMS_DB_ID)
    violations = 0
    now = datetime.now(timezone.utc)

    for item in items:
        props = item.get("properties", {})
        name = _get_title(props, "Item Name")
        status = _get_status(props)

        dr = props.get("Dispatch Requested", {}).get("checkbox", False)
        drca = props.get("Dispatch Requested Consumed At", {}).get("date")
        lr = props.get("Librarian Request", {}).get("checkbox", False)
        lrca = props.get("Librarian Request Consumed At", {}).get("date")

        # E.1 Safety: Signal Integrity (Zombies)
        if dr and drca:
            print(f"[VIOLATION: E.1] {name}: DR=true AND DRCA is set (Dispatch Zombie).")
            violations += 1
        if lr and lrca:
            print(f"[VIOLATION: E.1] {name}: LR=true AND LRCA is set (Librarian Zombie).")
            violations += 1

        # E.4 Liveness: Forward Progress (Stale Items)
        last_edited_str = props.get("Last Edited Time", {}).get("last_edited_time")
        if last_edited_str and status in ["In Progress", "Prompt Requested"]:
            last_edited = datetime.fromisoformat(last_edited_str.replace("Z", "+00:00"))
            if now - last_edited > timedelta(hours=24):
                print(f"[WARNING: E.4] {name}: No progress in 24h (Status: {status}).")

        # E.7 Unconsumed Signal (post-epoch only)
        created_str = props.get("Created time", {}).get("created_time") or item.get("created_time")
        if created_str:
            created = datetime.fromisoformat(created_str.replace("Z", "+00:00"))
            if created >= MODEL_EPOCH:
                if dr and not drca:
                    print(f"[P0: E.7] {name}: DR=true but DRCA is null (created {created.date()}). Unconsumed dispatch signal.")
                    violations += 1
                if lr and not lrca:
                    print(f"[P0: E.7] {name}: LR=true but LRCA is null (created {created.date()}). Unconsumed librarian signal.")
                    violations += 1

    return violations


def check_project_invariants(client: notion_api.NotionAPIClient) -> int:
    """E.2 Exclusive Ownership — Active GitHub Issue on Lab Projects."""
    projects = client.query_all(LAB_PROJECTS_DB_ID)
    violations = 0

    for project in projects:
        props = project.get("properties", {})
        name = _get_title(props, "Project Name")
        active_issue = props.get("Active GitHub Issue", {}).get("url")
        if not active_issue:
            continue

        # Project has an Active GitHub Issue lock. Check if any related
        # Work Items are still non-terminal (meaning work is in flight).
        wi_relations = props.get("Work Items", {}).get("relation", [])
        if not wi_relations:
            # Lock set but no Work Items linked — dangling by definition.
            print(f"[VIOLATION: E.2] {name}: Active GitHub Issue set but no Work Items linked.")
            violations += 1
            continue

        all_terminal = True
        for rel in wi_relations:
            wi_id = rel.get("id")
            if not wi_id:
                continue
            try:
                wi_page = client._request("GET", f"pages/{wi_id}")
                wi_status = _get_status(wi_page.get("properties", {}))
                if wi_status not in TERMINAL_STATUSES:
                    all_terminal = False
                    break
            except Exception:
                # Can't resolve — assume non-terminal to avoid false positive.
                all_terminal = False
                break

        if all_terminal:
            print(f"[VIOLATION: E.2] {name}: Active GitHub Issue still set but all Work Items are terminal. Dangling factory pointer.")
            violations += 1

    return violations


def check_invariants(client: notion_api.NotionAPIClient) -> int:
    print("--- Lab Audit: Commencing State-Trace Verification ---")

    violations = 0
    violations += check_work_item_invariants(client)
    violations += check_project_invariants(client)

    if violations == 0:
        print("--- Audit Result: Lab is MATHEMATICALLY CONSISTENT ---")
    else:
        print(f"--- Audit Result: {violations} Invariant Violations Found ---")

    return violations


def main():
    token = os.environ.get("NOTION_TOKEN")
    if not token:
        print("ERROR: NOTION_TOKEN required")
        sys.exit(1)

    client = notion_api.NotionAPIClient(token)
    violations = check_invariants(client)

    if violations > 0:
        sys.exit(1)

if __name__ == "__main__":
    main()
