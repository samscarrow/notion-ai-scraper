#!/usr/bin/env python3
"""
lab_auditor.py — State-Trace Audit & Model Checking for the Lab.

Verifies TLA+ invariants against the live Notion Work Items database.
1. Safety: No signal bit remains TRUE after consumption timestamp is set.
2. Exclusive Ownership: No Work Item is 'Done' while still having an 'Active GitHub Issue'.
3. Liveness: Detects items stuck in 'Prompt Requested' or 'In Progress' for > 24h.
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
AUDIT_LOG_DB_ID = "4621be9a-0709-443e-bee6-7e6166f76fae"
MODEL_EPOCH = datetime(2026, 3, 6, tzinfo=timezone.utc)

def check_invariants(client: notion_api.NotionAPIClient):
    print("--- Lab Audit: Commencing State-Trace Verification ---")
    
    # Query all active or recently closed work items
    items = client.query_all(WORK_ITEMS_DB_ID)
    
    violations = 0
    now = datetime.now(timezone.utc)
    
    for item in items:
        props = item.get("properties", {})
        title_list = props.get("Item Name", {}).get("title", [])
        name = title_list[0].get("plain_text", "Unknown") if title_list else "Untitled"
        status_prop = props.get("Status", {})
        status = (status_prop.get("status") or {}).get("name") if status_prop else None
        
        dr = props.get("Dispatch Requested", {}).get("checkbox", False)
        drca = props.get("Dispatch Requested Consumed At", {}).get("date")
        lr = props.get("Librarian Request", {}).get("checkbox", False)
        lrca = props.get("Librarian Request Consumed At", {}).get("date")
        active_issue = props.get("Active GitHub Issue", {}).get("url") # Assuming this is a URL or text
        
        # 1. Safety Invariant: Signal Integrity (Zombies)
        if dr and drca:
            print(f"[VIOLATION: SAFETY] {name} is a Dispatch Zombie: DR=True but DRCA is set.")
            violations += 1
            
        if lr and lrca:
            print(f"[VIOLATION: SAFETY] {name} is a Librarian Zombie: LR=True but LRCA is set.")
            violations += 1

        # 2. Interference Invariant: Exclusive Ownership (Dangling Pointers)
        if status == "Done" and active_issue:
            print(f"[VIOLATION: EXCLUSIVE] {name} is a Dangling Pointer: Status is Done but Active Issue is still linked.")
            violations += 1

        # 3. Liveness Invariant: Forward Progress (Stale Items)
        last_edited_str = props.get("Last Edited Time", {}).get("last_edited_time")
        if last_edited_str and status in ["In Progress", "Prompt Requested"]:
            last_edited = datetime.fromisoformat(last_edited_str.replace("Z", "+00:00"))
            if now - last_edited > timedelta(hours=24):
                print(f"[WARNING: LIVENESS] {name} is STALE: No progress in 24h (Current: {status}).")

        # 4. E.7 Unconsumed Signal Invariant (post-epoch only)
        # Items created after T0 with an active signal bit but no consumption timestamp
        # indicate agent non-compliance (consume-first protocol violation).
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
