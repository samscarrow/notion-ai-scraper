#!/usr/bin/env python3
"""dispatch_poller.py — Mechanical dispatch poller.

Queries the Lab for dispatchable work items, validates them, and submits
dispatch packets to OpenClaw for execution.

No LLM in the loop. Runs as a systemd timer or cron on nix.

Requires:
  - NOTION_TOKEN (public API)
  - OPENCLAW_HOOKS_TOKEN (for submitting to OpenClaw gateway)
  - OPENCLAW_URL (default: http://openclaw:18789)
  - Access to run-lab-dispatch.sh inside the OpenClaw container
"""

import argparse
import json
import logging
import os
import subprocess
import sys
import tempfile
import uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import notion_api
from config import get_config
from dispatch import get_dispatchable_items, build_dispatch_packet

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("dispatch-poller")

# Lane → OpenClaw agent routing (mirrors SKILL.md)
LANE_AGENT_MAP = {
    "main": "main",
    "planner": "main",
    "coder": "lab-coder-standard",
    "reviewer": "main",
    "dev": "lab-coder-standard",
    "sentinel": "sentinel",
    "sentinel-deploy": "sentinel",
    "scout": "scout",
    "thinker": "main",
}

DOCKER_CONTAINER = os.environ.get("OPENCLAW_CONTAINER", "openclaw")
DISPATCH_SCRIPT = "/home/node/nix-docker-configs/openclaw/run-lab-dispatch.sh"


def submit_to_openclaw(packet: dict, dry_run: bool = False) -> bool:
    """Submit a dispatch packet to OpenClaw via run-lab-dispatch.sh."""
    packet_json = json.dumps(packet)

    if dry_run:
        log.info("  [DRY RUN] Would submit packet for %s (lane=%s)",
                 packet.get("item_name"), packet.get("execution_lane"))
        return True

    try:
        result = subprocess.run(
            [
                "sudo", "docker", "exec", "-i", DOCKER_CONTAINER,
                DISPATCH_SCRIPT, "--inside",
            ],
            input=packet_json,
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode == 0:
            log.info("  Submitted to OpenClaw: %s", result.stdout.strip()[:200] if result.stdout else "ok")
            return True
        else:
            log.error("  OpenClaw submission failed (rc=%d): %s",
                      result.returncode, result.stderr.strip()[:200])
            return False
    except subprocess.TimeoutExpired:
        log.error("  OpenClaw submission timed out")
        return False
    except Exception as e:
        log.error("  OpenClaw submission error: %s", e)
        return False


def poll_and_dispatch(max_items: int = 5, dry_run: bool = False) -> int:
    """Poll for dispatchable items and submit them.

    Returns the number of items successfully dispatched.
    """
    cfg = get_config()
    client = notion_api.NotionAPIClient(cfg.notion_token)

    candidates = get_dispatchable_items(client)
    if not candidates:
        log.info("No dispatchable items")
        return 0

    log.info("Found %d candidate(s)", len(candidates))
    dispatched = 0

    for item in candidates[:max_items]:
        item_id = item["id"]
        item_name = item.get("name", item_id)

        log.info("Processing: %s", item_name)

        # Build and validate packet
        result = build_dispatch_packet(item_id, client)
        errors = result.get("errors", [])
        packet = result.get("packet")

        if errors:
            log.warning("  Validation failed for %s: %s", item_name, "; ".join(errors))
            continue

        if not packet:
            log.warning("  No packet produced for %s", item_name)
            continue

        run_id = packet.get("run_id", str(uuid.uuid4()))

        # Route by lane: writers-room goes to Notion triggers, everything else to OpenClaw
        lane = packet.get("execution_lane", "")
        if lane == "writers-room":
            if dry_run:
                log.info("  [DRY RUN] Would create Scene Item for %s", item_name)
                dispatched += 1
                continue

            wr_cfg = packet.get("writers_room_config", {})
            if not wr_cfg:
                log.error("  writers-room packet missing writers_room_config for %s", item_name)
                continue

            wr_result = dispatch.dispatch_scene(
                scene_name=wr_cfg.get("scene_name", item_name),
                season=wr_cfg.get("season", 1),
                task_type=wr_cfg.get("task_type", "Full Scene Draft"),
                creative_brief=wr_cfg.get("creative_brief", ""),
                character_list=wr_cfg.get("character_list"),
                episode=wr_cfg.get("episode"),
                work_item_id=item_id,
                client=client,
            )
            if wr_result.get("created"):
                log.info("  Scene Item created: %s (signal=%s)",
                         wr_result["scene_item_id"], wr_result["entry_signal"])
                # Stamp consumed on the parent Work Item so it doesn't re-dispatch
                dispatch.accept_dispatch_start(item_id, run_id, client)
                dispatched += 1
            else:
                log.error("  Scene Item creation failed for %s: %s",
                          item_name, wr_result.get("errors"))
        elif submit_to_openclaw(packet, dry_run=dry_run):
            dispatched += 1
        else:
            log.error("  Submission failed for %s (run_id=%s)", item_name, run_id)

    log.info("Done: %d/%d dispatched", dispatched, len(candidates[:max_items]))
    return dispatched


def main():
    parser = argparse.ArgumentParser(description="Poll and dispatch Lab work items to OpenClaw")
    parser.add_argument("--max-items", type=int, default=5,
                        help="Max items to dispatch per run (default: 5)")
    parser.add_argument("--dry-run", action="store_true",
                        help="List dispatchable items without dispatching")
    args = parser.parse_args()

    try:
        poll_and_dispatch(max_items=args.max_items, dry_run=args.dry_run)
    except Exception as e:
        log.error("Fatal: %s", e, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
