#!/usr/bin/env python3
"""
cycle_bridge.py — Sync Oracle cycle detections into Lab Work Items.

Design notes:
- Uses python-oracledb in Thin mode by default. Thick mode is optional.
- Tracks a Singer-style bookmark (timestamp + cycle_id tie-breaker) on disk.
- Uses query-before-create idempotency against Notion metadata, with a fallback
  check for legacy `CYCLE-<id>` rows that were created before this bridge had
  stable external IDs.
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

try:
    from . import notion_api
except ImportError:
    import notion_api

WORK_ITEMS_DB_ID = "daeb64d4-e5a8-4a7b-b0dc-7555cbc3def6"
CHATSEARCH_PROJECT_ID = "f7cca113-ad21-4261-a170-4a88441a0e66"
DEFAULT_PROJECT_LABEL = "chatsearch"
DEFAULT_STATE_PATH = Path(".cache/cycle_bridge_state.json")
DATASET_SOURCE = "chatsearch.cycle_detections"


@dataclass(frozen=True)
class OracleConfig:
    user: str
    password: str
    dsn: str
    config_dir: str | None = None
    wallet_location: str | None = None
    wallet_password: str | None = None
    lib_dir: str | None = None
    use_thick_mode: bool = False

    @classmethod
    def from_env(cls) -> "OracleConfig":
        password = os.environ["ORACLE_PASSWORD"]
        use_thick_mode = os.environ.get("ORACLE_USE_THICK_MODE", "").lower() in {
            "1",
            "true",
            "yes",
        }
        lib_dir = os.environ.get("ORACLE_LIB_DIR") or None
        return cls(
            user=os.environ.get("ORACLE_USER", "ADMIN"),
            password=password,
            dsn=os.environ["ORACLE_DSN"],
            config_dir=os.environ.get("ORACLE_CONFIG_DIR") or os.environ.get("ORACLE_WALLET"),
            wallet_location=os.environ.get("ORACLE_WALLET"),
            wallet_password=os.environ.get("ORACLE_WALLET_PASSWORD", password),
            lib_dir=lib_dir,
            use_thick_mode=use_thick_mode or bool(lib_dir),
        )


@dataclass(frozen=True)
class NotionConfig:
    token: str
    work_items_db_id: str = WORK_ITEMS_DB_ID
    project_id: str | None = CHATSEARCH_PROJECT_ID
    project_label: str = DEFAULT_PROJECT_LABEL
    dispatch_via: str | None = None

    @classmethod
    def from_env(cls) -> "NotionConfig":
        return cls(
            token=os.environ["NOTION_TOKEN"],
            work_items_db_id=os.environ.get("WORK_ITEMS_DB_ID", WORK_ITEMS_DB_ID),
            project_id=os.environ.get("WORK_ITEMS_PROJECT_ID", CHATSEARCH_PROJECT_ID) or None,
            project_label=os.environ.get("WORK_ITEMS_PROJECT_LABEL", DEFAULT_PROJECT_LABEL),
            dispatch_via=os.environ.get("CYCLE_BRIDGE_DISPATCH_VIA") or None,
        )


@dataclass(frozen=True)
class SyncConfig:
    oracle: OracleConfig
    notion: NotionConfig
    state_path: Path
    bootstrap_hours: int
    max_items: int | None
    dry_run: bool


@dataclass(frozen=True)
class SyncState:
    last_detected_at: datetime
    last_cycle_id: int

    def to_json(self) -> dict[str, Any]:
        return {
            "last_detected_at": self.last_detected_at.isoformat(),
            "last_cycle_id": self.last_cycle_id,
        }

    @classmethod
    def from_json(cls, payload: dict[str, Any]) -> "SyncState":
        return cls(
            last_detected_at=datetime.fromisoformat(payload["last_detected_at"]),
            last_cycle_id=int(payload["last_cycle_id"]),
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bootstrap-hours",
        type=int,
        default=24,
        help="Initial lookback window when no state file exists.",
    )
    parser.add_argument(
        "--state-file",
        default=str(DEFAULT_STATE_PATH),
        help="Bookmark file storing last processed detection state.",
    )
    parser.add_argument(
        "--max-items",
        type=int,
        default=None,
        help="Optional cap on synced items per run.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print intended writes without creating or updating Notion rows.",
    )
    return parser.parse_args()


def build_config(args: argparse.Namespace) -> SyncConfig:
    return SyncConfig(
        oracle=OracleConfig.from_env(),
        notion=NotionConfig.from_env(),
        state_path=Path(args.state_file),
        bootstrap_hours=args.bootstrap_hours,
        max_items=args.max_items,
        dry_run=args.dry_run,
    )


def load_state(path: Path) -> SyncState | None:
    if not path.exists():
        return None
    return SyncState.from_json(json.loads(path.read_text()))


def save_state(path: Path, state: SyncState) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state.to_json(), indent=2) + "\n")


def dataset_marker(cycle_id: int, session_id: int) -> str:
    return f"source={DATASET_SOURCE}; cycle_id={cycle_id}; session_id={session_id}"


def legacy_item_name(cycle_id: int) -> str:
    return f"CYCLE-{cycle_id}"


def display_item_name(cycle_id: int, project_label: str) -> str:
    return f"[CYCLE] {project_label} {legacy_item_name(cycle_id)}"


def cycle_sort_key(cycle: dict[str, Any]) -> tuple[datetime, int]:
    return cycle["DETECTED_AT"], int(cycle["CYCLE_ID"])


def objective_text(cycle: dict[str, Any]) -> str:
    description = (cycle.get("DESC_TEXT") or "").strip()
    if description:
        return description
    return (
        f"Intra-session cycle detected in session {cycle['SESSION_ID']}. "
        "Determine the smallest state or code intervention that breaks the loop."
    )


def prompt_notes_text(cycle: dict[str, Any]) -> str:
    return (
        f"Cycle type: {cycle['CYCLE_TYPE']}\n"
        f"Detected at: {cycle['DETECTED_AT'].isoformat()}\n"
        f"Cycle ID: {cycle['CYCLE_ID']}"
    )


def rich_text(value: str) -> list[dict[str, Any]]:
    chunks = notion_api.split_rich_text(value)
    return [{"type": "text", "text": {"content": chunk}} for chunk in chunks]


def build_properties(
    cycle: dict[str, Any],
    marker: str,
    notion_cfg: NotionConfig,
) -> dict[str, Any]:
    properties: dict[str, Any] = {
        "Item Name": {
            "title": rich_text(
                display_item_name(int(cycle["CYCLE_ID"]), notion_cfg.project_label)
            )
        },
        "Status": {"status": {"name": "Not Started"}},
        "Type": {"select": {"name": "Feasibility Analysis"}},
        "Objective": {"rich_text": rich_text(objective_text(cycle))},
        "Prompt Notes": {"rich_text": rich_text(prompt_notes_text(cycle))},
        "Dataset": {"rich_text": rich_text(marker)},
    }
    if notion_cfg.project_id:
        properties["Project"] = {"relation": [{"id": notion_cfg.project_id}]}
    if notion_cfg.dispatch_via:
        properties["Dispatch Via"] = {"select": {"name": notion_cfg.dispatch_via}}
    return properties


def build_children(cycle: dict[str, Any]) -> list[dict[str, Any]]:
    metadata = (
        f"Session ID: {cycle['SESSION_ID']}\n"
        f"Cycle Type: {cycle['CYCLE_TYPE']}\n"
        f"Detected At: {cycle['DETECTED_AT'].isoformat()}\n"
        f"Cycle ID: {cycle['CYCLE_ID']}"
    )
    children: list[dict[str, Any]] = [
        notion_api.heading_block("heading_2", "Cycle Details"),
        notion_api.paragraph_block(metadata),
    ]
    if cycle.get("DESC_TEXT"):
        children.extend(
            [
                notion_api.heading_block("heading_3", "Description"),
                notion_api.code_block(cycle["DESC_TEXT"], language="plain text"),
            ]
        )
    if cycle.get("STATE_A_TEXT"):
        children.extend(
            [
                notion_api.heading_block("heading_3", "State A"),
                notion_api.code_block(cycle["STATE_A_TEXT"], language="json"),
            ]
        )
    if cycle.get("STATE_B_TEXT"):
        children.extend(
            [
                notion_api.heading_block("heading_3", "State B"),
                notion_api.code_block(cycle["STATE_B_TEXT"], language="json"),
            ]
        )
    return children


def _oracledb():
    import oracledb

    return oracledb


def maybe_init_oracle_client(config: OracleConfig) -> None:
    if not config.use_thick_mode:
        return
    oracledb = _oracledb()
    init_args: dict[str, Any] = {}
    if config.lib_dir:
        init_args["lib_dir"] = config.lib_dir
    if config.config_dir:
        init_args["config_dir"] = config.config_dir
    oracledb.init_oracle_client(**init_args)


def oracle_connect(config: OracleConfig):
    oracledb = _oracledb()
    maybe_init_oracle_client(config)
    connect_args: dict[str, Any] = {
        "user": config.user,
        "password": config.password,
        "dsn": config.dsn,
    }
    if config.config_dir:
        connect_args["config_dir"] = config.config_dir
    if config.wallet_location:
        connect_args["wallet_location"] = config.wallet_location
    if config.wallet_password:
        connect_args["wallet_password"] = config.wallet_password
    return oracledb.connect(**connect_args)


def fetch_recent_cycles(config: SyncConfig, state: SyncState | None) -> list[dict[str, Any]]:
    if state is None:
        since_ts = datetime.now().astimezone() - timedelta(hours=config.bootstrap_hours)
        since_cycle_id = 0
    else:
        since_ts = state.last_detected_at
        since_cycle_id = state.last_cycle_id

    query = """
        SELECT
            CYCLE_ID,
            SESSION_ID,
            CYCLE_TYPE,
            DBMS_LOB.SUBSTR(DESCRIPTION, 4000, 1) AS DESC_TEXT,
            DBMS_LOB.SUBSTR(STATE_A, 4000, 1) AS STATE_A_TEXT,
            DBMS_LOB.SUBSTR(STATE_B, 4000, 1) AS STATE_B_TEXT,
            DETECTED_AT
        FROM CHATSEARCH.CYCLE_DETECTIONS
        WHERE DETECTED_AT > :since_ts
           OR (DETECTED_AT = :since_ts AND CYCLE_ID > :since_cycle_id)
        ORDER BY DETECTED_AT ASC, CYCLE_ID ASC
    """

    with oracle_connect(config.oracle) as conn:
        cursor = conn.cursor()
        cursor.execute(
            query,
            since_ts=since_ts,
            since_cycle_id=since_cycle_id,
        )
        cols = [col[0] for col in cursor.description]
        rows = [dict(zip(cols, row)) for row in cursor.fetchall()]

    if config.max_items is not None:
        rows = rows[: config.max_items]
    return rows


def find_existing_cycle_page(
    client: notion_api.NotionAPIClient,
    database_id: str,
    marker: str,
    cycle_id: int,
    project_label: str,
) -> dict[str, Any] | None:
    pages = client.query_all(
        database_id,
        filter_payload={"property": "Dataset", "rich_text": {"contains": marker}},
        page_size=10,
    )
    if pages:
        return pages[0]

    titled_pages = client.query_all(
        database_id,
        filter_payload={
            "property": "Item Name",
            "title": {"equals": display_item_name(cycle_id, project_label)},
        },
        page_size=10,
    )
    if titled_pages:
        return titled_pages[0]

    legacy_pages = client.query_all(
        database_id,
        filter_payload={"property": "Item Name", "title": {"equals": legacy_item_name(cycle_id)}},
        page_size=10,
    )
    if legacy_pages:
        return legacy_pages[0]
    return None


def create_work_item_for_cycle(
    client: notion_api.NotionAPIClient,
    cycle: dict[str, Any],
    notion_cfg: NotionConfig,
    dry_run: bool,
) -> str:
    marker = dataset_marker(int(cycle["CYCLE_ID"]), int(cycle["SESSION_ID"]))
    existing = find_existing_cycle_page(
        client,
        notion_cfg.work_items_db_id,
        marker,
        int(cycle["CYCLE_ID"]),
        notion_cfg.project_label,
    )
    if existing:
        title_fragments = existing["properties"]["Item Name"]["title"]
        title = "".join(fragment["plain_text"] for fragment in title_fragments)
        return f"skip:{title}"

    item_name = display_item_name(int(cycle["CYCLE_ID"]), notion_cfg.project_label)
    properties = build_properties(cycle, marker, notion_cfg)
    children = build_children(cycle)

    if dry_run:
        print(
            json.dumps(
                {
                    "action": "create",
                    "item_name": item_name,
                    "properties": properties,
                    "children": children,
                },
                indent=2,
                default=str,
            )
        )
        return f"dry-run:{item_name}"

    page = client.create_page(
        parent={"database_id": notion_cfg.work_items_db_id},
        properties=properties,
    )
    client.append_block_children(page["id"], children)
    return f"created:{item_name}"


def sync_cycles(config: SyncConfig) -> int:
    state = load_state(config.state_path)
    cycles = fetch_recent_cycles(config, state)
    if not cycles:
        print("No new cycles detected.")
        return 0

    client = notion_api.NotionAPIClient(config.notion.token)
    processed = 0
    current_state = state
    for cycle in cycles:
        result = create_work_item_for_cycle(client, cycle, config.notion, config.dry_run)
        print(result)
        current_state = SyncState(
            last_detected_at=cycle["DETECTED_AT"],
            last_cycle_id=int(cycle["CYCLE_ID"]),
        )
        if not config.dry_run:
            save_state(config.state_path, current_state)
        processed += 1
    return processed


def main() -> int:
    args = parse_args()
    try:
        config = build_config(args)
        return 0 if sync_cycles(config) >= 0 else 1
    except KeyError as exc:
        print(f"Missing required environment variable: {exc.args[0]}")
        return 2
    except Exception as exc:
        print(f"cycle_bridge failed: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
