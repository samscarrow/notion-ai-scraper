import os
import json
from dataclasses import dataclass
from typing import Any

# Global paths
TEMPLATE_DATA_JSON = os.path.expanduser(
    "~/projects/agent-env/home/.agents/template-data.json"
)

# Default hardcoded values (as fallbacks if template-data.json is missing).
# Lab-specific defaults are empty — populate via env vars or template-data.json.
DEFAULT_SPACE_ID = ""
DEFAULT_WORK_ITEMS_DB_ID = ""
DEFAULT_LAB_PROJECTS_DB_ID = ""
DEFAULT_PROMPT_ENGINEERING_DB_ID = ""
DEFAULT_AUDIT_LOG_DB_ID = ""
DEFAULT_LAB_CONTROL_DB_ID = ""
DEFAULT_CHATSEARCH_PROJECT_ID = ""
DEFAULT_EVIDENCE_DOSSIER_DB_ID = ""
DEFAULT_SCENE_ITEMS_DB_ID = ""
DEFAULT_LIBRARIAN_WORKFLOW_ID = ""
DEFAULT_LIBRARIAN_BOT_RUNTIME = ""
DEFAULT_LIBRARIAN_BOT_DRAFT = ""

@dataclass(frozen=True)
class Config:
    notion_token: str
    space_id: str
    work_items_db_id: str = ""
    lab_projects_db_id: str = ""
    prompt_engineering_db_id: str = ""
    audit_log_db_id: str = ""
    lab_control_db_id: str = ""
    chatsearch_project_id: str = ""
    evidence_dossier_db_id: str = ""
    scene_items_db_id: str = ""
    librarian_notion_internal_id: str = ""
    librarian_bot_runtime: str = ""
    librarian_bot_draft: str = ""

    @property
    def has_lab_config(self) -> bool:
        """True when Lab-specific database IDs are configured."""
        return bool(
            self.work_items_db_id
            and self.audit_log_db_id
            and self.lab_control_db_id
        )

    @classmethod
    def from_env(cls) -> "Config":
        # 1. Load template-data.json for DB/agent ID lookups
        res = {}
        if os.path.exists(TEMPLATE_DATA_JSON):
            try:
                with open(TEMPLATE_DATA_JSON, "r") as f:
                    res = json.load(f)
            except Exception as e:
                print(f"Warning: Failed to load {TEMPLATE_DATA_JSON}: {e}")

        def get_db_id(key, default):
            return res.get("databases", {}).get(key, {}).get("notion_public_id", default)

        def get_agent_id(key, field, default):
            return res.get("agents", {}).get(key, {}).get(field, default)

        token = os.environ.get("NOTION_TOKEN", "")
        if not token:
            raise ValueError(
                "NOTION_TOKEN not set. Launch via 'work' shell or set in systemd EnvironmentFile."
            )

        return cls(
            notion_token=token,
            space_id=os.environ.get("NOTION_SPACE_ID", res.get("workspace", {}).get("space_id", DEFAULT_SPACE_ID)),
            work_items_db_id=os.environ.get("WORK_ITEMS_DB_ID", get_db_id("work_items", DEFAULT_WORK_ITEMS_DB_ID)),
            lab_projects_db_id=os.environ.get("LAB_PROJECTS_DB_ID", get_db_id("lab_projects", DEFAULT_LAB_PROJECTS_DB_ID)),
            prompt_engineering_db_id=os.environ.get("PROMPT_ENGINEERING_DB_ID", get_db_id("prompt_engineering", DEFAULT_PROMPT_ENGINEERING_DB_ID)),
            audit_log_db_id=os.environ.get("AUDIT_LOG_DB_ID", get_db_id("lab_audit_log", DEFAULT_AUDIT_LOG_DB_ID)),
            lab_control_db_id=os.environ.get("LAB_CONTROL_DB_ID", get_db_id("lab_control", DEFAULT_LAB_CONTROL_DB_ID)),
            chatsearch_project_id=os.environ.get("CHATSEARCH_PROJECT_ID", DEFAULT_CHATSEARCH_PROJECT_ID),
            evidence_dossier_db_id=os.environ.get("EVIDENCE_DOSSIER_DB_ID", get_db_id("evidence_dossier", DEFAULT_EVIDENCE_DOSSIER_DB_ID)),
            scene_items_db_id=os.environ.get("SCENE_ITEMS_DB_ID", get_db_id("pontius_scene_items", DEFAULT_SCENE_ITEMS_DB_ID)),
            librarian_notion_internal_id=os.environ.get("LIBRARIAN_WORKFLOW_ID", get_agent_id("lab_librarian_knowledge_synthesis", "notion_internal_id", DEFAULT_LIBRARIAN_WORKFLOW_ID)),
            librarian_bot_runtime=os.environ.get("LIBRARIAN_BOT_RUNTIME", get_agent_id("lab_librarian_knowledge_synthesis", "notion_internal_id", DEFAULT_LIBRARIAN_BOT_RUNTIME)),
            librarian_bot_draft=os.environ.get("LIBRARIAN_BOT_DRAFT", DEFAULT_LIBRARIAN_BOT_DRAFT),
        )

# Global config instance
try:
    config = Config.from_env()
except Exception:
    config = None

def get_config() -> Config:
    if config is None:
        return Config.from_env()
    return config
