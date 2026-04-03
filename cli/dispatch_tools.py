"""
dispatch_tools.py — Lab-specific dispatch tools for the notion-agents MCP server.

Conditionally registered when Lab configuration is present
(work_items_db_id, audit_log_db_id, lab_control_db_id).
"""

import json


def register(mcp, cfg):
    """Register Lab dispatch tools on the MCP server instance."""
    import dispatch
    import notion_api
    from database_tools import _get_notion_api_client

    @mcp.tool()
    def check_gates(work_item_id: str = "") -> str:
        """
        Check Pre-Flight and Cascade Depth gates before performing writes.

        Call this BEFORE performing any writes. Returns a JSON object:
        - {"proceed": true, "cascade_depth": N}  — safe to continue
        - {"halt": true, "reason": "...", "detail": "..."}  — stop immediately

        Gates checked:
        - Pre-Flight Mode: halts all dispatch when active in Lab Control
        - Cascade Depth: halts if the work item's depth >= Max Cascade Depth
          (only checked when work_item_id is provided)

        work_item_id: UUID of the Work Item page (optional — omit for
                      agents not operating on a specific Work Item).
        """
        client = notion_api.NotionAPIClient(cfg.notion_token)
        result = dispatch.check_gates(work_item_id or None, client)
        return json.dumps(result, indent=2)

    @mcp.tool()
    def get_dispatchable_items() -> str:
        """
        Find Work Items ready for dispatch by an execution plane.

        Returns items where Lab Dispatch Requested At is set (or legacy Dispatch
        Requested Received At is set), Dispatch Requested Consumed At is empty,
        and Status is Not Started or Prompt Drafted.
        """
        client = _get_notion_api_client()
        items = dispatch.get_dispatchable_items(client)

        if not items:
            return "No dispatchable items found."

        lines = [f"**{len(items)} dispatchable item(s):**\n"]
        lines.append("| Item Name | Dispatch Via | Lane | Environment | Branch | Project | Focus | Retry | Type |")
        lines.append("| --- | --- | --- | --- | --- | --- | --- | --- | --- |")
        for item in items:
            lines.append(
                f"| [{item['name']}](https://www.notion.so/{item['id'].replace('-', '')}) "
                f"| {item.get('dispatch_via') or '—'} "
                f"| {item.get('execution_lane') or '—'} "
                f"| {item.get('environment') or '—'} "
                f"| {item.get('branch') or '—'} "
                f"| {item.get('project_name') or '—'} "
                f"| {'Yes' if item.get('project_focus') else '—'} "
                f"| {item.get('retry_count', 0)} "
                f"| {item.get('type') or '—'} |"
            )

        return "\n".join(lines)

    @mcp.tool()
    def build_dispatch_packet(work_item_id: str) -> str:
        """
        Build and validate a dispatch packet for a Work Item.

        Reads the Work Item, resolves inheritance from its Project, applies
        Execution Lane defaults, and runs all registered validation gates
        (V1-V21, including Pre-Flight Mode and Cascade Depth).

        work_item_id: UUID of the Work Item page.

        Returns a validated dispatch packet (JSON) ready for an execution plane,
        or a list of validation errors if the item is not dispatchable.
        """
        client = _get_notion_api_client()
        result = dispatch.build_dispatch_packet(work_item_id, client)

        if result["errors"]:
            error_list = "\n".join(f"- {e}" for e in result["errors"])
            return f"**Validation failed** ({len(result['errors'])} error(s)):\n\n{error_list}"

        packet = result["packet"]
        audit_note = ""
        if result.get("_production_audit"):
            audit_note = "\n\n**Note:** Production environment — elevated access logged."

        return f"**Dispatch packet built successfully.**\n\n```json\n{json.dumps(packet, indent=2)}\n```{audit_note}"

    @mcp.tool()
    def stamp_dispatch_consumed(work_item_id: str, run_id: str) -> str:
        """
        Mark a Work Item as accepted by an execution plane after preflight.

        Sets Dispatch Requested Consumed At=now(), Status=In Progress, and writes
        the run_id for idempotency tracking.
        Also creates an audit log entry.

        work_item_id: UUID of the Work Item page.
        run_id: The run_id from the dispatch packet (from build_dispatch_packet).
        """
        client = _get_notion_api_client()
        result = dispatch.accept_dispatch_start(work_item_id, run_id, client)

        if result.get("status") == "already_consumed":
            return (
                f"**Already consumed** (race guard).\n\n"
                f"- Work Item: `{result['work_item_id']}`\n"
                f"- Existing Run ID: `{result['run_id']}`\n"
                f"- Consumed At: `{result['consumed_at']}`"
            )
        if result.get("status") == "already_accepted":
            return (
                f"**Already accepted**.\n\n"
                f"- Work Item: `{result['work_item_id']}`\n"
                f"- Run ID: `{result['run_id']}`\n"
                f"- Accepted At: `{result['consumed_at']}`"
            )
        if result.get("status") == "wrong_status":
            return (
                f"**Cannot consume** — Work Item status is `{result['current_status']}` "
                f"(expected Not Started or Prompt Drafted).\n\n"
                f"- Work Item: `{result['work_item_id']}`"
            )

        return (
            f"**Dispatch consumed.**\n\n"
            f"- Work Item: `{result['work_item_id']}`\n"
            f"- Run ID: `{result['run_id']}`\n"
            f"- Consumed At: `{result['consumed_at']}`\n"
            f"- Status: In Progress"
        )

    @mcp.tool()
    def fail_dispatch_preflight(work_item_id: str, run_id: str, reason: str) -> str:
        """
        Record a dispatch preflight failure without leaving false In Progress state.

        Writes `Blocked Reason`. If the same `run_id` had already been accepted,
        clears `Dispatch Requested Consumed At`, clears `run_id`, and restores the
        item to `Not Started` or `Prompt Drafted`.
        """
        client = _get_notion_api_client()
        result = dispatch.fail_dispatch_preflight(work_item_id, run_id, reason, client)

        if result.get("status") == "conflict":
            return (
                f"**Cannot revert dispatch** — Work Item is already owned by another run.\n\n"
                f"- Work Item: `{result['work_item_id']}`\n"
                f"- Existing Run ID: `{result['run_id']}`\n"
                f"- Consumed At: `{result['consumed_at']}`"
            )

        return (
            f"**Dispatch preflight failure recorded.**\n\n"
            f"- Work Item: `{result['work_item_id']}`\n"
            f"- Run ID: `{result['run_id']}`\n"
            f"- Recorded At: `{result['recorded_at']}`\n"
            f"- Restored Status: `{result['restored_status']}`\n"
            f"- Reason: {result['reason']}"
        )

    @mcp.tool()
    def handle_final_return(
        work_item_id: str,
        run_id: str,
        status: str,
        summary: str,
        raw_output: str,
        duration_ms: int,
        model: str,
        lane: str,
        verdict: str = "",
        error: str = "",
        metrics: str = "",
        artifacts: str = "",
        files_changed: str = "",
        commit_sha: str = "",
        pr_url: str = "",
    ) -> str:
        """
        Ingest a final return payload from an execution plane.

        Called by lane agents (via mcporter) when work is complete. Sets Return
        Received At (triggers Lab Intake Clerk), maps verdict to Status/Verdict,
        appends findings to the Work Item page, and writes an audit log entry.

        Mirrors the aws-ec2 webhook bridge /return endpoint so both return paths
        produce identical Notion state.

        work_item_id: UUID of the Work Item page.
        run_id: The run_id from the dispatch packet.
        status: ok, error, gated, or timeout.
        summary: Brief summary of execution results (max 500 chars).
        raw_output: Full execution output (will be redacted and truncated).
        duration_ms: Execution duration in milliseconds.
        model: Model used for execution (e.g. claude-opus-4-6).
        lane: Execution lane that performed the work.
        verdict: PASS, FAIL, INCONCLUSIVE, or OBSERVATIONS (required if status=ok).
        error: Error message (required if status!=ok).
        metrics: Optional JSON string of execution metrics.
        artifacts: Optional JSON array string of artifact objects.
        files_changed: Optional comma-separated list of changed files.
        commit_sha: Optional git commit SHA.
        pr_url: Optional pull request URL.
        """
    @mcp.tool()
    def dispatch_scene(
        scene_name: str,
        season: int,
        task_type: str,
        creative_brief: str,
        character_list: str = "",
        episode: int = 0,
        prompt_notes: str = "",
        work_item_id: str = "",
    ) -> str:
        """
        Create a Scene Item in the writers-room pipeline and fire the entry signal.

        Creates a row in pontius_scene_items and stamps the appropriate
        *_Requested_At timestamp to trigger the first agent in the chain
        (Historical Prosecutor, Canon Steward, or Dramatic Architect depending
        on the task_type routing table).

        scene_name: Title of the scene (e.g. "The Petition Hour").
        season: Season number (1-4).
        task_type: One of: Full Scene Draft, Scene Revision, Beat Sheet Only,
                   Research Query, Character Development, Episode Outline,
                   Dialogue Polish, Motif Placement.
        creative_brief: The assignment — what the scene needs to do.
        character_list: Comma-separated character names (required for Full Scene
                        Draft, Character Development, Episode Outline).
        episode: Episode number (0 to omit).
        prompt_notes: Optional human guidance or constraints.
        work_item_id: Optional parent Work Item UUID (for Lab-dispatched scenes).
        """
        client = _get_notion_api_client()
        chars = [c.strip() for c in character_list.split(",") if c.strip()] if character_list else None

        result = dispatch.dispatch_scene(
            scene_name=scene_name,
            season=season,
            task_type=task_type,
            creative_brief=creative_brief,
            character_list=chars,
            episode=episode if episode else None,
            prompt_notes=prompt_notes or None,
            work_item_id=work_item_id or None,
            client=client,
        )

        if not result.get("created"):
            error_list = "\n".join(f"- {e}" for e in result.get("errors", []))
            return f"**Scene dispatch failed:**\n\n{error_list}"

        return (
            f"**Scene Item created.**\n\n"
            f"- Scene: {result['scene_name']}\n"
            f"- ID: `{result['scene_item_id']}`\n"
            f"- Task Type: {result['task_type']}\n"
            f"- Entry Signal: `{result['entry_signal']}` stamped at {result['stamped_at']}\n"
            f"- Pipeline will fire automatically via Notion triggers."
        )

