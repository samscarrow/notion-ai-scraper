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

        Reads the Work Item, resolves inheritance from its Project, applies the
        Dispatch Via -> Execution Lane default mapping, and runs V1-V14 validation
        (including Pre-Flight Mode and Cascade Depth gates).

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
        Mark a Work Item as consumed by an execution plane.

        Sets Dispatch Requested Consumed At=now(), Status=In Progress, and writes
        the run_id for idempotency tracking.
        Also creates an audit log entry.

        work_item_id: UUID of the Work Item page.
        run_id: The run_id from the dispatch packet (from build_dispatch_packet).
        """
        client = _get_notion_api_client()
        result = dispatch.stamp_dispatch_consumed(work_item_id, run_id, client)

        if result.get("status") == "already_consumed":
            return (
                f"**Already consumed** (race guard).\n\n"
                f"- Work Item: `{result['work_item_id']}`\n"
                f"- Existing Run ID: `{result['run_id']}`\n"
                f"- Consumed At: `{result['consumed_at']}`"
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
        client = _get_notion_api_client()

        # Parse optional JSON fields
        parsed_metrics = json.loads(metrics) if metrics else None
        parsed_artifacts = json.loads(artifacts) if artifacts else None
        parsed_files = [f.strip() for f in files_changed.split(",") if f.strip()] if files_changed else None

        result = dispatch.handle_final_return(
            work_item_id=work_item_id,
            run_id=run_id,
            status=status,
            summary=summary,
            raw_output=raw_output,
            duration_ms=int(duration_ms),
            model=model,
            lane=lane,
            verdict=verdict or None,
            error=error or None,
            metrics=parsed_metrics,
            artifacts=parsed_artifacts,
            files_changed=parsed_files,
            commit_sha=commit_sha or None,
            pr_url=pr_url or None,
            client=client,
        )

        if not result.get("ingested"):
            if result.get("errors"):
                error_list = "\n".join(f"- {e}" for e in result["errors"])
                return f"**Return rejected** ({len(result['errors'])} error(s)):\n\n{error_list}"
            return f"**Return rejected:** {result.get('reason', 'unknown')} (run_id: {result.get('run_id', '?')})"

        return (
            f"**Return ingested.**\n\n"
            f"- Work Item: `{result['work_item_id']}` ({result['item_name']})\n"
            f"- Run ID: `{result['run_id']}`\n"
            f"- Status: {result['status']} → Notion Status: {result['mapped_status']}\n"
            f"- Verdict: {result.get('verdict') or 'N/A'}\n"
            f"- Intake Clerk trigger: fired (Return Received At set)"
        )
