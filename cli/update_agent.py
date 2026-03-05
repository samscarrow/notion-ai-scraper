#!/usr/bin/env python3
"""
update_agent.py — Programmatically update and publish Notion AI Agent instructions.

Usage:
  python cli/update_agent.py librarian instructions.md        # update + publish
  python cli/update_agent.py librarian instructions.md --dry-run
  python cli/update_agent.py librarian --publish-only         # re-publish without changes
  python cli/update_agent.py librarian --dump                 # print current instructions

Requires:
  - Firefox with an active Notion session (token_v2 cookie)
  - cli/agents.yaml with the target agent's IDs
  - pyyaml: pip install pyyaml  (only external dependency)
"""

import argparse
import sys
import os

# Allow running from project root or cli/ directory
sys.path.insert(0, os.path.dirname(__file__))

try:
    import yaml
except ImportError:
    print("Error: pyyaml is required.  Install with: pip install pyyaml", file=sys.stderr)
    sys.exit(1)

import cookie_extract
import notion_client
import block_builder


AGENTS_YAML = os.path.join(os.path.dirname(__file__), "agents.yaml")


def load_agent_config(name: str) -> dict:
    with open(AGENTS_YAML) as f:
        registry = yaml.safe_load(f)

    if name not in registry:
        available = ", ".join(sorted(registry.keys()))
        print(f"Error: agent '{name}' not found in agents.yaml.", file=sys.stderr)
        print(f"Available agents: {available}", file=sys.stderr)
        sys.exit(1)

    cfg = registry[name]
    required = {"workflow_id", "space_id", "block_id"}
    missing = required - set(cfg.keys())
    if missing:
        print(f"Error: agents.yaml entry for '{name}' is missing: {missing}", file=sys.stderr)
        sys.exit(1)

    return cfg


def get_auth() -> tuple[str, str | None]:
    """Return (token_v2, user_id). Exits on failure."""
    try:
        token = cookie_extract.get_token_v2()
    except (FileNotFoundError, ValueError) as e:
        print(f"Auth error: {e}", file=sys.stderr)
        sys.exit(1)

    user_id = cookie_extract.get_user_id()
    return token, user_id


def cmd_dump(cfg: dict, token: str, user_id: str | None) -> None:
    """Print current agent instructions as Markdown."""
    print(f"Fetching instructions block {cfg['block_id']}...")
    data = notion_client.get_block_tree(cfg["block_id"], cfg["space_id"], token, user_id)
    blocks_map = data.get("recordMap", {}).get("block", {})

    if not blocks_map:
        print("(No content found — block may be empty or inaccessible)")
        return

    md = block_builder.blocks_to_markdown(blocks_map, cfg["block_id"])
    print(md or "(Empty instructions block)")


def cmd_update(cfg: dict, instructions_file: str,
               token: str, user_id: str | None,
               dry_run: bool, publish: bool) -> None:
    """Replace block content and optionally publish."""
    try:
        with open(instructions_file) as f:
            md_content = f.read()
    except FileNotFoundError:
        print(f"Error: instructions file not found: {instructions_file}", file=sys.stderr)
        sys.exit(1)

    new_blocks = block_builder.markdown_to_blocks(md_content)
    if not new_blocks:
        print("Error: instructions file produced no blocks. Check the file content.",
              file=sys.stderr)
        sys.exit(1)

    print(f"Parsed {len(new_blocks)} block(s) from {instructions_file}")

    if dry_run:
        print("\n[DRY RUN] Would replace block content and publish. Payload preview:\n")

    print(f"Replacing content of block {cfg['block_id']}...")
    notion_client.replace_block_content(
        cfg["block_id"], cfg["space_id"], new_blocks,
        token, user_id, dry_run,
    )

    if publish:
        cmd_publish(cfg, token, user_id, dry_run)
    else:
        print("Content updated. Skipping publish (use --publish-only to deploy).")


def cmd_publish(cfg: dict, token: str, user_id: str | None, dry_run: bool) -> None:
    """Publish the agent workflow."""
    print(f"Publishing agent (workflow {cfg['workflow_id']})...")
    result = notion_client.publish_agent(
        cfg["workflow_id"], cfg["space_id"], token, user_id, dry_run,
    )
    if not dry_run:
        artifact_id = result.get("workflowArtifactId", "?")
        version = result.get("version", "?")
        print(f"✓ Published — artifact: {artifact_id}  version: {version}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Update and publish Notion AI Agent instructions.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python cli/update_agent.py librarian instructions.md
  python cli/update_agent.py librarian instructions.md --dry-run
  python cli/update_agent.py librarian --publish-only
  python cli/update_agent.py librarian --dump
        """,
    )
    parser.add_argument("agent", help="Agent name (must exist in agents.yaml)")
    parser.add_argument("instructions", nargs="?",
                        help="Path to Markdown file with new instructions")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print payloads without making any API calls")
    parser.add_argument("--publish-only", action="store_true",
                        help="Re-publish without changing content")
    parser.add_argument("--dump", action="store_true",
                        help="Print current instructions as Markdown and exit")
    parser.add_argument("--no-publish", action="store_true",
                        help="Update content but skip the publish step")

    args = parser.parse_args()

    # Validate argument combinations
    if args.dump and (args.instructions or args.publish_only):
        parser.error("--dump cannot be combined with instructions file or --publish-only")
    if args.publish_only and args.instructions:
        parser.error("--publish-only cannot be combined with an instructions file")
    if not args.dump and not args.publish_only and not args.instructions:
        parser.error("Provide an instructions file, or use --dump / --publish-only")

    cfg = load_agent_config(args.agent)
    token, user_id = get_auth()

    print(f"Agent: {args.agent}  |  Space: {cfg['space_id'][:8]}...")

    if args.dump:
        cmd_dump(cfg, token, user_id)
    elif args.publish_only:
        cmd_publish(cfg, token, user_id, args.dry_run)
    else:
        cmd_update(
            cfg, args.instructions,
            token, user_id,
            dry_run=args.dry_run,
            publish=not args.no_publish,
        )


if __name__ == "__main__":
    main()
