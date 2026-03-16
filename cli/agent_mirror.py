#!/usr/bin/env python3
"""
agent_mirror.py — GitOps-style sync of local agent manifests to Notion AI agents.

Reads a YAML manifest describing the desired state of a Notion agent (instructions,
model, MCP servers, page access) and applies the diff against live Notion state.

Usage:
    python agent_mirror.py sync mirrors/my_agent.yaml          # apply changes
    python agent_mirror.py diff mirrors/my_agent.yaml          # preview only
    python agent_mirror.py dump bay_view_consulting             # dump live state as manifest
    python agent_mirror.py watch mirrors/                       # watch for changes (systemd)
"""

import argparse
import hashlib
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import yaml
import block_builder
import cookie_extract
import notion_client


# ── Auth ─────────────────────────────────────────────────────────────────────

AGENTS_YAML = os.path.join(os.path.dirname(os.path.abspath(__file__)), "agents.yaml")

MODEL_ALIASES = {
    "opus": "avocado-froyo-medium",
    "sonnet": "almond-croissant-low",
    "auto": None,
    "chatgpt": "oval-kumquat-medium",
    "minimax": "fireworks-minimax-m2.5",
}


def _get_auth() -> tuple[str, str | None]:
    return cookie_extract.get_auth()


def _load_registry() -> dict:
    if not os.path.exists(AGENTS_YAML):
        raise FileNotFoundError(f"Registry not found: {AGENTS_YAML}")
    with open(AGENTS_YAML) as f:
        return yaml.safe_load(f) or {}


def _get_agent_config(agent_name: str) -> dict:
    registry = _load_registry()
    if agent_name not in registry:
        raise KeyError(f"Agent '{agent_name}' not in agents.yaml. Run sync_registry first.")
    cfg = registry[agent_name]
    cfg.setdefault("space_id", "f04bc8a1-18df-42d1-ba9f-961c491cdc1b")
    return cfg


# ── Manifest loading ─────────────────────────────────────────────────────────

def load_manifest(path: str) -> dict:
    """Load and validate a mirror manifest YAML file."""
    with open(path) as f:
        manifest = yaml.safe_load(f)

    if not manifest or not manifest.get("target"):
        raise ValueError(f"Manifest {path} must have a 'target' field.")

    # Resolve relative instruction paths against cli/ directory
    if manifest.get("instructions"):
        instr_path = manifest["instructions"]
        if not os.path.isabs(instr_path):
            cli_dir = os.path.dirname(os.path.abspath(__file__))
            instr_path = os.path.join(cli_dir, instr_path)
        manifest["_instructions_path"] = instr_path

    return manifest


# ── Live state reader ────────────────────────────────────────────────────────

def read_live_state(agent_name: str) -> dict:
    """Read the current Notion agent state and return a normalized dict."""
    cfg = _get_agent_config(agent_name)
    token, user_id = _get_auth()

    # Get workflow record for model + modules
    wf = notion_client.get_workflow_record(cfg["notion_internal_id"], token, user_id)
    data = wf.get("data", {})

    # Read instructions
    try:
        chunk_data = notion_client.get_block_tree(
            cfg["notion_public_id"], cfg["space_id"], token, user_id,
        )
        blocks_map = chunk_data.get("recordMap", {}).get("block", {})
        instructions_md = block_builder.blocks_to_markdown(
            blocks_map, cfg["notion_public_id"],
        ) if blocks_map else None
    except Exception as e:
        instructions_md = None
        print(f"  Warning: could not read instructions: {e}")

    # Parse model
    model_data = data.get("model") or {}
    model_type = model_data.get("type")
    # Reverse-map to alias
    model_alias = model_type
    for alias, codename in MODEL_ALIASES.items():
        if codename == model_type:
            model_alias = alias
            break

    # Parse MCP servers
    mcp_servers = []
    for mod in data.get("modules", []):
        if mod.get("type") == "mcpServer":
            state = mod.get("state", {})
            mcp_servers.append({
                "name": mod.get("name", ""),
                "url": state.get("serverUrl", ""),
                "id": mod.get("id", ""),
            })

    # Parse page access
    page_access = []
    for mod in data.get("modules", []):
        if mod.get("type") != "notion":
            continue
        state = mod.get("state", {})
        for ident in state.get("identifiers", []):
            if ident.get("type") == "pageOrCollectionViewBlock":
                actions = ident.get("actions", [])
                role = "read_and_write" if "read_and_write" in actions else "reader"
                page_access.append({
                    "id": ident.get("blockId", ""),
                    "role": role,
                })

    return {
        "instructions": instructions_md,
        "model": model_alias,
        "mcp_servers": mcp_servers,
        "page_access": page_access,
        "raw_modules": data.get("modules", []),
    }


# ── Diff engine ──────────────────────────────────────────────────────────────

def compute_diff(manifest: dict, live: dict) -> list[dict]:
    """Compare manifest desired state against live state. Returns list of ops."""
    ops = []

    # Instructions diff
    if manifest.get("_instructions_path"):
        with open(manifest["_instructions_path"]) as f:
            desired_md = f.read().strip()
        live_md = (live.get("instructions") or "").strip()

        if _md_hash(desired_md) != _md_hash(live_md):
            ops.append({
                "type": "instructions",
                "action": "update",
                "desired": desired_md,
                "live_preview": live_md[:200] + "..." if len(live_md) > 200 else live_md,
            })

    # Model diff
    if manifest.get("model"):
        desired_model = manifest["model"]
        live_model = live.get("model")
        if desired_model != live_model:
            ops.append({
                "type": "model",
                "action": "update",
                "desired": desired_model,
                "live": live_model,
            })

    # MCP servers diff
    if manifest.get("mcp_servers"):
        mcp_cfg = manifest["mcp_servers"]
        desired_servers = {s["url"]: s["name"] for s in mcp_cfg.get("servers", [])}
        live_servers = {s["url"]: s["name"] for s in live.get("mcp_servers", [])}
        managed = mcp_cfg.get("managed", True)

        # Servers to add
        for url, name in desired_servers.items():
            if url not in live_servers:
                ops.append({
                    "type": "mcp_server",
                    "action": "add",
                    "name": name,
                    "url": url,
                })

        # Servers to remove (only if managed)
        if managed:
            for url, name in live_servers.items():
                if url not in desired_servers:
                    ops.append({
                        "type": "mcp_server",
                        "action": "remove",
                        "name": name,
                        "url": url,
                    })

    # Page access diff
    if manifest.get("page_access"):
        desired_pages = {p["id"]: p["role"] for p in manifest["page_access"]}
        live_pages = {p["id"]: p["role"] for p in live.get("page_access", [])}

        for pid, role in desired_pages.items():
            if pid not in live_pages or live_pages[pid] != role:
                ops.append({
                    "type": "page_access",
                    "action": "grant",
                    "id": pid,
                    "role": role,
                    "was": live_pages.get(pid),
                })

    return ops


def _md_hash(text: str) -> str:
    """Content hash for markdown comparison, ignoring trailing whitespace."""
    normalized = "\n".join(line.rstrip() for line in text.splitlines())
    return hashlib.sha256(normalized.encode()).hexdigest()[:16]


# ── Apply engine ─────────────────────────────────────────────────────────────

def apply_ops(agent_name: str, ops: list[dict], publish: bool = True) -> list[str]:
    """Apply a list of diff ops to the live Notion agent. Returns log lines."""
    if not ops:
        return ["No changes needed."]

    cfg = _get_agent_config(agent_name)
    token, user_id = _get_auth()
    log = []

    for op in ops:
        if op["type"] == "instructions":
            new_blocks = block_builder.markdown_to_blocks(op["desired"])
            stats = notion_client.diff_replace_block_content(
                cfg["notion_public_id"], cfg["space_id"], new_blocks, token, user_id,
            )
            log.append(f"Instructions: {stats}")

        elif op["type"] == "model":
            codename = MODEL_ALIASES.get(op["desired"], op["desired"])
            notion_client.update_agent_model(
                cfg["notion_internal_id"], cfg["space_id"],
                codename, token, user_id,
            )
            log.append(f"Model: {op.get('live')} → {op['desired']}")

        elif op["type"] == "mcp_server" and op["action"] == "add":
            _add_mcp_server(cfg, token, user_id, op["name"], op["url"])
            log.append(f"MCP add: {op['name']} ({op['url']})")

        elif op["type"] == "mcp_server" and op["action"] == "remove":
            _remove_mcp_server(cfg, token, user_id, op["name"], op["url"])
            log.append(f"MCP remove: {op['name']} ({op['url']})")

        elif op["type"] == "page_access":
            notion_client.grant_agent_resource_access(
                cfg["notion_internal_id"], cfg["space_id"],
                op["id"], op["role"], token, user_id,
            )
            was = op.get("was", "none")
            log.append(f"Page access: {op['id']} → {op['role']} (was: {was})")

    # Publish once after all changes
    if publish:
        result = notion_client.publish_agent(
            cfg["notion_internal_id"], cfg["space_id"], token, user_id,
        )
        version = result.get("version", "?")
        log.append(f"Published v{version}")

    return log


def _add_mcp_server(cfg: dict, token: str, user_id: str | None,
                    name: str, url: str) -> None:
    """Add an MCP server module to the agent."""
    import uuid as _uuid
    wf = notion_client.get_workflow_record(cfg["notion_internal_id"], token, user_id)
    modules = wf.get("data", {}).get("modules", [])
    modules.append({
        "id": str(_uuid.uuid4()),
        "name": name,
        "type": "mcpServer",
        "version": "1.0.0",
        "state": {"serverUrl": url},
    })
    notion_client.update_agent_modules(
        cfg["notion_internal_id"], cfg["space_id"], modules, token, user_id,
    )


def _remove_mcp_server(cfg: dict, token: str, user_id: str | None,
                       name: str, url: str) -> None:
    """Remove an MCP server module by URL match."""
    wf = notion_client.get_workflow_record(cfg["notion_internal_id"], token, user_id)
    modules = wf.get("data", {}).get("modules", [])
    filtered = [
        m for m in modules
        if not (m.get("type") == "mcpServer" and m.get("state", {}).get("serverUrl") == url)
    ]
    if len(filtered) < len(modules):
        notion_client.update_agent_modules(
            cfg["notion_internal_id"], cfg["space_id"], filtered, token, user_id,
        )


# ── Dump (reverse: live → manifest) ─────────────────────────────────────────

def dump_as_manifest(agent_name: str) -> str:
    """Read live Notion agent state and output a mirror manifest YAML."""
    live = read_live_state(agent_name)

    manifest = {
        "target": agent_name,
        "model": live["model"],
    }

    if live.get("mcp_servers"):
        manifest["mcp_servers"] = {
            "managed": True,
            "servers": [{"name": s["name"], "url": s["url"]} for s in live["mcp_servers"]],
        }

    if live.get("page_access"):
        manifest["page_access"] = live["page_access"]

    lines = [
        f"# Mirror manifest for {agent_name}",
        f"# Generated from live Notion state",
        f"# Instructions should be saved to mirrors/{agent_name}.md",
        "",
    ]

    # instructions field (just the reference, actual content goes to .md file)
    manifest["instructions"] = f"mirrors/{agent_name}.md"

    lines.append(yaml.dump(manifest, default_flow_style=False, sort_keys=False))

    return "\n".join(lines), live.get("instructions", "")


try:
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler
    HAVE_WATCHDOG = True
except ImportError:
    HAVE_WATCHDOG = False

def _get_files_to_watch(manifest_path: str) -> list[str]:
    files = [manifest_path]
    try:
        m = load_manifest(manifest_path)
        if m.get("_instructions_path") and os.path.exists(m["_instructions_path"]):
            files.append(m["_instructions_path"])
    except Exception:
        pass
    return files

def _process_change(manifest_path: str) -> None:
    print(f"\n{'='*60}")
    print(f"Change detected: {os.path.basename(manifest_path)}")
    try:
        m = load_manifest(manifest_path)
        live = read_live_state(m["target"])
        ops = compute_diff(m, live)
        if ops:
            log = apply_ops(m["target"], ops)
            for line in log:
                print(f"  {line}")
        else:
            print("  Already in sync.")
    except Exception as e:
        print(f"  Error: {e}")
    print(f"{'='*60}")

def watch_directory(mirrors_dir: str, interval: float = 5.0) -> None:
    """Watch directory for changes to sync automatically.
    
    Uses watchdog if installed, otherwise falls back to polling.
    """
    if HAVE_WATCHDOG:
        _watch_directory_watchdog(mirrors_dir)
    else:
        _watch_directory_polling(mirrors_dir, interval)

def _watch_directory_watchdog(mirrors_dir: str) -> None:
    print(f"Watching {mirrors_dir} (event-driven via watchdog)")

    class MirrorEventHandler(FileSystemEventHandler):
        def __init__(self):
            self.last_sync = {}

        def on_modified(self, event):
            if event.is_directory:
                return
            path = os.path.abspath(event.src_path)
            
            try:
                manifests = [
                    os.path.join(mirrors_dir, f)
                    for f in os.listdir(mirrors_dir)
                    if f.endswith(".yaml") and not f.startswith("_")
                ]
            except OSError:
                return

            for manifest_path in manifests:
                deps = [os.path.abspath(f) for f in _get_files_to_watch(manifest_path)]
                if path in deps:
                    now = time.time()
                    if manifest_path not in self.last_sync or now - self.last_sync[manifest_path] > 2.0:
                        self.last_sync[manifest_path] = now
                        time.sleep(0.1) # Debounce/wait for write flush
                        _process_change(manifest_path)

    observer = Observer()
    observer.schedule(MirrorEventHandler(), os.path.abspath(mirrors_dir), recursive=True)
    
    # Also watch parent directory if instructions might be there
    parent_dir = os.path.dirname(os.path.abspath(mirrors_dir))
    if parent_dir != os.path.abspath(mirrors_dir):
        # Catch instructions that might be in the root directory (one level up)
        observer.schedule(MirrorEventHandler(), parent_dir, recursive=False)

    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

def _watch_directory_polling(mirrors_dir: str, interval: float) -> None:
    print(f"Watching {mirrors_dir} (poll interval: {interval}s)")
    mtimes: dict[str, float] = {}  # path -> last known mtime

    while True:
        try:
            manifests = [
                os.path.join(mirrors_dir, f)
                for f in os.listdir(mirrors_dir)
                if f.endswith(".yaml") and not f.startswith("_")
            ]

            for manifest_path in manifests:
                files_to_watch = _get_files_to_watch(manifest_path)
                changed = False
                for fpath in files_to_watch:
                    try:
                        mtime = os.path.getmtime(fpath)
                    except OSError:
                        continue
                    if fpath not in mtimes:
                        mtimes[fpath] = mtime  # first run, don't trigger
                    elif mtime != mtimes[fpath]:
                        changed = True
                        mtimes[fpath] = mtime

                if changed:
                    _process_change(manifest_path)

        except Exception as e:
            print(f"Watch error: {e}")

        time.sleep(interval)


# ── CLI ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Mirror local agent manifests to Notion AI agents.",
    )
    sub = parser.add_subparsers(dest="command")

    # sync
    p_sync = sub.add_parser("sync", help="Apply manifest to Notion agent")
    p_sync.add_argument("manifest", help="Path to manifest YAML")
    p_sync.add_argument("--no-publish", action="store_true", help="Skip publishing")

    # diff
    p_diff = sub.add_parser("diff", help="Preview changes without applying")
    p_diff.add_argument("manifest", help="Path to manifest YAML")

    # dump
    p_dump = sub.add_parser("dump", help="Dump live agent state as manifest")
    p_dump.add_argument("agent", help="Agent name from agents.yaml")
    p_dump.add_argument("--out", help="Output directory (default: mirrors/)")

    # watch
    p_watch = sub.add_parser("watch", help="Watch directory for changes and auto-sync")
    p_watch.add_argument("directory", help="Mirrors directory to watch")
    p_watch.add_argument("--interval", type=float, default=5.0, help="Poll interval in seconds")

    args = parser.parse_args()

    if args.command == "sync":
        manifest = load_manifest(args.manifest)
        print(f"Syncing {manifest['target']}...")
        live = read_live_state(manifest["target"])
        ops = compute_diff(manifest, live)
        if not ops:
            print("Already in sync. No changes needed.")
            return
        print(f"Applying {len(ops)} change(s):")
        for op in ops:
            print(f"  [{op['type']}] {op['action']}: {op.get('name', op.get('desired', '')[:60])}")
        log = apply_ops(manifest["target"], ops, publish=not args.no_publish)
        for line in log:
            print(f"  {line}")

    elif args.command == "diff":
        manifest = load_manifest(args.manifest)
        print(f"Diffing {manifest['target']}...")
        live = read_live_state(manifest["target"])
        ops = compute_diff(manifest, live)
        if not ops:
            print("In sync. No changes.")
        else:
            print(f"{len(ops)} pending change(s):")
            for op in ops:
                if op["type"] == "instructions":
                    print(f"  [instructions] update (content hash differs)")
                elif op["type"] == "model":
                    print(f"  [model] {op.get('live')} → {op['desired']}")
                elif op["type"] == "mcp_server":
                    print(f"  [mcp] {op['action']}: {op['name']} ({op['url']})")
                elif op["type"] == "page_access":
                    was = op.get("was", "none")
                    print(f"  [page] {op['id']} → {op['role']} (was: {was})")

    elif args.command == "dump":
        print(f"Reading live state for {args.agent}...")
        yaml_content, instructions_md = dump_as_manifest(args.agent)

        out_dir = args.out or os.path.join(os.path.dirname(os.path.abspath(__file__)), "mirrors")
        os.makedirs(out_dir, exist_ok=True)

        manifest_path = os.path.join(out_dir, f"{args.agent}.yaml")
        with open(manifest_path, "w") as f:
            f.write(yaml_content)
        print(f"Manifest: {manifest_path}")

        if instructions_md:
            md_path = os.path.join(out_dir, f"{args.agent}.md")
            with open(md_path, "w") as f:
                f.write(instructions_md)
            print(f"Instructions: {md_path}")

    elif args.command == "watch":
        watch_directory(args.directory, interval=args.interval)

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
