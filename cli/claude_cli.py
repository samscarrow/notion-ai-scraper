#!/usr/bin/env python3
"""
claude_cli.py — Claude.ai Project sync tool.

Usage:
    python claude_cli.py list-projects
    python cli.py list-docs PROJECT_ID
    python cli.py get-instructions PROJECT_ID
    python cli.py set-instructions PROJECT_ID FILE
    python cli.py sync-docs PROJECT_ID FILE [FILE ...]
    python cli.py upload-doc PROJECT_ID FILE
    python cli.py delete-doc PROJECT_ID DOC_UUID
    python cli.py get-memory PROJECT_ID
    python cli.py diff-instructions PROJECT_ID FILE

Environment:
    CLAUDE_ORG_ID       — Organization UUID (auto-detected if not set)
    CLAUDE_SESSION_KEY  — Session key (extracted from Firefox if not set)
"""

import argparse
import difflib
import json
import os
import sys

from claude_cookie_extract import get_cookie_header
from claude_client import ClaudeProjectClient


def get_client() -> ClaudeProjectClient:
    cookie_header = get_cookie_header()
    org_id = os.environ.get("CLAUDE_ORG_ID")
    if not org_id:
        from urllib.request import Request, urlopen

        req = Request("https://claude.ai/api/organizations")
        req.add_header("Cookie", cookie_header)
        req.add_header("Content-Type", "application/json")
        req.add_header("User-Agent", "Mozilla/5.0 (X11; Linux x86_64; rv:147.0) Gecko/20100101 Firefox/147.0")
        with urlopen(req) as resp:
            orgs = json.loads(resp.read())
        if not orgs:
            print("No organizations found.", file=sys.stderr)
            sys.exit(1)
        org_id = orgs[0]["uuid"]
    return ClaudeProjectClient(cookie_header, org_id)


def cmd_list_projects(args):
    client = get_client()
    projects = client.list_projects(limit=args.limit)
    for p in projects:
        docs = p.get("docs_count", 0) or 0
        print(f'{p["uuid"]}  {p["name"]}  ({docs} docs)')


def cmd_list_docs(args):
    client = get_client()
    docs = client.list_docs(args.project_id)
    for d in docs:
        tokens = d.get("estimated_token_count", "?")
        print(f'{d["uuid"]}  {d["file_name"]}  ({tokens} tokens)')


def cmd_get_instructions(args):
    client = get_client()
    project = client.get_project(args.project_id)
    template = project.get("prompt_template", "")
    if args.output:
        with open(args.output, "w") as f:
            f.write(template)
        print(f"Written to {args.output}", file=sys.stderr)
    else:
        print(template)


def cmd_set_instructions(args):
    client = get_client()
    with open(args.file) as f:
        content = f.read()
    client.update_project(args.project_id, prompt_template=content)
    print(f"Instructions updated from {args.file}")


def cmd_diff_instructions(args):
    client = get_client()
    project = client.get_project(args.project_id)
    remote = project.get("prompt_template", "").splitlines(keepends=True)
    with open(args.file) as f:
        local = f.readlines()
    diff = difflib.unified_diff(remote, local, fromfile="remote", tofile=args.file)
    output = "".join(diff)
    if output:
        print(output)
    else:
        print("No differences.")


def cmd_upload_doc(args):
    client = get_client()
    with open(args.file) as f:
        content = f.read()
    file_name = os.path.basename(args.file)
    result = client.upload_doc(args.project_id, file_name, content)
    print(f'Uploaded {file_name} → {result["uuid"]} ({result.get("estimated_token_count", "?")} tokens)')


def cmd_sync_docs(args):
    """Sync local files to project docs. Replaces docs with matching filenames, uploads new ones."""
    client = get_client()
    remote_docs = client.list_docs(args.project_id)
    remote_by_name = {d["file_name"]: d for d in remote_docs}

    for file_path in args.files:
        file_name = os.path.basename(file_path)
        with open(file_path) as f:
            content = f.read()

        if file_name in remote_by_name:
            remote_doc = remote_by_name[file_name]
            if remote_doc.get("content", "").strip() == content.strip():
                print(f"  skip  {file_name} (unchanged)")
                continue
            # Delete old, upload new
            client.delete_doc(args.project_id, remote_doc["uuid"])
            result = client.upload_doc(args.project_id, file_name, content)
            print(f"  update  {file_name} → {result['uuid']}")
        else:
            result = client.upload_doc(args.project_id, file_name, content)
            print(f"  add  {file_name} → {result['uuid']}")


def cmd_delete_doc(args):
    client = get_client()
    client.delete_doc(args.project_id, args.doc_uuid)
    print(f"Deleted {args.doc_uuid}")


def cmd_get_memory(args):
    client = get_client()
    memory = client.get_memory(args.project_id)
    print(json.dumps(memory, indent=2))


def main():
    parser = argparse.ArgumentParser(
        prog="claude-project-sync",
        description="Manage Claude.ai Projects from the command line.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # list-projects
    p = sub.add_parser("list-projects", help="List all projects")
    p.add_argument("--limit", type=int, default=30)
    p.set_defaults(func=cmd_list_projects)

    # list-docs
    p = sub.add_parser("list-docs", help="List knowledge files in a project")
    p.add_argument("project_id")
    p.set_defaults(func=cmd_list_docs)

    # get-instructions
    p = sub.add_parser("get-instructions", help="Print project instructions")
    p.add_argument("project_id")
    p.add_argument("-o", "--output", help="Write to file instead of stdout")
    p.set_defaults(func=cmd_get_instructions)

    # set-instructions
    p = sub.add_parser("set-instructions", help="Update project instructions from a file")
    p.add_argument("project_id")
    p.add_argument("file")
    p.set_defaults(func=cmd_set_instructions)

    # diff-instructions
    p = sub.add_parser("diff-instructions", help="Diff local file against remote instructions")
    p.add_argument("project_id")
    p.add_argument("file")
    p.set_defaults(func=cmd_diff_instructions)

    # upload-doc
    p = sub.add_parser("upload-doc", help="Upload a knowledge file")
    p.add_argument("project_id")
    p.add_argument("file")
    p.set_defaults(func=cmd_upload_doc)

    # sync-docs
    p = sub.add_parser("sync-docs", help="Sync local files to project knowledge (replace if changed, add if new)")
    p.add_argument("project_id")
    p.add_argument("files", nargs="+")
    p.set_defaults(func=cmd_sync_docs)

    # delete-doc
    p = sub.add_parser("delete-doc", help="Delete a knowledge file by UUID")
    p.add_argument("project_id")
    p.add_argument("doc_uuid")
    p.set_defaults(func=cmd_delete_doc)

    # get-memory
    p = sub.add_parser("get-memory", help="Get project memory")
    p.add_argument("project_id")
    p.set_defaults(func=cmd_get_memory)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
