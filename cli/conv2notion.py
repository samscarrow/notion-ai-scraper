#!/usr/bin/env python3
"""
conv2notion.py — Convert AI conversation JSON to CSV or PDF for Notion AI import.

Supported input formats:
  - Gemini export       ([{role: "user"|"model", parts: [{text: ...}]}])
  - Claude Code JSONL   (.jsonl, one session per file)
  - Claude.ai export    (.zip or .json with chat_messages)
  - ChatGPT export      (conversations.json with mapping tree)
  - Generic             ([{role, content}] or {messages: [{role, content}]})

Notion import behavior:
  CSV  -> Creates a Notion database (one row per conversation)
  PDF  -> Creates a Notion page (full document)

Usage:
  python conv2notion.py gemini-conversation-*.json --format csv -o convs.csv
  python conv2notion.py claude-export.zip --format pdf -o output/
  python conv2notion.py conversations.json --format both -o output/

Dependencies:
  CSV:  stdlib only
  PDF:  pip install fpdf2   (MIT license, pure Python)
"""

import argparse
import csv
import json
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

class Conversation:
    def __init__(self, title: str, date: str, source: str, messages: list[dict]):
        self.title = title
        self.date = date
        self.source = source
        self.messages = messages  # list of {role: str, content: str}

    @property
    def turns(self) -> int:
        return len(self.messages)

    @property
    def full_transcript(self) -> str:
        parts = []
        for m in self.messages:
            label = "USER" if m["role"] == "user" else "ASSISTANT"
            parts.append(f"[{label}]\n{m['content'].strip()}")
        return "\n\n".join(parts)


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _ts_to_date(ts: int | float | None) -> str:
    if not ts:
        return ""
    if ts > 1e12:
        ts = ts / 1000
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


def _extract_content(content: Any) -> str:
    """Flatten any content shape to a plain string."""
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                # Gemini parts: {text: ...} — skip functionCall / functionResponse
                if "text" in item:
                    parts.append(item["text"])
                elif "content" in item:
                    parts.append(_extract_content(item["content"]))
        return " ".join(p for p in parts if p).strip()
    if isinstance(content, dict):
        # Try common keys
        for key in ("text", "content", "value"):
            if key in content:
                return _extract_content(content[key])
    return ""


# ---------------------------------------------------------------------------
# Format parsers
# ---------------------------------------------------------------------------

def _is_gemini_format(data: Any) -> bool:
    """[{role: "user"|"model", parts: [...]}, ...]"""
    return (
        isinstance(data, list)
        and len(data) > 0
        and isinstance(data[0], dict)
        and "parts" in data[0]
        and data[0].get("role") in ("user", "model")
    )


def _parse_gemini(data: list, source_name: str) -> list[Conversation]:
    """Inspired by Gemini web export format."""
    messages = []
    for entry in data:
        role_raw = entry.get("role", "user")
        role = "user" if role_raw == "user" else "assistant"
        content = _extract_content(entry.get("parts", []))
        if content:
            messages.append({"role": role, "content": content})
    if not messages:
        return []
    return [Conversation(title=source_name, date="", source="gemini", messages=messages)]


def _parse_chatgpt_export(data: list) -> list[Conversation]:
    """ChatGPT conversations.json: list of convs with mapping tree.
    # Inspired by remorses/gist pattern (MIT)
    """
    convs = []
    for item in data:
        title = item.get("title", "Untitled")
        ts = item.get("create_time")
        date = _ts_to_date(ts) if ts else ""

        mapping = item.get("mapping", {})

        # Find root node (no parent or parent not in mapping)
        root_id = None
        for node_id, node in mapping.items():
            parent = node.get("parent")
            if parent is None or parent not in mapping:
                root_id = node_id
                break

        ordered = _walk_mapping(mapping, root_id) if root_id else list(mapping.values())

        messages = []
        for node in ordered:
            msg = node.get("message")
            if not msg:
                continue
            role_raw = msg.get("author", {}).get("role", "")
            if role_raw not in ("user", "assistant"):
                continue
            content = _extract_content(msg.get("content", {}).get("parts", [""]))
            if content:
                messages.append({"role": role_raw, "content": content})

        if messages:
            convs.append(Conversation(title=title, date=date, source="chatgpt", messages=messages))

    return convs


def _walk_mapping(mapping: dict, node_id: str, visited: set | None = None) -> list[dict]:
    if visited is None:
        visited = set()
    if node_id in visited or node_id not in mapping:
        return []
    visited.add(node_id)
    node = mapping[node_id]
    result = [node]
    for child_id in node.get("children", []):
        result.extend(_walk_mapping(mapping, child_id, visited))
    return result


def _parse_claude_export_zip(path: Path) -> list[Conversation]:
    """Claude.ai web export zip archive."""
    convs = []
    with zipfile.ZipFile(path, "r") as z:
        for fname in z.namelist():
            if not fname.endswith(".json"):
                continue
            try:
                with z.open(fname) as f:
                    data = json.load(f)
            except (json.JSONDecodeError, KeyError):
                continue
            conv = _parse_claude_export_dict(data, fname)
            if conv:
                convs.append(conv)
    return convs


def _parse_claude_export_dict(data: dict, fname: str = "") -> Conversation | None:
    """Single Claude.ai web export JSON: {uuid, chat_messages: [...]}"""
    if not isinstance(data, dict) or "chat_messages" not in data:
        return None
    title = data.get("name") or Path(fname).stem or "Untitled"
    messages = []
    date = ""
    for msg in data.get("chat_messages", []):
        role = "user" if msg.get("sender") == "human" else "assistant"
        content = _extract_content(msg.get("content", ""))
        if not content:
            continue
        if not date:
            date = (msg.get("created_at") or "")[:10]
        messages.append({"role": role, "content": content})
    if not messages:
        return None
    return Conversation(title=title, date=date, source="claude-web", messages=messages)


def _parse_jsonl_claude(path: Path) -> list[Conversation]:
    """Claude Code session JSONL: one file = one session."""
    messages = []
    date = ""
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue

            msg_type = entry.get("type")
            if msg_type not in ("user", "assistant"):
                continue

            msg = entry.get("message", {})
            content = _extract_content(msg.get("content", ""))
            if not content:
                continue

            ts = entry.get("timestamp", "")
            if isinstance(ts, str) and ts and not date:
                date = ts[:10]
            elif isinstance(ts, (int, float)) and not date:
                date = _ts_to_date(ts)

            messages.append({"role": msg_type, "content": content})

    if not messages:
        return []
    return [Conversation(title=path.stem, date=date, source="claude-code", messages=messages)]


def _parse_notion_forge(data: dict, source_name: str) -> list[Conversation]:
    """Notion AI thread export from notion-forge: {threadId, turns, toolCalls, ...}"""
    messages = []
    date = _ts_to_date(data.get("createdAt"))

    # Extract user/assistant turns if present
    for turn in data.get("turns", []):
        role = turn.get("role", "user")
        if role not in ("user", "assistant"):
            role = "assistant"
        content = _extract_content(turn.get("content") or turn.get("text") or turn.get("value") or "")
        if content:
            messages.append({"role": role, "content": content})

    # Extract tool calls as assistant actions
    for tc in data.get("toolCalls", []):
        tool = tc.get("tool", "")
        inp = tc.get("input", {})
        result = tc.get("result", {})

        if tool == "result":
            # Final agent summary
            msg = inp.get("message") or result.get("message") or ""
            if msg:
                messages.append({"role": "assistant", "content": f"[Result]\n{msg}"})
        elif tool == "update-page-v2":
            cmd = inp.get("command", "")
            url = inp.get("pageUrl", "")
            edited = result.get("numPagesEdited", "")
            summary = f"[{tool}] {cmd}" + (f" {url}" if url else "") + (f" ({edited} pages edited)" if edited else "")
            messages.append({"role": "assistant", "content": summary})
        else:
            # Summarize other tool calls (view, query-data-sources, etc.)
            urls = inp.get("urls", [])
            query = inp.get("query", "")
            detail = ", ".join(urls) if urls else query or ""
            if detail:
                messages.append({"role": "assistant", "content": f"[{tool}] {detail}"})

    if not messages:
        return []

    title = source_name
    return [Conversation(title=title, date=date, source="notion-forge", messages=messages)]


def _parse_generic(data: Any, source_name: str = "generic") -> list[Conversation]:
    """Fallback: [{role, content}] or {messages: [...]} or {conversations: [...]}"""
    if isinstance(data, list):
        if data and isinstance(data[0], dict):
            # List of messages?
            if "role" in data[0] and ("content" in data[0] or "text" in data[0]):
                msgs = []
                for m in data:
                    content = _extract_content(m.get("content") or m.get("text") or "")
                    if content:
                        msgs.append({"role": m.get("role", "user"), "content": content})
                if msgs:
                    return [Conversation(source_name, "", "generic", msgs)]
            # List of conversation objects?
            convs = []
            for item in data:
                if isinstance(item, dict) and ("messages" in item or "chat_messages" in item):
                    msgs_raw = item.get("messages") or item.get("chat_messages") or []
                    msgs = []
                    for m in msgs_raw:
                        content = _extract_content(m.get("content") or m.get("text") or "")
                        if content:
                            msgs.append({"role": m.get("role", "user"), "content": content})
                    if msgs:
                        title = item.get("title") or item.get("name") or source_name
                        date = (item.get("created_at") or "")[:10]
                        convs.append(Conversation(title, date, "generic", msgs))
            return convs

    if isinstance(data, dict):
        msgs_raw = data.get("messages") or data.get("chat_messages") or []
        if msgs_raw:
            msgs = []
            for m in msgs_raw:
                content = _extract_content(m.get("content") or m.get("text") or "")
                if content:
                    msgs.append({"role": m.get("role", "user"), "content": content})
            if msgs:
                title = data.get("title") or data.get("name") or source_name
                date = (data.get("created_at") or "")[:10]
                return [Conversation(title, date, "generic", msgs)]
        if "conversations" in data:
            return _parse_generic(data["conversations"], source_name)

    return []


def load_conversations(path: Path) -> list[Conversation]:
    """Auto-detect format and return normalized conversations."""
    suffix = path.suffix.lower()

    if suffix == ".jsonl":
        return _parse_jsonl_claude(path)

    if suffix == ".zip":
        return _parse_claude_export_zip(path)

    if suffix in (".json", ""):
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, UnicodeDecodeError):
            # Try as JSONL
            return _parse_jsonl_claude(path)

        # Gemini: [{role: "user"|"model", parts: [...]}]
        if _is_gemini_format(data):
            return _parse_gemini(data, path.stem)

        # ChatGPT: list with mapping trees
        if isinstance(data, list) and data and isinstance(data[0], dict) and "mapping" in data[0]:
            return _parse_chatgpt_export(data)

        # Notion-forge thread export: {threadId, toolCalls, ...}
        if isinstance(data, dict) and "threadId" in data and "toolCalls" in data:
            return _parse_notion_forge(data, path.stem)

        # Claude.ai single export file
        if isinstance(data, dict) and "chat_messages" in data:
            conv = _parse_claude_export_dict(data, path.name)
            return [conv] if conv else []

        # Everything else
        return _parse_generic(data, path.stem)

    # Unknown extension — try JSON, then JSONL
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return _parse_generic(data, path.stem)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return _parse_jsonl_claude(path)


# ---------------------------------------------------------------------------
# CSV output
# ---------------------------------------------------------------------------

def write_csv(conversations: list[Conversation], out_path: Path) -> None:
    """One-row-per-conversation schema optimized for Notion AI database queries."""
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Title", "Date", "Source", "Turns", "Full Transcript"])
        for conv in conversations:
            writer.writerow([
                conv.title,
                conv.date,
                conv.source,
                conv.turns,
                conv.full_transcript,
            ])
    print(f"  CSV  -> {out_path}  ({len(conversations)} conversations)")


# ---------------------------------------------------------------------------
# PDF output
# ---------------------------------------------------------------------------

def write_pdf(conversations: list[Conversation], out_path: Path) -> None:
    """Combined PDF: all conversations separated by page breaks.
    Requires: pip install fpdf2  (MIT license — py-pdf/fpdf2)
    """
    try:
        from fpdf import FPDF
    except ImportError:
        print("ERROR: fpdf2 not installed.\n  Run: pip install fpdf2", file=sys.stderr)
        sys.exit(1)

    # Role styles
    ROLE_BG = {
        "user":      (220, 235, 255),  # soft blue
        "assistant": (242, 242, 242),  # light grey
    }
    ROLE_LABEL = {
        "user":      "USER",
        "assistant": "ASSISTANT",
    }

    # Locate a Unicode TTF font — conversations contain box-drawing chars, CJK, etc.
    _FONT_CANDIDATES = [
        "/usr/share/fonts/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/noto/NotoSans-Regular.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",    # Debian/Ubuntu
        "/usr/share/fonts/TTF/DejaVuSans.ttf",                 # Arch
    ]
    _FONT_BOLD_CANDIDATES = [
        "/usr/share/fonts/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/noto/NotoSans-Bold.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/TTF/DejaVuSans-Bold.ttf",
    ]
    unicode_font = next((f for f in _FONT_CANDIDATES if Path(f).exists()), None)
    unicode_bold = next((f for f in _FONT_BOLD_CANDIDATES if Path(f).exists()), None)

    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.set_margins(20, 20, 20)

    if unicode_font:
        pdf.add_font("Unicode", style="", fname=unicode_font)
        FONT_REG = ("Unicode", "", )
    else:
        FONT_REG = ("Helvetica", "",)

    if unicode_bold:
        pdf.add_font("UnicodeBold", style="", fname=unicode_bold)
        FONT_BOLD = ("UnicodeBold", "",)
    elif unicode_font:
        FONT_BOLD = ("Unicode", "",)
    else:
        FONT_BOLD = ("Helvetica", "B",)

    for conv in conversations:
        pdf.add_page()

        # --- Conversation header bar ---
        pdf.set_fill_color(30, 40, 70)
        pdf.set_text_color(255, 255, 255)
        pdf.set_font(*FONT_BOLD, 13)
        pdf.cell(0, 10, conv.title[:90], fill=True, new_x="LEFT", new_y="NEXT")

        pdf.set_text_color(100, 100, 120)
        pdf.set_font(*FONT_REG, 8)
        meta = f"{conv.date}  |  {conv.source}  |  {conv.turns} turns"
        pdf.cell(0, 5, meta, new_x="LEFT", new_y="NEXT")
        pdf.ln(4)

        # --- Messages ---
        for msg in conv.messages:
            role = msg["role"]
            content = msg["content"].strip()
            bg = ROLE_BG.get(role, (250, 250, 250))
            label = ROLE_LABEL.get(role, role.upper())

            # Role label strip
            pdf.set_fill_color(*bg)
            pdf.set_text_color(50, 60, 90)
            pdf.set_font(*FONT_BOLD, 8)
            pdf.cell(0, 5, label, fill=True, new_x="LEFT", new_y="NEXT")

            # Message body — cap at 8000 chars to avoid runaway pages
            pdf.set_font(*FONT_REG, 9)
            pdf.set_text_color(25, 25, 25)
            pdf.multi_cell(0, 5, text=content[:8000], fill=True, align="L")
            pdf.ln(3)

    pdf.output(str(out_path))
    print(f"  PDF  -> {out_path}  ({len(conversations)} conversations)")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Convert AI conversation JSON to CSV or PDF for Notion AI import."
    )
    parser.add_argument(
        "inputs",
        nargs="+",
        metavar="FILE",
        help="Input file(s): .json, .jsonl, or .zip",
    )
    parser.add_argument(
        "--format", "-f",
        choices=["csv", "pdf", "both"],
        default="csv",
        help="Output format (default: csv)",
    )
    parser.add_argument(
        "--output", "-o",
        default=".",
        help="Output file path (for csv/pdf) or directory (for both). Default: current dir.",
    )
    args = parser.parse_args()

    # Collect all conversations from all input files
    all_convs: list[Conversation] = []
    for pattern in args.inputs:
        paths = list(Path(".").glob(pattern)) if "*" in pattern else [Path(pattern)]
        for p in sorted(paths):
            if not p.exists():
                print(f"  WARN: {p} not found, skipping", file=sys.stderr)
                continue
            convs = load_conversations(p)
            print(f"  {p.name}: {len(convs)} conversation(s) detected")
            all_convs.extend(convs)

    if not all_convs:
        print("No conversations found.", file=sys.stderr)
        sys.exit(1)

    print(f"Total: {len(all_convs)} conversation(s)")

    out = Path(args.output)
    fmt = args.format

    if fmt in ("csv", "both"):
        csv_path = (out / "conversations.csv") if out.is_dir() or fmt == "both" else out
        if fmt == "both":
            out.mkdir(parents=True, exist_ok=True)
            csv_path = out / "conversations.csv"
        write_csv(all_convs, csv_path)

    if fmt in ("pdf", "both"):
        pdf_path = (out / "conversations.pdf") if out.is_dir() or fmt == "both" else out
        if fmt == "both":
            out.mkdir(parents=True, exist_ok=True)
            pdf_path = out / "conversations.pdf"
        write_pdf(all_convs, pdf_path)


if __name__ == "__main__":
    main()
