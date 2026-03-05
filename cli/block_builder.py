"""
block_builder.py — Convert Markdown text to Notion block dicts.

Produces block values compatible with saveTransactionsFanout's `set` command.
Supports the subset of Markdown used in Notion Agent instruction pages.
"""

import re
from typing import Any

# Mention type codes used by Notion rich text annotations.
# Single-letter code in annotation → human-readable type name.
MENTION_TYPES = {"p": "page", "u": "user", "d": "date", "a": "agent", "s": "space"}
MENTION_CODES = {v: k for k, v in MENTION_TYPES.items()}

_MENTION_TOKEN_RE = re.compile(r'(\{\{\w+:[0-9a-f-]+\}\})')
_MENTION_PARSE_RE = re.compile(r'^\{\{(\w+):([0-9a-f-]+)\}\}$')
_FMT_RE = re.compile(r'\*\*(.+?)\*\*|\*(.+?)\*|`(.+?)`|([^*`]+)')


def _rich_text(text: str) -> list[list]:
    """
    Convert a plain text string with basic inline markdown to Notion rich_text format.
    Notion rich_text is a list of segments: [["text"], ["text", [["b"]]], ...]

    Handles: **bold**, *italic*, `code`, {{type:uuid}} mentions, and plain text.
    """
    segments: list[list] = []
    # Split on mention tokens first, then parse formatting in each chunk
    parts = _MENTION_TOKEN_RE.split(text)
    for part in parts:
        if not part:
            continue
        mention = _MENTION_PARSE_RE.match(part)
        if mention:
            ann = MENTION_CODES.get(mention.group(1), mention.group(1))
            segments.append(["\u2023", [[ann, mention.group(2)]]])
            continue
        # Parse formatting within this non-mention chunk
        for m in _FMT_RE.finditer(part):
            bold_text, italic_text, code_text, plain = m.groups()
            if bold_text:
                segments.append([bold_text, [["b"]]])
            elif italic_text:
                segments.append([italic_text, [["i"]]])
            elif code_text:
                segments.append([code_text, [["c"]]])
            elif plain:
                segments.append([plain])

    return segments if segments else [[text]]


def _rich_text_to_markdown(segments: list[list]) -> str:
    """Convert Notion rich_text segments back to Markdown, resolving mentions."""
    parts: list[str] = []
    for seg in segments:
        text = seg[0] if seg else ""
        ann = seg[1] if len(seg) > 1 else None
        if not ann or not isinstance(ann, list):
            parts.append(text)
            continue
        # Mention pointer: ‣ with a type+uuid annotation
        if text == "\u2023":
            for a in ann:
                if isinstance(a, list) and len(a) >= 2:
                    type_name = MENTION_TYPES.get(a[0], a[0])
                    parts.append(f"{{{{{type_name}:{a[1]}}}}}")
                    break
            else:
                parts.append(text)
            continue
        # Formatting annotations
        out = text
        for a in ann:
            if not isinstance(a, list):
                continue
            if a[0] == "b":
                out = f"**{out}**"
            elif a[0] == "i":
                out = f"*{out}*"
            elif a[0] == "c":
                out = f"`{out}`"
        parts.append(out)
    return "".join(parts)


def _paragraph(text: str) -> dict[str, Any]:
    return {
        "type": "text",
        "properties": {"title": _rich_text(text)},
    }


def _heading(text: str, level: int) -> dict[str, Any]:
    type_map = {1: "header", 2: "sub_header", 3: "sub_sub_header"}
    return {
        "type": type_map.get(level, "sub_sub_header"),
        "properties": {"title": _rich_text(text)},
    }


def _bulleted_list(text: str) -> dict[str, Any]:
    return {
        "type": "bulleted_list",
        "properties": {"title": _rich_text(text)},
    }


def _numbered_list(text: str) -> dict[str, Any]:
    return {
        "type": "numbered_list",
        "properties": {"title": _rich_text(text)},
    }


def _code_block(code: str, language: str = "plain text") -> dict[str, Any]:
    return {
        "type": "code",
        "properties": {
            "title": [[code]],
            "language": [[language]],
        },
    }


def _callout(text: str, emoji: str = "📌") -> dict[str, Any]:
    return {
        "type": "callout",
        "properties": {"title": _rich_text(text)},
        "format": {"page_icon": emoji},
    }


def _divider() -> dict[str, Any]:
    return {"type": "divider", "properties": {}}


def _parse_line(stripped: str) -> dict[str, Any]:
    """Parse a single stripped line into a Notion block dict."""
    # Headings
    h3 = re.match(r'^### (.+)', stripped)
    h2 = re.match(r'^## (.+)', stripped)
    h1 = re.match(r'^# (.+)', stripped)
    if h3:
        return _heading(h3.group(1), 3)
    if h2:
        return _heading(h2.group(1), 2)
    if h1:
        return _heading(h1.group(1), 1)

    # Divider
    if re.match(r'^[-*_]{3,}$', stripped):
        return _divider()

    # Blockquote → callout
    if stripped.startswith(">"):
        text = stripped.lstrip(">").strip()
        emoji_match = re.match(r'^([\U00010000-\U0010ffff]|[\u2600-\u27BF])\s*(.*)', text)
        if emoji_match:
            return _callout(emoji_match.group(2), emoji_match.group(1))
        return _callout(text)

    # Bulleted list
    if re.match(r'^[-*+] ', stripped):
        text = re.sub(r'^[-*+] ', '', stripped)
        return _bulleted_list(text)

    # Numbered list
    if re.match(r'^\d+\. ', stripped):
        text = re.sub(r'^\d+\. ', '', stripped)
        return _numbered_list(text)

    # Paragraph
    return _paragraph(stripped)


def markdown_to_blocks(md: str) -> list[dict[str, Any]]:
    """
    Parse markdown string into a list of Notion block dicts.
    Indented lines (2 spaces per level) become children of the preceding block.

    Supported syntax:
      # H1  ## H2  ### H3
      - bullet item
      1. numbered item
      > blockquote (rendered as callout)
      --- (divider)
      ```lang ... ``` (fenced code block)
      **bold**  *italic*  `inline code`
      {{page:uuid}}  {{user:uuid}}  {{agent:uuid}} (mentions)
      Plain paragraph text
    """
    blocks: list[dict[str, Any]] = []
    lines = md.splitlines()
    i = 0

    while i < len(lines):
        line = lines[i]

        # Fenced code block (no nesting)
        fence = re.match(r'^(\s*)```(\w*)$', line)
        if fence:
            lang = fence.group(2) or "plain text"
            code_lines = []
            i += 1
            while i < len(lines) and not lines[i].strip().startswith("```"):
                code_lines.append(lines[i])
                i += 1
            blocks.append(_code_block("\n".join(code_lines), lang))
            i += 1
            continue

        stripped = line.strip()

        # Skip empty lines
        if not stripped:
            i += 1
            continue

        # Measure indent level (2 spaces per level)
        indent = (len(line) - len(line.lstrip())) >> 1
        block = _parse_line(stripped)

        if indent > 0 and blocks:
            # Find the ancestor at (indent - 1) depth
            parent = blocks[-1]
            for _d in range(1, indent):
                children = parent.get("children")
                if children:
                    parent = children[-1]
                else:
                    break
            parent.setdefault("children", []).append(block)
        else:
            blocks.append(block)

        i += 1

    return blocks


def blocks_to_markdown(blocks_map: dict, root_id: str, indent: int = 0) -> str:
    """
    Convert a Notion recordMap block dict back to approximate Markdown.
    Resolves mentions to {{type:uuid}} tokens. Used by --dump.
    """
    lines = []
    root = blocks_map.get(root_id, {}).get("value", {})
    children = root.get("content", [])

    for child_id in children:
        block = blocks_map.get(child_id, {}).get("value", {})
        if not block:
            continue

        btype = block.get("type", "text")
        props = block.get("properties", {})
        title_rt = props.get("title", [])
        text = _rich_text_to_markdown(title_rt)

        prefix = "  " * indent
        if btype == "header":
            lines.append(f"{prefix}# {text}")
        elif btype == "sub_header":
            lines.append(f"{prefix}## {text}")
        elif btype == "sub_sub_header":
            lines.append(f"{prefix}### {text}")
        elif btype == "bulleted_list":
            lines.append(f"{prefix}- {text}")
        elif btype == "numbered_list":
            lines.append(f"{prefix}1. {text}")
        elif btype == "code":
            lang = _rich_text_to_markdown(props.get("language", [[""]]))
            lines.append(f"{prefix}```{lang}")
            lines.append(text)
            lines.append(f"{prefix}```")
        elif btype == "divider":
            lines.append(f"{prefix}---")
        elif btype == "callout":
            fmt = block.get("format", {})
            icon = fmt.get("page_icon", "")
            lines.append(f"{prefix}> {icon} {text}".strip())
        else:
            lines.append(f"{prefix}{text}")

        # Recurse into children
        if block.get("content"):
            lines.append(blocks_to_markdown(blocks_map, child_id, indent + 1))

    return "\n".join(lines)
