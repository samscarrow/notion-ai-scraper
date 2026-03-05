"""
block_builder.py — Markdown ↔ compact IR ↔ Notion block conversion.

The canonical in-memory representation is a sparse semantic IR. Notion-shaped
dicts only exist at the emit boundary for saveTransactionsFanout payloads.
"""

from __future__ import annotations

import re
from typing import Any

# Mention type codes used by Notion rich text annotations.
# Single-letter code in annotation → human-readable type name.
MENTION_TYPES = {"p": "page", "u": "user", "d": "date", "a": "agent", "s": "space"}
MENTION_CODES = {v: k for k, v in MENTION_TYPES.items()}

MARK_TO_ANNOTATION = {"bold": "b", "italic": "i", "code": "c"}
ANNOTATION_TO_MARK = {v: k for k, v in MARK_TO_ANNOTATION.items()}
MARK_ORDER = ("bold", "italic", "code")

_MENTION_TOKEN_RE = re.compile(r'(\{\{\w+:[0-9a-f-]+\}\})')
_MENTION_PARSE_RE = re.compile(r'^\{\{(\w+):([0-9a-f-]+)\}\}$')
_FMT_RE = re.compile(r'\*\*(.+?)\*\*|\*(.+?)\*|`(.+?)`|([^*`]+)')

IRBlock = dict[str, Any]
IRSpan = dict[str, Any]


def _text_span(text: str, *marks: str) -> IRSpan:
    span: IRSpan = {"type": "text", "text": text}
    ordered = [mark for mark in MARK_ORDER if mark in marks]
    if ordered:
        span["marks"] = ordered
    return span


def _mention_span(kind: str, id_: str) -> IRSpan:
    return {"type": "mention", "kind": kind, "id": id_}


def _block(type_: str, **extra: Any) -> IRBlock:
    block: IRBlock = {"type": type_}
    block.update(extra)
    return block


def _markdown_to_spans(text: str) -> list[IRSpan]:
    """Convert inline markdown to compact spans."""
    spans: list[IRSpan] = []
    parts = _MENTION_TOKEN_RE.split(text)
    for part in parts:
        if not part:
            continue
        mention = _MENTION_PARSE_RE.match(part)
        if mention:
            spans.append(_mention_span(mention.group(1), mention.group(2)))
            continue

        for match in _FMT_RE.finditer(part):
            bold_text, italic_text, code_text, plain = match.groups()
            if bold_text:
                spans.append(_text_span(bold_text, "bold"))
            elif italic_text:
                spans.append(_text_span(italic_text, "italic"))
            elif code_text:
                spans.append(_text_span(code_text, "code"))
            elif plain:
                spans.append(_text_span(plain))

    return spans if spans else [_text_span(text)]


def _notion_rich_text_to_spans(segments: list[list]) -> list[IRSpan]:
    """Convert Notion rich_text segments into compact spans."""
    spans: list[IRSpan] = []
    for seg in segments:
        text = seg[0] if seg else ""
        annotations = seg[1] if len(seg) > 1 else []
        if text == "\u2023":
            for annotation in annotations:
                if isinstance(annotation, list) and len(annotation) >= 2:
                    spans.append(_mention_span(MENTION_TYPES.get(annotation[0], annotation[0]), annotation[1]))
                    break
            else:
                spans.append(_text_span(text))
            continue

        marks = [
            ANNOTATION_TO_MARK[annotation[0]]
            for annotation in annotations
            if isinstance(annotation, list) and annotation and annotation[0] in ANNOTATION_TO_MARK
        ]
        spans.append(_text_span(text, *marks))

    return spans


def _spans_to_notion_rich_text(spans: list[IRSpan]) -> list[list]:
    rich_text: list[list] = []
    for span in spans:
        if span["type"] == "mention":
            rich_text.append(["\u2023", [[MENTION_CODES.get(span["kind"], span["kind"]), span["id"]]]])
            continue

        chunk = [span["text"]]
        marks = [[MARK_TO_ANNOTATION[mark]] for mark in span.get("marks", []) if mark in MARK_TO_ANNOTATION]
        if marks:
            chunk.append(marks)
        rich_text.append(chunk)
    return rich_text if rich_text else [[""]]


def _spans_to_markdown(spans: list[IRSpan]) -> str:
    parts: list[str] = []
    for span in spans:
        if span["type"] == "mention":
            parts.append(f"{{{{{span['kind']}:{span['id']}}}}}")
            continue

        out = span["text"]
        marks = set(span.get("marks", []))
        if "code" in marks:
            out = f"`{out}`"
        if "italic" in marks:
            out = f"*{out}*"
        if "bold" in marks:
            out = f"**{out}**"
        parts.append(out)
    return "".join(parts)


def _ir_block_to_notion(block: IRBlock) -> dict[str, Any]:
    type_map = {
        "paragraph": "text",
        "heading": {1: "header", 2: "sub_header", 3: "sub_sub_header"},
        "list_item": {"bulleted": "bulleted_list", "numbered": "numbered_list"},
        "callout": "callout",
        "code": "code",
        "divider": "divider",
    }

    if block["type"] == "divider":
        notion_block: dict[str, Any] = {"type": "divider", "properties": {}}
    elif block["type"] == "code":
        notion_block = {
            "type": "code",
            "properties": {
                "title": [[block["text"]]],
                "language": [[block.get("language", "plain text")]],
            },
        }
    elif block["type"] == "heading":
        notion_type = type_map["heading"].get(block.get("level", 3), "sub_sub_header")
        notion_block = {
            "type": notion_type,
            "properties": {"title": _spans_to_notion_rich_text(block["spans"])},
        }
    elif block["type"] == "list_item":
        notion_type = type_map["list_item"].get(block.get("list_kind", "bulleted"), "bulleted_list")
        notion_block = {
            "type": notion_type,
            "properties": {"title": _spans_to_notion_rich_text(block["spans"])},
        }
    elif block["type"] == "callout":
        notion_block = {
            "type": "callout",
            "properties": {"title": _spans_to_notion_rich_text(block["spans"])},
            "format": {"page_icon": block.get("icon", "📌")},
        }
    else:
        notion_block = {
            "type": "text",
            "properties": {"title": _spans_to_notion_rich_text(block["spans"])},
        }

    children = block.get("children") or []
    if children:
        notion_block["children"] = [_ir_block_to_notion(child) for child in children]
    return notion_block


def ir_to_notion_blocks(blocks: list[IRBlock]) -> list[dict[str, Any]]:
    """Convert compact IR blocks into Notion-shaped block dicts."""
    return [_ir_block_to_notion(block) for block in blocks]


def _notion_block_to_ir(block: dict[str, Any], blocks_map: dict) -> IRBlock:
    block_type = block.get("type", "text")
    props = block.get("properties", {})
    title_spans = _notion_rich_text_to_spans(props.get("title", []))

    if block_type == "header":
        ir_block = _block("heading", level=1, spans=title_spans)
    elif block_type == "sub_header":
        ir_block = _block("heading", level=2, spans=title_spans)
    elif block_type == "sub_sub_header":
        ir_block = _block("heading", level=3, spans=title_spans)
    elif block_type == "bulleted_list":
        ir_block = _block("list_item", list_kind="bulleted", spans=title_spans)
    elif block_type == "numbered_list":
        ir_block = _block("list_item", list_kind="numbered", spans=title_spans)
    elif block_type == "callout":
        ir_block = _block(
            "callout",
            spans=title_spans,
            icon=block.get("format", {}).get("page_icon", "📌"),
        )
    elif block_type == "code":
        language = props.get("language", [["plain text"]])
        lang = language[0][0] if language and language[0] else "plain text"
        ir_block = _block(
            "code",
            text=props.get("title", [[""]])[0][0] if props.get("title") else "",
            language=lang,
        )
    elif block_type == "divider":
        ir_block = _block("divider")
    else:
        ir_block = _block("paragraph", spans=title_spans)

    child_ids = block.get("content", [])
    if child_ids:
        children = []
        for child_id in child_ids:
            child = blocks_map.get(child_id, {}).get("value", {})
            if child and child.get("alive", True):
                children.append(_notion_block_to_ir(child, blocks_map))
        if children:
            ir_block["children"] = children
    return ir_block


def notion_blocks_to_ir(blocks_map: dict, root_id: str) -> list[IRBlock]:
    """Convert a Notion recordMap subtree into compact IR blocks."""
    root = blocks_map.get(root_id, {}).get("value", {})
    blocks: list[IRBlock] = []
    for child_id in root.get("content", []):
        block = blocks_map.get(child_id, {}).get("value", {})
        if block and block.get("alive", True):
            blocks.append(_notion_block_to_ir(block, blocks_map))
    return blocks


def _parse_line_to_ir(stripped: str) -> IRBlock:
    """Parse a single stripped line into a compact IR block."""
    h3 = re.match(r'^### (.+)', stripped)
    h2 = re.match(r'^## (.+)', stripped)
    h1 = re.match(r'^# (.+)', stripped)
    if h3:
        return _block("heading", level=3, spans=_markdown_to_spans(h3.group(1)))
    if h2:
        return _block("heading", level=2, spans=_markdown_to_spans(h2.group(1)))
    if h1:
        return _block("heading", level=1, spans=_markdown_to_spans(h1.group(1)))

    if re.match(r'^[-*_]{3,}$', stripped):
        return _block("divider")

    if stripped.startswith(">"):
        text = stripped.lstrip(">").strip()
        emoji_match = re.match(r'^([\U00010000-\U0010ffff]|[\u2600-\u27BF])\s*(.*)', text)
        if emoji_match:
            return _block("callout", icon=emoji_match.group(1), spans=_markdown_to_spans(emoji_match.group(2)))
        return _block("callout", icon="📌", spans=_markdown_to_spans(text))

    if re.match(r'^[-*+] ', stripped):
        text = re.sub(r'^[-*+] ', '', stripped)
        return _block("list_item", list_kind="bulleted", spans=_markdown_to_spans(text))

    if re.match(r'^\d+\. ', stripped):
        text = re.sub(r'^\d+\. ', '', stripped)
        return _block("list_item", list_kind="numbered", spans=_markdown_to_spans(text))

    return _block("paragraph", spans=_markdown_to_spans(stripped))


def markdown_to_ir(md: str) -> list[IRBlock]:
    """
    Parse markdown into compact IR blocks.

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
    blocks: list[IRBlock] = []
    lines = md.splitlines()
    i = 0

    while i < len(lines):
        line = lines[i]

        fence = re.match(r'^(\s*)```(\w*)$', line)
        if fence:
            lang = fence.group(2) or "plain text"
            code_lines = []
            i += 1
            while i < len(lines) and not lines[i].strip().startswith("```"):
                code_lines.append(lines[i])
                i += 1
            block = _block("code", text="\n".join(code_lines), language=lang)
            indent = (len(line) - len(line.lstrip())) >> 1
            _append_block(blocks, indent, block)
            i += 1
            continue

        stripped = line.strip()
        if not stripped:
            i += 1
            continue

        indent = (len(line) - len(line.lstrip())) >> 1
        block = _parse_line_to_ir(stripped)
        _append_block(blocks, indent, block)
        i += 1

    return blocks


def _append_block(blocks: list[IRBlock], indent: int, block: IRBlock) -> None:
    if indent > 0 and blocks:
        parent = blocks[-1]
        for _depth in range(1, indent):
            children = parent.get("children")
            if children:
                parent = children[-1]
            else:
                break
        parent.setdefault("children", []).append(block)
        return
    blocks.append(block)


def markdown_to_blocks(md: str) -> list[dict[str, Any]]:
    """Parse markdown into Notion block dicts via the compact IR."""
    return ir_to_notion_blocks(markdown_to_ir(md))


def _ir_block_to_markdown_lines(block: IRBlock, indent: int = 0) -> list[str]:
    prefix = "  " * indent
    lines: list[str] = []

    if block["type"] == "heading":
        hashes = "#" * min(max(block.get("level", 3), 1), 3)
        lines.append(f"{prefix}{hashes} {_spans_to_markdown(block['spans'])}")
    elif block["type"] == "list_item":
        marker = "-" if block.get("list_kind", "bulleted") == "bulleted" else "1."
        lines.append(f"{prefix}{marker} {_spans_to_markdown(block['spans'])}")
    elif block["type"] == "callout":
        icon = block.get("icon", "📌")
        lines.append(f"{prefix}> {icon} {_spans_to_markdown(block['spans'])}".rstrip())
    elif block["type"] == "code":
        lines.append(f"{prefix}```{block.get('language', 'plain text')}")
        lines.append(block.get("text", ""))
        lines.append(f"{prefix}```")
    elif block["type"] == "divider":
        lines.append(f"{prefix}---")
    else:
        lines.append(f"{prefix}{_spans_to_markdown(block['spans'])}")

    for child in block.get("children", []):
        lines.extend(_ir_block_to_markdown_lines(child, indent + 1))
    return lines


def ir_to_markdown(blocks: list[IRBlock], indent: int = 0) -> str:
    """Convert compact IR blocks back to markdown."""
    lines: list[str] = []
    for block in blocks:
        lines.extend(_ir_block_to_markdown_lines(block, indent))
    return "\n".join(lines)


def blocks_to_markdown(blocks_map: dict, root_id: str, indent: int = 0) -> str:
    """Convert a Notion recordMap block dict back to markdown via the compact IR."""
    return ir_to_markdown(notion_blocks_to_ir(blocks_map, root_id), indent)
