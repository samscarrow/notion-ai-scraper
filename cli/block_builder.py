"""
block_builder.py — Markdown ↔ compact IR ↔ Notion block conversion.

The canonical in-memory representation is a typed Pydantic IR.  Notion-shaped
dicts only exist at the emit boundary for saveTransactionsFanout payloads.

IR models use ``model_dump(exclude_defaults=True)`` for sparse serialization —
only non-default fields appear in the output, matching the original dict-based
sparsity without manual bookkeeping.

Mark system (Portable Text pattern):
  - Decorators: simple string marks on spans ("bold", "italic", "code",
    "strikethrough", "underline").
  - Annotations: marks that carry data (links, colors). Stored as MarkDef
    objects on the block; spans reference them by key in their marks array.
"""

from __future__ import annotations

import re
from typing import Annotated, Any, Literal, Union

from pydantic import BaseModel, Field, TypeAdapter

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Mention type codes used by Notion rich text annotations.
MENTION_TYPES = {"p": "page", "u": "user", "d": "date", "a": "agent", "s": "space", "ds": "data_source"}
MENTION_CODES = {v: k for k, v in MENTION_TYPES.items()}

# Decorator marks: simple string marks with no associated data.
MARK_TO_ANNOTATION = {
    "bold": "b", "italic": "i", "code": "c",
    "strikethrough": "s", "underline": "_",
}
ANNOTATION_TO_MARK = {v: k for k, v in MARK_TO_ANNOTATION.items()}
MARK_ORDER = ("bold", "italic", "code", "strikethrough", "underline")
DECORATOR_NAMES = frozenset(MARK_ORDER)

_MENTION_TOKEN_RE = re.compile(r'(\{\{\w+:[0-9a-f-]+\}\})')
_MENTION_PARSE_RE = re.compile(r'^\{\{(\w+):([0-9a-f-]+)\}\}$')
# Inline markdown: **bold**, *italic*, `code`, ~~strikethrough~~, [text](url), plain
_FMT_RE = re.compile(
    r'\*\*(.+?)\*\*'         # group 1: bold
    r'|\*(.+?)\*'            # group 2: italic
    r'|`(.+?)`'              # group 3: inline code
    r'|~~(.+?)~~'            # group 4: strikethrough
    r'|\[([^\]]+)\]\(([^)]+)\)'  # group 5,6: link text, url
    r'|([^*`~\[]+)'         # group 7: plain text
)

# ---------------------------------------------------------------------------
# IR Span Models
# ---------------------------------------------------------------------------

MentionKind = Literal["page", "user", "date", "agent", "space", "data_source"]


class TextSpan(BaseModel, frozen=True):
    """Inline text segment with optional formatting marks.

    marks contains decorator names ("bold", "italic", etc.) and/or keys
    referencing MarkDef entries on the parent block.
    """
    type: Literal["text"] = "text"
    text: str
    marks: list[str] = Field(default_factory=list)


class MentionSpan(BaseModel, frozen=True):
    """Inline mention of a Notion entity."""
    type: Literal["mention"] = "mention"
    kind: MentionKind
    id: str


IRSpan = Annotated[Union[TextSpan, MentionSpan], Field(discriminator="type")]

# ---------------------------------------------------------------------------
# MarkDef Model (Portable Text pattern)
# ---------------------------------------------------------------------------


class MarkDef(BaseModel, frozen=True):
    """Annotation mark definition, hoisted to block level.

    Spans reference this by key in their marks array. Decorator marks
    (bold, italic, etc.) do NOT use MarkDef — they're plain strings.
    """
    key: str
    mark_type: str       # "link", "color"
    href: str = ""       # for mark_type == "link"
    value: str = ""      # for mark_type == "color"


# ---------------------------------------------------------------------------
# IR Block Models
# ---------------------------------------------------------------------------


class Paragraph(BaseModel):
    type: Literal["paragraph"] = "paragraph"
    spans: list[IRSpan]
    mark_defs: list[MarkDef] = Field(default_factory=list)
    children: list[IRBlock] = Field(default_factory=list)


class Heading(BaseModel):
    type: Literal["heading"] = "heading"
    level: Literal[1, 2, 3]
    spans: list[IRSpan]
    mark_defs: list[MarkDef] = Field(default_factory=list)
    children: list[IRBlock] = Field(default_factory=list)


class ListItem(BaseModel):
    type: Literal["list_item"] = "list_item"
    list_kind: Literal["bulleted", "numbered"]
    spans: list[IRSpan]
    mark_defs: list[MarkDef] = Field(default_factory=list)
    children: list[IRBlock] = Field(default_factory=list)


class Callout(BaseModel):
    type: Literal["callout"] = "callout"
    spans: list[IRSpan]
    icon: str = "📌"
    mark_defs: list[MarkDef] = Field(default_factory=list)
    children: list[IRBlock] = Field(default_factory=list)


class Quote(BaseModel):
    type: Literal["quote"] = "quote"
    spans: list[IRSpan]
    mark_defs: list[MarkDef] = Field(default_factory=list)
    children: list[IRBlock] = Field(default_factory=list)


class Toggle(BaseModel):
    type: Literal["toggle"] = "toggle"
    spans: list[IRSpan]
    mark_defs: list[MarkDef] = Field(default_factory=list)
    children: list[IRBlock] = Field(default_factory=list)


class Code(BaseModel):
    type: Literal["code"] = "code"
    text: str
    language: str = "plain text"
    children: list[IRBlock] = Field(default_factory=list)


class Divider(BaseModel):
    type: Literal["divider"] = "divider"
    children: list[IRBlock] = Field(default_factory=list)


class Unknown(BaseModel):
    """Lossless fallback for unrecognized Notion block types.

    Preserves the original Notion type string and raw properties/format
    so round-tripping through the IR doesn't silently discard data.
    """
    type: Literal["unknown"] = "unknown"
    notion_type: str
    spans: list[IRSpan] = Field(default_factory=list)
    mark_defs: list[MarkDef] = Field(default_factory=list)
    raw_properties: dict[str, Any] = Field(default_factory=dict)
    raw_format: dict[str, Any] = Field(default_factory=dict)
    children: list[IRBlock] = Field(default_factory=list)


IRBlock = Annotated[
    Union[Paragraph, Heading, ListItem, Callout, Quote, Toggle, Code, Divider, Unknown],
    Field(discriminator="type"),
]

# Rebuild forward refs now that IRBlock is defined.
Paragraph.model_rebuild()
Heading.model_rebuild()
ListItem.model_rebuild()
Callout.model_rebuild()
Quote.model_rebuild()
Toggle.model_rebuild()
Code.model_rebuild()
Divider.model_rebuild()
Unknown.model_rebuild()

_IRBlockAdapter: TypeAdapter[IRBlock] = TypeAdapter(IRBlock)

# Block types that carry spans and mark_defs.
_SPAN_BLOCK_TYPES = (Paragraph, Heading, ListItem, Callout, Quote, Toggle, Unknown)

# ---------------------------------------------------------------------------
# Span constructors
# ---------------------------------------------------------------------------


def _text_span(text: str, *marks: str) -> TextSpan:
    ordered = [m for m in MARK_ORDER if m in marks]
    # Append any non-decorator marks (markDef keys) in original order.
    ordered.extend(m for m in marks if m not in DECORATOR_NAMES)
    return TextSpan(text=text, marks=ordered)


def _mention_span(kind: str, id_: str) -> MentionSpan:
    return MentionSpan(kind=kind, id=id_)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Span normalization (ProseMirror pattern)
# ---------------------------------------------------------------------------


def normalize_spans(spans: list[IRSpan]) -> list[IRSpan]:
    """Normalize a span list: merge adjacent same-mark text spans, drop empties.

    This produces a canonical form where equality checks and diffing are
    reliable — stolen from ProseMirror's inline normalization rules.
    """
    if not spans:
        return [TextSpan(text="")]
    result: list[IRSpan] = []
    for span in spans:
        # Drop empty text spans.
        if isinstance(span, TextSpan) and not span.text:
            continue
        # Merge adjacent TextSpans with identical marks.
        if (
            result
            and isinstance(span, TextSpan)
            and isinstance(result[-1], TextSpan)
            and span.marks == result[-1].marks
        ):
            result[-1] = TextSpan(text=result[-1].text + span.text, marks=list(span.marks))
            continue
        result.append(span)
    return result if result else [TextSpan(text="")]


# ---------------------------------------------------------------------------
# Serialization helpers
# ---------------------------------------------------------------------------


def _dump_span(span: IRSpan) -> dict[str, Any]:
    """Sparse dict for a span (exclude defaults)."""
    return span.model_dump(exclude_defaults=True)


def _dump_block(block: IRBlock) -> dict[str, Any]:
    """Sparse dict for a block (exclude defaults, recursive)."""
    return block.model_dump(exclude_defaults=True)


def _dump_blocks(blocks: list[IRBlock]) -> list[dict[str, Any]]:
    return [_dump_block(b) for b in blocks]


# ---------------------------------------------------------------------------
# Markdown → IR spans
# ---------------------------------------------------------------------------


def _markdown_to_spans(text: str) -> tuple[list[IRSpan], list[MarkDef]]:
    """Convert inline markdown to typed spans + any markDefs for links."""
    spans: list[IRSpan] = []
    mark_defs: list[MarkDef] = []
    link_counter = 0
    parts = _MENTION_TOKEN_RE.split(text)
    for part in parts:
        if not part:
            continue
        mention = _MENTION_PARSE_RE.match(part)
        if mention:
            spans.append(_mention_span(mention.group(1), mention.group(2)))
            continue

        for match in _FMT_RE.finditer(part):
            bold, italic, code, strike, link_text, link_url, plain = match.groups()
            if bold:
                spans.append(_text_span(bold, "bold"))
            elif italic:
                spans.append(_text_span(italic, "italic"))
            elif code:
                spans.append(_text_span(code, "code"))
            elif strike:
                spans.append(_text_span(strike, "strikethrough"))
            elif link_text and link_url:
                key = f"_link-{link_counter}"
                link_counter += 1
                mark_defs.append(MarkDef(key=key, mark_type="link", href=link_url))
                spans.append(_text_span(link_text, key))
            elif plain:
                spans.append(_text_span(plain))

    spans = normalize_spans(spans) if spans else [_text_span(text)]
    return spans, mark_defs


# ---------------------------------------------------------------------------
# Notion rich text ↔ IR spans
# ---------------------------------------------------------------------------


def _notion_rich_text_to_spans(segments: list[list]) -> tuple[list[IRSpan], list[MarkDef]]:
    """Convert Notion rich_text segments into typed spans + markDefs.

    Notion annotations that carry data (links, colors) are hoisted into
    MarkDef objects; spans reference them by key.
    """
    spans: list[IRSpan] = []
    mark_defs: list[MarkDef] = []
    link_counter = 0
    color_counter = 0

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

        marks: list[str] = []
        for annotation in annotations:
            if not isinstance(annotation, list) or not annotation:
                continue
            code = annotation[0]
            if code in ANNOTATION_TO_MARK:
                marks.append(ANNOTATION_TO_MARK[code])
            elif code == "a" and len(annotation) >= 2:
                # Link annotation → hoist to markDef.
                key = f"_link-{link_counter}"
                link_counter += 1
                mark_defs.append(MarkDef(key=key, mark_type="link", href=annotation[1]))
                marks.append(key)
            elif code == "h" and len(annotation) >= 2:
                # Color/highlight annotation → hoist to markDef.
                key = f"_color-{color_counter}"
                color_counter += 1
                mark_defs.append(MarkDef(key=key, mark_type="color", value=annotation[1]))
                marks.append(key)

        spans.append(_text_span(text, *marks))

    return normalize_spans(spans), mark_defs


def _spans_to_notion_rich_text(
    spans: list[IRSpan],
    mark_defs: list[MarkDef] | None = None,
) -> list[list]:
    """Convert IR spans back to Notion rich_text segments.

    markDef keys in span.marks are resolved back to Notion annotations
    using the block's mark_defs list.
    """
    defs_by_key = {md.key: md for md in (mark_defs or [])}
    rich_text: list[list] = []
    for span in spans:
        if isinstance(span, MentionSpan):
            rich_text.append(["\u2023", [[MENTION_CODES.get(span.kind, span.kind), span.id]]])
            continue

        chunk: list[Any] = [span.text]
        notion_marks: list[list] = []
        for mark in span.marks:
            if mark in MARK_TO_ANNOTATION:
                notion_marks.append([MARK_TO_ANNOTATION[mark]])
            elif mark in defs_by_key:
                md = defs_by_key[mark]
                if md.mark_type == "link":
                    notion_marks.append(["a", md.href])
                elif md.mark_type == "color":
                    notion_marks.append(["h", md.value])
        if notion_marks:
            chunk.append(notion_marks)
        rich_text.append(chunk)
    return rich_text if rich_text else [[""]]


def _spans_to_markdown(
    spans: list[IRSpan],
    mark_defs: list[MarkDef] | None = None,
) -> str:
    """Convert IR spans back to markdown text.

    Links are emitted as [text](url). Colors have no markdown
    representation and are silently dropped.
    """
    defs_by_key = {md.key: md for md in (mark_defs or [])}
    parts: list[str] = []
    for span in spans:
        if isinstance(span, MentionSpan):
            parts.append(f"{{{{{span.kind}:{span.id}}}}}")
            continue

        out = span.text
        mark_set = set(span.marks)

        # Apply decorator wrapping (innermost to outermost).
        if "code" in mark_set:
            out = f"`{out}`"
        if "strikethrough" in mark_set:
            out = f"~~{out}~~"
        if "italic" in mark_set:
            out = f"*{out}*"
        if "bold" in mark_set:
            out = f"**{out}**"

        # Wrap in link if a link markDef is referenced.
        link_md = next(
            (defs_by_key[m] for m in span.marks if m in defs_by_key and defs_by_key[m].mark_type == "link"),
            None,
        )
        if link_md:
            out = f"[{out}]({link_md.href})"

        parts.append(out)
    return "".join(parts)


# ---------------------------------------------------------------------------
# Block mark_defs accessor
# ---------------------------------------------------------------------------


def _block_mark_defs(block: IRBlock) -> list[MarkDef]:
    """Get mark_defs from a block, returning [] for types that don't have them."""
    if isinstance(block, _SPAN_BLOCK_TYPES):
        return block.mark_defs
    return []


# ---------------------------------------------------------------------------
# IR → Notion blocks
# ---------------------------------------------------------------------------


def _ir_block_to_notion(block: IRBlock) -> dict[str, Any]:
    if isinstance(block, Divider):
        notion_block: dict[str, Any] = {"type": "divider", "properties": {}}
    elif isinstance(block, Code):
        notion_block = {
            "type": "code",
            "properties": {
                "title": [[block.text]],
                "language": [[block.language]],
            },
        }
    elif isinstance(block, Heading):
        level_map = {1: "header", 2: "sub_header", 3: "sub_sub_header"}
        notion_block = {
            "type": level_map.get(block.level, "sub_sub_header"),
            "properties": {"title": _spans_to_notion_rich_text(block.spans, block.mark_defs)},
        }
    elif isinstance(block, ListItem):
        kind_map = {"bulleted": "bulleted_list", "numbered": "numbered_list"}
        notion_block = {
            "type": kind_map.get(block.list_kind, "bulleted_list"),
            "properties": {"title": _spans_to_notion_rich_text(block.spans, block.mark_defs)},
        }
    elif isinstance(block, Callout):
        notion_block = {
            "type": "callout",
            "properties": {"title": _spans_to_notion_rich_text(block.spans, block.mark_defs)},
            "format": {"page_icon": block.icon},
        }
    elif isinstance(block, Quote):
        notion_block = {
            "type": "quote",
            "properties": {"title": _spans_to_notion_rich_text(block.spans, block.mark_defs)},
        }
    elif isinstance(block, Toggle):
        notion_block = {
            "type": "toggle_list",
            "properties": {"title": _spans_to_notion_rich_text(block.spans, block.mark_defs)},
        }
    elif isinstance(block, Unknown):
        # Reconstruct the original Notion block as faithfully as possible.
        notion_block = {"type": block.notion_type}
        props = dict(block.raw_properties)
        if block.spans:
            props["title"] = _spans_to_notion_rich_text(block.spans, block.mark_defs)
        notion_block["properties"] = props
        if block.raw_format:
            notion_block["format"] = dict(block.raw_format)
    else:
        # Paragraph (or fallback)
        notion_block = {
            "type": "text",
            "properties": {"title": _spans_to_notion_rich_text(block.spans, block.mark_defs)},
        }

    if block.children:
        notion_block["children"] = [_ir_block_to_notion(child) for child in block.children]
    return notion_block


def ir_to_notion_blocks(blocks: list[IRBlock]) -> list[dict[str, Any]]:
    """Convert typed IR blocks into Notion-shaped block dicts."""
    return [_ir_block_to_notion(block) for block in blocks]


# ---------------------------------------------------------------------------
# Notion blocks → IR
# ---------------------------------------------------------------------------


def _notion_block_to_ir(block: dict[str, Any], blocks_map: dict) -> IRBlock:
    block_type = block.get("type", "text")
    props = block.get("properties", {})
    title_spans, mark_defs = _notion_rich_text_to_spans(props.get("title", []))

    ir_block: IRBlock
    if block_type == "header":
        ir_block = Heading(level=1, spans=title_spans, mark_defs=mark_defs)
    elif block_type == "sub_header":
        ir_block = Heading(level=2, spans=title_spans, mark_defs=mark_defs)
    elif block_type == "sub_sub_header":
        ir_block = Heading(level=3, spans=title_spans, mark_defs=mark_defs)
    elif block_type == "bulleted_list":
        ir_block = ListItem(list_kind="bulleted", spans=title_spans, mark_defs=mark_defs)
    elif block_type == "numbered_list":
        ir_block = ListItem(list_kind="numbered", spans=title_spans, mark_defs=mark_defs)
    elif block_type == "callout":
        ir_block = Callout(
            spans=title_spans,
            mark_defs=mark_defs,
            icon=block.get("format", {}).get("page_icon", "📌"),
        )
    elif block_type == "code":
        language = props.get("language", [["plain text"]])
        lang = language[0][0] if language and language[0] else "plain text"
        ir_block = Code(
            text=props.get("title", [[""]])[0][0] if props.get("title") else "",
            language=lang,
        )
    elif block_type == "quote":
        ir_block = Quote(spans=title_spans, mark_defs=mark_defs)
    elif block_type == "toggle_list":
        ir_block = Toggle(spans=title_spans, mark_defs=mark_defs)
    elif block_type == "divider":
        ir_block = Divider()
    elif block_type in ("text", ""):
        ir_block = Paragraph(spans=title_spans, mark_defs=mark_defs)
    else:
        # Unknown block type — preserve raw data for lossless round-trip.
        raw_props = {k: v for k, v in props.items() if k != "title"}
        ir_block = Unknown(
            notion_type=block_type,
            spans=title_spans,
            mark_defs=mark_defs,
            raw_properties=raw_props,
            raw_format=block.get("format", {}),
        )

    child_ids = block.get("content", [])
    if child_ids:
        children = []
        for child_id in child_ids:
            child = blocks_map.get(child_id, {}).get("value", {})
            if child and child.get("alive", True):
                children.append(_notion_block_to_ir(child, blocks_map))
        if children:
            ir_block.children = children
    return ir_block


def notion_blocks_to_ir(blocks_map: dict, root_id: str) -> list[IRBlock]:
    """Convert a Notion recordMap subtree into typed IR blocks."""
    root = blocks_map.get(root_id, {}).get("value", {})
    blocks: list[IRBlock] = []
    for child_id in root.get("content", []):
        block = blocks_map.get(child_id, {}).get("value", {})
        if block and block.get("alive", True):
            blocks.append(_notion_block_to_ir(block, blocks_map))
    return blocks


# ---------------------------------------------------------------------------
# Markdown → IR blocks
# ---------------------------------------------------------------------------


def _parse_line_to_ir(stripped: str) -> IRBlock:
    """Parse a single stripped line into a typed IR block."""
    h3 = re.match(r'^### (.+)', stripped)
    h2 = re.match(r'^## (.+)', stripped)
    h1 = re.match(r'^# (.+)', stripped)
    if h3:
        spans, md = _markdown_to_spans(h3.group(1))
        return Heading(level=3, spans=spans, mark_defs=md)
    if h2:
        spans, md = _markdown_to_spans(h2.group(1))
        return Heading(level=2, spans=spans, mark_defs=md)
    if h1:
        spans, md = _markdown_to_spans(h1.group(1))
        return Heading(level=1, spans=spans, mark_defs=md)

    if re.match(r'^[-*_]{3,}$', stripped):
        return Divider()

    if stripped.startswith(">"):
        text = stripped.lstrip(">").strip()
        emoji_match = re.match(r'^([\U00010000-\U0010ffff]|[\u2600-\u27BF])\s*(.*)', text)
        if emoji_match:
            spans, md = _markdown_to_spans(emoji_match.group(2))
            return Callout(icon=emoji_match.group(1), spans=spans, mark_defs=md)
        spans, md = _markdown_to_spans(text)
        return Callout(icon="📌", spans=spans, mark_defs=md)

    if re.match(r'^[-*+] ', stripped):
        text = re.sub(r'^[-*+] ', '', stripped)
        spans, md = _markdown_to_spans(text)
        return ListItem(list_kind="bulleted", spans=spans, mark_defs=md)

    if re.match(r'^\d+\. ', stripped):
        text = re.sub(r'^\d+\. ', '', stripped)
        spans, md = _markdown_to_spans(text)
        return ListItem(list_kind="numbered", spans=spans, mark_defs=md)

    spans, md = _markdown_to_spans(stripped)
    return Paragraph(spans=spans, mark_defs=md)


def markdown_to_ir(md: str) -> list[IRBlock]:
    """
    Parse markdown into typed IR blocks.

    Supported syntax:
      # H1  ## H2  ### H3
      - bullet item
      1. numbered item
      > blockquote (rendered as callout)
      --- (divider)
      ```lang ... ``` (fenced code block)
      **bold**  *italic*  `inline code`  ~~strikethrough~~
      [link text](url)
      {{page:uuid}}  {{user:uuid}}  {{agent:uuid}} (mentions)
      Plain paragraph text
    """
    blocks: list[IRBlock] = []
    lines = md.splitlines()
    i = 0

    while i < len(lines):
        line = lines[i]

        fence = re.match(r'^(\s*)```([\w ]*?)$', line)
        if fence:
            lang = fence.group(2).strip() or "plain text"
            code_lines = []
            i += 1
            while i < len(lines) and not lines[i].strip().startswith("```"):
                code_lines.append(lines[i])
                i += 1
            block = Code(text="\n".join(code_lines), language=lang)
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
            if parent.children:
                parent = parent.children[-1]
            else:
                break
        parent.children.append(block)
        return
    blocks.append(block)


# ---------------------------------------------------------------------------
# IR → Markdown
# ---------------------------------------------------------------------------


def _ir_block_to_markdown_lines(block: IRBlock, indent: int = 0) -> list[str]:
    prefix = "  " * indent
    lines: list[str] = []
    md_list = _block_mark_defs(block)

    if isinstance(block, Heading):
        hashes = "#" * min(max(block.level, 1), 3)
        lines.append(f"{prefix}{hashes} {_spans_to_markdown(block.spans, md_list)}")
    elif isinstance(block, ListItem):
        marker = "-" if block.list_kind == "bulleted" else "1."
        lines.append(f"{prefix}{marker} {_spans_to_markdown(block.spans, md_list)}")
    elif isinstance(block, Callout):
        lines.append(f"{prefix}> {block.icon} {_spans_to_markdown(block.spans, md_list)}".rstrip())
    elif isinstance(block, Quote):
        lines.append(f"{prefix}> {_spans_to_markdown(block.spans, md_list)}".rstrip())
    elif isinstance(block, Toggle):
        lines.append(f"{prefix}<details>")
        lines.append(f"{prefix}<summary>{_spans_to_markdown(block.spans, md_list)}</summary>")
    elif isinstance(block, Code):
        lines.append(f"{prefix}```{block.language}")
        lines.append(block.text)
        lines.append(f"{prefix}```")
    elif isinstance(block, Divider):
        lines.append(f"{prefix}---")
    elif isinstance(block, Unknown):
        # Best-effort: render as paragraph with a type comment.
        text = _spans_to_markdown(block.spans, md_list) if block.spans else ""
        lines.append(f"{prefix}<!-- notion:{block.notion_type} -->{text}")
    else:
        lines.append(f"{prefix}{_spans_to_markdown(block.spans, md_list)}")

    for child in block.children:
        lines.extend(_ir_block_to_markdown_lines(child, indent + 1))
    if isinstance(block, Toggle):
        lines.append(f"{prefix}</details>")
    return lines


def ir_to_markdown(blocks: list[IRBlock], indent: int = 0) -> str:
    """Convert typed IR blocks back to markdown."""
    lines: list[str] = []
    for block in blocks:
        lines.extend(_ir_block_to_markdown_lines(block, indent))
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Legacy convenience wrappers
# ---------------------------------------------------------------------------


def markdown_to_blocks(md: str) -> list[dict[str, Any]]:
    """Parse markdown into Notion block dicts via the typed IR."""
    return ir_to_notion_blocks(markdown_to_ir(md))


def blocks_to_markdown(blocks_map: dict, root_id: str, indent: int = 0) -> str:
    """Convert a Notion recordMap block dict back to markdown via the typed IR."""
    return ir_to_markdown(notion_blocks_to_ir(blocks_map, root_id), indent)
