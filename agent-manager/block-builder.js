// block-builder.js — Markdown ↔ compact IR ↔ Notion block conversion.
// Used in the extension page to keep Notion-shaped payloads at the edge only.

// Mention type codes used by Notion rich text annotations.
const MENTION_TYPES = { p: 'page', u: 'user', d: 'date', a: 'agent', s: 'space' };
const MENTION_CODES = Object.fromEntries(Object.entries(MENTION_TYPES).map(([k, v]) => [v, k]));

const MARK_TO_ANNOTATION = { bold: 'b', italic: 'i', code: 'c' };
const ANNOTATION_TO_MARK = Object.fromEntries(Object.entries(MARK_TO_ANNOTATION).map(([k, v]) => [v, k]));
const MARK_ORDER = ['bold', 'italic', 'code'];

function textSpan(text, ...marks) {
  const span = { type: 'text', text };
  const ordered = MARK_ORDER.filter(mark => marks.includes(mark));
  if (ordered.length) span.marks = ordered;
  return span;
}

function mentionSpan(kind, id) {
  return { type: 'mention', kind, id };
}

function block(type, extra = {}) {
  return { type, ...extra };
}

function markdownToSpans(text) {
  const spans = [];
  const parts = text.split(/(\{\{\w+:[0-9a-f-]+\}\})/g);
  for (const part of parts) {
    if (!part) continue;
    const mentionMatch = /^\{\{(\w+):([0-9a-f-]+)\}\}$/.exec(part);
    if (mentionMatch) {
      spans.push(mentionSpan(mentionMatch[1], mentionMatch[2]));
      continue;
    }

    const fmtPattern = /\*\*(.+?)\*\*|\*(.+?)\*|`(.+?)`|([^*`]+)/g;
    let match;
    while ((match = fmtPattern.exec(part)) !== null) {
      const [, bold, italic, inlineCode, plain] = match;
      if (bold) spans.push(textSpan(bold, 'bold'));
      else if (italic) spans.push(textSpan(italic, 'italic'));
      else if (inlineCode) spans.push(textSpan(inlineCode, 'code'));
      else if (plain) spans.push(textSpan(plain));
    }
  }
  return spans.length ? spans : [textSpan(text)];
}

function notionRichTextToSpans(segments) {
  const spans = [];
  for (const seg of segments) {
    const text = seg?.[0] ?? '';
    const annotations = seg?.[1] ?? [];

    if (text === '\u2023') {
      const mention = annotations.find(annotation => Array.isArray(annotation) && annotation.length >= 2);
      if (mention) spans.push(mentionSpan(MENTION_TYPES[mention[0]] || mention[0], mention[1]));
      else spans.push(textSpan(text));
      continue;
    }

    const marks = annotations
      .filter(annotation => Array.isArray(annotation) && annotation.length >= 1)
      .map(annotation => ANNOTATION_TO_MARK[annotation[0]])
      .filter(Boolean);
    spans.push(textSpan(text, ...marks));
  }
  return spans;
}

function spansToRichText(spans) {
  const richText = [];
  for (const span of spans) {
    if (span.type === 'mention') {
      richText.push(['\u2023', [[MENTION_CODES[span.kind] || span.kind, span.id]]]);
      continue;
    }

    const chunk = [span.text];
    const marks = (span.marks || [])
      .filter(mark => MARK_TO_ANNOTATION[mark])
      .map(mark => [MARK_TO_ANNOTATION[mark]]);
    if (marks.length) chunk.push(marks);
    richText.push(chunk);
  }
  return richText.length ? richText : [['']];
}

function spansToMarkdown(spans) {
  return spans.map(span => {
    if (span.type === 'mention') return `{{${span.kind}:${span.id}}}`;

    const marks = new Set(span.marks || []);
    let out = span.text;
    if (marks.has('code')) out = `\`${out}\``;
    if (marks.has('italic')) out = `*${out}*`;
    if (marks.has('bold')) out = `**${out}**`;
    return out;
  }).join('');
}

function irBlockToNotion(blockNode) {
  let notionBlock;

  if (blockNode.type === 'divider') {
    notionBlock = { type: 'divider', properties: {} };
  } else if (blockNode.type === 'code') {
    notionBlock = {
      type: 'code',
      properties: {
        title: [[blockNode.text]],
        language: [[blockNode.language || 'plain text']],
      },
    };
  } else if (blockNode.type === 'heading') {
    const notionType = { 1: 'header', 2: 'sub_header', 3: 'sub_sub_header' }[blockNode.level] || 'sub_sub_header';
    notionBlock = { type: notionType, properties: { title: spansToRichText(blockNode.spans) } };
  } else if (blockNode.type === 'list_item') {
    const notionType = blockNode.list_kind === 'numbered' ? 'numbered_list' : 'bulleted_list';
    notionBlock = { type: notionType, properties: { title: spansToRichText(blockNode.spans) } };
  } else if (blockNode.type === 'callout') {
    notionBlock = {
      type: 'callout',
      properties: { title: spansToRichText(blockNode.spans) },
      format: { page_icon: blockNode.icon || '📌' },
    };
  } else {
    notionBlock = { type: 'text', properties: { title: spansToRichText(blockNode.spans) } };
  }

  if (blockNode.children?.length) {
    notionBlock.children = blockNode.children.map(irBlockToNotion);
  }
  return notionBlock;
}

function notionBlockToIr(blockNode, blocksMap) {
  const type = blockNode.type || 'text';
  const properties = blockNode.properties || {};
  const titleSpans = notionRichTextToSpans(properties.title || []);

  let irBlock;
  if (type === 'header') {
    irBlock = block('heading', { level: 1, spans: titleSpans });
  } else if (type === 'sub_header') {
    irBlock = block('heading', { level: 2, spans: titleSpans });
  } else if (type === 'sub_sub_header') {
    irBlock = block('heading', { level: 3, spans: titleSpans });
  } else if (type === 'bulleted_list') {
    irBlock = block('list_item', { list_kind: 'bulleted', spans: titleSpans });
  } else if (type === 'numbered_list') {
    irBlock = block('list_item', { list_kind: 'numbered', spans: titleSpans });
  } else if (type === 'callout') {
    irBlock = block('callout', {
      spans: titleSpans,
      icon: blockNode.format?.page_icon || '📌',
    });
  } else if (type === 'code') {
    const language = properties.language?.[0]?.[0] || 'plain text';
    const text = properties.title?.[0]?.[0] || '';
    irBlock = block('code', { text, language });
  } else if (type === 'divider') {
    irBlock = block('divider');
  } else {
    irBlock = block('paragraph', { spans: titleSpans });
  }

  if (blockNode.content?.length) {
    const children = blockNode.content
      .map(id => blocksMap[id]?.value)
      .filter(child => child && child.alive !== false)
      .map(child => notionBlockToIr(child, blocksMap));
    if (children.length) irBlock.children = children;
  }
  return irBlock;
}

function parseLineToIr(line) {
  let match;
  if ((match = /^### (.+)/.exec(line))) return block('heading', { level: 3, spans: markdownToSpans(match[1]) });
  if ((match = /^## (.+)/.exec(line))) return block('heading', { level: 2, spans: markdownToSpans(match[1]) });
  if ((match = /^# (.+)/.exec(line))) return block('heading', { level: 1, spans: markdownToSpans(match[1]) });
  if (/^[-*_]{3,}$/.test(line)) return block('divider');
  if (line.startsWith('>')) {
    const text = line.replace(/^>\s*/, '');
    const icon = /^(\p{Emoji_Presentation}|\p{Extended_Pictographic})\s*(.*)/u.exec(text);
    if (icon) return block('callout', { icon: icon[1], spans: markdownToSpans(icon[2]) });
    return block('callout', { icon: '📌', spans: markdownToSpans(text) });
  }
  if (/^[-*+] /.test(line)) return block('list_item', { list_kind: 'bulleted', spans: markdownToSpans(line.replace(/^[-*+] /, '')) });
  if (/^\d+\. /.test(line)) return block('list_item', { list_kind: 'numbered', spans: markdownToSpans(line.replace(/^\d+\. /, '')) });
  return block('paragraph', { spans: markdownToSpans(line) });
}

function appendBlock(blocks, indent, blockNode) {
  if (indent > 0 && blocks.length > 0) {
    let parent = blocks[blocks.length - 1];
    for (let depth = 1; depth < indent; depth++) {
      if (parent.children?.length) parent = parent.children[parent.children.length - 1];
      else break;
    }
    if (!parent.children) parent.children = [];
    parent.children.push(blockNode);
    return;
  }
  blocks.push(blockNode);
}

function irBlockToMarkdownLines(blockNode, indent = 0) {
  const prefix = '  '.repeat(indent);
  const lines = [];

  if (blockNode.type === 'heading') {
    const hashes = '#'.repeat(Math.min(Math.max(blockNode.level || 3, 1), 3));
    lines.push(`${prefix}${hashes} ${spansToMarkdown(blockNode.spans)}`);
  } else if (blockNode.type === 'list_item') {
    const marker = blockNode.list_kind === 'numbered' ? '1.' : '-';
    lines.push(`${prefix}${marker} ${spansToMarkdown(blockNode.spans)}`);
  } else if (blockNode.type === 'callout') {
    lines.push(`${prefix}> ${blockNode.icon || '📌'} ${spansToMarkdown(blockNode.spans)}`.trimEnd());
  } else if (blockNode.type === 'code') {
    lines.push(`${prefix}\`\`\`${blockNode.language || 'plain text'}`);
    lines.push(blockNode.text || '');
    lines.push(`${prefix}\`\`\``);
  } else if (blockNode.type === 'divider') {
    lines.push(`${prefix}---`);
  } else {
    lines.push(`${prefix}${spansToMarkdown(blockNode.spans)}`);
  }

  for (const child of blockNode.children || []) {
    lines.push(...irBlockToMarkdownLines(child, indent + 1));
  }
  return lines;
}

export function markdownToIr(md) {
  const blocks = [];
  const lines = md.split('\n');
  let i = 0;

  while (i < lines.length) {
    const line = lines[i];
    const fenceMatch = /^(\s*)```(\w*)$/.exec(line);
    if (fenceMatch) {
      const lang = fenceMatch[2] || 'plain text';
      const codeLines = [];
      i++;
      while (i < lines.length && !lines[i].trim().startsWith('```')) {
        codeLines.push(lines[i]);
        i++;
      }
      appendBlock(blocks, (line.length - line.trimStart().length) >> 1, block('code', {
        text: codeLines.join('\n'),
        language: lang,
      }));
      i++;
      continue;
    }

    const stripped = line.trim();
    if (!stripped) {
      i++;
      continue;
    }

    appendBlock(blocks, (line.length - line.trimStart().length) >> 1, parseLineToIr(stripped));
    i++;
  }

  return blocks;
}

export function irToNotionBlocks(blocks) {
  return blocks.map(irBlockToNotion);
}

export function notionBlocksToIr(blocksMap, rootId) {
  const root = blocksMap[rootId]?.value ?? {};
  return (root.content || [])
    .map(id => blocksMap[id]?.value)
    .filter(blockNode => blockNode && blockNode.alive !== false)
    .map(blockNode => notionBlockToIr(blockNode, blocksMap));
}

export function irToMarkdown(blocks, indent = 0) {
  return blocks.flatMap(blockNode => irBlockToMarkdownLines(blockNode, indent)).join('\n');
}

export function markdownToBlocks(md) {
  return irToNotionBlocks(markdownToIr(md));
}

export function blocksToMarkdown(blocksMap, rootId, indent = 0) {
  return irToMarkdown(notionBlocksToIr(blocksMap, rootId), indent);
}
