/**
 * Notion AI Scraper — Background Service Worker
 *
 * Receives parsed NDJSON transcript data from the content script,
 * stores conversation turns, and handles export.
 *
 * Two data sources:
 *   1. LIVE: /api/v3/runInferenceTranscript → NDJSON patch ops
 *   2. HISTORICAL: /api/v3/syncRecordValuesSpaceInitial → thread + thread_message records
 */

"use strict";

// ── Message handler ────────────────────────────────────────────────────────

browser.runtime.onMessage.addListener((msg, sender) => {
  if (!msg?.type) return;

  switch (msg.type) {
    case "TRANSCRIPT":
      return handleTranscript(msg.conversation);
    case "SYNC_RECORDS":
      return handleSyncRecords(msg.threads, msg.messages);
    case "GET_CONVERSATIONS":
      return getConversations();
    case "CLEAR_CONVERSATIONS":
      return clearConversations();
    case "EXPORT_MD":
      return exportMarkdown(msg.conversationId);
    case "EXPORT_JSON":
      return exportJSON(msg.conversationId);
  }
});

// ── Transcript handling ────────────────────────────────────────────────────

async function handleTranscript(convo) {
  if (!convo) return;

  const key = convo.traceId ?? `unknown-${Date.now()}`;
  const store = await loadStore();

  if (!store[key]) {
    store[key] = {
      id: key,
      spaceId: convo.spaceId,
      model: convo.model,
      turns: [],
      toolCalls: [],
      createdAt: Date.now(),
    };
  }

  const entry = store[key];

  // Add user turn
  if (convo.userMessage) {
    const lastUserTurn = entry.turns.findLast((t) => t.role === "user");
    // Dedup: skip if last user turn is same message within 2s
    if (
      !(lastUserTurn &&
        lastUserTurn.content === convo.userMessage &&
        Math.abs((lastUserTurn.timestamp ?? 0) - convo.timestamp) < 2000)
    ) {
      entry.turns.push({
        role: "user",
        content: convo.userMessage,
        timestamp: convo.timestamp,
      });
    }
  }

  // Add assistant turn
  if (convo.assistantMessage) {
    const turn = {
      role: "assistant",
      content: convo.assistantMessage,
      model: convo.model,
      timestamp: convo.timestamp,
    };
    if (convo.thinking) turn.thinking = convo.thinking;
    entry.turns.push(turn);
  }

  // Store tool calls for reference
  if (convo.toolCalls?.length) {
    entry.toolCalls.push(...convo.toolCalls);
  }

  entry.model = convo.model ?? entry.model;
  entry.updatedAt = Date.now();

  await saveStore(store);
  console.debug(
    `[notion-ai-scraper] stored ${convo.userMessage ? "user+" : ""}${convo.assistantMessage ? "assistant" : ""} turn for trace ${key}`
  );
}

// ── Historical chat (sync records) ────────────────────────────────────────

function cleanText(text) {
  return text
    .replace(/<lang[^>]*\/>/g, "")
    .replace(/<edit_reference[^>]*>[\s\S]*?<\/edit_reference>/g, "")
    .trim();
}

function extractRichText(value) {
  if (!Array.isArray(value)) return typeof value === "string" ? value.trim() : null;
  return value
    .map((chunk) => (Array.isArray(chunk) ? chunk[0] ?? "" : typeof chunk === "string" ? chunk : ""))
    .join("").trim() || null;
}

function extractInferenceTurn(step) {
  const values = step.value ?? [];
  if (!Array.isArray(values)) return null;

  const responseParts = [];
  let thinkingContent = null;
  let model = step.model ?? null;

  for (const item of values) {
    if (item.type === "text") {
      const cleaned = cleanText(item.content ?? "");
      if (cleaned) responseParts.push(cleaned);
    } else if (item.type === "thinking") {
      thinkingContent = (item.content ?? "").trim() || null;
    }
  }

  const content = responseParts.join("\n").trim();
  if (!content) return null;

  const turn = { role: "assistant", content };
  if (thinkingContent) turn.thinking = thinkingContent;
  if (model) turn.model = model;
  return turn;
}

async function handleSyncRecords(threads, messages) {
  if (!threads && !messages) return;

  const store = await loadStore();

  // Process threads (contain message ordering + title)
  for (const [threadId, thread] of Object.entries(threads ?? {})) {
    const key = `thread-${threadId}`;
    if (!store[key]) {
      store[key] = {
        id: key,
        threadId,
        title: thread.data?.title ?? null,
        spaceId: thread.space_id,
        model: null,
        turns: [],
        toolCalls: [],
        createdAt: thread.created_time ?? Date.now(),
        messageOrder: thread.messages,
      };
    } else {
      store[key].messageOrder = thread.messages;
      if (thread.data?.title) store[key].title = thread.data.title;
    }
    store[key].updatedAt = Date.now();
  }

  // Process thread_messages into turns
  for (const [msgId, msg] of Object.entries(messages ?? {})) {
    const step = msg.step ?? {};
    const parentThread = msg.parent_id;
    const key = `thread-${parentThread}`;

    if (!store[key]) {
      store[key] = {
        id: key,
        threadId: parentThread,
        title: null,
        spaceId: msg.space_id,
        model: null,
        turns: [],
        toolCalls: [],
        createdAt: msg.created_time ?? Date.now(),
        messageOrder: [],
      };
    }

    const entry = store[key];
    if (!entry._processedMsgIds) entry._processedMsgIds = [];
    if (entry._processedMsgIds.includes(msgId)) continue;

    if (!step.type && msg.role === "editor") {
      // User messages in historical data have role:"editor" but no content
      entry.turns.push({
        role: "user",
        content: "(user message)",
        msgId,
        timestamp: msg.created_time ?? Date.now(),
      });
      entry._processedMsgIds.push(msgId);
    } else if (step.type === "agent-inference") {
      const turn = extractInferenceTurn(step);
      if (turn) {
        turn.msgId = msgId;
        turn.timestamp = msg.created_time ?? Date.now();
        entry.turns.push(turn);
        entry._processedMsgIds.push(msgId);
        if (turn.model) entry.model = turn.model;
      }
    } else if (step.type === "user" || step.type === "human") {
      const content = extractRichText(step.value);
      if (content) {
        entry.turns.push({
          role: "user",
          content,
          msgId,
          timestamp: msg.created_time ?? Date.now(),
        });
        entry._processedMsgIds.push(msgId);
      }
    } else if (step.type === "agent-tool-result" && step.state !== "pending") {
      if (step.toolName) {
        entry.toolCalls.push({ tool: step.toolName, input: step.input ?? {} });
      }
      entry._processedMsgIds.push(msgId);
    } else {
      entry._processedMsgIds.push(msgId);
    }

    entry.updatedAt = Date.now();
  }

  // Sort turns by message order from thread
  for (const entry of Object.values(store)) {
    if (!entry.messageOrder?.length || entry.turns.length < 2) continue;
    const order = entry.messageOrder;
    entry.turns.sort((a, b) => {
      const ai = order.indexOf(a.msgId);
      const bi = order.indexOf(b.msgId);
      if (ai === -1 && bi === -1) return (a.timestamp ?? 0) - (b.timestamp ?? 0);
      if (ai === -1) return 1;
      if (bi === -1) return -1;
      return ai - bi;
    });
  }

  await saveStore(store);
  console.debug(`[notion-ai-scraper] processed sync records: ${Object.keys(threads ?? {}).length} threads, ${Object.keys(messages ?? {}).length} messages`);
}

// ── Storage ────────────────────────────────────────────────────────────────

const STORAGE_KEY = "notion_ai_conversations";

async function loadStore() {
  const result = await browser.storage.local.get(STORAGE_KEY);
  return result[STORAGE_KEY] ?? {};
}

async function saveStore(store) {
  await browser.storage.local.set({ [STORAGE_KEY]: store });
}

async function getConversations() {
  const store = await loadStore();
  return Object.values(store).sort(
    (a, b) => (b.updatedAt ?? 0) - (a.updatedAt ?? 0)
  );
}

async function clearConversations() {
  await browser.storage.local.remove(STORAGE_KEY);
  return { ok: true };
}

// ── Export: Markdown ──────────────────────────────────────────────────────

async function exportMarkdown(conversationId) {
  const store = await loadStore();
  const convo = conversationId ? store[conversationId] : null;
  const targets = convo ? [convo] : Object.values(store);

  const md = targets
    .map((c) => {
      const model = c.model ? ` (${c.model})` : "";
      const header = `# Notion AI Chat${model}\n_Trace: ${c.id}_\n_Captured: ${new Date(c.createdAt).toISOString()}_\n\n`;

      const body = (c.turns ?? [])
        .map((t) => {
          const label = t.role === "assistant" ? "**Notion AI**" : "**You**";
          return `${label}\n\n${t.content}`;
        })
        .join("\n\n---\n\n");

      // Append tool call summary if any
      let toolSection = "";
      if (c.toolCalls?.length) {
        toolSection =
          "\n\n---\n\n<details><summary>Tool calls</summary>\n\n" +
          c.toolCalls
            .map((tc) => `- **${tc.tool}**: \`${JSON.stringify(tc.input).slice(0, 200)}\``)
            .join("\n") +
          "\n</details>";
      }

      return header + body + toolSection;
    })
    .join("\n\n===\n\n");

  return { ok: true, content: md, filename: `notion-ai-${Date.now()}.md` };
}

// ── Export: JSON ──────────────────────────────────────────────────────────

async function exportJSON(conversationId) {
  const store = await loadStore();
  const data = conversationId
    ? store[conversationId] ?? {}
    : Object.values(store);
  const cleaned = JSON.stringify(data, (key, val) =>
    key === "_processedMsgIds" || key === "messageOrder" ? undefined : val
  , 2);
  return {
    ok: true,
    content: cleaned,
    filename: `notion-ai-${Date.now()}.json`,
  };
}
