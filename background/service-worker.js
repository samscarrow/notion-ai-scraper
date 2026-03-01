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
      return getConversations(msg.pageId);
    case "CLEAR_CONVERSATIONS":
      return clearConversations();
    case "EXPORT_MD":
      return exportMarkdown(msg.conversationId, msg.pageId);
    case "EXPORT_JSON":
      return exportJSON(msg.conversationId, msg.pageId);
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
    .map((chunk) => {
      if (!Array.isArray(chunk)) return typeof chunk === "string" ? chunk : "";
      const text = chunk[0] ?? "";
      const annotations = chunk[1];
      if (text === "\u2023" && Array.isArray(annotations)) {
        for (const ann of annotations) {
          if (Array.isArray(ann) && ann.length >= 2) {
            if (ann[0] === "p") return `[page:${ann[1]}]`;
            if (ann[0] === "u") return `[user:${ann[1]}]`;
            if (ann[0] === "a") return `[agent:${ann[1]}]`;
          }
        }
      }
      return text;
    })
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

// Merge consecutive same-role turns (e.g. multiple assistant tool-call steps)
function mergeConsecutiveTurns(turns) {
  const merged = [];
  for (const turn of turns) {
    const prev = merged.at(-1);
    if (prev && prev.role === turn.role && turn.role === "assistant") {
      prev.content = prev.content + "\n\n" + turn.content;
      if (turn.thinking) prev.thinking = (prev.thinking ? prev.thinking + "\n\n" : "") + turn.thinking;
      if (turn.model) prev.model = turn.model;
    } else {
      merged.push({ ...turn });
    }
  }
  return merged;
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

  // Sort turns by message order, then merge consecutive same-role turns
  for (const entry of Object.values(store)) {
    if (entry.turns.length < 2) continue;
    if (entry.messageOrder?.length) {
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
    entry.turns = mergeConsecutiveTurns(entry.turns);
  }

  // Dedup: absorb live-capture (traceId-keyed) entries into matching thread entries
  const threadEntries = Object.values(store).filter((c) => c.threadId);
  const liveEntries = Object.values(store).filter((c) => !c.threadId);
  for (const live of liveEntries) {
    const liveFirstUser = live.turns.find((t) => t.role === "user")?.content;
    if (!liveFirstUser) continue;
    const match = threadEntries.find((th) => {
      if (th.spaceId !== live.spaceId) return false;
      return th.turns.some((t) => t.role === "user" && t.content === liveFirstUser);
    });
    if (match) {
      // Thread entry wins — absorb model/toolCalls from live if missing
      if (!match.model && live.model) match.model = live.model;
      if (live.toolCalls?.length) match.toolCalls.push(...live.toolCalls);
      delete store[live.id];
      console.debug(`[notion-ai-scraper] merged live ${live.id} into ${match.id}`);
    }
  }

  // Auto-title untitled threads from first user message
  for (const entry of Object.values(store)) {
    if (entry.title) continue;
    const firstUser = entry.turns.find((t) => t.role === "user")?.content;
    if (firstUser) entry.title = firstUser.slice(0, 60).replace(/\s+/g, " ").trim();
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

async function getConversations(pageId) {
  const store = await loadStore();
  return Object.values(store)
    .filter((c) => {
      if (!c.turns?.length) return false;
      if (!pageId) return false;
      const tid = (c.threadId ?? "").replace(/-/g, "");
      return tid === pageId.replace(/-/g, "");
    })
    .sort((a, b) => (b.updatedAt ?? 0) - (a.updatedAt ?? 0));
}

async function clearConversations() {
  await browser.storage.local.remove(STORAGE_KEY);
  return { ok: true };
}

// ── Filename helpers ───────────────────────────────────────────────────────

function makeFilename(convo, ext) {
  const raw = convo?.title
    || convo?.turns?.find((t) => t.role === "user")?.content
    || convo?.id
    || "notion-ai";
  const slug = raw
    .slice(0, 48)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  const ts = Date.now();
  return `${slug}-${ts}.${ext}`;
}

function makeAllFilename(ext) {
  return `notion-ai-export-${Date.now()}.${ext}`;
}

// ── Export: Markdown ──────────────────────────────────────────────────────

async function exportMarkdown(conversationId, pageId) {
  const store = await loadStore();
  const convo = conversationId ? store[conversationId] : null;
  const targets = convo ? [convo] : Object.values(store).filter((c) => {
    if (!c.turns?.length) return false;
    if (!pageId) return false;
    const tid = (c.threadId ?? "").replace(/-/g, "");
    return tid === pageId.replace(/-/g, "");
  });

  const md = targets
    .map((c) => {
      const model = c.model ? ` (${c.model})` : "";
      const title = c.title ? `# ${c.title}${model}` : `# Notion AI Chat${model}`;
      const header = `${title}\n_ID: ${c.id}_\n_Captured: ${new Date(c.createdAt).toISOString()}_\n\n`;

      const body = (c.turns ?? [])
        .map((t) => {
          const label = t.role === "assistant" ? "**Notion AI**" : "**You**";
          return `${label}\n\n${t.content}`;
        })
        .join("\n\n---\n\n");

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

  const filename = convo ? makeFilename(convo, "md") : makeAllFilename("md");
  return { ok: true, content: md, filename };
}

// ── Export: JSON ──────────────────────────────────────────────────────────

async function exportJSON(conversationId, pageId) {
  const store = await loadStore();
  const convo = conversationId ? store[conversationId] : null;
  const data = convo
    ? convo
    : Object.values(store).filter((c) => {
        if (!c.turns?.length) return false;
        if (!pageId) return false;
        const tid = (c.threadId ?? "").replace(/-/g, "");
        return tid === pageId.replace(/-/g, "");
      });
  const cleaned = JSON.stringify(data, (key, val) =>
    key === "_processedMsgIds" || key === "messageOrder" ? undefined : val
  , 2);
  const filename = convo ? makeFilename(convo, "json") : makeAllFilename("json");
  return { ok: true, content: cleaned, filename };
}
