/**
 * Notion AI Scraper — Background Service Worker
 *
 * Receives parsed NDJSON transcript data from the content script,
 * stores conversation turns, and handles export.
 *
 * Real protocol: /api/v3/runInferenceTranscript → NDJSON patch ops
 */

"use strict";

// ── Message handler ────────────────────────────────────────────────────────

browser.runtime.onMessage.addListener((msg, sender) => {
  if (!msg?.type) return;

  switch (msg.type) {
    case "TRANSCRIPT":
      return handleTranscript(msg.conversation);
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
  return {
    ok: true,
    content: JSON.stringify(data, null, 2),
    filename: `notion-ai-${Date.now()}.json`,
  };
}
