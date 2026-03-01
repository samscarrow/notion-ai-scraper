// ==UserScript==
// @name         Notion AI Chat Scraper
// @namespace    https://notion.so
// @version      0.3.0
// @description  Captures Notion AI chat conversations (live + historical) and exports as Markdown or JSON
// @author       notion-ai-scraper
// @match        https://www.notion.so/*
// @match        https://notion.so/*
// @grant        GM_setValue
// @grant        GM_getValue
// @grant        GM_download
// @grant        GM_registerMenuCommand
// @run-at       document-start
// ==/UserScript==

(function () {
  "use strict";

  const STORAGE_KEY = "notion_ai_conversations";
  const LIVE_PATH = "/api/v3/runInferenceTranscript";
  const SYNC_PATH = "/api/v3/syncRecordValuesSpaceInitial";

  // ── Storage ───────────────────────────────────────────────────────────────

  function loadStore() {
    try { return JSON.parse(GM_getValue(STORAGE_KEY, "{}")); } catch { return {}; }
  }

  function saveStore(store) {
    GM_setValue(STORAGE_KEY, JSON.stringify(store));
  }

  // ── Shared text cleaning ──────────────────────────────────────────────────

  function cleanText(text) {
    return text
      .replace(/<lang[^>]*\/>/g, "")
      .replace(/<edit_reference[^>]*>[\s\S]*?<\/edit_reference>/g, "")
      .trim();
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // HISTORICAL CHAT: syncRecordValuesSpaceInitial → thread + thread_message
  // ═══════════════════════════════════════════════════════════════════════════

  function handleSyncResponse(data) {
    const rm = data?.recordMap ?? {};

    // Collect threads (contain message ordering + title)
    const threads = {};
    for (const [id, rec] of Object.entries(rm.thread ?? {})) {
      const val = rec?.value?.value ?? rec?.value ?? {};
      if (val.type === "workflow" && val.messages?.length) {
        threads[id] = val;
      }
    }

    // Collect thread_messages
    const messages = {};
    for (const [id, rec] of Object.entries(rm.thread_message ?? {})) {
      const val = rec?.value?.value ?? rec?.value ?? {};
      if (val.step || val.role) messages[id] = val;
    }

    if (!Object.keys(threads).length && !Object.keys(messages).length) return;

    const store = loadStore();

    // Process each thread we find
    for (const [threadId, thread] of Object.entries(threads)) {
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
        // Update message order if we get a newer version
        store[key].messageOrder = thread.messages;
        if (thread.data?.title) store[key].title = thread.data.title;
      }
      store[key].updatedAt = Date.now();
    }

    // Process thread_messages into turns
    for (const [msgId, msg] of Object.entries(messages)) {
      const step = msg.step ?? {};
      const parentThread = msg.parent_id;
      const key = `thread-${parentThread}`;

      // Ensure thread entry exists
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

      // Skip if we already processed this message
      if (entry._processedMsgIds?.includes(msgId)) continue;
      if (!entry._processedMsgIds) entry._processedMsgIds = [];

      if (!step.type && msg.role === "editor") {
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
        // config, context, user-specified-context, agent-turn-full-record-map, etc.
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
        // Messages not in order list go to end, sorted by timestamp
        if (ai === -1 && bi === -1) return (a.timestamp ?? 0) - (b.timestamp ?? 0);
        if (ai === -1) return 1;
        if (bi === -1) return -1;
        return ai - bi;
      });
    }

    saveStore(store);

    const newThreads = Object.keys(threads).length;
    const newMsgs = Object.keys(messages).length;
    if (newThreads || newMsgs) {
      console.debug(`[notion-ai-scraper] sync: ${newThreads} thread(s), ${newMsgs} message(s)`);
    }
  }

  /** Extract an assistant turn from an agent-inference step */
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
      // Skip tool_use items — those are tool call args
    }

    const content = responseParts.join("\n").trim();
    if (!content) return null;

    const turn = { role: "assistant", content };
    if (thinkingContent) turn.thinking = thinkingContent;
    if (model) turn.model = model;
    return turn;
  }

  /** Extract plain text from Notion rich-text array [[text], [text, [[annotation]]]] */
  function extractRichText(value) {
    if (!Array.isArray(value)) return typeof value === "string" ? value.trim() : null;
    return value
      .map((chunk) => (Array.isArray(chunk) ? chunk[0] ?? "" : typeof chunk === "string" ? chunk : ""))
      .join("").trim() || null;
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // LIVE CHAT: runInferenceTranscript → NDJSON streaming
  // ═══════════════════════════════════════════════════════════════════════════

  function extractUserMessage(reqBody) {
    if (!reqBody?.transcript) return null;
    const userEntries = reqBody.transcript.filter((e) => e.type === "user");
    const last = userEntries.at(-1);
    if (!last?.value) return null;
    return extractRichText(last.value);
  }

  function processNDJSON(lines, meta) {
    const contentByPath = {};
    const steps = [];
    const toolResults = [];

    for (const obj of lines) {
      if (obj.type !== "patch") continue;
      for (const v of obj.v ?? []) {
        const op = v.o;
        const path = v.p ?? "";
        const val = v.v;

        if (op === "a" && path.endsWith("/-") && typeof val === "object" && val !== null) {
          if (val.type === "agent-inference") {
            steps.push({ id: val.id, model: null });
          } else if (val.type === "agent-tool-result") {
            toolResults.push({ toolName: val.toolName, state: val.state, input: val.input });
          }
        }
        if (op === "x" && path.includes("/content") && typeof val === "string") {
          contentByPath[path] = (contentByPath[path] ?? "") + val;
        }
        if (op === "p" && path.includes("/content") && typeof val === "string") {
          contentByPath[path] = val;
        }
        if (op === "a" && path.includes("/model") && typeof val === "string") {
          const lastStep = steps.at(-1);
          if (lastStep) lastStep.model = val;
        }
      }
    }

    const inferenceTexts = [];
    for (const [path, text] of Object.entries(contentByPath)) {
      const match = path.match(/^\/s\/(\d+)\/value\/(\d+)\/content$/);
      if (!match) continue;
      const trimmed = text.trim();
      if (trimmed.startsWith("{") && (trimmed.includes('"urls"') || trimmed.includes('"pageUrl"') || trimmed.includes('"command"'))) continue;
      if (trimmed.length > 0) inferenceTexts.push({ stepIdx: +match[1], valueIdx: +match[2], content: trimmed });
    }
    inferenceTexts.sort((a, b) => a.stepIdx - b.stepIdx || a.valueIdx - b.valueIdx);

    // Separate CoT from user-facing
    const stepGroups = new Map();
    for (const t of inferenceTexts) {
      if (!stepGroups.has(t.stepIdx)) stepGroups.set(t.stepIdx, []);
      stepGroups.get(t.stepIdx).push(t);
    }
    const groupKeys = [...stepGroups.keys()].sort((a, b) => a - b);
    const lastGroup = groupKeys.at(-1);
    const responseParts = [];
    const thinkingParts = [];
    for (const key of groupKeys) {
      const texts = stepGroups.get(key).map((t) => t.content);
      if (key === lastGroup) {
        responseParts.push(...texts);
      } else {
        for (const text of texts) {
          if (text.length > 200) thinkingParts.push(text);
          else responseParts.push(text);
        }
      }
    }

    const assistantContent = cleanText(responseParts.join("\n"));
    const thinkingContent = thinkingParts.join("\n").trim() || null;

    if (!assistantContent && !meta.userMessage) return;

    const key = meta.traceId ?? `unknown-${Date.now()}`;
    const store = loadStore();

    if (!store[key]) {
      store[key] = {
        id: key,
        spaceId: meta.spaceId,
        model: null,
        turns: [],
        toolCalls: [],
        createdAt: Date.now(),
      };
    }

    const entry = store[key];

    if (meta.userMessage) {
      const lastUserTurn = entry.turns.findLast((t) => t.role === "user");
      if (!(lastUserTurn && lastUserTurn.content === meta.userMessage && Math.abs((lastUserTurn.timestamp ?? 0) - Date.now()) < 2000)) {
        entry.turns.push({ role: "user", content: meta.userMessage, timestamp: Date.now() });
      }
    }

    if (assistantContent) {
      const turn = { role: "assistant", content: assistantContent, timestamp: Date.now() };
      if (thinkingContent) turn.thinking = thinkingContent;
      entry.turns.push(turn);
    }

    entry.model = steps.find((s) => s.model)?.model ?? entry.model;
    const fc = toolResults.filter((t) => t.toolName && t.state !== "pending").map((t) => ({ tool: t.toolName, input: t.input }));
    if (fc.length) entry.toolCalls.push(...fc);
    entry.updatedAt = Date.now();
    saveStore(store);

    console.debug(`[notion-ai-scraper] live: trace ${key}, user="${(meta.userMessage ?? "").slice(0, 40)}" assistant=${assistantContent.length} chars`);
  }

  async function handleNDJSONStream(response, meta) {
    const reader = response.body?.getReader();
    if (!reader) return;
    const decoder = new TextDecoder();
    let buffer = "";
    const ndjsonLines = [];

    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split("\n");
        buffer = lines.pop() ?? "";
        for (const line of lines) {
          const trimmed = line.trim();
          if (!trimmed) continue;
          try { ndjsonLines.push(JSON.parse(trimmed)); } catch {}
        }
      }
      if (buffer.trim()) {
        try { ndjsonLines.push(JSON.parse(buffer.trim())); } catch {}
      }
    } catch (err) {
      console.warn("[notion-ai-scraper] NDJSON read error:", err);
    }

    if (ndjsonLines.length > 0) processNDJSON(ndjsonLines, meta);
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // FETCH INTERCEPT
  // ═══════════════════════════════════════════════════════════════════════════

  const _fetch = window.fetch.bind(window);

  window.fetch = async function (input, init) {
    const url = typeof input === "string" ? input : input?.url ?? "";
    let path;
    try { path = new URL(url, location.origin).pathname; } catch { path = ""; }

    // Live AI chat
    if (path === LIVE_PATH) {
      let reqBody = null;
      try {
        const raw = init?.body ?? (input instanceof Request ? await input.clone().text() : null);
        if (raw) reqBody = JSON.parse(raw);
      } catch {}

      const response = await _fetch(input, init);
      handleNDJSONStream(response.clone(), {
        userMessage: extractUserMessage(reqBody),
        traceId: reqBody?.traceId ?? null,
        spaceId: reqBody?.spaceId ?? null,
      });
      return response;
    }

    // Historical chat via sync
    if (path === SYNC_PATH) {
      const response = await _fetch(input, init);
      response.clone().json().then((data) => {
        try { handleSyncResponse(data); } catch (err) {
          console.warn("[notion-ai-scraper] sync parse error:", err);
        }
      }).catch(() => {});
      return response;
    }

    return _fetch(input, init);
  };

  // ═══════════════════════════════════════════════════════════════════════════
  // EXPORT
  // ═══════════════════════════════════════════════════════════════════════════

  function toMarkdown(store) {
    return Object.values(store)
      .filter((c) => c.turns?.length > 0)
      .map((c) => {
        const title = c.title ? ` — ${c.title}` : "";
        const model = c.model ? ` (${c.model})` : "";
        const header = `# Notion AI Chat${title}${model}\n_ID: ${c.id}_\n_Captured: ${new Date(c.createdAt).toISOString()}_\n\n`;
        const body = (c.turns ?? [])
          .map((t) => `**${t.role === "assistant" ? "Notion AI" : "You"}**\n\n${t.content}`)
          .join("\n\n---\n\n");
        let toolSection = "";
        if (c.toolCalls?.length) {
          toolSection = "\n\n---\n\n<details><summary>Tool calls</summary>\n\n" +
            c.toolCalls.map((tc) => `- **${tc.tool}**: \`${JSON.stringify(tc.input).slice(0, 200)}\``).join("\n") +
            "\n</details>";
        }
        return header + body + toolSection;
      }).join("\n\n===\n\n");
  }

  function downloadBlob(content, filename, mime) {
    const blob = new Blob([content], { type: mime });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url; a.download = filename; a.click();
    URL.revokeObjectURL(url);
  }

  // ── Menu commands ─────────────────────────────────────────────────────────

  GM_registerMenuCommand("Export All → Markdown", () => {
    const store = loadStore();
    const withTurns = Object.values(store).filter((c) => c.turns?.length);
    if (!withTurns.length) { alert("No conversations captured yet."); return; }
    downloadBlob(toMarkdown(store), `notion-ai-${Date.now()}.md`, "text/markdown");
  });

  GM_registerMenuCommand("Export All → JSON", () => {
    const store = loadStore();
    const withTurns = Object.values(store).filter((c) => c.turns?.length);
    if (!withTurns.length) { alert("No conversations captured yet."); return; }
    const data = withTurns.map((c) => {
      const { _processedMsgIds, messageOrder, ...clean } = c;
      return clean;
    });
    downloadBlob(JSON.stringify(data, null, 2), `notion-ai-${Date.now()}.json`, "application/json");
  });

  GM_registerMenuCommand("Clear captured conversations", () => {
    if (confirm("Clear all captured Notion AI conversations?")) {
      saveStore({});
      alert("Cleared.");
    }
  });

  GM_registerMenuCommand("Show capture stats", () => {
    const store = loadStore();
    const convos = Object.values(store).filter((c) => c.turns?.length);
    const count = convos.length;
    const turns = convos.reduce((n, c) => n + (c.turns?.length ?? 0), 0);
    const models = [...new Set(convos.map((c) => c.model).filter(Boolean))];
    const titles = convos.map((c) => c.title).filter(Boolean).slice(0, 5);
    alert(
      `${count} conversation(s), ${turns} total turn(s)\n` +
      `Models: ${models.join(", ") || "unknown"}\n` +
      (titles.length ? `Recent: ${titles.join(", ")}` : "")
    );
  });

  console.log("[notion-ai-scraper] v0.3.0 active — watching live + historical chat");
})();
