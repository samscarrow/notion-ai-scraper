// ==UserScript==
// @name         Notion AI Chat Scraper
// @namespace    https://notion.so
// @version      0.2.0
// @description  Captures Notion AI chat conversations via runInferenceTranscript and exports as Markdown or JSON
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
  const TARGET_PATH = "/api/v3/runInferenceTranscript";

  // ── Storage ───────────────────────────────────────────────────────────────

  function loadStore() {
    try { return JSON.parse(GM_getValue(STORAGE_KEY, "{}")); } catch { return {}; }
  }

  function saveStore(store) {
    GM_setValue(STORAGE_KEY, JSON.stringify(store));
  }

  // ── Extract user message from request transcript ──────────────────────────

  function extractUserMessage(reqBody) {
    if (!reqBody?.transcript) return null;
    const userEntries = reqBody.transcript.filter((e) => e.type === "user");
    const last = userEntries.at(-1);
    if (!last?.value) return null;
    if (Array.isArray(last.value)) {
      return last.value
        .map((chunk) => (Array.isArray(chunk) ? chunk[0] ?? "" : typeof chunk === "string" ? chunk : ""))
        .join("").trim() || null;
    }
    return typeof last.value === "string" ? last.value : null;
  }

  // ── NDJSON stream parser ──────────────────────────────────────────────────

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

        // New step
        if (op === "a" && path.endsWith("/-") && typeof val === "object" && val !== null) {
          if (val.type === "agent-inference") {
            steps.push({ id: val.id, type: "inference", model: null });
            // Don't seed from initial value — it contains partial XML tags (<lang...)
            // that get replaced by o:"p" ops. Let the stream ops populate content.
          } else if (val.type === "agent-tool-result") {
            toolResults.push({ toolName: val.toolName, state: val.state, input: val.input });
          }
        }

        // Append token
        if (op === "x" && path.includes("/content") && typeof val === "string") {
          contentByPath[path] = (contentByPath[path] ?? "") + val;
        }

        // Replace content
        if (op === "p" && path.includes("/content") && typeof val === "string") {
          contentByPath[path] = val;
        }

        // Model info
        if (op === "a" && path.includes("/model") && typeof val === "string") {
          const lastStep = steps.at(-1);
          if (lastStep) lastStep.model = val;
        }
      }
    }

    // Reconstruct text from content paths: /s/N/value/M/content
    const inferenceTexts = [];

    for (const [path, text] of Object.entries(contentByPath)) {
      const match = path.match(/^\/s\/(\d+)\/value\/(\d+)\/content$/);
      if (!match) continue;
      const stepIdx = parseInt(match[1], 10);
      const valueIdx = parseInt(match[2], 10);
      const trimmed = text.trim();
      // Skip tool-call JSON blobs
      if (trimmed.startsWith("{") && (trimmed.includes('"urls"') || trimmed.includes('"pageUrl"') || trimmed.includes('"command"'))) continue;
      if (trimmed.length > 0) inferenceTexts.push({ stepIdx, valueIdx, content: trimmed });
    }

    inferenceTexts.sort((a, b) => a.stepIdx - b.stepIdx || a.valueIdx - b.valueIdx);

    // Separate chain-of-thought from user-facing response.
    // Notion AI often has multiple inference steps:
    //   - Early steps: brief user-facing text ("Let me grab...", "Got it — applying...")
    //   - Middle steps: internal reasoning (chain-of-thought) + tool call args
    //   - Last step: final user-facing summary
    // We keep ALL non-empty text but mark the last step group as the primary response.
    const stepGroups = new Map();
    for (const t of inferenceTexts) {
      if (!stepGroups.has(t.stepIdx)) stepGroups.set(t.stepIdx, []);
      stepGroups.get(t.stepIdx).push(t);
    }
    const groupKeys = [...stepGroups.keys()].sort((a, b) => a - b);

    // The last inference step group is the user-facing response
    const lastGroup = groupKeys.at(-1);
    const responseParts = [];
    const thinkingParts = [];
    for (const key of groupKeys) {
      const texts = stepGroups.get(key).map((t) => t.content);
      if (key === lastGroup) {
        responseParts.push(...texts);
      } else {
        // Earlier steps: keep short user-facing text, tag long reasoning as thinking
        for (const text of texts) {
          if (text.length > 200) {
            thinkingParts.push(text);
          } else {
            responseParts.push(text);
          }
        }
      }
    }

    const assistantContent = responseParts.join("\n")
      .replace(/<edit_reference[^>]*>[\s\S]*?<\/edit_reference>/g, "")
      .trim();
    const thinkingContent = thinkingParts.join("\n").trim() || null;

    if (!assistantContent && !meta.userMessage) return;

    // Store the conversation
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

    const model = steps.find((s) => s.model)?.model ?? null;
    entry.model = model ?? entry.model;

    const finishedToolCalls = toolResults
      .filter((t) => t.toolName && t.state !== "pending")
      .map((t) => ({ tool: t.toolName, input: t.input }));
    if (finishedToolCalls.length) entry.toolCalls.push(...finishedToolCalls);

    entry.updatedAt = Date.now();
    saveStore(store);

    console.debug(`[notion-ai-scraper] captured trace ${key}: user="${(meta.userMessage ?? "").slice(0, 40)}" assistant=${assistantContent.length} chars`);
  }

  // ── NDJSON stream reader ──────────────────────────────────────────────────

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

  // ── Fetch intercept ───────────────────────────────────────────────────────

  const _fetch = window.fetch.bind(window);

  window.fetch = async function (input, init) {
    const url = typeof input === "string" ? input : input?.url ?? "";
    let path;
    try { path = new URL(url, location.origin).pathname; } catch { path = ""; }

    if (path !== TARGET_PATH) return _fetch(input, init);

    let reqBody = null;
    try {
      const raw = init?.body ?? (input instanceof Request ? await input.clone().text() : null);
      if (raw) reqBody = JSON.parse(raw);
    } catch {}

    const userMessage = extractUserMessage(reqBody);
    const meta = {
      userMessage,
      traceId: reqBody?.traceId ?? null,
      spaceId: reqBody?.spaceId ?? null,
    };

    const response = await _fetch(input, init);
    handleNDJSONStream(response.clone(), meta);
    return response;
  };

  // ── Export helpers ─────────────────────────────────────────────────────────

  function toMarkdown(store) {
    return Object.values(store).map((c) => {
      const model = c.model ? ` (${c.model})` : "";
      const header = `# Notion AI Chat${model}\n_Trace: ${c.id}_\n_Captured: ${new Date(c.createdAt).toISOString()}_\n\n`;
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

  // ── Menu commands ─────────────────────────────────────────────────────────

  GM_registerMenuCommand("Export All → Markdown", () => {
    const store = loadStore();
    if (!Object.keys(store).length) { alert("No conversations captured yet."); return; }
    const md = toMarkdown(store);
    const blob = new Blob([md], { type: "text/markdown" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url; a.download = `notion-ai-${Date.now()}.md`; a.click();
    URL.revokeObjectURL(url);
  });

  GM_registerMenuCommand("Export All → JSON", () => {
    const store = loadStore();
    if (!Object.keys(store).length) { alert("No conversations captured yet."); return; }
    const json = JSON.stringify(Object.values(store), null, 2);
    const blob = new Blob([json], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url; a.download = `notion-ai-${Date.now()}.json`; a.click();
    URL.revokeObjectURL(url);
  });

  GM_registerMenuCommand("Clear captured conversations", () => {
    if (confirm("Clear all captured Notion AI conversations?")) {
      saveStore({});
      alert("Cleared.");
    }
  });

  GM_registerMenuCommand("Show capture stats", () => {
    const store = loadStore();
    const convos = Object.values(store);
    const count = convos.length;
    const turns = convos.reduce((n, c) => n + (c.turns?.length ?? 0), 0);
    const models = [...new Set(convos.map((c) => c.model).filter(Boolean))];
    alert(`${count} conversation(s), ${turns} total turn(s)\nModels: ${models.join(", ") || "unknown"}`);
  });

  console.log("[notion-ai-scraper] v0.2.0 active — watching runInferenceTranscript");
})();
