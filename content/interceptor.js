/**
 * Notion AI Chat Interceptor
 * Injected at document_start — monkey-patches fetch before Notion scripts load.
 *
 * Captures TWO sources of chat data:
 *   1. LIVE: POST /api/v3/runInferenceTranscript — NDJSON streaming
 *   2. HISTORICAL: POST /api/v3/syncRecordValuesSpaceInitial — thread + thread_message records
 */

(function () {
  "use strict";

  const LIVE_PATH = "/api/v3/runInferenceTranscript";
  const SYNC_PATH = "/api/v3/syncRecordValuesSpaceInitial";

  function getPath(url) {
    try { return new URL(url, location.origin).pathname; } catch { return ""; }
  }

  // ── Shared text cleaning ──────────────────────────────────────────────────

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

  // ── Extract user message from request transcript ──────────────────────────

  function extractUserMessage(reqBody) {
    if (!reqBody?.transcript) return null;
    const last = reqBody.transcript.filter((e) => e.type === "user").at(-1);
    if (!last?.value) return null;
    return extractRichText(last.value);
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // HISTORICAL: syncRecordValuesSpaceInitial → thread + thread_message
  // ═══════════════════════════════════════════════════════════════════════════

  function handleSyncResponse(data) {
    const rm = data?.recordMap ?? {};

    const threads = {};
    for (const [id, rec] of Object.entries(rm.thread ?? {})) {
      const val = rec?.value?.value ?? rec?.value ?? {};
      if (val.type === "workflow" && val.messages?.length) threads[id] = val;
    }

    const messages = {};
    for (const [id, rec] of Object.entries(rm.thread_message ?? {})) {
      const val = rec?.value?.value ?? rec?.value ?? {};
      if (val.step || val.role) messages[id] = val;
    }

    if (!Object.keys(threads).length && !Object.keys(messages).length) return;

    // Send to background for storage
    sendToBackground({ type: "SYNC_RECORDS", threads, messages });
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // LIVE: runInferenceTranscript → NDJSON streaming
  // ═══════════════════════════════════════════════════════════════════════════

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

    if (ndjsonLines.length > 0) {
      processNDJSON(ndjsonLines, meta);
    }
  }

  function processNDJSON(lines, meta) {
    const contentByPath = {};
    const steps = [];
    const toolResults = [];

    for (const obj of lines) {
      if (obj.type !== "patch") continue;
      for (const v of obj.v ?? []) {
        const op = v.o, path = v.p ?? "", val = v.v;
        if (op === "a" && path.endsWith("/-") && typeof val === "object" && val !== null) {
          if (val.type === "agent-inference") steps.push({ id: val.id, model: null });
          else if (val.type === "agent-tool-result") toolResults.push({ toolName: val.toolName, state: val.state, input: val.input });
        }
        if (op === "x" && path.includes("/content") && typeof val === "string") contentByPath[path] = (contentByPath[path] ?? "") + val;
        if (op === "p" && path.includes("/content") && typeof val === "string") contentByPath[path] = val;
        if (op === "a" && path.includes("/model") && typeof val === "string") { const ls = steps.at(-1); if (ls) ls.model = val; }
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

    const stepGroups = new Map();
    for (const t of inferenceTexts) {
      if (!stepGroups.has(t.stepIdx)) stepGroups.set(t.stepIdx, []);
      stepGroups.get(t.stepIdx).push(t);
    }
    const groupKeys = [...stepGroups.keys()].sort((a, b) => a - b);
    const lastGroup = groupKeys.at(-1);
    const responseParts = [], thinkingParts = [];
    for (const key of groupKeys) {
      const texts = stepGroups.get(key).map((t) => t.content);
      if (key === lastGroup) responseParts.push(...texts);
      else for (const text of texts) { if (text.length > 200) thinkingParts.push(text); else responseParts.push(text); }
    }

    const assistantContent = cleanText(responseParts.join("\n"));
    const thinkingContent = thinkingParts.join("\n").trim() || null;

    if (!assistantContent && !meta.userMessage) return;

    sendToBackground({
      type: "TRANSCRIPT",
      conversation: {
        traceId: meta.traceId,
        spaceId: meta.spaceId,
        userMessage: meta.userMessage,
        assistantMessage: assistantContent || null,
        thinking: thinkingContent,
        toolCalls: toolResults.filter((t) => t.toolName && t.state !== "pending").map((t) => ({ tool: t.toolName, input: t.input })),
        model: steps.find((s) => s.model)?.model ?? null,
        timestamp: Date.now(),
      },
    });
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // FETCH INTERCEPT
  // ═══════════════════════════════════════════════════════════════════════════

  const _fetch = window.fetch.bind(window);

  window.fetch = async function (input, init) {
    const url = typeof input === "string" ? input : input?.url ?? "";
    const path = getPath(url);

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

    if (path === SYNC_PATH) {
      const response = await _fetch(input, init);
      response.clone().json().then((data) => {
        try {
          const rm = data?.recordMap ?? {};
          const hasThread = !!rm.thread;
          const hasMsg = !!rm.thread_message;
          if (hasThread || hasMsg) console.debug("[notion-ai-scraper] sync response has thread data:", { thread: Object.keys(rm.thread ?? {}).length, thread_message: Object.keys(rm.thread_message ?? {}).length });
          handleSyncResponse(data);
        } catch (e) { console.warn("[notion-ai-scraper] handleSyncResponse error:", e); }
      }).catch(() => {});
      return response;
    }

    return _fetch(input, init);
  };

  // ── XHR intercept (fallback) ───────────────────────────────────────────────

  const _XHROpen = XMLHttpRequest.prototype.open;
  const _XHRSend = XMLHttpRequest.prototype.send;

  XMLHttpRequest.prototype.open = function (method, url, ...rest) {
    this._notionUrl = url;
    return _XHROpen.call(this, method, url, ...rest);
  };

  XMLHttpRequest.prototype.send = function (body) {
    if (this._notionUrl) {
      const path = getPath(this._notionUrl);
      if (path === SYNC_PATH) {
        this.addEventListener("load", () => {
          try { handleSyncResponse(JSON.parse(this.responseText)); } catch {}
        });
      }
    }
    return _XHRSend.call(this, body);
  };

  // ── Message bridge ─────────────────────────────────────────────────────────

  function sendToBackground(payload) {
    console.debug("[notion-ai-scraper] sending to background:", payload.type,
      payload.type === "SYNC_RECORDS"
        ? `threads=${Object.keys(payload.threads ?? {}).length} messages=${Object.keys(payload.messages ?? {}).length}`
        : "");
    try { browser.runtime.sendMessage(payload).catch((err) => console.warn("[notion-ai-scraper] sendMessage error:", err)); } catch (e) { console.warn("[notion-ai-scraper] sendMessage exception:", e); }
  }
})();
