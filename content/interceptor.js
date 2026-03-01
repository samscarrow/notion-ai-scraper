/**
 * Notion AI Chat Interceptor
 * Injected at document_start — monkey-patches fetch before Notion scripts load.
 *
 * Real Notion AI protocol (verified via HAR):
 *   Endpoint: POST /api/v3/runInferenceTranscript
 *   Request:  { transcript: [...], spaceId, traceId }
 *   Response: NDJSON (application/x-ndjson), NOT SSE
 *   Stream:   patch ops with o:"x" (append token), o:"p" (replace), o:"a" (add step)
 *
 * Inspired by Trifall/chat-export (MIT) — fetch-intercept pattern
 */

(function () {
  "use strict";

  const TARGET_PATH = "/api/v3/runInferenceTranscript";

  // ── Fetch intercept ────────────────────────────────────────────────────────

  const _fetch = window.fetch.bind(window);

  window.fetch = async function (input, init) {
    const url = typeof input === "string" ? input : input?.url ?? "";

    let path;
    try { path = new URL(url, location.origin).pathname; } catch { path = ""; }

    if (path !== TARGET_PATH) {
      return _fetch(input, init);
    }

    // Extract user message from request transcript
    let reqBody = null;
    try {
      const rawBody =
        init?.body ??
        (input instanceof Request ? await input.clone().text() : null);
      if (rawBody) reqBody = JSON.parse(rawBody);
    } catch {}

    const userMessage = extractUserMessage(reqBody);
    const traceId = reqBody?.traceId ?? null;
    const spaceId = reqBody?.spaceId ?? null;

    const response = await _fetch(input, init);

    // Read the NDJSON stream in the background
    handleNDJSONStream(response.clone(), { userMessage, traceId, spaceId });

    return response;
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
      let path;
      try { path = new URL(this._notionUrl, location.origin).pathname; } catch { path = ""; }
      if (path === TARGET_PATH) {
        let reqBody = null;
        try { reqBody = JSON.parse(body); } catch {}
        const userMessage = extractUserMessage(reqBody);
        const traceId = reqBody?.traceId ?? null;
        const spaceId = reqBody?.spaceId ?? null;

        this.addEventListener("load", () => {
          try {
            parseNDJSONText(this.responseText, { userMessage, traceId, spaceId });
          } catch {}
        });
      }
    }
    return _XHRSend.call(this, body);
  };

  // ── Extract user message from request transcript ───────────────────────────

  function extractUserMessage(reqBody) {
    if (!reqBody?.transcript) return null;
    // Find the last "user" type entry in the transcript
    const userEntries = reqBody.transcript.filter((e) => e.type === "user");
    const last = userEntries.at(-1);
    if (!last?.value) return null;
    // value is Notion rich-text: [["text content"]]
    if (Array.isArray(last.value)) {
      return last.value.map((chunk) => {
        if (Array.isArray(chunk)) return chunk[0] ?? "";
        if (typeof chunk === "string") return chunk;
        return "";
      }).join("").trim() || null;
    }
    return typeof last.value === "string" ? last.value : null;
  }

  // ── NDJSON stream reader ───────────────────────────────────────────────────

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
          try {
            ndjsonLines.push(JSON.parse(trimmed));
          } catch {}
        }
      }
      // Flush remaining buffer
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

  // ── Parse NDJSON from XHR responseText (non-streaming fallback) ────────────

  function parseNDJSONText(text, meta) {
    const ndjsonLines = [];
    for (const line of text.split("\n")) {
      const trimmed = line.trim();
      if (!trimmed) continue;
      try { ndjsonLines.push(JSON.parse(trimmed)); } catch {}
    }
    if (ndjsonLines.length > 0) {
      processNDJSON(ndjsonLines, meta);
    }
  }

  // ── Process parsed NDJSON into conversation turns ──────────────────────────

  function processNDJSON(lines, meta) {
    // Track content accumulation per path (streaming tokens)
    const contentByPath = {};
    // Track step metadata (type, model, tool info)
    const steps = [];
    // Track tool results
    const toolResults = [];

    for (const obj of lines) {
      if (obj.type !== "patch") continue;

      for (const v of obj.v ?? []) {
        const op = v.o;
        const path = v.p ?? "";
        const val = v.v;

        // New step added (agent-inference, agent-tool-result, etc.)
        if (op === "a" && path.endsWith("/-") && typeof val === "object" && val !== null) {
          if (val.type === "agent-inference") {
            steps.push({
              id: val.id,
              type: "inference",
              index: steps.length,
              startedAt: val.startedAt,
              model: null,
            });
            // Don't seed from initial value — it contains partial XML tags (<lang...)
            // that get replaced by o:"p" ops. Let the stream ops populate content.
          } else if (val.type === "agent-tool-result") {
            toolResults.push({
              id: val.id,
              toolName: val.toolName,
              toolType: val.toolType,
              state: val.state,
              input: val.input,
            });
          }
        }

        // Append text token
        if (op === "x" && path.includes("/content") && typeof val === "string") {
          contentByPath[path] = (contentByPath[path] ?? "") + val;
        }

        // Replace content
        if (op === "p" && path.includes("/content") && typeof val === "string") {
          contentByPath[path] = val;
        }

        // Capture model info from inference step finalization
        if (op === "a" && path.includes("/model") && typeof val === "string") {
          const lastStep = steps.at(-1);
          if (lastStep) lastStep.model = val;
        }
      }
    }

    // Reconstruct AI response text from inference steps
    // Content paths look like /s/N/value/M/content
    // We group by the step index (N) and concatenate value segments
    const inferenceTexts = [];
    const toolCallTexts = [];

    for (const [path, text] of Object.entries(contentByPath)) {
      const match = path.match(/^\/s\/(\d+)\/value\/(\d+)\/content$/);
      if (!match) continue;

      const stepIdx = parseInt(match[1], 10);
      const valueIdx = parseInt(match[2], 10);

      // Check if this is a tool_use content (JSON with tool args) vs plain text
      const trimmed = text.trim();
      if (trimmed.startsWith("{") && (trimmed.includes('"urls"') || trimmed.includes('"pageUrl"') || trimmed.includes('"command"'))) {
        toolCallTexts.push({ stepIdx, valueIdx, content: trimmed });
      } else if (trimmed.length > 0) {
        inferenceTexts.push({ stepIdx, valueIdx, content: trimmed });
      }
    }

    // Sort by step then value index for correct ordering
    inferenceTexts.sort((a, b) => a.stepIdx - b.stepIdx || a.valueIdx - b.valueIdx);

    // Separate chain-of-thought from user-facing response.
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

    const conversation = {
      traceId: meta.traceId,
      spaceId: meta.spaceId,
      userMessage: meta.userMessage,
      assistantMessage: assistantContent || null,
      thinking: thinkingContent,
      toolCalls: toolResults.filter((t) => t.toolName && t.state !== "pending").map((t) => ({
        tool: t.toolName,
        input: t.input,
      })),
      model: steps.find((s) => s.model)?.model ?? null,
      timestamp: Date.now(),
    };

    sendToBackground({ type: "TRANSCRIPT", conversation });
  }

  // ── Message bridge ─────────────────────────────────────────────────────────

  function sendToBackground(payload) {
    try {
      browser.runtime.sendMessage(payload).catch(() => {});
    } catch {
      // Extension context may be invalidated after hot-reload
    }
  }
})();
