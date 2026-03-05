// ==UserScript==
// @name         Notion AI Chat Scraper
// @namespace    https://github.com/git-scarrow/notion-forge
// @version      0.3.2
// @description  Export your Notion AI chat conversations as Markdown or JSON. Captures live streaming responses and full historical threads, including chain-of-thought, tool calls, and model info.
// @author       samscarrow
// @homepageURL  https://github.com/git-scarrow/notion-forge
// @supportURL   https://github.com/git-scarrow/notion-forge/issues
// @match        https://www.notion.so/*
// @match        https://notion.so/*
// @grant        GM_setValue
// @grant        GM_getValue
// @grant        GM_registerMenuCommand
// @grant        unsafeWindow
// @license      MIT
// @run-at       document-start
// ==/UserScript==

/**
 * Uses unsafeWindow to patch the real page's fetch directly.
 * This avoids the sandbox isolation that comes with GM_* grants.
 */
(function () {
  "use strict";

  const STORAGE_KEY = "notion_ai_conversations";
  const LIVE_PATH = "/api/v3/runInferenceTranscript";
  const SYNC_PATH = "/api/v3/syncRecordValuesSpaceInitial";

  // ── Storage ───────────────────────────────────────────────────────────────

  function loadStore() {
    try { return JSON.parse(GM_getValue(STORAGE_KEY, "{}")); } catch (e) { return {}; }
  }

  function saveStore(store) {
    GM_setValue(STORAGE_KEY, JSON.stringify(store));
  }

  // ── Shared helpers ────────────────────────────────────────────────────────

  function cleanText(text) {
    return text
      .replace(/<lang[^>]*\/>/g, "")
      .replace(/<edit_reference[^>]*>[\s\S]*?<\/edit_reference>/g, "")
      .trim();
  }

  function extractRichText(value) {
    if (!Array.isArray(value)) return typeof value === "string" ? value.trim() : null;
    return value.map(function (chunk) {
      if (!Array.isArray(chunk)) return typeof chunk === "string" ? chunk : "";
      var text = chunk[0] || "";
      var annotations = chunk[1];
      if (text === "\u2023" && Array.isArray(annotations)) {
        for (var i = 0; i < annotations.length; i++) {
          var ann = annotations[i];
          if (Array.isArray(ann) && ann.length >= 2) {
            if (ann[0] === "p") return "[page:" + ann[1] + "]";
            if (ann[0] === "u") return "[user:" + ann[1] + "]";
            if (ann[0] === "a") return "[agent:" + ann[1] + "]";
          }
        }
      }
      return text;
    }).join("").trim() || null;
  }

  function extractUserMessage(reqBody) {
    if (!reqBody || !reqBody.transcript) return null;
    var userEntries = reqBody.transcript.filter(function (e) { return e.type === "user"; });
    var last = userEntries[userEntries.length - 1];
    if (!last || !last.value) return null;
    return extractRichText(last.value);
  }

  // ── TRANSCRIPT handler (live chat) ────────────────────────────────────────

  function handleTranscript(ndjsonLines, meta) {
    var contentByPath = {};
    var steps = [];
    var toolResults = [];

    for (var i = 0; i < ndjsonLines.length; i++) {
      var obj = ndjsonLines[i];
      if (obj.type !== "patch") continue;
      var patches = obj.v || [];
      for (var j = 0; j < patches.length; j++) {
        var v = patches[j];
        var op = v.o, path = v.p || "", val = v.v;
        if (op === "a" && path.slice(-2) === "/-" && val !== null && typeof val === "object") {
          if (val.type === "agent-inference") steps.push({ id: val.id, model: null });
          else if (val.type === "agent-tool-result") toolResults.push({ toolName: val.toolName, state: val.state, input: val.input });
        }
        if (op === "x" && path.indexOf("/content") !== -1 && typeof val === "string") {
          contentByPath[path] = (contentByPath[path] || "") + val;
        }
        if (op === "p" && path.indexOf("/content") !== -1 && typeof val === "string") {
          contentByPath[path] = val;
        }
        if (op === "a" && path.indexOf("/model") !== -1 && typeof val === "string") {
          var lastStep = steps[steps.length - 1];
          if (lastStep) lastStep.model = val;
        }
      }
    }

    var inferenceTexts = [];
    var pathKeys = Object.keys(contentByPath);
    for (var k = 0; k < pathKeys.length; k++) {
      var p = pathKeys[k];
      var match = p.match(/^\/s\/(\d+)\/value\/(\d+)\/content$/);
      if (!match) continue;
      var trimmed = contentByPath[p].trim();
      if (trimmed.charAt(0) === "{" && (trimmed.indexOf('"urls"') !== -1 || trimmed.indexOf('"pageUrl"') !== -1 || trimmed.indexOf('"command"') !== -1)) continue;
      if (trimmed.length > 0) inferenceTexts.push({ stepIdx: +match[1], valueIdx: +match[2], content: trimmed });
    }
    inferenceTexts.sort(function (a, b) { return a.stepIdx - b.stepIdx || a.valueIdx - b.valueIdx; });

    var stepGroups = {};
    for (var t = 0; t < inferenceTexts.length; t++) {
      var si = inferenceTexts[t].stepIdx;
      if (!stepGroups[si]) stepGroups[si] = [];
      stepGroups[si].push(inferenceTexts[t]);
    }
    var groupKeys = Object.keys(stepGroups).map(Number).sort(function (a, b) { return a - b; });
    var lastGroup = groupKeys[groupKeys.length - 1];
    var responseParts = [], thinkingParts = [];
    for (var g = 0; g < groupKeys.length; g++) {
      var gk = groupKeys[g];
      var texts = stepGroups[gk].map(function (x) { return x.content; });
      if (gk === lastGroup) {
        responseParts = responseParts.concat(texts);
      } else {
        for (var tt = 0; tt < texts.length; tt++) {
          if (texts[tt].length > 200) thinkingParts.push(texts[tt]);
          else responseParts.push(texts[tt]);
        }
      }
    }

    var assistantContent = cleanText(responseParts.join("\n"));
    var thinkingContent = thinkingParts.join("\n").trim() || null;
    if (!assistantContent && !meta.userMessage) return;

    var key = meta.traceId || ("unknown-" + Date.now());
    var store = loadStore();
    if (!store[key]) {
      store[key] = { id: key, spaceId: meta.spaceId, model: null, turns: [], toolCalls: [], createdAt: Date.now() };
    }
    var entry = store[key];

    if (meta.userMessage) {
      var turns = entry.turns;
      var lastUser = null;
      for (var tu = turns.length - 1; tu >= 0; tu--) {
        if (turns[tu].role === "user") { lastUser = turns[tu]; break; }
      }
      if (!(lastUser && lastUser.content === meta.userMessage && Math.abs((lastUser.timestamp || 0) - Date.now()) < 2000)) {
        entry.turns.push({ role: "user", content: meta.userMessage, timestamp: Date.now() });
      }
    }
    if (assistantContent) {
      var turn = { role: "assistant", content: assistantContent, timestamp: Date.now() };
      if (thinkingContent) turn.thinking = thinkingContent;
      entry.turns.push(turn);
    }
    var modelStep = steps.find(function (s) { return s.model; });
    entry.model = (modelStep && modelStep.model) || entry.model;
    var fc = toolResults.filter(function (t) { return t.toolName && t.state !== "pending"; })
      .map(function (t) { return { tool: t.toolName, input: t.input }; });
    if (fc.length) entry.toolCalls = entry.toolCalls.concat(fc);
    entry.updatedAt = Date.now();
    saveStore(store);
    console.debug("[notion-forge] live: trace " + key + ", turns=" + entry.turns.length);
  }

  // ── NDJSON stream reader ──────────────────────────────────────────────────

  async function handleNDJSONStream(response, meta) {
    var reader = response.body && response.body.getReader();
    if (!reader) return;
    var decoder = new TextDecoder();
    var buffer = "";
    var lines = [];
    try {
      while (true) {
        var result = await reader.read();
        if (result.done) break;
        buffer += decoder.decode(result.value, { stream: true });
        var parts = buffer.split("\n");
        buffer = parts.pop() || "";
        for (var i = 0; i < parts.length; i++) {
          var t = parts[i].trim();
          if (!t) continue;
          try { lines.push(JSON.parse(t)); } catch (e) {}
        }
      }
      if (buffer.trim()) { try { lines.push(JSON.parse(buffer.trim())); } catch (e) {} }
    } catch (err) {
      console.warn("[notion-forge] NDJSON read error:", err);
    }
    if (lines.length) handleTranscript(lines, meta);
  }

  // ── SYNC_RECORDS handler (historical chat) ────────────────────────────────

  function handleSyncResponse(data) {
    var rm = (data && data.recordMap) || {};
    var threads = {};
    var threadEntries = Object.entries(rm.thread || {});
    for (var i = 0; i < threadEntries.length; i++) {
      var id = threadEntries[i][0], rec = threadEntries[i][1];
      var val = (rec && rec.value && rec.value.value) || (rec && rec.value) || {};
      if (val.type === "workflow" && val.messages && val.messages.length) threads[id] = val;
    }
    var messages = {};
    var msgEntries = Object.entries(rm.thread_message || {});
    for (var j = 0; j < msgEntries.length; j++) {
      var mid = msgEntries[j][0], mrec = msgEntries[j][1];
      var mval = (mrec && mrec.value && mrec.value.value) || (mrec && mrec.value) || {};
      if (mval.step || mval.role) messages[mid] = mval;
    }
    if (!Object.keys(threads).length && !Object.keys(messages).length) return;

    var store = loadStore();
    var touchedKeys = [];

    var tids = Object.keys(threads);
    for (var ti = 0; ti < tids.length; ti++) {
      var threadId = tids[ti], thread = threads[threadId];
      var key = "thread-" + threadId;
      if (!store[key]) {
        store[key] = {
          id: key, threadId: threadId, title: (thread.data && thread.data.title) || null,
          spaceId: thread.space_id, model: null, turns: [], toolCalls: [],
          createdAt: thread.created_time || Date.now(), messageOrder: thread.messages,
          createdById: thread.created_by_id, updatedById: thread.updated_by_id,
        };
      } else {
        store[key].messageOrder = thread.messages;
        if (thread.data && thread.data.title) store[key].title = thread.data.title;
        if (thread.updated_by_id) store[key].updatedById = thread.updated_by_id;
      }
      store[key].updatedAt = Date.now();
      if (touchedKeys.indexOf(key) === -1) touchedKeys.push(key);
    }

    var mids = Object.keys(messages);
    for (var mi = 0; mi < mids.length; mi++) {
      var msgId = mids[mi], msg = messages[msgId];
      var step = msg.step || {};
      var mkey = "thread-" + msg.parent_id;
      if (!store[mkey]) {
        store[mkey] = {
          id: mkey, threadId: msg.parent_id, title: null, spaceId: msg.space_id,
          model: null, turns: [], toolCalls: [], createdAt: msg.created_time || Date.now(), messageOrder: [],
        };
      }
      var entry = store[mkey];
      if (!entry._processedMsgIds) entry._processedMsgIds = [];
      if (entry._processedMsgIds.indexOf(msgId) !== -1) continue;

      if (!step.type && msg.role === "editor") {
        entry._processedMsgIds.push(msgId);
      } else if (step.type === "agent-inference") {
        var values = Array.isArray(step.value) ? step.value : [];
        var rParts = [], tParts = [];
        var stepModel = step.model || null;
        for (var vi = 0; vi < values.length; vi++) {
          var item = values[vi];
          if (item.type === "text") { var c = cleanText(item.content || ""); if (c) rParts.push(c); }
          else if (item.type === "thinking") { var tc = (item.content || "").trim(); if (tc) tParts.push(tc); }
        }
        var content = rParts.join("\n").trim();
        if (content) {
          var aturn = { role: "assistant", content: content, msgId: msgId, timestamp: msg.created_time || Date.now(), createdById: msg.created_by_id };
          if (tParts.length) aturn.thinking = tParts.join("\n");
          if (stepModel) { aturn.model = stepModel; entry.model = stepModel; }
          entry.turns.push(aturn);
          if (touchedKeys.indexOf(mkey) === -1) touchedKeys.push(mkey);
        }
        entry._processedMsgIds.push(msgId);
      } else if (step.type === "user" || step.type === "human") {
        var ucontent = extractRichText(step.value);
        if (ucontent) {
          entry.turns.push({ role: "user", content: ucontent, msgId: msgId, timestamp: msg.created_time || Date.now(), createdById: msg.created_by_id });
          entry._processedMsgIds.push(msgId);
          if (touchedKeys.indexOf(mkey) === -1) touchedKeys.push(mkey);
        }
      } else {
        entry._processedMsgIds.push(msgId);
      }
      entry.updatedAt = Date.now();
    }

    // Sort and merge ONLY for touched entries
    for (var tk = 0; tk < touchedKeys.length; tk++) {
      var ae = store[touchedKeys[tk]];
      if (!ae || !ae.messageOrder || !ae.messageOrder.length || ae.turns.length < 2) continue;
      var order = ae.messageOrder;
      ae.turns.sort(function (a, b) {
        var ai = order.indexOf(a.msgId), bi = order.indexOf(b.msgId);
        if (ai === -1 && bi === -1) return (a.timestamp || 0) - (b.timestamp || 0);
        if (ai === -1) return 1; if (bi === -1) return -1;
        return ai - bi;
      });
      ae.turns = mergeConsecutiveTurns(ae.turns);
    }


    saveStore(store);
    console.debug("[notion-forge] sync: " + Object.keys(threads).length + " thread(s), " + Object.keys(messages).length + " msg(s)");
  }

  // ── Fetch intercept (via unsafeWindow) ────────────────────────────────────

  var _fetch = unsafeWindow.fetch.bind(unsafeWindow);

  unsafeWindow.fetch = async function (input, init) {
    var url = typeof input === "string" ? input : (input && input.url) || "";
    var path = "";
    try { path = new URL(url, unsafeWindow.location.origin).pathname; } catch (e) {}

    if (path === LIVE_PATH) {
      var reqBody = null;
      try {
        var raw = (init && init.body) || (input instanceof unsafeWindow.Request ? await input.clone().text() : null);
        if (raw) reqBody = JSON.parse(raw);
      } catch (e) {}
      var response = await _fetch(input, init);
      handleNDJSONStream(response.clone(), {
        userMessage: extractUserMessage(reqBody),
        traceId: reqBody && reqBody.traceId,
        spaceId: reqBody && reqBody.spaceId,
      });
      return response;
    }

    if (path === SYNC_PATH) {
      var syncResponse = await _fetch(input, init);
      syncResponse.clone().json().then(function (data) {
        try { handleSyncResponse(data); } catch (err) {
          console.warn("[notion-forge] sync parse error:", err);
        }
      }).catch(function () {});
      return syncResponse;
    }

    return _fetch(input, init);
  };

  // ── Menu commands ─────────────────────────────────────────────────────────

  function toMarkdown(store) {
    return Object.values(store)
      .filter(function (c) { return c.turns && c.turns.length > 0; })
      .map(function (c) {
        var title = c.title ? " \u2014 " + c.title : "";
        var model = c.model ? " (" + c.model + ")" : "";
        var header = "# Notion AI Chat" + title + model + "\n_ID: " + c.id + "_\n_Captured: " + new Date(c.createdAt).toISOString() + "_\n\n";
        var body = (c.turns || [])
          .map(function (t) { return "**" + (t.role === "assistant" ? "Notion AI" : "You") + "**\n\n" + t.content; })
          .join("\n\n---\n\n");
        var toolSection = "";
        if (c.toolCalls && c.toolCalls.length) {
          toolSection = "\n\n---\n\n<details><summary>Tool calls</summary>\n\n" +
            c.toolCalls.map(function (tc) { return "- **" + tc.tool + "**: `" + JSON.stringify(tc.input).slice(0, 200) + "`"; }).join("\n") +
            "\n</details>";
        }
        return header + body + toolSection;
      }).join("\n\n===\n\n");
  }

  function downloadText(content, filename, mime) {
    var blob = new Blob([content], { type: mime });
    var url = URL.createObjectURL(blob);
    var a = document.createElement("a");
    a.href = url; a.download = filename; a.click();
    URL.revokeObjectURL(url);
  }

  GM_registerMenuCommand("Export All \u2192 Markdown", function () {
    var store = loadStore();
    var withTurns = Object.values(store).filter(function (c) { return c.turns && c.turns.length; });
    if (!withTurns.length) { alert("No conversations captured yet."); return; }
    downloadText(toMarkdown(store), "notion-ai-" + Date.now() + ".md", "text/markdown");
  });

  GM_registerMenuCommand("Export All \u2192 JSON", function () {
    var store = loadStore();
    var withTurns = Object.values(store).filter(function (c) { return c.turns && c.turns.length; });
    if (!withTurns.length) { alert("No conversations captured yet."); return; }
    var data = withTurns.map(function (c) {
      var clean = Object.assign({}, c);
      delete clean._processedMsgIds;
      delete clean.messageOrder;
      return clean;
    });
    downloadText(JSON.stringify(data, null, 2), "notion-ai-" + Date.now() + ".json", "application/json");
  });

  GM_registerMenuCommand("Clear captured conversations", function () {
    if (confirm("Clear all captured Notion AI conversations?")) { saveStore({}); alert("Cleared."); }
  });

  GM_registerMenuCommand("Show capture stats", function () {
    var store = loadStore();
    var convos = Object.values(store).filter(function (c) { return c.turns && c.turns.length; });
    var turns = convos.reduce(function (n, c) { return n + (c.turns ? c.turns.length : 0); }, 0);
    var models = convos.map(function (c) { return c.model; }).filter(Boolean).filter(function (v, i, a) { return a.indexOf(v) === i; });
    var titles = convos.map(function (c) { return c.title; }).filter(Boolean).slice(0, 5);
    alert(
      convos.length + " conversation(s), " + turns + " total turn(s)\n" +
      "Models: " + (models.join(", ") || "unknown") + "\n" +
      (titles.length ? "Recent: " + titles.join(", ") : "")
    );
  });

  console.log("[notion-forge] v0.3.2 active \u2014 watching live + historical chat");
})();
