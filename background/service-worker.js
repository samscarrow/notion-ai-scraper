/**
 * Notion AI Scraper — Background Service Worker
 *
 * Receives parsed transcript data via native webRequest interception,
 * stores conversation turns, and handles export.
 */

"use strict";

const LIVE_PATH = "/api/v3/runInferenceTranscript";
const SYNC_PATH = "/api/v3/syncRecordValuesSpaceInitial";

// A map to store request meta by requestId
const requestMetaMap = new Map();

// ── ID helpers ───────────────────────────────────────────────────────────────

function normId(id) {
  return (id ?? "").replace(/^thread-/, "").replace(/-/g, "").toLowerCase();
}

// ── Sharded Storage ─────────────────────────────────────────────────────────

const STORAGE_KEY = "notion_ai_conversations"; // Legacy key
const INDEX_KEY = "notion_ai_index";           // Metadata for all threads
const THREAD_PREFIX = "thread_data:";          // Data for individual threads

async function loadIndex() {
  const res = await browser.storage.local.get(INDEX_KEY);
  return res[INDEX_KEY] || {};
}

async function getThread(id) {
  const key = `${THREAD_PREFIX}${normId(id)}`;
  const res = await browser.storage.local.get(key);
  return res[key] || null;
}

async function saveThread(id, data) {
  const nid = normId(id);
  const key = `${THREAD_PREFIX}${nid}`;
  const index = await loadIndex();
  index[nid] = {
    id: data.id,
    threadId: data.threadId,
    pageId: data.pageId,
    title: data.title,
    turnsCount: data.turns?.length || 0,
    updatedAt: data.updatedAt || Date.now(),
    createdAt: data.createdAt || Date.now(),
  };
  await browser.storage.local.set({ [key]: data, [INDEX_KEY]: index });
}

async function migrateToShardedStorage() {
  const res = await browser.storage.local.get(STORAGE_KEY);
  const monolith = res[STORAGE_KEY];
  if (!monolith) return;

  console.log(`[notion-forge] Migration started: splitting ${Object.keys(monolith).length} conversations...`);
  const index = {};
  const updates = {};
  for (const [id, data] of Object.entries(monolith)) {
    const nid = normId(id);
    const key = `${THREAD_PREFIX}${nid}`;
    updates[key] = data;
    index[nid] = {
      id: data.id, threadId: data.threadId, pageId: data.pageId, title: data.title,
      turnsCount: data.turns?.length || 0, updatedAt: data.updatedAt || data.createdAt || Date.now(),
      createdAt: data.createdAt || Date.now(),
    };
  }
  updates[INDEX_KEY] = index;
  await browser.storage.local.set(updates);
  await browser.storage.local.remove(STORAGE_KEY);
  console.log("[notion-forge] Migration complete.");
}

// ── Networking & Interception ───────────────────────────────────────────────

async function getTabContext(tabId) {
  if (tabId < 0) return { pageId: null, threadId: null };
  try {
    const tab = await browser.tabs.get(tabId);
    if (!tab || !tab.url) return { pageId: null, threadId: null };
    const url = new URL(tab.url);
    const threadId = url.searchParams.get("t") || url.searchParams.get("at");
    const pageMatch = url.pathname.match(/([0-9a-f]{32})/i);
    return {
      pageId: pageMatch ? pageMatch[1].toLowerCase() : null,
      threadId: threadId ? threadId.toLowerCase() : null,
    };
  } catch (e) { return { pageId: null, threadId: null }; }
}

function setMeta(requestId, meta) {
  requestMetaMap.set(requestId, meta);
  // Auto-expire after 60 s to prevent leaks from cancelled/errored requests
  setTimeout(() => requestMetaMap.delete(requestId), 60_000);
}

browser.webRequest.onBeforeRequest.addListener(
  async (details) => {
    if (details.method !== "POST") return;
    try {
      const url = new URL(details.url);
      const ctx = await getTabContext(details.tabId);
      if (url.pathname === LIVE_PATH) {
        if (details.requestBody?.raw?.length) {
          const raw = new TextDecoder().decode(details.requestBody.raw[0].bytes);
          const reqBody = JSON.parse(raw);
          setMeta(details.requestId, {
            userMessage: extractUserMessage(reqBody),
            traceId: reqBody.traceId, spaceId: reqBody.spaceId,
            pageId: ctx.pageId, threadId: ctx.threadId,
          });
        }
      } else if (url.pathname === SYNC_PATH) {
        setMeta(details.requestId, { currentPageId: ctx.pageId });
      }
    } catch (e) {}
  },
  { urls: ["*://*.notion.so/*"] },
  ["blocking", "requestBody"]
);

browser.webRequest.onHeadersReceived.addListener(
  (details) => {
    try {
      const url = new URL(details.url);
      if (url.pathname === LIVE_PATH) {
        const meta = requestMetaMap.get(details.requestId) || {};
        const filter = browser.webRequest.filterResponseData(details.requestId);
        const decoder = new TextDecoder("utf-8");
        let buffer = "", lines = [];
        filter.ondata = (event) => {
          filter.write(event.data);
          buffer += decoder.decode(event.data, { stream: true });
          let parts = buffer.split("\n");
          buffer = parts.pop() || "";
          for (const part of parts) {
            const t = part.trim();
            if (t) try { lines.push(JSON.parse(t)); } catch(e) {}
          }
        };
        filter.onstop = () => {
          filter.close();
          if (buffer.trim()) try { lines.push(JSON.parse(buffer.trim())); } catch(e) {}
          if (lines.length) processNDJSONLines(lines, meta);
          requestMetaMap.delete(details.requestId);
        };
      } else if (url.pathname === SYNC_PATH) {
        const meta = requestMetaMap.get(details.requestId) || {};
        const filter = browser.webRequest.filterResponseData(details.requestId);
        const decoder = new TextDecoder("utf-8");
        let buffer = "";
        filter.ondata = (event) => {
          filter.write(event.data);
          buffer += decoder.decode(event.data, { stream: true });
        };
        filter.onstop = () => {
          filter.close();
          try { handleSyncResponse(JSON.parse(buffer), meta.currentPageId); } catch(e) {}
          requestMetaMap.delete(details.requestId);
        };
      }
    } catch (e) {}
  },
  { urls: ["*://*.notion.so/*"] },
  ["blocking"]
);

// ── Handlers ────────────────────────────────────────────────────────────────

function extractUserMessage(reqBody) {
  if (!reqBody?.transcript) return null;
  let last = null;
  for (const e of reqBody.transcript) {
    if (e.type === "user") last = e;
  }
  return last?.value ? extractRichText(last.value) : null;
}

async function handleTranscript(convo) {
  if (!convo) return;
  const traceId = convo.traceId;
  const tid = convo.threadId ? normId(convo.threadId) : null;
  let key = tid ? `thread-${tid}` : (traceId ?? `unknown-${Date.now()}`);

  if (tid && traceId && traceId !== `thread-${tid}`) {
    const traceEntry = await getThread(traceId);
    if (traceEntry) {
      traceEntry.id = `thread-${tid}`;
      traceEntry.threadId = convo.threadId;
      await saveThread(tid, traceEntry);
      await browser.storage.local.remove(`${THREAD_PREFIX}${normId(traceId)}`);
      const index = await loadIndex();
      delete index[normId(traceId)];
      await browser.storage.local.set({ [INDEX_KEY]: index });
      key = `thread-${tid}`;
    }
  }

  let entry = await getThread(key);
  if (!entry) {
    entry = {
      id: key, spaceId: convo.spaceId, pageId: convo.pageId, threadId: convo.threadId,
      model: convo.model, turns: [], toolCalls: [], createdAt: Date.now(),
    };
  } else {
    if (convo.pageId && !entry.pageId) entry.pageId = convo.pageId;
    if (convo.threadId && !entry.threadId) entry.threadId = convo.threadId;
  }

  if (convo.userMessage) {
    const lastUserTurn = entry.turns.findLast((t) => t.role === "user");
    if (!(lastUserTurn && lastUserTurn.content === convo.userMessage && Math.abs((lastUserTurn.timestamp ?? 0) - convo.timestamp) < 2000)) {
      entry.turns.push({ role: "user", content: convo.userMessage, timestamp: convo.timestamp });
    }
  }
  if (convo.assistantMessage || convo.toolCalls?.length) {
    const turn = { role: "assistant", content: convo.assistantMessage || "", model: convo.model, timestamp: convo.timestamp };
    if (convo.thinking) turn.thinking = convo.thinking;
    if (convo.toolCalls?.length) turn.toolCalls = convo.toolCalls;
    entry.turns.push(turn);
  }
  entry.model = convo.model ?? entry.model;
  entry.updatedAt = Date.now();
  await saveThread(key, entry);
}

async function handleSyncRecords(threads, messages, currentPageId) {
  const touchedKeys = new Set();
  const batch = {};

  // Pre-fetch all unique thread keys in one storage call
  const threadKeys = new Set();
  for (const tid of Object.keys(threads ?? {})) threadKeys.add(`${THREAD_PREFIX}${normId(`thread-${tid}`)}`);
  for (const msg of Object.values(messages ?? {})) threadKeys.add(`${THREAD_PREFIX}${normId(`thread-${msg.parent_id}`)}`);
  if (threadKeys.size) {
    const fetched = await browser.storage.local.get([...threadKeys]);
    for (const [k, v] of Object.entries(fetched)) {
      if (v) batch[k.replace(THREAD_PREFIX, "thread-")] = v;
    }
  }

  const getBatch = async (k) => {
    if (batch[k]) return batch[k];
    const d = await getThread(k);
    if (d) batch[k] = d;
    return d;
  };

  for (const [tid, thread] of Object.entries(threads ?? {})) {
    const key = `thread-${tid}`;
    let entry = await getBatch(key);
    if (!entry) {
      entry = {
        id: key, threadId: tid, pageId: thread._parentPageId || currentPageId,
        title: thread.data?.title, spaceId: thread.space_id, model: null,
        turns: [], toolCalls: [], createdAt: thread.created_time || Date.now(),
        messageOrder: thread.messages,
        createdById: thread.created_by_id, updatedById: thread.updated_by_id,
      };
      batch[key] = entry;
    } else {
      entry.messageOrder = thread.messages;
      if (thread.data?.title) entry.title = thread.data.title;
      if (!entry.pageId) entry.pageId = thread._parentPageId || currentPageId;
      if (thread.updated_by_id) entry.updatedById = thread.updated_by_id;
    }
    entry.updatedAt = Date.now();
    touchedKeys.add(key);
  }

  for (const [mid, msg] of Object.entries(messages ?? {})) {
    const key = `thread-${msg.parent_id}`;
    let entry = await getBatch(key);
    if (!entry) {
      entry = {
        id: key, threadId: msg.parent_id, pageId: currentPageId, spaceId: msg.space_id,
        turns: [], toolCalls: [], createdAt: msg.created_time, messageOrder: [],
      };
      batch[key] = entry;
    }
    if (entry._processedMsgIds?.includes(mid)) continue;
    const step = msg.step || {};
    if (step.type === "agent-inference") {
      const turn = extractInferenceTurn(step);
      if (turn) {
        turn.msgId = mid; turn.timestamp = msg.created_time;
        if (msg.created_by_id) turn.createdById = msg.created_by_id;
        entry.turns.push(turn); touchedKeys.add(key);
      }
    } else if (step.type === "user" || step.type === "human") {
      const content = extractRichText(step.value);
      if (content) {
        const turnData = { role: "user", content, msgId: mid, timestamp: msg.created_time };
        if (msg.created_by_id) turnData.createdById = msg.created_by_id;
        entry.turns.push(turnData);
        touchedKeys.add(key);
      }
    } else if (step.type === "agent-tool-result" && step.state === "applied" && step.toolName) {
      const toolCall = { tool: step.toolName, input: step.input ?? {}, result: step.result };
      const parentIdx = entry.turns.findIndex(t => t.msgId === step.agentStepId);
      if (parentIdx >= 0) {
        if (!entry.turns[parentIdx].toolCalls) entry.turns[parentIdx].toolCalls = [];
        entry.turns[parentIdx].toolCalls.push(toolCall);
      } else {
        if (!entry.toolCalls) entry.toolCalls = [];
        entry.toolCalls.push(toolCall);
      }
      touchedKeys.add(key);
    }
    if (!entry._processedMsgIds) entry._processedMsgIds = [];
    entry._processedMsgIds.push(mid);
    entry.updatedAt = Date.now();
  }

  for (const key of touchedKeys) {
    const e = batch[key];
    if (e.turns.length >= 2) {
      if (e.messageOrder?.length) {
        e.turns.sort((a, b) => {
          const ai = e.messageOrder.indexOf(a.msgId), bi = e.messageOrder.indexOf(b.msgId);
          return (ai === -1 || bi === -1) ? (a.timestamp - b.timestamp) : (ai - bi);
        });
      }
      e.turns = mergeConsecutiveTurns(e.turns);
    }
    if (!e.title) {
      const first = e.turns.find(t => t.role === "user")?.content;
      if (first) e.title = first.slice(0, 60).replace(/\s+/g, " ").trim();
    }
    await saveThread(key, e);
  }
}

// ── Text processing ──────────────────────────────────────────────────────────

function cleanText(text) {
  return text.replace(/<lang[^>]*\/>/g, "").replace(/<edit_reference[^>]*>[\s\S]*?<\/edit_reference>/g, "").trim();
}

function extractRichText(value) {
  if (!Array.isArray(value)) return typeof value === "string" ? value.trim() : null;
  return value.map(chunk => {
    if (!Array.isArray(chunk)) return typeof chunk === "string" ? chunk : "";
    const text = chunk[0] ?? "", ann = chunk[1];
    if (text === "\u2023" && Array.isArray(ann)) {
      for (const a of ann) if (Array.isArray(a) && a.length >= 2) return `[${a[0]}:${a[1]}]`;
    }
    return text;
  }).join("").trim() || null;
}

function extractInferenceTurn(step) {
  const vals = step.value || [];
  let resp = [], think = null;
  for (const v of vals) {
    if (v.type === "text") { const c = cleanText(v.content || ""); if (c) resp.push(c); }
    else if (v.type === "thinking") think = (v.content || "").trim();
  }
  if (!resp.length) return null;
  const turn = { role: "assistant", content: resp.join("\n") };
  if (think) turn.thinking = think;
  if (step.model) turn.model = step.model;
  return turn;
}

function mergeConsecutiveTurns(turns) {
  const m = [];
  for (const t of turns) {
    const p = m.at(-1);
    if (p && p.role === t.role && t.role === "assistant") {
      p.content += "\n\n" + t.content;
      if (t.thinking) p.thinking = (p.thinking ? p.thinking + "\n\n" : "") + t.thinking;
      if (t.toolCalls?.length) p.toolCalls = [...(p.toolCalls || []), ...t.toolCalls];
    } else m.push({ ...t });
  }
  return m;
}

function handleSyncResponse(data, currentPageId) {
  const rm = data?.recordMap || {};
  const threads = {}, messages = {};
  for (const id in rm.thread || {}) {
    const v = rm.thread[id].value?.value || rm.thread[id].value || {};
    if (v.type === "workflow") { v._parentPageId = v.parent_id?.replace(/-/g, ""); threads[id] = v; }
  }
  for (const id in rm.thread_message || {}) {
    const v = rm.thread_message[id].value?.value || rm.thread_message[id].value || {};
    if (v.step || v.role) messages[id] = v;
  }
  if (Object.keys(threads).length || Object.keys(messages).length) handleSyncRecords(threads, messages, currentPageId);
}

function processNDJSONLines(lines, meta) {
  const contentByPath = {}, steps = [], streamingTools = [], recordMapTools = [];
  const seenToolIds = new Set();
  for (const obj of lines) {
    if (obj.type === "record-map") {
      // record-map lines contain authoritative completed tool call records
      for (const [mid, mrec] of Object.entries(obj.recordMap?.thread_message || {})) {
        if (seenToolIds.has(mid)) continue;
        const v = mrec.value?.value || mrec.value || {};
        const step = v.step;
        if (step?.type === "agent-tool-result" && step.state === "applied" && step.toolName) {
          seenToolIds.add(mid);
          recordMapTools.push({ tool: step.toolName, input: step.input ?? {}, result: step.result });
        }
      }
    }
    if (obj.type !== "patch") continue;
    for (const v of obj.v || []) {
      const op = v.o, path = v.p || "", val = v.v;
      if (op === "a" && path.endsWith("/-") && typeof val === "object" && val) {
        if (val.type === "agent-inference") steps.push({ id: val.id, model: null });
        else if (val.type === "agent-tool-result") streamingTools.push({ toolName: val.toolName, state: val.state, input: val.input });
      }
      if ((op === "x" || op === "p") && path.includes("/content") && typeof val === "string") contentByPath[path] = (op === "x" ? (contentByPath[path] || "") + val : val);
      if (op === "a" && path.includes("/model") && typeof val === "string" && steps.length) steps.at(-1).model = val;
    }
  }
  const texts = [];
  for (const p in contentByPath) {
    const m = p.match(/^\/s\/(\d+)\/value\/(\d+)\/content$/);
    if (m && contentByPath[p].trim().length) texts.push({ s: +m[1], v: +m[2], c: contentByPath[p].trim() });
  }
  texts.sort((a, b) => a.s - b.s || a.v - b.v);
  const groups = {};
  for (const t of texts) { if (!groups[t.s]) groups[t.s] = []; groups[t.s].push(t); }
  const sortedKeys = Object.keys(groups).map(Number).sort((a, b) => a - b);
  const last = sortedKeys.at(-1);
  let resp = [], think = [];
  for (const k of sortedKeys) {
    const txts = groups[k].map(x => x.c);
    if (k === last) resp.push(...txts);
    else think.push(...txts.filter(x => x.length > 200));
  }
  const assistant = cleanText(resp.join("\n"));
  // Prefer record-map tool calls (complete, applied state); fall back to streaming patches
  const toolCalls = recordMapTools.length
    ? recordMapTools
    : streamingTools.filter(t => t.toolName && t.state !== "pending").map(t => ({ tool: t.toolName, input: t.input }));
  if (!assistant && !meta.userMessage && !toolCalls.length) return;
  handleTranscript({
    traceId: meta.traceId, spaceId: meta.spaceId, pageId: meta.pageId, threadId: meta.threadId,
    userMessage: meta.userMessage, assistantMessage: assistant || null, thinking: think.join("\n") || null,
    toolCalls, model: steps.find(s => s.model)?.model || null, timestamp: Date.now(),
  });
}

// ── Messaging ───────────────────────────────────────────────────────────────

browser.runtime.onMessage.addListener((msg) => {
  if (msg.type === "GET_CONVERSATIONS") return getConversations(msg.pageId, msg.threadId);
  if (msg.type === "CLEAR_CONVERSATIONS") return clearConversations();
  if (msg.type === "EXPORT_MD") return exportMarkdown(msg.conversationId, msg.pageId, msg.threadId);
  if (msg.type === "EXPORT_JSON") return exportJSON(msg.conversationId, msg.pageId, msg.threadId);
  if (msg.type === "AGENT_ACTION") return runAgentAction(msg.payload);
  if (msg.type === "AGENT_STATUS") return Promise.resolve({ ...agentOp });
});

// ── Agent operations (durable — survives popup close) ───────────────────────

let agentOp = { state: "idle", label: null, log: [], result: null, error: null };

function agentOpReset(label) {
  agentOp = { state: "running", label, log: [], result: null, error: null, startedAt: Date.now() };
}

async function findNotionTab() {
  const tabs = await browser.tabs.query({ url: "*://*.notion.so/*" });
  if (!tabs.length) return null;
  return tabs.find(t => t.active) ?? tabs[0];
}

async function runAgentAction(payload) {
  const tab = await findNotionTab();
  if (!tab) throw new Error("No Notion tab found. Open Notion first.");

  agentOpReset(payload._label || payload.action);

  // Fire-and-forget: run in background, don't block the message response
  executeAgentAction(tab.id, payload).catch(() => {});

  return { state: "running" };
}

async function executeAgentAction(tabId, payload) {
  try {
    const results = await browser.scripting.executeScript({
      target: { tabId },
      func: notionApiAction,
      args: [payload],
    });
    const r = results[0];
    if (r.error) throw new Error(r.error.message ?? String(r.error));
    agentOp.state = "done";
    agentOp.result = r.result;
    agentOp.log = r.result?.log || [];
  } catch (e) {
    agentOp.state = "error";
    agentOp.error = e.message;
    agentOp.log.push(`Error: ${e.message}`);
  }
}

// The function injected into the Notion tab — must be self-contained
async function notionApiAction(payload) {
  const { action, blockId, spaceId, workflowId, newBlocks } = payload;
  const log = [];

  function uuid() { return crypto.randomUUID(); }

  async function post(endpoint, body) {
    const r = await fetch(`/api/v3/${endpoint}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      body: JSON.stringify(body),
    });
    if (!r.ok) {
      const text = await r.text().catch(() => "");
      throw new Error(`${endpoint} → HTTP ${r.status}: ${text.slice(0, 200)}`);
    }
    return r.json();
  }

  function tx(operations) {
    return {
      requestId: uuid(),
      transactions: [{ id: uuid(), spaceId, debug: { userAction: "extension.agent_manager" }, operations }],
    };
  }

  function ptr(id) { return { table: "block", id, spaceId }; }

  async function getBlockChildren(pid) {
    const data = await post("loadPageChunk", { pageId: pid, limit: 500, cursor: { stack: [] }, chunkNumber: 0, verticalColumns: false });
    const blocks = data?.recordMap?.block ?? {};
    return { children: blocks[pid]?.value?.content ?? [], recordMap: data.recordMap };
  }

  async function deleteBlock(bid, parentId) {
    await post("saveTransactionsFanout", tx([
      { pointer: ptr(bid),      path: [],          command: "update",     args: { alive: false } },
      { pointer: ptr(parentId), path: ["content"], command: "listRemove", args: { id: bid } },
    ]));
  }

  async function insertBlock(block, parentId, afterId) {
    const bid = uuid(), now = Date.now();
    const { children, ...rest } = block;
    const value = { id: bid, parent_id: parentId, parent_table: "block", alive: true, created_time: now, last_edited_time: now, space_id: spaceId, ...rest };
    await post("saveTransactionsFanout", tx([
      { pointer: ptr(bid),      path: [],          command: "set",       args: value },
      { pointer: ptr(parentId), path: ["content"], command: "listAfter", args: afterId ? { id: bid, after: afterId } : { id: bid } },
    ]));
    if (children?.length) {
      let childAfter = null;
      for (const child of children) childAfter = await insertBlock(child, bid, childAfter);
    }
    return bid;
  }

  if (action === "dump") {
    const { recordMap } = await getBlockChildren(blockId);
    log.push("Fetched block tree.");
    return { success: true, recordMap, log };
  }

  if (action === "update") {
    const { children } = await getBlockChildren(blockId);
    log.push(`Deleting ${children.length} block(s)…`);
    for (const cid of children) await deleteBlock(cid, blockId);
    log.push(`Inserting ${newBlocks.length} block(s)…`);
    let afterId = null;
    for (const block of newBlocks) afterId = await insertBlock(block, blockId, afterId);
    log.push("Content updated.");
    if (workflowId) {
      log.push("Publishing…");
      const result = await post("publishCustomAgentVersion", { workflowId, spaceId });
      log.push(`Published  artifact=${result.workflowArtifactId}  v${result.version}`);
      return { success: true, publishResult: result, log };
    }
    return { success: true, log };
  }

  if (action === "publish") {
    const result = await post("publishCustomAgentVersion", { workflowId, spaceId });
    log.push(`Published  artifact=${result.workflowArtifactId}  v${result.version}`);
    return { success: true, publishResult: result, log };
  }

  throw new Error(`Unknown action: ${action}`);
}

async function clearConversations() {
  const all = await browser.storage.local.get(null);
  const keys = Object.keys(all).filter(k => k === INDEX_KEY || k.startsWith(THREAD_PREFIX));
  if (keys.length) await browser.storage.local.remove(keys);
  return { ok: true };
}

async function getConversations(pageId, threadId) {
  const index = await loadIndex();
  const pid = pageId ? normId(pageId) : null;
  const tid = threadId ? normId(threadId) : null;
  const all = Object.values(index);
  const filtered = (pid || tid) ? all.filter(m => {
    if (tid) return normId(m.threadId) === tid || normId(m.id) === tid;
    return normId(m.pageId) === pid;
  }) : all;
  return filtered.sort((a, b) => b.updatedAt - a.updatedAt);
}

async function loadExportTargets(id, pid, tid) {
  return id
    ? [await getThread(id)]
    : await Promise.all((await getConversations(pid, tid)).map(m => getThread(m.id)));
}

async function exportMarkdown(id, pid, tid) {
  const targets = (await loadExportTargets(id, pid, tid)).filter(Boolean);
  const md = targets.map(c => {
    const head = `# ${c.title || "Notion AI Chat"}${c.model ? ` (${c.model})` : ""}\n_ID: ${c.id}_\n\n`;
    const body = c.turns.map(t => {
      const label = `**${t.role === "assistant" ? "Notion AI" : "You"}**`;
      let text = t.content || "";
      if (t.toolCalls?.length) {
        const calls = t.toolCalls.map(tc => `- \`${tc.tool}\`: ${JSON.stringify(tc.input || {})}`).join("\n");
        text += (text ? "\n\n" : "") + `**Tool calls:**\n${calls}`;
      }
      return `${label}\n\n${text}`;
    }).join("\n\n---\n\n");
    return head + body;
  }).join("\n\n===\n\n");
  return { ok: true, content: md, filename: targets.length === 1 ? makeFilename(targets[0], "md") : makeFilename(null, "md") };
}

async function exportJSON(id, pid, tid) {
  const targets = (await loadExportTargets(id, pid, tid)).filter(Boolean);
  const cleaned = JSON.stringify(targets.length === 1 ? targets[0] : targets, (k, v) => ["_processedMsgIds", "messageOrder"].includes(k) ? undefined : v, 2);
  return { ok: true, content: cleaned, filename: targets.length === 1 ? makeFilename(targets[0], "json") : makeFilename(null, "json") };
}

function makeFilename(c, ext) {
  if (!c) return `notion-ai-export-${Date.now()}.${ext}`;
  const name = (c.title || c.turns?.[0]?.content || "chat").slice(0, 40).toLowerCase().replace(/[^a-z0-9]+/g, "-");
  return `${name}-${Date.now()}.${ext}`;
}

migrateToShardedStorage();
console.log("[notion-forge] service worker loaded (sharded)");
