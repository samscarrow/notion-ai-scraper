"use strict";

const $ = (sel) => document.querySelector(sel);

async function send(msg) {
  return browser.runtime.sendMessage(msg);
}

function downloadBlob(content, filename, mime = "text/plain") {
  const blob = new Blob([content], { type: mime });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

async function getPageId() {
  const tabs = await browser.tabs.query({ active: true, currentWindow: true });
  const url = tabs[0]?.url ?? "";
  // Notion URLs contain the page/thread ID as a 32-hex-char segment
  const match = url.match(/([0-9a-f]{32})/i);
  return match ? match[1].replace(/-/g, "") : null;
}

async function render() {
  const pageId = await getPageId();
  const convos = await send({ type: "GET_CONVERSATIONS", pageId });
  const list = $("#conversation-list");
  const badge = $("#status-badge");

  badge.textContent = `${convos.length} conversation${convos.length !== 1 ? "s" : ""}`;

  list.replaceChildren();

  if (convos.length === 0) {
    const p = document.createElement("p");
    p.className = "empty-state";
    p.textContent = "No conversations captured yet.";
    p.appendChild(document.createElement("br"));
    p.appendChild(document.createTextNode("Open a Notion page with AI chat."));
    list.appendChild(p);
    return;
  }

  for (const c of convos) {
    const item = document.createElement("div");
    item.className = "convo-item";
    item.dataset.id = c.id;

    const meta = document.createElement("div");
    meta.className = "convo-meta";

    const idEl = document.createElement("div");
    idEl.className = "convo-id";
    idEl.title = c.id;
    idEl.textContent = c.title ?? c.id;

    const turnsEl = document.createElement("div");
    turnsEl.className = "convo-turns";
    turnsEl.textContent = `${c.turns?.length ?? 0} turns · ${new Date(c.updatedAt ?? c.createdAt).toLocaleString()}`;

    meta.appendChild(idEl);
    meta.appendChild(turnsEl);

    const actions = document.createElement("div");
    actions.className = "convo-actions";

    const mdBtn = document.createElement("button");
    mdBtn.className = "btn-icon";
    mdBtn.dataset.action = "md";
    mdBtn.dataset.id = c.id;
    mdBtn.textContent = "MD";

    const jsonBtn = document.createElement("button");
    jsonBtn.className = "btn-icon";
    jsonBtn.dataset.action = "json";
    jsonBtn.dataset.id = c.id;
    jsonBtn.textContent = "JSON";

    actions.appendChild(mdBtn);
    actions.appendChild(jsonBtn);
    item.appendChild(meta);
    item.appendChild(actions);
    list.appendChild(item);
  }
}

// ── Event listeners ─────────────────────────────────────────────────────────

$("#btn-export-all-md").addEventListener("click", async () => {
  const res = await send({ type: "EXPORT_MD" });
  if (res?.ok) downloadBlob(res.content, res.filename);
});

$("#btn-export-all-json").addEventListener("click", async () => {
  const res = await send({ type: "EXPORT_JSON" });
  if (res?.ok) downloadBlob(res.content, res.filename, "application/json");
});

$("#btn-clear").addEventListener("click", async () => {
  if (!confirm("Clear all captured conversations?")) return;
  await send({ type: "CLEAR_CONVERSATIONS" });
  render();
});

document.addEventListener("click", async (e) => {
  const btn = e.target.closest("[data-action]");
  if (!btn) return;
  const { action, id } = btn.dataset;
  if (action === "md") {
    const res = await send({ type: "EXPORT_MD", conversationId: id });
    if (res?.ok) downloadBlob(res.content, res.filename);
  } else if (action === "json") {
    const res = await send({ type: "EXPORT_JSON", conversationId: id });
    if (res?.ok) downloadBlob(res.content, res.filename, "application/json");
  }
});

// Initial render
render();
