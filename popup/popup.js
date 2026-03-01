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

async function render() {
  const convos = await send({ type: "GET_CONVERSATIONS" });
  const list = $("#conversation-list");
  const badge = $("#status-badge");

  badge.textContent = `${convos.length} conversation${convos.length !== 1 ? "s" : ""}`;

  if (convos.length === 0) {
    list.innerHTML = `<p class="empty-state">No conversations captured yet.<br/>Open a Notion page with AI chat.</p>`;
    return;
  }

  list.innerHTML = convos
    .map(
      (c) => `
      <div class="convo-item" data-id="${c.id}">
        <div class="convo-meta">
          <div class="convo-id" title="${c.id}">${c.id}</div>
          <div class="convo-turns">${c.turns?.length ?? 0} turns · ${new Date(c.updatedAt ?? c.createdAt).toLocaleString()}</div>
        </div>
        <div class="convo-actions">
          <button class="btn-icon" data-action="md" data-id="${c.id}">MD</button>
          <button class="btn-icon" data-action="json" data-id="${c.id}">JSON</button>
        </div>
      </div>`
    )
    .join("");
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
