/**
 * Content Script Bridge (runs in ISOLATED world)
 *
 * 1. Injects interceptor.js into the MAIN world via a <script> tag
 *    (more reliable than manifest "world": "MAIN" across Firefox versions)
 * 2. Listens for window.postMessage from the interceptor
 *    and forwards payloads to the background service worker.
 */

"use strict";

// ── Inject interceptor into MAIN world ──────────────────────────────────────

const script = document.createElement("script");
script.src = browser.runtime.getURL("content/interceptor.js");
script.onload = () => script.remove();
(document.documentElement || document.head || document.body).appendChild(script);

// ── Relay messages to background ────────────────────────────────────────────

const MSG_TAG = "__notion_ai_scraper__";

window.addEventListener("message", (event) => {
  if (event.source !== window) return;
  if (event.data?.tag !== MSG_TAG) return;

  const payload = event.data.payload;
  if (!payload?.type) return;

  browser.runtime.sendMessage(payload).catch((err) => {
    console.warn("[notion-ai-scraper] bridge: failed to relay to background:", err);
  });
});
