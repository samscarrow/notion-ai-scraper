/**
 * Content Script Bridge (runs in ISOLATED world)
 *
 * Listens for window.postMessage from the interceptor (MAIN world)
 * and forwards payloads to the background service worker.
 */

"use strict";

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
