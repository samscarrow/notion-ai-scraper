# Architecture

## How it works

```
notion.so page
      │
      │  fetch("POST /api/v3/runInferenceTranscript")
      │  (patched at document_start)
      ▼
content/interceptor.js
      │  1. Extract user message from request transcript
      │  2. Stream-read NDJSON response
      │  3. Reconstruct AI text from patch ops
      │  browser.runtime.sendMessage()
      ▼
background/service-worker.js
      │  browser.storage.local
      ▼
  Stored conversations
      │  popup message (GET/EXPORT)
      ▼
popup/popup.js  →  download MD / JSON
```

## Real Notion AI Protocol (verified from HAR capture)

### Endpoint

`POST /api/v3/runInferenceTranscript`

### Request shape

```json
{
  "traceId": "uuid",
  "spaceId": "uuid",
  "transcript": [
    { "type": "config", "value": { "model": "avocado-froyo-medium", ... } },
    { "type": "context", "value": { "userName": "...", "timezone": "...", ... } },
    { "type": "user-specified-context", "value": { "pointers": [...] } },
    { "type": "user", "value": [["user message text"]], "userId": "uuid" }
  ]
}
```

User messages use Notion rich-text format: `value: [["text segment 1"], ["segment 2"]]`

### Response shape (NDJSON, not SSE)

Content-Type: `application/x-ndjson`

Each line is a JSON object. Three message types:

| Type | Purpose |
|---|---|
| `patch-start` | Begins a response segment, lists initial steps |
| `patch` | Array of patch operations (`v: [...]`) |
| `record-map` | Notion record sync data |

### Patch operations

| `o` (op) | Meaning | Example |
|---|---|---|
| `a` | Add (new step or append to array) | `{ o: "a", p: "/s/-", v: { type: "agent-inference", ... } }` |
| `x` | Append text token | `{ o: "x", p: "/s/2/value/0/content", v: "Hello" }` |
| `p` | Replace value | `{ o: "p", p: "/s/2/value/0/content", v: "final text" }` |
| `r` | Remove | `{ o: "r", p: "/s/1/durationMs" }` |

### Step types in patches

| Step type | What it contains |
|---|---|
| `agent-inference` | AI thinking/response text, streamed via `o:"x"` token appends |
| `agent-tool-result` | Tool calls: `view`, `update-page-v2`, `search`, etc. |
| `agent-transcript-summary` | Context summary for long conversations |
| `agent-turn-full-record-map` | Initial step marker |

### Text reconstruction

AI text lives at paths like `/s/N/value/M/content`:
- `N` = step index in the response array
- `M` = value segment index (0 = text, 1+ = often tool_use content)
- Tokens arrive via `o:"x"` appends, sometimes replaced by `o:"p"`

Tool-call JSON (containing `"urls"`, `"pageUrl"`, `"command"`) is filtered out of the displayed assistant text.

### Model names

Notion uses internal model codenames: `avocado-froyo-medium` (observed). The `model` field appears in inference step finalization patches.

## Other endpoints observed

| Endpoint | Purpose | Notes |
|---|---|---|
| `syncRecordValuesSpaceInitial` | Block/record sync | Most frequent; not AI-specific |
| `estimateInstructionsTokens` | Token count estimate | Pre-flight for AI calls |
| `markInferenceTranscriptSeen` | Mark chat as read | |
| `saveTransactionsFanout` | Save block edits | |
| `undoAgentOperations` | Undo AI-made changes | |
| `getAssetsJsonV2` | Client asset manifest | |

## Debugging

1. Load the extension: `about:debugging` → This Firefox → Load Temporary Add-on → `manifest.json`
2. Open Browser Console (Ctrl+Shift+J), filter by extension
3. Open any Notion page, trigger Notion AI
4. Watch for `[notion-ai-scraper]` log entries
5. DevTools → Network → filter `runInferenceTranscript` to see raw NDJSON
