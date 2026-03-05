# Notion Agent Instruction Updater

A Python CLI for programmatically updating and publishing [Notion AI Agent](https://www.notion.com/help/custom-agent) instructions, bypassing the public API's inability to access `workflow`-parented blocks.

## Background

Notion AI Agents store their instruction pages under `parent_table: workflow` — an internal type not exposed by the public Notion API. This means tools like Gemini, Claude, and external MCP integrations cannot read or write these pages through normal means.

This tool uses two internal endpoints discovered via browser network interception:
- `POST /api/v3/saveTransactionsFanout` — edit block content
- `POST /api/v3/publishCustomAgentVersion` — deploy the updated agent

Auth is handled by reading your existing Firefox session cookie (`token_v2`) — no separate credentials needed.

## Requirements

- Python 3.11+
- Firefox with an active Notion session (logged in)
- `pyyaml`: `pip install pyyaml`

## Setup

1. Add your agents to `cli/agents.yaml` (the Librarian is pre-populated).

   To find IDs for a new agent, run this in the Notion browser console while on the agent's instruction page:
   ```javascript
   fetch('/api/v3/getRecordValues', {
     method: 'POST', headers: {'Content-Type':'application/json'},
     body: JSON.stringify({requests:[{id:'<page-id-from-url>',table:'block'}]})
   }).then(r=>r.json()).then(d=>{
     const v = d.results[0].value;
     console.log('parent_id (workflow_id):', v.parent_id);
     console.log('block_id:', v.id);
     console.log('space_id:', v.space_id);
   });
   ```

2. Run from the project root.

## Usage

```bash
# Update instructions and publish
python cli/update_agent.py librarian path/to/instructions.md

# Dry-run — print payloads, no API calls
python cli/update_agent.py librarian path/to/instructions.md --dry-run

# Update content without publishing
python cli/update_agent.py librarian path/to/instructions.md --no-publish

# Re-publish without changing content
python cli/update_agent.py librarian --publish-only

# Dump current instructions as Markdown
python cli/update_agent.py librarian --dump
```

## For External Agents (Gemini, Claude)

This CLI is designed to be callable as a tool from AI agent frameworks:

```python
# Example tool definition for an external agent
def update_notion_agent(agent_name: str, instructions_markdown: str) -> str:
    import tempfile, subprocess
    with tempfile.NamedTemporaryFile(mode='w', suffix='.md', delete=False) as f:
        f.write(instructions_markdown)
        tmp = f.name
    result = subprocess.run(
        ['python', 'cli/update_agent.py', agent_name, tmp],
        capture_output=True, text=True,
        cwd='/home/sam/projects/notion-ai-scraper'
    )
    return result.stdout + result.stderr
```

## Supported Markdown

The instruction file should be standard Markdown:

| Syntax | Block type |
|--------|-----------|
| `# Heading` | H1 |
| `## Heading` | H2 |
| `### Heading` | H3 |
| `- item` | Bulleted list |
| `1. item` | Numbered list |
| `` ``` ``code`` ``` `` | Code block |
| `> text` | Callout |
| `---` | Divider |
| Plain text | Paragraph |
| `**bold**` `*italic*` `` `code` `` | Inline formatting |

## Security Note

`token_v2` is a long-lived browser session token that grants full access to your Notion workspace. This tool reads it directly from Firefox's local SQLite store and uses it only for the API calls you trigger. It is never logged, stored, or transmitted elsewhere. If you are concerned, revoke the session in Notion's settings after use.

## FOSS Credits

- [`jamalex/notion-py`](https://github.com/jamalex/notion-py) (MIT) — cookie auth pattern
- [`notion-enhancer/api`](https://github.com/notion-enhancer/api) (MIT) — modern `saveTransactions` envelope format
- [`kjk/notionapi`](https://github.com/kjk/notionapi) (BSD-2) — XHR intercept methodology
