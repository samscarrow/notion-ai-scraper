#!/usr/bin/env bash
# MCP server launcher — auto-selects local (biometric) or remote (service account) auth.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PYTHON="$SCRIPT_DIR/.venv/bin/python"
SERVER="$SCRIPT_DIR/mcp_server.py"
ENV_LOCAL="$HOME/.env"
ENV_REMOTE="$HOME/.env.remote"
SA_TOKEN_FILE="$HOME/.config/op/notion-forge-sa-token"
SA_TOKEN="${OP_SERVICE_ACCOUNT_TOKEN:-$(cat "$SA_TOKEN_FILE" 2>/dev/null || true)}"

# Try local (biometric) auth first; fall back to service account
if op account get &>/dev/null 2>&1; then
    exec op run --env-file "$ENV_LOCAL" --no-masking -- "$PYTHON" "$SERVER"
else
    export OP_SERVICE_ACCOUNT_TOKEN="$SA_TOKEN"
    exec op run --env-file "$ENV_REMOTE" --no-masking -- "$PYTHON" "$SERVER"
fi
