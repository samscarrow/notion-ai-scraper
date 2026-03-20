#!/usr/bin/env bash
# Launcher for notion-mcp-server (node) — injects NOTION_TOKEN via op run.
set -euo pipefail

ENV_LOCAL="$HOME/.env"
ENV_REMOTE="$HOME/.env.remote"
SA_TOKEN_FILE="$HOME/.config/op/notion-forge-sa-token"
SA_TOKEN="${OP_SERVICE_ACCOUNT_TOKEN:-$(cat "$SA_TOKEN_FILE" 2>/dev/null || true)}"

if op account get &>/dev/null 2>&1; then
    exec op run --env-file "$ENV_LOCAL" --no-masking -- node /mnt/fast/npm-global/bin/notion-mcp-server "$@"
else
    export OP_SERVICE_ACCOUNT_TOKEN="$SA_TOKEN"
    exec op run --env-file "$ENV_REMOTE" --no-masking -- node /mnt/fast/npm-global/bin/notion-mcp-server "$@"
fi
