#!/bin/bash
set -euo pipefail

if [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then
  exit 0
fi

# Install Python dependencies (main + optional extras + dev group)
uv sync --group dev --extra optional

# Install pyright for type checking (used in linting) and OpenSpec CLI
npm install -g pyright @fission-ai/openspec@latest

# Install unrar for RAR archive support
if ! command -v unrar >/dev/null 2>&1; then
  apt-get update --allow-releaseinfo-change || apt-get update -o APT::Update::Error-Mode=none || true
  apt-get install -y unrar
fi
