#!/bin/bash
set -eo pipefail

# 初始化本地上传目录
APP_WORKSPACE_PATH="${APP_WORKSPACE_PATH:-$PWD}"
mkdir -p "${APP_WORKSPACE_PATH}/assets"

# uv 安装依赖
if [ -n "$PIP_TARGET" ]; then
  echo "[setup] Deploy mode (uv): installing to PIP_TARGET=$PIP_TARGET"
  uv export --frozen --no-hashes --no-dev | uv pip install --no-cache --target "$PIP_TARGET" -r -
else
  echo "[setup] Devbox mode (uv): installing to .venv"
  if [ -f "uv.lock" ]; then
    uv sync --frozen || uv sync
  else
    uv sync
  fi
  touch .venv/.uv_ready
fi
