#!/usr/bin/env python3
"""Run Qwen3-14B Q4_K_M as a temporary API in a Kaggle GPU Notebook."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from urllib.request import urlopen


PORT = 8000
MODEL_REPO = "Qwen/Qwen3-14B-GGUF"
MODEL_FILE = "Qwen3-14B-Q4_K_M.gguf"
SERVED_MODEL_NAME = "qwen3-14b-q4_k_m"
MINIMUM_FREE_VRAM_MIB = 14_000


def build_llama_server_arguments(model_path: str, api_key: str, context_size: int) -> list[str]:
    return [
        "/kaggle/working/llama.cpp/build/bin/llama-server", "--model", model_path,
        "--host", "127.0.0.1", "--port", str(PORT), "--alias", SERVED_MODEL_NAME,
        "--api-key", api_key, "--ctx-size", str(context_size), "--n-gpu-layers", "999",
        "--flash-attn", "on",
    ]


def build_ngrok_arguments(binary: str) -> list[str]:
    return [binary, "http", f"127.0.0.1:{PORT}"]


def public_base_url(value: str) -> str:
    value = value.rstrip("/")
    if not value.startswith("https://"):
        raise ValueError("Tunnel URL must use HTTPS")
    return value if value.endswith("/v1") else f"{value}/v1"


def render_local_env_profile(tunnel_url: str, _api_key: str) -> str:
    return "\n".join([
        f"LLM_BASE_URL={public_base_url(tunnel_url)}",
        f"LLM_MODEL={SERVED_MODEL_NAME}",
        "LLM_API_KEY=<the Kaggle Secret value>",
    ])


def load_secrets(client: object) -> tuple[str, str, str | None]:
    get_secret = getattr(client, "get_secret")
    api_key = get_secret("LLM_API_KEY")
    ngrok_token = get_secret("NGROK_AUTHTOKEN")
    if not api_key or not ngrok_token:
        raise RuntimeError("Kaggle Secrets LLM_API_KEY and NGROK_AUTHTOKEN are required")
    try:
        hf_token = get_secret("HF_TOKEN") or None
    except Exception:
        hf_token = None
    return api_key, ngrok_token, hf_token


def free_vram_mib() -> int:
    output = subprocess.check_output(
        ["nvidia-smi", "--query-gpu=memory.free", "--format=csv,noheader,nounits"], text=True
    )
    return max(int(value.strip()) for value in output.splitlines() if value.strip())


def require_gpu() -> None:
    if not shutil.which("nvidia-smi"):
        raise RuntimeError("No NVIDIA GPU detected. Enable GPU in Kaggle Notebook settings.")
    free_vram = free_vram_mib()
    if free_vram < MINIMUM_FREE_VRAM_MIB:
        raise RuntimeError(f"Qwen3-14B Q4 needs {MINIMUM_FREE_VRAM_MIB} MiB free VRAM; only {free_vram} MiB is available.")


def extract_ngrok_public_base_url(payload: object) -> str:
    if isinstance(payload, dict):
        for tunnel in payload.get("tunnels", []):
            if isinstance(tunnel, dict) and isinstance(tunnel.get("public_url"), str) and tunnel["public_url"].startswith("https://"):
                return public_base_url(tunnel["public_url"])
    raise RuntimeError("ngrok did not expose an HTTPS tunnel")


def wait_for_ngrok_public_base_url(timeout_seconds: int = 60) -> str:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            with urlopen("http://127.0.0.1:4040/api/tunnels", timeout=3) as response:
                return extract_ngrok_public_base_url(json.load(response))
        except (OSError, RuntimeError, json.JSONDecodeError):
            time.sleep(1)
    raise TimeoutError("ngrok did not publish an HTTPS endpoint before the timeout")
