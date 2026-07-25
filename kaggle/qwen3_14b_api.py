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
from urllib.error import HTTPError
from urllib.request import Request, urlopen


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


def require_authentication() -> None:
    try:
        with urlopen(Request(f"http://127.0.0.1:{PORT}/v1/props"), timeout=5) as response:
            raise RuntimeError(f"Authentication self-check failed: HTTP {response.status}")
    except HTTPError as error:
        if error.code != 401:
            raise RuntimeError(f"Authentication self-check failed: HTTP {error.code}") from error


def run(command: list[str], *, env: dict[str, str] | None = None) -> None:
    print("+", " ".join(command))
    subprocess.run(command, check=True, env=env)


def main() -> None:
    from kaggle_secrets import UserSecretsClient

    require_gpu()
    api_key, ngrok_token, hf_token = load_secrets(UserSecretsClient())
    run(["git", "clone", "--depth", "1", "https://github.com/ggerganov/llama.cpp.git", "/kaggle/working/llama.cpp"])
    run(["cmake", "-S", "/kaggle/working/llama.cpp", "-B", "/kaggle/working/llama.cpp/build", "-DGGML_CUDA=ON"])
    run(["cmake", "--build", "/kaggle/working/llama.cpp/build", "-j", "2", "--target", "llama-server"])
    run([sys.executable, "-m", "pip", "install", "--quiet", "huggingface_hub"])
    environment = os.environ.copy()
    if hf_token:
        environment["HF_TOKEN"] = hf_token
    download = "from huggingface_hub import hf_hub_download; print(hf_hub_download(repo_id=%r, filename=%r))" % (MODEL_REPO, MODEL_FILE)
    model_path = subprocess.check_output([sys.executable, "-c", download], text=True, env=environment).strip().splitlines()[-1]
    server = subprocess.Popen(build_llama_server_arguments(model_path, api_key, 8192))
    config = "/kaggle/working/.ngrok.yml"
    tunnel = None
    try:
        deadline = time.monotonic() + 600
        while time.monotonic() < deadline:
            try:
                with urlopen(Request(f"http://127.0.0.1:{PORT}/v1/models", headers={"Authorization": f"Bearer {api_key}"}), timeout=5):
                    break
            except OSError:
                time.sleep(2)
        else:
            raise TimeoutError("llama.cpp did not become ready")
        require_authentication()
        run(["bash", "-lc", "curl -s https://ngrok-agent.s3.amazonaws.com/ngrok.asc | tee /etc/apt/trusted.gpg.d/ngrok.asc >/dev/null && echo 'deb https://ngrok-agent.s3.amazonaws.com buster main' | tee /etc/apt/sources.list.d/ngrok.list >/dev/null && apt-get update -qq && apt-get install -y ngrok"])
        subprocess.run(["ngrok", "config", "add-authtoken", ngrok_token, "--config", config], check=True, stdout=subprocess.DEVNULL)
        tunnel = subprocess.Popen(build_ngrok_arguments("ngrok") + ["--config", config])
        print("Service is live. Set these values in local .env:")
        print(render_local_env_profile(wait_for_ngrok_public_base_url(), api_key))
        tunnel.wait()
    finally:
        if tunnel and tunnel.poll() is None:
            tunnel.terminate()
        server.terminate()
        Path(config).unlink(missing_ok=True)


if __name__ == "__main__":
    main()
