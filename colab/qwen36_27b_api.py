#!/usr/bin/env python3
"""Google Colab launcher for Qwen3.6-27B Q4_K_M via a Cloudflare Named Tunnel.

Upload this file to a GPU Colab runtime and run ``!python qwen36_27b_api.py``.
The script is deliberately self-contained: it compiles CUDA-enabled llama.cpp,
downloads the public GGUF, starts an OpenAI-compatible server, then connects it
to a preconfigured Cloudflare Named Tunnel.
"""

# %% imports and constants
from __future__ import annotations

import getpass
import json
import os
import shlex
import shutil
import subprocess
import sys
import time
from pathlib import Path
from urllib.request import urlopen


PORT = 8000
MODEL_REPO = "unsloth/Qwen3.6-27B-GGUF"
MODEL_FILE = "Qwen3.6-27B-Q4_K_M.gguf"
SERVED_MODEL_NAME = "qwen3.6-27b-q4_k_m"
MINIMUM_FREE_VRAM_MIB = 20_000


def build_llama_server_command(model_path: str, api_key: str, context_size: int) -> str:
    """Return a printable command for llama.cpp's OpenAI-compatible server."""
    command = [
        "/content/llama.cpp/build/bin/llama-server",
        "--model",
        model_path,
        "--host",
        "127.0.0.1",
        "--port",
        str(PORT),
        "--alias",
        SERVED_MODEL_NAME,
        "--api-key",
        api_key,
        "--ctx-size",
        str(context_size),
        "--n-gpu-layers",
        "999",
        "--flash-attn",
        "on",
    ]
    return shlex.join(command)


def run(command: list[str], *, quiet: bool = False) -> None:
    print("+", shlex.join(command))
    subprocess.run(
        command,
        check=True,
        stdout=subprocess.DEVNULL if quiet else None,
        stderr=subprocess.STDOUT if quiet else None,
    )


def free_vram_mib() -> int:
    output = subprocess.check_output(
        ["nvidia-smi", "--query-gpu=memory.free", "--format=csv,noheader,nounits"],
        text=True,
    )
    return max(int(value.strip()) for value in output.splitlines() if value.strip())


def require_gpu() -> None:
    if not shutil.which("nvidia-smi"):
        raise RuntimeError("No NVIDIA GPU detected. In Colab select Runtime > Change runtime type > GPU.")
    free_vram = free_vram_mib()
    if free_vram < MINIMUM_FREE_VRAM_MIB:
        raise RuntimeError(
            f"Q4_K_M needs at least {MINIMUM_FREE_VRAM_MIB} MiB free VRAM; only {free_vram} MiB is available. "
            "Use an L4/A100 Colab runtime or choose a smaller quantization."
        )
    print(f"GPU check passed: {free_vram} MiB free VRAM.")


def install_llama_cpp() -> Path:
    repository = Path("/content/llama.cpp")
    if not repository.exists():
        run(["git", "clone", "--depth", "1", "https://github.com/ggerganov/llama.cpp.git", str(repository)])
    binary = repository / "build/bin/llama-server"
    if not binary.exists():
        run(["cmake", "-S", str(repository), "-B", str(repository / "build"), "-DGGML_CUDA=ON"])
        run(["cmake", "--build", str(repository / "build"), "--config", "Release", "-j", "2", "--target", "llama-server"])
    return binary


def download_model(hf_token: str | None) -> Path:
    run([sys.executable, "-m", "pip", "install", "--quiet", "huggingface_hub"])
    environment = os.environ.copy()
    if hf_token:
        environment["HF_TOKEN"] = hf_token
    script = (
        "from huggingface_hub import hf_hub_download; "
        f"print(hf_hub_download(repo_id={MODEL_REPO!r}, filename={MODEL_FILE!r}))"
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        check=True,
        capture_output=True,
        text=True,
        env=environment,
    )
    return Path(result.stdout.strip().splitlines()[-1])


def install_cloudflared() -> Path:
    binary = Path("/usr/local/bin/cloudflared")
    if not binary.exists():
        run([
            "bash", "-lc",
            "wget -q https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64.deb "
            "-O /tmp/cloudflared.deb && dpkg -i /tmp/cloudflared.deb",
        ])
    return binary


def wait_for_health(api_key: str, timeout_seconds: int = 600) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        request = __import__("urllib.request", fromlist=["Request"]).Request(
            f"http://127.0.0.1:{PORT}/v1/models",
            headers={"Authorization": f"Bearer {api_key}"},
        )
        try:
            with urlopen(request, timeout=5) as response:
                if response.status == 200:
                    print("llama.cpp is ready.")
                    return
        except OSError:
            time.sleep(2)
    raise TimeoutError("llama.cpp did not become ready before the timeout")


def validate_public_base_url(value: str) -> str:
    value = value.rstrip("/")
    if not value.startswith("https://") or not value.endswith("/v1"):
        raise ValueError("Public endpoint must be an HTTPS URL ending in /v1")
    return value


def main() -> None:
    require_gpu()
    public_base_url = validate_public_base_url(input("Cloudflare public HTTPS endpoint (ending in /v1): ").strip())
    tunnel_token = getpass.getpass("Cloudflare Named Tunnel token: ").strip()
    api_key = getpass.getpass("New API key for this LLM service: ").strip()
    hf_token = getpass.getpass("Optional Hugging Face token (press Enter for public download): ").strip() or None
    if not tunnel_token or not api_key:
        raise ValueError("Cloudflare tunnel token and API key are required")

    install_llama_cpp()
    model_path = download_model(hf_token)
    context_size = 8192
    command = build_llama_server_command(str(model_path), api_key, context_size)
    server = subprocess.Popen(command, shell=True)
    try:
        wait_for_health(api_key)
        cloudflared = install_cloudflared()
        tunnel = subprocess.Popen([str(cloudflared), "tunnel", "--no-autoupdate", "run", "--token", tunnel_token])
        print("\nService is live. Copy this profile to your local config/llm_profiles.json:")
        print(json.dumps({"profiles": {"colab-qwen36": {"base_url": public_base_url, "model": SERVED_MODEL_NAME, "api_key_env": "COLAB_LLM_API_KEY"}}}, indent=2))
        print("\nKeep this Colab cell running. Interrupt it to stop both the API and tunnel.")
        tunnel.wait()
    finally:
        server.terminate()


if __name__ == "__main__":
    main()
