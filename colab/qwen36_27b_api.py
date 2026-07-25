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
import re
import select
import shlex
import shutil
import subprocess
import sys
import time
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen


PORT = 8000
MODEL_REPO = "unsloth/Qwen3.6-27B-GGUF"
MODEL_FILE = "Qwen3.6-27B-Q4_K_M.gguf"
SERVED_MODEL_NAME = "qwen3.6-27b-q4_k_m"
MINIMUM_FREE_VRAM_MIB = 20_000
TUNNEL_PROVIDERS = {"1": "cloudflare", "2": "ngrok", "3": "quick"}


def build_llama_server_arguments(model_path: str, api_key: str, context_size: int) -> list[str]:
    """Return process arguments for llama.cpp's OpenAI-compatible server."""
    return [
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


def build_llama_server_command(model_path: str, api_key: str, context_size: int) -> str:
    """Return a printable version of the llama.cpp server arguments."""
    return shlex.join(build_llama_server_arguments(model_path, api_key, context_size))


def run(command: list[str], *, quiet: bool = False, redacted_values: set[str] | None = None) -> None:
    printable = ["[REDACTED]" if value in (redacted_values or set()) else value for value in command]
    print("+", shlex.join(printable))
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


def install_ngrok() -> Path:
    binary = Path("/usr/local/bin/ngrok")
    if not binary.exists():
        run([
            "bash", "-lc",
            "curl -s https://ngrok-agent.s3.amazonaws.com/ngrok.asc "
            "| tee /etc/apt/trusted.gpg.d/ngrok.asc >/dev/null && "
            "echo 'deb https://ngrok-agent.s3.amazonaws.com buster main' "
            "| tee /etc/apt/sources.list.d/ngrok.list >/dev/null && "
            "apt-get update -qq && apt-get install -y ngrok",
        ])
    return binary


def wait_for_health(api_key: str, timeout_seconds: int = 600) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        request = Request(
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


def require_authentication() -> None:
    """Refuse to publish the server unless an unauthenticated protected route returns 401."""
    request = Request(f"http://127.0.0.1:{PORT}/v1/props")
    try:
        with urlopen(request, timeout=5) as response:
            raise RuntimeError(
                f"llama.cpp authentication self-check failed: unauthenticated /v1/props returned {response.status}"
            )
    except HTTPError as error:
        if error.code == 401:
            print("llama.cpp authentication self-check passed.")
            return
        raise RuntimeError(
            f"llama.cpp authentication self-check failed: /v1/props returned HTTP {error.code}"
        ) from error
    except OSError as error:
        raise RuntimeError("llama.cpp authentication self-check could not reach /v1/props") from error


def validate_public_base_url(value: str) -> str:
    value = value.rstrip("/")
    if not value.startswith("https://") or not value.endswith("/v1"):
        raise ValueError("Public endpoint must be an HTTPS URL ending in /v1")
    return value


def public_base_url(value: str) -> str:
    """Normalize a tunnel URL to the OpenAI-compatible `/v1` base URL."""
    value = value.rstrip("/")
    if not value.startswith("https://"):
        raise ValueError("Tunnel URL must use HTTPS")
    return value if value.endswith("/v1") else f"{value}/v1"


def select_tunnel_provider(value: str) -> str:
    """Return the selected tunnel provider or explain the available choices."""
    try:
        return TUNNEL_PROVIDERS[value.strip()]
    except KeyError as error:
        raise ValueError("Tunnel mode must be 1 (Cloudflare), 2 (ngrok), or 3 (Cloudflare Quick Tunnel)") from error


def build_ngrok_arguments(binary: str) -> list[str]:
    """Return the ngrok HTTP tunnel command for the loopback-only API server."""
    return [binary, "http", f"127.0.0.1:{PORT}"]


def build_named_tunnel_arguments(binary: str, tunnel_token: str) -> list[str]:
    """Return the Cloudflare Named Tunnel command."""
    return [binary, "tunnel", "--no-autoupdate", "run", "--token", tunnel_token]


def build_quick_tunnel_arguments(binary: str) -> list[str]:
    """Return the Cloudflare Quick Tunnel command for the loopback-only API server."""
    return [binary, "tunnel", "--no-autoupdate", "--url", f"http://127.0.0.1:{PORT}"]


def extract_quick_tunnel_url(line: str) -> str | None:
    """Extract and normalize the generated TryCloudflare URL from one log line."""
    match = re.search(r"https://[a-z0-9-]+\.trycloudflare\.com", line)
    return public_base_url(match.group(0)) if match else None


def extract_ngrok_public_base_url(payload: object) -> str:
    """Return the HTTPS URL from ngrok's local inspection API payload."""
    if not isinstance(payload, dict):
        raise RuntimeError("ngrok inspection API returned an invalid response")
    tunnels = payload.get("tunnels")
    if not isinstance(tunnels, list):
        raise RuntimeError("ngrok inspection API returned no tunnels")
    for tunnel in tunnels:
        if isinstance(tunnel, dict) and isinstance(tunnel.get("public_url"), str):
            url = tunnel["public_url"]
            if url.startswith("https://"):
                return public_base_url(url)
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


def wait_for_quick_tunnel_url(process: subprocess.Popen[str], timeout_seconds: int = 60) -> str:
    """Read cloudflared logs until TryCloudflare prints the generated URL."""
    if process.stdout is None:
        raise RuntimeError("cloudflared output is unavailable")
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        ready, _, _ = select.select([process.stdout], [], [], min(1, deadline - time.monotonic()))
        if not ready:
            continue
        line = process.stdout.readline()
        if not line:
            if process.poll() is not None:
                raise RuntimeError("Cloudflare Quick Tunnel exited before publishing a URL")
            continue
        print(line, end="")
        public_url = extract_quick_tunnel_url(line)
        if public_url:
            return public_url
    raise TimeoutError("Cloudflare Quick Tunnel did not publish an HTTPS endpoint before the timeout")


def main() -> None:
    require_gpu()
    print("Select public tunnel: 1) Cloudflare Named Tunnel  2) ngrok  3) Cloudflare Quick Tunnel")
    provider = select_tunnel_provider(input("Tunnel mode [1]: ").strip() or "1")

    named_public_base_url: str | None = None
    tunnel_secret: str | None = None
    if provider == "cloudflare":
        named_public_base_url = validate_public_base_url(
            input("Cloudflare public HTTPS endpoint (ending in /v1): ").strip()
        )
        tunnel_secret = getpass.getpass("Cloudflare Named Tunnel token: ").strip()
    elif provider == "ngrok":
        tunnel_secret = getpass.getpass("ngrok authtoken: ").strip()
    else:
        print("WARNING: Cloudflare Quick Tunnel is temporary and does not support stream: true. Use it only for stream: false testing.")

    api_key = getpass.getpass("New API key for this LLM service: ").strip()
    hf_token = getpass.getpass("Optional Hugging Face token (press Enter for public download): ").strip() or None
    if not api_key or (provider in {"cloudflare", "ngrok"} and not tunnel_secret):
        raise ValueError("The selected tunnel credential and the LLM API key are required")

    install_llama_cpp()
    model_path = download_model(hf_token)
    context_size = 8192
    command = build_llama_server_arguments(str(model_path), api_key, context_size)
    server = subprocess.Popen(command)
    tunnel = None
    ngrok_config_path: Path | None = None
    try:
        wait_for_health(api_key)
        require_authentication()
        if provider == "cloudflare":
            cloudflared = install_cloudflared()
            tunnel = subprocess.Popen(build_named_tunnel_arguments(str(cloudflared), tunnel_secret))
            public_url = named_public_base_url
        elif provider == "ngrok":
            ngrok = install_ngrok()
            ngrok_config_path = Path("/content/.ngrok-colab.yml")
            run(
                [str(ngrok), "config", "add-authtoken", tunnel_secret, "--config", str(ngrok_config_path)],
                quiet=True,
                redacted_values={tunnel_secret},
            )
            tunnel = subprocess.Popen(build_ngrok_arguments(str(ngrok)) + ["--config", str(ngrok_config_path)])
            public_url = wait_for_ngrok_public_base_url()
        else:
            cloudflared = install_cloudflared()
            tunnel = subprocess.Popen(
                build_quick_tunnel_arguments(str(cloudflared)),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
            public_url = wait_for_quick_tunnel_url(tunnel)

        if public_url is None:
            raise RuntimeError("Tunnel did not provide a public URL")
        print("\nService is live. Set these values in your local .env:")
        print(f"LLM_BASE_URL={public_url}")
        print(f"LLM_MODEL={SERVED_MODEL_NAME}")
        print("LLM_API_KEY=<the API key entered above>")
        print("\nKeep this Colab cell running. Interrupt it to stop both the API and tunnel.")
        tunnel.wait()
    finally:
        if tunnel is not None and tunnel.poll() is None:
            tunnel.terminate()
        server.terminate()
        if ngrok_config_path is not None:
            ngrok_config_path.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
