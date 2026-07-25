#!/usr/bin/env python3
"""Provision a persistent Colab GPU session for the Qwen API via Colab CLI."""

from __future__ import annotations

import argparse
import shutil
import subprocess
from pathlib import Path


REMOTE_LAUNCHER_PATH = "/content/qwen36_27b_api.py"
SUPPORTED_GPUS = ("G4", "L4", "A100", "H100")


def build_session_commands(
    session_name: str,
    gpu: str,
    launcher_path: Path,
) -> list[list[str]]:
    """Build commands that preserve secret prompts inside the remote console."""
    return [
        ["colab", "new", "-s", session_name, "--gpu", gpu],
        ["colab", "upload", "-s", session_name, str(launcher_path), REMOTE_LAUNCHER_PATH],
        ["colab", "console", "-s", session_name],
    ]


def run(command: list[str]) -> None:
    print("+", " ".join(command))
    subprocess.run(command, check=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session", default="qwen36-api")
    parser.add_argument("--gpu", default="L4", choices=SUPPORTED_GPUS)
    parser.add_argument("--setup-only", action="store_true")
    parser.add_argument("--stop", action="store_true")
    args = parser.parse_args()

    if not shutil.which("colab"):
        raise SystemExit("Colab CLI is required. Install it with: uv tool install google-colab-cli")

    if args.stop:
        run(["colab", "stop", "-s", args.session])
        return

    launcher_path = Path(__file__).resolve().parents[1] / "colab" / "qwen36_27b_api.py"
    commands = build_session_commands(args.session, args.gpu, launcher_path)
    for command in commands[:2]:
        run(command)

    if args.setup_only:
        print(f"Session '{args.session}' is ready. Start it with: colab console -s {args.session}")
        return

    print("\nAt the remote prompt, run the following command. It will ask for secrets without saving them locally:")
    print(f"python {REMOTE_LAUNCHER_PATH}\n")
    run(commands[2])


if __name__ == "__main__":
    main()
