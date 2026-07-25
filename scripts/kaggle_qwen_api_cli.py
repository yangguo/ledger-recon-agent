#!/usr/bin/env python3
"""Prepare and push the credential-free Kaggle Qwen3-14B API Notebook."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
from pathlib import Path


def build_metadata(kernel: str) -> dict[str, object]:
    return {
        "id": kernel, "title": "Qwen3 14B temporary API", "code_file": "api.ipynb",
        "language": "python", "kernel_type": "notebook", "is_private": True,
        "enable_gpu": True, "enable_internet": True, "dataset_sources": [],
        "competition_sources": [], "kernel_sources": [], "model_sources": [],
    }


def build_commands(path: Path, kernel: str, accelerator: str) -> list[list[str]]:
    return [["kaggle", "kernels", "push", "-p", str(path), "--accelerator", accelerator], ["kaggle", "kernels", "status", kernel]]


def prepare(path: Path, kernel: str) -> None:
    root = Path(__file__).resolve().parents[1]
    path.mkdir(parents=True, exist_ok=True)
    for name in ("api.ipynb", "qwen3_14b_api.py"):
        shutil.copy2(root / "kaggle" / name, path / name)
    (path / "kernel-metadata.json").write_text(json.dumps(build_metadata(kernel), indent=2) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--kernel", required=True, help="Kaggle kernel ID: owner/slug")
    parser.add_argument("--path", type=Path, default=Path(".kaggle-qwen-api"))
    parser.add_argument("--accelerator", default="NvidiaTeslaP100")
    args = parser.parse_args()
    prepare(args.path, args.kernel)
    for command in build_commands(args.path, args.kernel, args.accelerator):
        subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
