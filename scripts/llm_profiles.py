#!/usr/bin/env python3
"""Render safe, switchable OpenAI-compatible LLM endpoint profiles."""

from __future__ import annotations

import argparse
import json
import shlex
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


def load_profiles(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data.get("profiles"), dict):
        raise ValueError("profiles file must contain a 'profiles' object")
    return data


def load_profile(path: Path, name: str) -> dict[str, str]:
    profiles = load_profiles(path)["profiles"]
    if name not in profiles:
        raise ValueError(f"unknown profile: {name}")
    profile = profiles[name]
    required = ("base_url", "model", "api_key_env")
    if not isinstance(profile, dict) or any(not profile.get(key) for key in required):
        raise ValueError(f"profile '{name}' must define: {', '.join(required)}")
    parsed = urlparse(profile["base_url"])
    if parsed.scheme != "https" or not parsed.netloc or not parsed.path.rstrip().endswith("/v1"):
        raise ValueError("base_url must be an HTTPS OpenAI-compatible /v1 endpoint")
    return {key: str(profile[key]) for key in required}


def build_exports(profile: dict[str, str]) -> dict[str, str]:
    return {
        "LLM_BASE_URL": profile["base_url"].rstrip("/"),
        "LLM_MODEL": profile["model"],
        "LLM_API_KEY": f"${{{profile['api_key_env']}}}",
    }


def render_shell(exports: dict[str, str]) -> str:
    lines = []
    for key, value in exports.items():
        if value.startswith("${") and value.endswith("}"):
            lines.append(f'export {key}="{value}"')
        else:
            lines.append(f"export {key}={shlex.quote(value)}")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True)
    parser.add_argument(
        "--profiles-file",
        type=Path,
        default=Path("config/llm_profiles.json"),
    )
    parser.add_argument("--format", choices=("shell", "dotenv"), default="shell")
    args = parser.parse_args()

    exports = build_exports(load_profile(args.profiles_file, args.profile))
    if args.format == "dotenv":
        print("\n".join(f"{key}={value}" for key, value in exports.items()))
    else:
        print(render_shell(exports))


if __name__ == "__main__":
    main()
