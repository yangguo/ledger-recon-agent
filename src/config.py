"""Generic environment-based application settings."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


load_dotenv()


def workspace_path() -> Path:
    """Return the project workspace, defaulting to the repository root."""
    configured = os.getenv("APP_WORKSPACE_PATH")
    return Path(configured).expanduser().resolve() if configured else Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class LLMSettings:
    api_key: str
    base_url: str
    model: str
    temperature: float
    timeout_seconds: int
    max_tokens: int | None
    extra_body: dict[str, object]


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"{name} must be set in .env")
    return value


def llm_settings() -> LLMSettings:
    """Read settings accepted by any OpenAI-compatible API service."""
    extra_body_text = os.getenv("LLM_EXTRA_BODY_JSON", "").strip()
    try:
        extra_body = json.loads(extra_body_text) if extra_body_text else {}
    except json.JSONDecodeError as error:
        raise ValueError("LLM_EXTRA_BODY_JSON must be a JSON object") from error
    if not isinstance(extra_body, dict):
        raise ValueError("LLM_EXTRA_BODY_JSON must be a JSON object")

    max_tokens_text = os.getenv("LLM_MAX_TOKENS", "").strip()
    return LLMSettings(
        api_key=_required("LLM_API_KEY"),
        base_url=_required("LLM_BASE_URL").rstrip("/"),
        model=_required("LLM_MODEL"),
        temperature=float(os.getenv("LLM_TEMPERATURE", "0.3")),
        timeout_seconds=int(os.getenv("LLM_TIMEOUT_SECONDS", "600")),
        max_tokens=int(max_tokens_text) if max_tokens_text else None,
        extra_body=extra_body,
    )
