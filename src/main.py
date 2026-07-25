"""Local FastAPI service for the ledger reconciliation agent."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import time
import uuid
from collections.abc import AsyncIterator
from functools import lru_cache
from pathlib import Path
from typing import Any

import uvicorn
from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse

from agents.agent import build_agent
from config import llm_settings, workspace_path


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

MAX_UPLOAD_SIZE = 100 * 1024 * 1024
ALLOWED_UPLOAD_EXTENSIONS = {".xlsx", ".xlsm", ".csv"}

app = FastAPI(title="Ledger Reconciliation Agent")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://127.0.0.1:3000", "http://localhost:3000"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@lru_cache(maxsize=1)
def get_agent():
    """Build the configured agent once for the process."""
    return build_agent()


def _content_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "".join(
            item if isinstance(item, str) else item.get("text", "") if isinstance(item, dict) else ""
            for item in content
        )
    return ""


def _openai_chunk(*, request_id: str, model: str, content: str | None = None, role: str | None = None) -> str:
    delta: dict[str, str] = {}
    if role:
        delta["role"] = role
    if content:
        delta["content"] = content
    payload = {
        "id": request_id,
        "object": "chat.completion.chunk",
        "created": int(time.time()),
        "model": model,
        "choices": [{"index": 0, "delta": delta, "finish_reason": None}],
    }
    return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"


async def _stream_completion(payload: dict[str, Any], request_id: str) -> AsyncIterator[str]:
    settings = llm_settings()
    session_id = str(payload.get("session_id") or request_id)
    yield _openai_chunk(request_id=request_id, model=settings.model, role="assistant")
    async for event in get_agent().astream(
        {"messages": payload["messages"]},
        config={"configurable": {"thread_id": session_id}},
        stream_mode="messages",
    ):
        message = event[0] if isinstance(event, tuple) else event
        content = _content_text(getattr(message, "content", ""))
        if content:
            yield _openai_chunk(request_id=request_id, model=settings.model, content=content)
    yield "data: [DONE]\n\n"


async def _run_agent(payload: dict[str, Any]) -> dict[str, Any]:
    session_id = str(payload.get("session_id") or uuid.uuid4())
    result = await get_agent().ainvoke(
        {"messages": payload["messages"]},
        config={"configurable": {"thread_id": session_id}},
    )
    result["run_id"] = session_id
    return result


def _validate_chat_payload(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict) or not isinstance(payload.get("messages"), list) or not payload["messages"]:
        raise HTTPException(status_code=400, detail="messages must be a non-empty list")
    return payload


@app.get("/health")
async def health_check() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/run")
async def run(request: Request) -> dict[str, Any]:
    try:
        payload = _validate_chat_payload(await request.json())
    except json.JSONDecodeError as error:
        raise HTTPException(status_code=400, detail="invalid JSON") from error
    try:
        return await _run_agent(payload)
    except Exception as error:
        logger.exception("Agent run failed")
        raise HTTPException(status_code=502, detail=str(error)) from error


@app.post("/v1/chat/completions")
async def chat_completions(request: Request):
    try:
        payload = _validate_chat_payload(await request.json())
    except json.JSONDecodeError as error:
        raise HTTPException(status_code=400, detail="invalid JSON") from error

    request_id = f"chatcmpl-{uuid.uuid4().hex}"
    if payload.get("stream", False):
        return StreamingResponse(_stream_completion(payload, request_id), media_type="text/event-stream")

    try:
        result = await _run_agent(payload)
        messages = result.get("messages", [])
        content = _content_text(messages[-1].content) if messages else ""
        return JSONResponse(
            {
                "id": request_id,
                "object": "chat.completion",
                "created": int(time.time()),
                "model": llm_settings().model,
                "choices": [{"index": 0, "message": {"role": "assistant", "content": content}, "finish_reason": "stop"}],
            }
        )
    except Exception as error:
        logger.exception("Chat completion failed")
        raise HTTPException(status_code=502, detail=str(error)) from error


@app.post("/upload")
async def upload_files(files: list[UploadFile] = File(...)) -> dict[str, list[dict[str, str]]]:
    destination_directory = workspace_path() / "assets" / "uploads"
    destination_directory.mkdir(parents=True, exist_ok=True)
    saved_files: list[dict[str, str]] = []

    for uploaded_file in files:
        original_name = Path(uploaded_file.filename or "upload").name
        suffix = Path(original_name).suffix.lower()
        if suffix not in ALLOWED_UPLOAD_EXTENSIONS:
            raise HTTPException(status_code=400, detail=f"unsupported file type: {suffix}")
        destination = destination_directory / f"{uuid.uuid4().hex}{suffix}"
        size = 0
        try:
            with destination.open("wb") as output:
                while chunk := await uploaded_file.read(1024 * 1024):
                    size += len(chunk)
                    if size > MAX_UPLOAD_SIZE:
                        raise HTTPException(status_code=413, detail="upload exceeds 100 MB")
                    output.write(chunk)
        except Exception:
            destination.unlink(missing_ok=True)
            raise
        saved_files.append({"original_name": original_name, "saved_path": str(destination.relative_to(workspace_path()))})
    return {"files": saved_files}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Start the ledger reconciliation API")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8001)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    uvicorn.run("main:app", host=args.host, port=args.port)
