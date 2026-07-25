# Generic OpenAI Runtime Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Run the ledger reconciliation agent with only generic OpenAI-compatible configuration, without Coze or S3 dependencies.

**Architecture:** A small settings module reads `LLM_API_KEY`, `LLM_BASE_URL`, `LLM_MODEL`, and model tuning values from `.env`. The backend directly constructs the LangChain agent and translates its streamed messages into OpenAI-compatible SSE, retaining only `/health`, `/v1/chat/completions`, and `/upload` for the existing frontend.

**Tech Stack:** Python, FastAPI, LangChain, LangGraph, python-dotenv, OpenAI-compatible HTTP APIs, Next.js frontend.

### Task 1: Add generic settings and agent construction

**Files:**
- Create: `src/config.py`
- Modify: `src/agents/agent.py`
- Modify: `tests/test_agent_model_override.py`

**Step 1:** Write failing tests for `.env`-style generic settings and `ChatOpenAI` arguments.

**Step 2:** Implement settings validation and replace `COZE_*` reads and JSON configuration with `LLM_*` variables.

**Step 3:** Run focused tests.

### Task 2: Replace the Coze HTTP runtime

**Files:**
- Modify: `src/main.py`
- Modify: `src/tools/reconciliation_tool.py`
- Create: `tests/test_openai_api.py`

**Step 1:** Write failing API tests for `/health`, unauthenticated local upload, and streamed `/v1/chat/completions` response framing.

**Step 2:** Replace Coze context, runners, tracing and graph selection with a direct FastAPI/LangGraph implementation.

**Step 3:** Run focused API tests.

### Task 3: Remove unused platform integrations and document configuration

**Files:**
- Delete: `src/storage/s3/s3_storage.py`
- Delete: `src/storage/s3/__init__.py`
- Delete: `src/storage/database/db.py`
- Delete: `src/storage/database/shared/model.py`
- Delete: `src/storage/database/shared/__init__.py`
- Delete: `src/utils/helper.py`
- Delete: `src/utils/log/__init__.py`
- Delete: `src/utils/log/loop_trace.py`
- Delete: `scripts/load_env.py`
- Delete: `scripts/load_env.sh`
- Delete: `config/agent_llm_config.json`
- Modify: `pyproject.toml`, `Dockerfile`, `.env.example`, `README.md`, `scripts/http_run.sh`, `scripts/local_run.sh`, `scripts/llm_profiles.py`, `config/llm_profiles.example.json`

**Step 1:** Write failing profile-render tests for `LLM_*` exports.

**Step 2:** Remove Coze/S3 dependencies and update the Docker/local launch environment.

**Step 3:** Run all tests, compile checks, lockfile sync, and a local HTTP smoke test.
