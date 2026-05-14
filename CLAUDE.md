# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

企业财务分录对账智能体 — a financial ledger reconciliation agent that compares JE (Journal Entry, 分录/序时账) files against TB (Trial Balance, 科目余额表) files. Built on LangGraph/LangChain agents + FastAPI, designed to run on the Coze platform.

## Build & run commands

```bash
# Install dependencies (uv required)
bash scripts/setup.sh

# Load environment variables locally
cp .env.example .env        # edit with real values first
set -a && . ./.env && set +a

# Run HTTP service
bash scripts/http_run.sh -p 5000
# or: python src/main.py -m http -p 5000

# Run local flow mode (single invocation)
bash scripts/local_run.sh -m flow -i '{"messages":[{"role":"user","content":"请对账 JE=/path/je.xlsx TB=/path/tb.xlsx"}]}'

# Run interactive agent test mode
bash scripts/local_run.sh -m agent

# Run single graph node
python src/main.py -m node -n <node_id> -i '<json_payload>'

# Smoke test with local fixtures
python tests/smoke_actual_fixtures.py --je tests/fixtures/local/je.xlsx --tb tests/fixtures/local/tb.xlsx

# Lock dependencies
bash scripts/pack.sh          # runs uv lock
```

## Architecture

```
HTTP layer (FastAPI)            LangGraph Agent
src/main.py                     src/agents/agent.py
  /run (sync)                     create_agent() with:
  /stream_run (SSE)                 - ChatOpenAI LLM (from config/agent_llm_config.json)
  /cancel/{run_id}                  - 3 tools (reconciliation_tool.py)
  /v1/chat/completions              - sliding window messages (40 msg max)
                                    - Postgres checkpoint (falls back to MemorySaver)

Tools (called by agent)         Storage layer
src/tools/reconciliation_tool.py  src/storage/
  load_je_data()                     database/db.py       — Postgres connection pool (SQLAlchemy)
  load_tb_data()                     memory/memory_saver.py — LangGraph checkpointer (PG → MemorySaver fallback)
  run_reconciliation()              s3/s3_storage.py     — S3-compatible object storage (boto3)
```

**Agent config** (`config/agent_llm_config.json`): Contains the LLM model, temperature, timeout, and the system prompt (`sp` field). The system prompt enforces strict rules: never paste raw Excel/CSV content into context, always pass file paths to tools, keep responses compact to avoid `context_window_exceeded` errors.

## Key design constraints

1. **Ultra-compact tool output**: `DEFAULT_RESULT_PREVIEW_LIMIT = 0` in `reconciliation_tool.py`. `run_reconciliation` returns only summary statistics and CSV file paths — no detail rows in the tool JSON. This prevents `413` / `context_window_exceeded` errors when the LLM API receives the response. Full details go to temporary CSV files referenced via `result_files`.

2. **Large file batch processing**: Excel files are read via `openpyxl` `read_only=True` + `iter_rows()`, CSV via pandas `chunksize`. Avoids loading the entire workbook into a single DataFrame list before chunking, which caused memory bloat.

3. **Balanced sheet two-level headers**: TB files from some ERP systems have a group header row + sub-header row (发生额及余额表 format). Detected via `_is_balance_sheet_group_header()` and mapped by fixed column positions (`_map_balance_sheet_row`).

4. **Column auto-detection**: Both JE and TB files use Chinese column name matching (defined in `JE_COLUMNS` / `TB_COLUMNS` dicts). `_select_excel_sheet_and_header_row` scores candidate sheets/header rows to find the right one automatically.

5. **Reconciliation output conservatism**: Voucher balance issues are capped at `MAX_VOUCHER_ISSUES = 10` samples. All detail categories (differences, only_in_je, only_in_tb) are written to CSV files, not returned inline.

## Environment variables

See `.env.example` for full list. Critical ones:
- `COZE_WORKSPACE_PATH` — repo root (required)
- `COZE_WORKLOAD_IDENTITY_API_KEY` — LLM API key
- `COZE_INTEGRATION_MODEL_BASE_URL` — LLM OpenAI-compatible base URL
- `PGDATABASE_URL` — Postgres for checkpoint persistence (optional; falls back to in-memory)

## Working dir assumption

Code reads LLM config from `$COZE_WORKSPACE_PATH/config/agent_llm_config.json`. Local runs must have `COZE_WORKSPACE_PATH` set to the repo root.
