# Ledger Recon Agent

The service is a FastAPI + LangGraph application for reconciling JE and TB files.

```bash
uv sync
cp .env.example .env
bash scripts/http_run.sh -p 8001
```

Configure any OpenAI-compatible endpoint in `.env` with `LLM_API_KEY`, `LLM_BASE_URL`,
and `LLM_MODEL`. The public API is `GET /health`, `POST /upload`, `POST /run`, and
`POST /v1/chat/completions`. The frontend uses the backend URL set by
`NEXT_PUBLIC_BACKEND_URL`.

The reconciliation tools keep raw spreadsheet data local and only return compact summaries
and CSV result paths to the model.
