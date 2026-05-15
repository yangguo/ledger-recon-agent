FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev curl \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml uv.lock ./

RUN pip install --no-cache-dir uv \
    && uv sync --frozen --no-dev \
    && uv cache clean

COPY src/ ./src/
COPY config/ ./config/
COPY scripts/ ./scripts/

ENV COZE_WORKSPACE_PATH=/app
ENV COZE_PROJECT_TYPE=agent
ENV PYTHONUNBUFFERED=1

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD curl -sf http://localhost:${PORT:-8000}/health || exit 1

CMD uv run uvicorn src.main:app --host 0.0.0.0 --port ${PORT:-8000}
