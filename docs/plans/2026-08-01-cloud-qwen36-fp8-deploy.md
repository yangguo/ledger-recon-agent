# Cloud Qwen3.6-27B FP8 Deployment Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Provide separate, safe-by-default deployment scripts for running Qwen3.6-27B-FP8 on Alibaba Cloud PAI and Tencent Cloud TI-ONE, preferring each platform's built-in model catalog and retaining custom storage only as a fallback.

**Architecture:** Each script reads a small `.env` file and switches explicitly between `builtin` and `custom` model sources. The built-in path emits a no-storage plan; PAI can submit it with the official `alipai` Model Gallery SDK, while TI-ONE uses the console/API Inspector flow because its public API does not document a stable model-gallery selection payload. The custom path emits the platform's EAS/CreateModelService request with OSS/CFS mounts and remains opt-in.

**Tech Stack:** Python 3.12 standard library, JSON, vLLM OpenAI server, PAI EAS JSON/EASCMD, Tencent Cloud TI-ONE `CreateModelService` API.

## Design decisions

- Keep PAI and TI-ONE scripts independent so each cloud's storage, resource, networking, and authentication vocabulary remains visible.
- Default to the platform-provided model catalog so users do not upload weights that the platform already hosts.
- Require the model storage path only for `custom`; do not make OSS/CFS a prerequisite for `builtin`.
- Keep `LLM_API_KEY` out of generated payloads. PAI can use its generated service token, while TI-ONE enables platform authorization. The local client should use the token returned by the platform.
- Default to a conservative 16K context and expose tensor parallelism, image, port, and extra vLLM arguments through environment variables.
- Generate configuration first; require an explicit `--apply` for a billable remote operation.

## Implementation tasks

1. Add unit tests for PAI and TI-ONE payload generation, validation, storage mounts, TP arguments, and distributed Ray startup.
2. Implement `scripts/deploy_pai_qwen36_fp8.py` with a minimal `.env` parser, built-in Model Gallery plan, optional `alipai` submission, and custom EAS JSON/eascmd fallback.
3. Implement `scripts/deploy_tione_qwen36_fp8.py` with a built-in model-gallery plan and an explicit custom TI-ONE request/SDK fallback.
4. Add platform-specific variables and commands to `.env.example` and `README.md`, separating no-storage native deployment from model upload/storage prerequisites.
5. Run focused tests, the existing launcher tests, compile checks, and both scripts in dry-run mode with temporary environment files.
