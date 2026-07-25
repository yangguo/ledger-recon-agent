# Colab Qwen API Authentication Self-check Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Prevent Cloudflare from exposing a llama.cpp server unless its API-key middleware rejects an unauthenticated protected route.

**Architecture:** After the existing readiness check, the launcher sends an unauthenticated request to `/v1/props`, a route outside llama.cpp's documented public health/model routes. It accepts only HTTP 401; any other response raises before `cloudflared` starts, so the `finally` block terminates the local server.

**Tech Stack:** Python standard library, unittest, llama.cpp OpenAI-compatible server.

### Task 1: Add an authentication gate

**Files:**
- Modify: `tests/test_colab_qwen_launcher.py`
- Modify: `colab/qwen36_27b_api.py`

**Step 1:** Write a test that patches `urlopen` to return a 401 `HTTPError`, and assert the new gate returns successfully.

**Step 2:** Run the focused test and confirm it fails because the function is absent.

**Step 3:** Implement the minimal gate: request `/v1/props` without headers; only `HTTPError` status 401 is success; otherwise raise `RuntimeError`.

**Step 4:** Run the focused test, then the complete test suite and bytecode compilation.

**Step 5:** Commit the launcher, test, and plan.
