# Cloud CLI README Documentation Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Document reproducible CLI deployment paths for custom PAI EAS and TI-ONE model services without implying that platform-provided model cards require model uploads.

**Architecture:** Keep the existing Python launchers as JSON/config generators. Add CLI instructions around their generated artifacts: EASCMD for Alibaba PAI custom EAS, and TCCLI for Tencent TI-ONE `CreateModelService`. Keep native model-gallery deployment on the console/API Inspector path because its card payload is platform-specific.

**Tech Stack:** Markdown, EASCMD, TCCLI, existing Python deployment scripts.

### Task 1: Add Alibaba PAI EASCMD workflow

**Files:**
- Modify: `README.md`

Document executable download, `chmod`, `eascmd64 config`, environment-variable safety, dry-run, `--apply`, and service verification. Link the official EASCMD setup and command-reference pages.

### Task 2: Add Tencent TI-ONE TCCLI workflow

**Files:**
- Modify: `README.md`

Document TCCLI installation/version check, credential configuration, JSON request generation, `tccli tione CreateModelService --cli-input-json`, and response/status verification. Clarify that this applies to the custom CFS request, not the native model-gallery card.

### Task 3: Verify documentation consistency

**Files:**
- Verify: `README.md`, `scripts/deploy_pai_qwen36_fp8.py`, `scripts/deploy_tione_qwen36_fp8.py`

Check referenced commands, links, and generated JSON filenames with `rg`, `git diff --check`, and the existing test suite.
