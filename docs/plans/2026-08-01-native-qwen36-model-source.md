# Native Qwen3.6-27B Model Source Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make the Alibaba PAI and Tencent TI-ONE launchers use each platform's built-in Qwen3.6-27B model as the primary path, while retaining OSS/CFS only as an explicit custom-model fallback.

**Architecture:** Add an explicit `PAI_MODEL_SOURCE`/`TIONE_MODEL_SOURCE` switch. The native path emits a provider-specific deployment plan without any model storage mount; PAI can optionally submit through the documented `alipai` Model Gallery SDK, while TI-ONE directs built-in-model creation through the platform/API Inspector because the public API does not document a stable one-call payload. The existing EAS `eascmd` and TI-ONE `CreateModelService` storage paths remain available only with `custom`.

**Tech Stack:** Python standard library, optional `alipai` SDK, optional Tencent Cloud TI-ONE SDK, JSON, pytest.

### Task 1: Lock the source-selection contract with tests

**Files:**
- Modify: `tests/test_cloud_qwen36_deploy.py`

**Step 1: Write failing tests**

- Assert PAI native mode does not require `PAI_MODEL_PATH` and emits no `storage` section.
- Assert PAI custom mode still requires `PAI_MODEL_PATH`.
- Assert TI-ONE native mode does not require `TIONE_CFS_ID`/`TIONE_CFS_PATH` and emits no `VolumeMounts`.
- Assert TI-ONE custom mode still requires CFS fields.

**Step 2: Run the focused tests and confirm the failures are caused by the missing source mode.**

### Task 2: Implement native PAI planning and optional SDK submission

**Files:**
- Modify: `scripts/deploy_pai_qwen36_fp8.py`

**Step 1:** Add `PAI_MODEL_SOURCE` validation (`builtin` or `custom`) and a native plan builder requiring only `PAI_MODEL_NAME`, `PAI_REGION_ID`, and `PAI_WORKSPACE_ID` when applying.

**Step 2:** Keep the current EAS JSON builder under `custom`, including its OSS mount and `eascmd` apply path.

**Step 3:** Add an optional `alipai` apply path using `RegisteredModel(...).deploy(...)`; never put access keys or tokens into the generated plan.

### Task 3: Implement native TI-ONE planning/discovery without pretending custom storage is native

**Files:**
- Modify: `scripts/deploy_tione_qwen36_fp8.py`

**Step 1:** Add `TIONE_MODEL_SOURCE` validation (`builtin` or `custom`).

**Step 2:** For `builtin`, emit a plan containing the built-in model name/ID and no CFS mount, image URL, or custom command. Keep the model-card ID as metadata for console/API Inspector reconciliation; do not treat it as a complete public API submission contract.

**Step 3:** Do not synthesize a `DescribePublicAlgoVersionList` or native create flow until the account's model-card/API Inspector payload is verified. Keep final creation in the console/API Inspector path and fail clearly on native `--apply` rather than guessing undocumented fields.

### Task 4: Update environment examples and platform-specific documentation

**Files:**
- Modify: `.env.example`
- Modify: `README.md`
- Modify: `docs/plans/2026-08-01-cloud-qwen36-fp8-deploy.md`

**Step 1:** Make built-in model source the documented default and move OSS/CFS variables under an explicit custom fallback section.

**Step 2:** Explain that PAI Model Gallery and TI-ONE 大模型广场 can deploy platform-provided models without user uploads; distinguish this from TI-ONE's built-in inference image, which is a runtime and may still require CFS for custom models.

**Step 3:** Document the exact `curl`/OpenAI environment values after the platform service reports Running/Normal.

### Task 5: Verify

Run:

```bash
PYTHONPATH=src:. uv run --with pytest python -m pytest -q
python -m compileall -q scripts
git diff --check
```

Also run both launchers in native and custom dry-run modes and verify native JSON contains no OSS/CFS references.
