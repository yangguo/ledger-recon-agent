# Kaggle dual-GPU Qwen3.6 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Select Qwen3-14B for one GPU or Qwen3.6-27B for eligible T4×2 Kaggle runs.

**Architecture:** Make the model choice a pure configuration selected from optional `MODEL_SIZE`; use parsed per-GPU free VRAM to reject unsuitable 27B runs; produce llama-server arguments with explicit 1:1 layer splitting.

### Task 1: Tests and model selection
- Add failing tests in `tests/test_kaggle_qwen_launcher.py` for `14B`/`27B` configurations and a two-GPU VRAM guard.
- Implement the minimal selection and validation helpers in `kaggle/qwen3_14b_api.py`.
- Run focused tests and commit.

### Task 2: Server arguments and documentation
- Add failing test for `--split-mode layer --tensor-split 1,1` on 27B.
- Update server launch, CLI default/help, and README T4×2 guidance.
- Run full tests, compile, review, and commit.
