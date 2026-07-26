# Kaggle CUDA Driver Shim Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make the Kaggle Qwen launcher configure llama.cpp with CUDA when Kaggle's CMake package omits the `CUDA::cuda_driver` target.

**Architecture:** The launcher writes a small CMake top-level include file in `/kaggle/working` that discovers the host driver library and registers an imported `CUDA::cuda_driver` target only when CMake did not supply one. The normal `cmake` configure command loads that include file before llama.cpp configures its CUDA backend.

**Tech Stack:** Python 3.12, CMake, llama.cpp CUDA backend, unittest.

### Task 1: Cover the CMake shim contract

**Files:**
- Modify: `tests/test_kaggle_qwen_launcher.py`
- Modify: `kaggle/qwen3_14b_api.py`

**Step 1: Write the failing test**

Add a test that calls `write_cuda_driver_shim()` in a temporary directory and asserts that its generated CMake contains a guarded `CUDA::cuda_driver` imported target and checks the Kaggle host library path.

**Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src:. uv run pytest tests/test_kaggle_qwen_launcher.py -q`

Expected: FAIL because `write_cuda_driver_shim` does not yet exist.

**Step 3: Implement minimal shim generation**

Add `write_cuda_driver_shim(path)` that writes the CMake include file and returns its path.

**Step 4: Run test to verify it passes**

Run: `PYTHONPATH=src:. uv run pytest tests/test_kaggle_qwen_launcher.py -q`

Expected: PASS.

### Task 2: Load the shim during configure

**Files:**
- Modify: `tests/test_kaggle_qwen_launcher.py`
- Modify: `kaggle/qwen3_14b_api.py`

**Step 1: Write the failing test**

Add a test for `build_cmake_configure_arguments()` asserting the `CMAKE_PROJECT_TOP_LEVEL_INCLUDES` flag references the generated shim.

**Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src:. uv run pytest tests/test_kaggle_qwen_launcher.py -q`

Expected: FAIL because the command builder does not exist.

**Step 3: Implement and wire the command builder**

Replace the inline configure command in `main()` with the builder and generated shim path.

**Step 4: Verify tests and Kaggle submission**

Run the focused suite, then use `scripts/kaggle_qwen_api_cli.py` to submit and inspect the remote kernel status/logs.
