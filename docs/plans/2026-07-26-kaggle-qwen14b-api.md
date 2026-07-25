# Kaggle Qwen3-14B API Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a credential-safe Kaggle Notebook and local CLI that run Qwen3-14B Q4_K_M behind a temporary ngrok HTTPS API.

**Architecture:** A standalone Kaggle launcher reads secrets through a small injected adapter, starts a loopback-only llama.cpp server, and discovers ngrok's URL from its local API.  A local CLI copies the tracked Notebook template to a user-selected directory, writes only non-secret kernel metadata, and wraps `kaggle kernels push/status` commands.

**Tech Stack:** Python 3 standard library, Kaggle `UserSecretsClient`, llama.cpp, ngrok, `unittest`, Kaggle CLI.

### Task 1: Implement and test the Kaggle launcher primitives

**Files:**
- Create: `kaggle/qwen3_14b_api.py`
- Create: `tests/test_kaggle_qwen_launcher.py`

**Step 1: Write failing tests**

Cover secret loading, the Qwen3-14B Q4_K_M model constants, loopback llama-server
arguments, the 14 GiB VRAM threshold, ngrok command construction, and safe local
environment profile rendering.

**Step 2: Verify tests fail**

Run: `PYTHONPATH=src uv run python -m unittest discover -s tests -p 'test_kaggle_qwen_launcher.py' -v`

Expected: FAIL because the Kaggle launcher does not exist.

**Step 3: Implement the minimal launcher helpers**

Create a self-contained launcher with `get_secret()` delegated to
`kaggle_secrets.UserSecretsClient`, required/optional secret validation, Qwen3
14B constants, loopback-only llama-server arguments, and no-secret profile
rendering.  Reuse only standard-library-compatible logic; do not import the
Colab script.

**Step 4: Verify tests pass**

Run the focused test command. Expected: PASS.

**Step 5: Commit**

```bash
git add kaggle/qwen3_14b_api.py tests/test_kaggle_qwen_launcher.py
git commit -m "feat: add Kaggle Qwen14B launcher"
```

### Task 2: Add runtime orchestration and Notebook template

**Files:**
- Modify: `kaggle/qwen3_14b_api.py`
- Create: `kaggle/api.ipynb`
- Create: `kaggle/kernel-metadata.example.json`
- Modify: `tests/test_kaggle_qwen_launcher.py`

**Step 1: Write failing tests**

Add tests for parsing an ngrok HTTPS inspection payload and rejecting missing
secrets without exposing their values.

**Step 2: Verify tests fail**

Run the focused Kaggle launcher tests. Expected: FAIL because the parsers and
error behavior are absent.

**Step 3: Implement minimal runtime behavior**

Compile llama.cpp, download Qwen3-14B GGUF, start it on `127.0.0.1:8000`,
require authenticated inference, install/configure ngrok using a temporary
configuration file, wait for the HTTPS address, print the safe local profile,
and remove the temporary configuration on exit.  Add a Notebook that executes
the launcher and a metadata example that keeps the kernel private with GPU and
Internet enabled.

**Step 4: Verify tests pass**

Run the focused test command. Expected: PASS.

**Step 5: Commit**

```bash
git add kaggle/qwen3_14b_api.py kaggle/api.ipynb kaggle/kernel-metadata.example.json tests/test_kaggle_qwen_launcher.py
git commit -m "feat: add Kaggle Qwen14B Notebook runtime"
```

### Task 3: Add a credential-free local Kaggle CLI

**Files:**
- Create: `scripts/kaggle_qwen_api_cli.py`
- Create: `tests/test_kaggle_cli_runner.py`

**Step 1: Write failing tests**

Specify template-copy behavior and generated `kaggle kernels push --accelerator
NvidiaTeslaP100` / `kaggle kernels status owner/slug` commands.  Assert metadata
does not contain secret names or values.

**Step 2: Verify tests fail**

Run: `PYTHONPATH=src uv run python -m unittest discover -s tests -p 'test_kaggle_cli_runner.py' -v`

Expected: FAIL because the CLI module does not exist.

**Step 3: Implement the minimal CLI**

Use `argparse`; require `--kernel`, accept `--path` and `--accelerator`, copy
the tracked launcher and Notebook template to the destination, generate private
metadata, then run the official Kaggle CLI commands.  Do not accept or process
any credential command-line option.

**Step 4: Verify tests pass**

Run the focused CLI test command. Expected: PASS.

**Step 5: Commit**

```bash
git add scripts/kaggle_qwen_api_cli.py tests/test_kaggle_cli_runner.py
git commit -m "feat: add Kaggle Qwen API CLI"
```

### Task 4: Document use and verify the full project

**Files:**
- Modify: `README.md`
- Modify: `tests/test_kaggle_qwen_launcher.py`

**Step 1: Write failing test**

Add a test asserting the generated kernel metadata enables GPU and Internet,
uses the copied Notebook name, and remains private.

**Step 2: Verify test fails**

Run the focused CLI test command. Expected: FAIL because metadata generation is
not yet asserted.

**Step 3: Update documentation**

Add setup steps for installing/authenticating the Kaggle CLI, creating the
three Kaggle Secrets, preparing/pushing the kernel, retrieving its status and
ngrok URL, and updating local `.env`.  State the 14 GiB VRAM guard and that
this is a temporary service.

**Step 4: Verify focused tests pass**

Run both Kaggle test files. Expected: PASS.

**Step 5: Commit**

```bash
git add README.md tests/test_kaggle_qwen_launcher.py tests/test_kaggle_cli_runner.py
git commit -m "docs: explain Kaggle Qwen API workflow"
```

### Task 5: Full verification

**Files:**
- Verify only

**Step 1: Run all tests**

Run: `PYTHONPATH=src uv run python -m unittest discover -s tests -p 'test_*.py' -v`

Expected: all tests pass.

**Step 2: Compile Python sources**

Run: `uv run python -m compileall -q src scripts colab kaggle`

Expected: exit code 0.

**Step 3: Inspect diff**

Run: `git diff main...HEAD --check && git status -sb`

Expected: no whitespace errors and a clean worktree.
