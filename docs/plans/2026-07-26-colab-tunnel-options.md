# Colab Tunnel Options Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Enable the Colab Qwen launcher to publish through a Cloudflare Named Tunnel, ngrok, or a no-hostname Cloudflare Quick Tunnel.

**Architecture:** Keep llama.cpp bound to loopback and retain its existing authentication gate.  Extract tunnel command and public-URL parsing into small pure functions, then let `main()` select the required provider, launch its process, discover the HTTPS address, and print a local `.env` profile.

**Tech Stack:** Python 3 standard library, llama.cpp, cloudflared, ngrok, `unittest`.

### Task 1: Specify tunnel helpers with tests

**Files:**
- Modify: `tests/test_colab_qwen_launcher.py`
- Modify: `colab/qwen36_27b_api.py`

**Step 1: Write the failing tests**

Add tests for:

```python
def test_ngrok_command_targets_loopback_port():
    self.assertEqual(build_ngrok_arguments("/usr/local/bin/ngrok"), [
        "/usr/local/bin/ngrok", "http", "127.0.0.1:8000",
    ])

def test_extract_quick_tunnel_url_returns_https_address():
    self.assertEqual(
        extract_quick_tunnel_url("INF | https://demo.trycloudflare.com |"),
        "https://demo.trycloudflare.com/v1",
    )
```

**Step 2: Verify the tests fail**

Run: `PYTHONPATH=src uv run python -m unittest tests.test_colab_qwen_launcher -v`

Expected: FAIL because the helper functions do not exist.

**Step 3: Implement the minimal helpers**

Add a `public_base_url()` normalizer, provider-specific command builders, and
a Quick Tunnel URL extractor.  Make every tunnel point to `127.0.0.1:8000`.

**Step 4: Verify the tests pass**

Run: `PYTHONPATH=src uv run python -m unittest tests.test_colab_qwen_launcher -v`

Expected: PASS.

**Step 5: Commit**

```bash
git add colab/qwen36_27b_api.py tests/test_colab_qwen_launcher.py
git commit -m "feat: add Colab tunnel helpers"
```

### Task 2: Add provider selection and dynamic URL discovery

**Files:**
- Modify: `tests/test_colab_qwen_launcher.py`
- Modify: `colab/qwen36_27b_api.py`

**Step 1: Write failing tests**

Add tests that select the three supported provider names, parse an HTTPS URL
from ngrok's inspection JSON, and reject a missing HTTPS tunnel.

**Step 2: Verify the tests fail**

Run: `PYTHONPATH=src uv run python -m unittest tests.test_colab_qwen_launcher -v`

Expected: FAIL because provider validation and ngrok discovery are absent.

**Step 3: Implement the minimal runtime behavior**

Install ngrok only when selected.  Use `ngrok config add-authtoken` with a
temporary configuration path rather than process arguments, start `ngrok http
127.0.0.1:8000`, and poll its loopback inspection API for an HTTPS public URL.
For Quick Tunnel, stream cloudflared output until a `trycloudflare.com` HTTPS
URL is observed.  Keep Named Tunnel behavior and validate its supplied URL.

**Step 4: Verify the tests pass**

Run: `PYTHONPATH=src uv run python -m unittest tests.test_colab_qwen_launcher -v`

Expected: PASS.

**Step 5: Commit**

```bash
git add colab/qwen36_27b_api.py tests/test_colab_qwen_launcher.py
git commit -m "feat: support dynamic Colab tunnel providers"
```

### Task 3: Document Named Tunnel, ngrok, and hostname-free operation

**Files:**
- Modify: `README.md`
- Modify: `tests/test_colab_qwen_launcher.py`

**Step 1: Write the failing test**

Add a test for the rendered profile helper so every provider produces a
`/v1` base URL and never emits the API-key value.

**Step 2: Verify the test fails**

Run: `PYTHONPATH=src uv run python -m unittest tests.test_colab_qwen_launcher -v`

Expected: FAIL because the profile helper is absent.

**Step 3: Implement docs and profile rendering**

Document the three-way comparison, ngrok authtoken location, launcher prompts,
and the `.env` values.  State that Quick Tunnel is temporary and unsupported
for `stream: true`; require `stream: false` for its test calls.  Print this
same restriction from the launcher before starting Quick Tunnel.

**Step 4: Verify the focused test passes**

Run: `PYTHONPATH=src uv run python -m unittest tests.test_colab_qwen_launcher -v`

Expected: PASS.

**Step 5: Commit**

```bash
git add README.md colab/qwen36_27b_api.py tests/test_colab_qwen_launcher.py
git commit -m "docs: explain Colab tunnel choices"
```

### Task 4: Full verification and review

**Files:**
- Verify only

**Step 1: Run the project test suite**

Run: `PYTHONPATH=src uv run python -m unittest discover -s tests -p 'test_*.py' -v`

Expected: all tests pass.

**Step 2: Compile changed Python modules**

Run: `uv run python -m compileall -q colab scripts src`

Expected: exit code 0.

**Step 3: Inspect diff and branch state**

Run: `git diff main...HEAD --check && git status -sb`

Expected: no whitespace errors and a clean worktree.

**Step 4: Commit any remaining verification-related change**

Only if a tracked change remains:

```bash
git add <files>
git commit -m "test: verify Colab tunnel options"
```
