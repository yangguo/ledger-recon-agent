# Colab Qwen3.6 API Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Provide a Google Colab launcher for a quantized Qwen3.6-27B OpenAI-compatible API behind a Cloudflare Named Tunnel, plus a safe local profile switcher for this application.

**Architecture:** The Colab launcher is a Python script organised with `# %%` cells so it can be uploaded to, or pasted into, a Colab notebook. It validates the GPU before downloading the model, starts `llama-server` with a Q4_K_M GGUF model, waits for `/health`, and runs `cloudflared` using the user-supplied named-tunnel token. A versioned profile file holds only public endpoint settings; the activation command emits exports or updates an explicitly named local environment file without ever storing a token in Git.

**Tech Stack:** Python 3.12+, Bash, Google Colab, llama.cpp CUDA server, Hugging Face Hub, Cloudflare Named Tunnel, OpenAI-compatible HTTP API.

### Task 1: Define portable endpoint profiles

**Files:**
- Create: `config/llm_profiles.example.json`
- Create: `scripts/llm_profiles.py`
- Test: `tests/test_llm_profiles.py`

**Step 1: Write the failing test**

```python
from scripts.llm_profiles import build_exports, load_profile

def test_colab_profile_exports_openai_compatible_variables(tmp_path):
    profiles = tmp_path / "profiles.json"
    profiles.write_text('{"profiles":{"colab":{"base_url":"https://llm.example.com/v1","model":"qwen3.6-27b-q4","api_key_env":"COLAB_LLM_API_KEY"}}}')
    profile = load_profile(profiles, "colab")
    assert build_exports(profile) == {
        "COZE_INTEGRATION_MODEL_BASE_URL": "https://llm.example.com/v1",
        "COZE_INTEGRATION_MODEL": "qwen3.6-27b-q4",
        "COZE_WORKLOAD_IDENTITY_API_KEY": "${COLAB_LLM_API_KEY}",
    }
```

**Step 2: Run test to verify it fails**

Run: `uv run python -m unittest tests.test_llm_profiles -v`

Expected: FAIL because `scripts.llm_profiles` does not exist.

**Step 3: Write minimal implementation**

Create functions to validate the selected profile, require an HTTPS `/v1` base URL, and construct the three environment variables consumed by the current agent. Add a CLI with `--profile`, `--profiles-file`, and `--format shell|dotenv`.

**Step 4: Run test to verify it passes**

Run: `uv run python -m unittest tests.test_llm_profiles -v`

Expected: PASS.

**Step 5: Commit**

```bash
git add config/llm_profiles.example.json scripts/llm_profiles.py tests/test_llm_profiles.py
git commit -m "feat: add switchable LLM endpoint profiles"
```

### Task 2: Add a Colab-safe launcher

**Files:**
- Create: `colab/qwen36_27b_api.py`
- Test: `tests/test_colab_qwen_launcher.py`

**Step 1: Write the failing test**

```python
from colab.qwen36_27b_api import build_llama_server_command

def test_server_command_uses_configured_model_and_api_key():
    command = build_llama_server_command("/content/model.gguf", "secret", 8192)
    assert "--model /content/model.gguf" in command
    assert "--api-key secret" in command
    assert "--port 8000" in command
```

**Step 2: Run test to verify it fails**

Run: `uv run python -m unittest tests.test_colab_qwen_launcher -v`

Expected: FAIL because the launcher module does not exist.

**Step 3: Write minimal implementation**

Add reusable helpers for GPU validation, llama.cpp installation/download commands, command construction, local health polling, named-tunnel startup (`cloudflared tunnel --no-autoupdate run --token`), and tunnel hostname validation. The executable path prompts with `getpass` for `CLOUDFLARE_TUNNEL_TOKEN`, `HF_TOKEN` and the API key; secrets remain only in Colab runtime memory. Default model is `unsloth/Qwen3.6-27B-GGUF` `UD-Q4_K_XL.gguf` and it rejects GPUs with insufficient free VRAM before download.

**Step 4: Run test to verify it passes**

Run: `uv run python -m unittest tests.test_colab_qwen_launcher -v`

Expected: PASS.

**Step 5: Commit**

```bash
git add colab/qwen36_27b_api.py tests/test_colab_qwen_launcher.py
git commit -m "feat: add Colab Qwen API launcher"
```

### Task 3: Document the end-to-end workflow

**Files:**
- Modify: `README.md`
- Test: `tests/test_llm_profiles.py`

**Step 1: Write the failing test**

```python
def test_example_profile_has_colab_and_existing_provider_entries():
    profile_names = set(load_profiles(EXAMPLE_PROFILE_PATH)["profiles"])
    assert {"colab-qwen36", "existing-provider"} <= profile_names
```

**Step 2: Run test to verify it fails**

Run: `uv run python -m unittest tests.test_llm_profiles -v`

Expected: FAIL until example profiles are complete.

**Step 3: Write minimal implementation**

Document Colab GPU requirements, named-tunnel setup, secret entry, copy-back of the printed HTTPS `/v1` URL, local `source <(uv run python scripts/llm_profiles.py ...)` usage, non-streaming and streaming curl health checks, shutdown, and the warning that free Colab sessions are transient.

**Step 4: Run tests and static checks**

Run: `uv run python -m unittest discover -s tests -p 'test_*.py' -v && uv run python -m compileall -q colab scripts src`

Expected: PASS.

**Step 5: Commit**

```bash
git add README.md config/llm_profiles.example.json tests/test_llm_profiles.py
git commit -m "docs: explain Colab Qwen API workflow"
```
