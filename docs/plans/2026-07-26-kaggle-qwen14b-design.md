# Kaggle Qwen3-14B API launcher

## Goal

Provide a Kaggle-specific, non-interactive way to run an OpenAI-compatible
Qwen3-14B API for temporary development use.  The Notebook must obtain all
credentials from Kaggle Secrets, use ngrok for its dynamic HTTPS address, and
avoid exposing secrets through the repository, CLI arguments, or output.

## Scope and model choice

The Kaggle launcher will use the official `Qwen/Qwen3-14B-GGUF` repository and
the `Qwen3-14B-Q4_K_M.gguf` file.  This quantization is approximately 9 GB and
is appropriate for Kaggle's typical 16 GB GPU runtime with an 8192-token
context limit.  The launcher will explicitly require at least 14 GiB of free
VRAM before startup.  Qwen3.6-27B remains a Colab-only option because its
current 20 GiB free-VRAM requirement exceeds a typical Kaggle GPU.

## Notebook runtime

`kaggle/qwen3_14b_api.py` will run within a GPU-enabled, Internet-enabled
Kaggle Notebook.  It will read these secrets through Kaggle's
`UserSecretsClient`:

| Secret | Required | Purpose |
| --- | --- | --- |
| `LLM_API_KEY` | Yes | llama.cpp inference authentication |
| `NGROK_AUTHTOKEN` | Yes | ngrok account authentication |
| `HF_TOKEN` | No | authenticated Hugging Face model download |

It will compile CUDA llama.cpp, download the model, bind llama.cpp only to
`127.0.0.1:8000`, verify an unauthenticated protected route returns `401`,
and start `ngrok http 127.0.0.1:8000`.  It will discover ngrok's HTTPS address
from the local inspection API, print the exact local `.env` values without
printing any secret, and clean up its temporary ngrok configuration when
stopped.

## Local CLI and artifacts

`scripts/kaggle_qwen_api_cli.py` will be a credential-free local wrapper for
the official Kaggle CLI.  It will generate a private kernel directory from
tracked templates, push the kernel, and query its status.  It will never write
tokens into `kernel-metadata.json` or a notebook.  The tracked Kaggle template
will contain `kernel-metadata.json` with GPU and Internet enabled plus an
`api.ipynb` that invokes the launcher.

Because Kaggle CLI launches a Notebook job rather than an interactive shell,
the operator obtains the dynamic ngrok URL from the Notebook log, then updates
their local `.env`.  A new run produces a new URL.

## Error handling, tests, and documentation

The launcher will give clear errors for missing Kaggle Secrets, insufficient
VRAM, an unavailable ngrok HTTPS URL, and failed auth verification.  Unit
tests will cover secret loading via an injected client, command construction,
safe `.env` rendering, and local CLI metadata/command construction.  The
README will document GPU/Internet setup, Kaggle Secrets, CLI usage, the
dynamic URL workflow, and the temporary-service limitations.
