# Kaggle dual-GPU Qwen3.6 design

## Goal

Extend the Kaggle API launcher to run Qwen3-14B on one suitable GPU or
Qwen3.6-27B Q4_K_M when the Kaggle runtime exposes two suitable GPUs.

## Selection

The optional Kaggle Secret `MODEL_SIZE` selects `14B` (default) or `27B`.
`27B` selects `unsloth/Qwen3.6-27B-GGUF` and its Q4_K_M file.  It requires at
least two visible NVIDIA GPUs, at least 28 GiB total free VRAM, and 12 GiB free
VRAM on each participating GPU.  The launcher never silently falls back to a
different model.

## Multi-GPU runtime

The 27B server is loopback-only and uses `--n-gpu-layers 999`,
`--split-mode layer`, and `--tensor-split 1,1`.  Context remains 8192 by
default; the error message recommends 4096 if runtime overhead produces OOM.
The 14B path preserves its single-GPU-compatible behavior.

## CLI and safety

The Kaggle CLI will default to P100 but accept explicit `--accelerator
NvidiaTeslaT4`.  Actual eligibility is always decided from `nvidia-smi` inside
the Notebook, not the requested accelerator name.  Secrets remain in Kaggle
Secrets and are not printed or added to metadata.

## Tests and docs

Tests will cover model selection, per-GPU aggregate validation, and 27B server
arguments.  README will describe T4×2 requirements and the reduced-context OOM
fallback.
