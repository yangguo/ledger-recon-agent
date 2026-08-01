#!/usr/bin/env python3
"""Plan or submit a PAI deployment for the platform-provided Qwen3.6-27B FP8 model.

The default ``builtin`` source uses PAI Model Gallery and does not mount OSS.
Set ``PAI_MODEL_SOURCE=custom`` for the explicit OSS/EAS fallback. The script
never submits a paid deployment unless ``--apply`` is supplied.
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import shutil
import subprocess
from pathlib import Path
from typing import Mapping


DEFAULT_ENV_FILE = ".env"
DEFAULT_OUTPUT = "pai-qwen36-27b-fp8.service.json"
DEFAULT_MODEL_NAME = "qwen3.6-27b-fp8"
DEFAULT_BUILTIN_MODEL_NAME = "Qwen3.6-27B-FP8"
DEFAULT_MODEL_MOUNT_PATH = "/mnt/model"
DEFAULT_PORT = 8000
DEFAULT_VLLM_IMAGE = "vllm/vllm-openai:v0.11.0"


def load_env_file(path: Path, environ: Mapping[str, str] | None = None) -> dict[str, str]:
    """Load simple ``KEY=value`` entries without overwriting process env vars."""

    values = dict(environ or os.environ)
    if not path.exists():
        if path.name == DEFAULT_ENV_FILE:
            return values
        raise FileNotFoundError(f"Environment file not found: {path}")

    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        if "=" not in line:
            raise ValueError(f"Invalid environment entry at {path}:{line_number}")
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            raise ValueError(f"Empty environment key at {path}:{line_number}")
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        values.setdefault(key, value)
    return values


def _required(env: Mapping[str, str], key: str) -> str:
    value = env.get(key, "").strip()
    if not value:
        raise ValueError(f"{key} is required")
    return value


def _model_source(env: Mapping[str, str]) -> str:
    source = env.get("PAI_MODEL_SOURCE", "builtin").strip().lower() or "builtin"
    if source not in {"builtin", "custom"}:
        raise ValueError("PAI_MODEL_SOURCE must be builtin or custom")
    return source


def _int_value(env: Mapping[str, str], key: str, default: int, minimum: int = 1) -> int:
    raw = env.get(key, str(default)).strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{key} must be an integer") from exc
    if value < minimum:
        raise ValueError(f"{key} must be >= {minimum}")
    return value


def _float_value(env: Mapping[str, str], key: str, default: float, minimum: float, maximum: float) -> float:
    raw = env.get(key, str(default)).strip()
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{key} must be a number") from exc
    if not minimum <= value <= maximum:
        raise ValueError(f"{key} must be between {minimum} and {maximum}")
    return value


def _bool_value(env: Mapping[str, str], key: str, default: bool) -> bool:
    raw = env.get(key, "true" if default else "false").strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{key} must be true or false")


def build_vllm_command(env: Mapping[str, str], model_path: str, model_name: str, port: int, tensor_parallel: int) -> str:
    """Build a shell-safe vLLM command for the PAI container."""

    args = [
        "vllm",
        "serve",
        model_path,
        "--host",
        "0.0.0.0",
        "--port",
        str(port),
        "--served-model-name",
        model_name,
        "--tensor-parallel-size",
        str(tensor_parallel),
        "--max-model-len",
        str(_int_value(env, "PAI_MAX_MODEL_LEN", 16384)),
        "--gpu-memory-utilization",
        str(_float_value(env, "PAI_GPU_MEMORY_UTILIZATION", 0.90, 0.1, 1.0)),
    ]
    if _bool_value(env, "PAI_TRUST_REMOTE_CODE", True):
        args.append("--trust-remote-code")
    load_strategy = env.get("PAI_SAFETENSORS_LOAD_STRATEGY", "").strip()
    if load_strategy:
        args.extend(["--safetensors-load-strategy", load_strategy])
    extra_args = env.get("PAI_VLLM_EXTRA_ARGS", "").strip()
    if extra_args:
        try:
            args.extend(shlex.split(extra_args))
        except ValueError as exc:
            raise ValueError("PAI_VLLM_EXTRA_ARGS is not valid shell syntax") from exc
    return shlex.join(args)


def build_config(env: Mapping[str, str]) -> dict[str, object]:
    """Build an Alibaba PAI EAS JSON deployment configuration."""

    if _model_source(env) != "custom":
        raise ValueError("build_config requires PAI_MODEL_SOURCE=custom; use build_deployment for builtin")

    model_source = _required(env, "PAI_MODEL_PATH")
    instance_type = _required(env, "PAI_INSTANCE_TYPE")
    service_name = env.get("PAI_SERVICE_NAME", DEFAULT_MODEL_NAME).strip() or DEFAULT_MODEL_NAME
    model_name = env.get("PAI_SERVED_MODEL_NAME", DEFAULT_MODEL_NAME).strip() or DEFAULT_MODEL_NAME
    model_mount_path = env.get("PAI_MODEL_MOUNT_PATH", DEFAULT_MODEL_MOUNT_PATH).strip() or DEFAULT_MODEL_MOUNT_PATH
    if not model_mount_path.startswith("/"):
        raise ValueError("PAI_MODEL_MOUNT_PATH must be an absolute path")

    gpu_count = _int_value(env, "PAI_GPU_COUNT", 1)
    tensor_parallel = _int_value(env, "PAI_TP", gpu_count)
    if tensor_parallel > gpu_count:
        raise ValueError("PAI_TP cannot exceed PAI_GPU_COUNT")
    port = _int_value(env, "PAI_SERVICE_PORT", DEFAULT_PORT)
    if port in {8080, 9090}:
        raise ValueError("PAI_SERVICE_PORT cannot be 8080 or 9090")

    metadata: dict[str, object] = {
        "name": service_name,
        "instance": _int_value(env, "PAI_INSTANCE_COUNT", 1),
        "gpu": gpu_count,
    }
    optional_metadata = {
        "workspace_id": env.get("PAI_WORKSPACE_ID", "").strip(),
        "resource": env.get("PAI_RESOURCE_GROUP", "").strip(),
        "cpu": env.get("PAI_CPU", "").strip(),
        "memory": env.get("PAI_MEMORY_MB", "").strip(),
        "shm_size": env.get("PAI_SHM_SIZE_GB", "").strip(),
    }
    for key, value in optional_metadata.items():
        if not value:
            continue
        metadata[key] = int(value) if key in {"cpu", "memory", "shm_size"} else value

    cloud: dict[str, object] = {"computing": {"instances": [{"type": instance_type}]}}
    spot_price = env.get("PAI_SPOT_PRICE_LIMIT", "").strip()
    if spot_price:
        cloud["computing"]["instances"][0]["spot_price_limit"] = float(spot_price)  # type: ignore[index]

    network_values = {
        "vpc_id": env.get("PAI_VPC_ID", "").strip(),
        "vswitch_id": env.get("PAI_VSWITCH_ID", "").strip(),
        "security_group_id": env.get("PAI_SECURITY_GROUP_ID", "").strip(),
    }
    present_network_values = {key: value for key, value in network_values.items() if value}
    if present_network_values and len(present_network_values) != len(network_values):
        missing = ", ".join(key for key, value in network_values.items() if not value)
        raise ValueError(f"PAI VPC configuration is incomplete; missing {missing}")
    if present_network_values:
        cloud["networking"] = present_network_values

    container: dict[str, object] = {
        "image": env.get("PAI_VLLM_IMAGE", DEFAULT_VLLM_IMAGE).strip() or DEFAULT_VLLM_IMAGE,
        "script": build_vllm_command(env, model_mount_path, model_name, port, tensor_parallel),
        "port": port,
    }
    container_env = []
    if _bool_value(env, "PAI_HF_HUB_OFFLINE", True):
        container_env.append({"name": "HF_HUB_OFFLINE", "value": "1"})
    if container_env:
        container["env"] = container_env

    config: dict[str, object] = {
        "metadata": metadata,
        "cloud": cloud,
        "storage": [
            {
                "oss": {
                    "path": model_source,
                    "readOnly": _bool_value(env, "PAI_MODEL_READ_ONLY", True),
                },
                "mount_path": model_mount_path,
            }
        ],
        "containers": [container],
    }
    gateway = env.get("PAI_GATEWAY_ID", "").strip()
    if gateway:
        config["networking"] = {"gateway": gateway}
    service_token = env.get("PAI_SERVICE_TOKEN", "").strip()
    if service_token:
        config["token"] = service_token
    docker_auth = env.get("PAI_DOCKER_AUTH", "").strip()
    if docker_auth:
        config["dockerAuth"] = docker_auth
    return config


def build_builtin_plan(env: Mapping[str, str]) -> dict[str, object]:
    """Build a no-storage plan for a PAI Model Gallery model."""

    if _model_source(env) != "builtin":
        raise ValueError("build_builtin_plan requires PAI_MODEL_SOURCE=builtin")
    service_name = env.get("PAI_SERVICE_NAME", "qwen36-27b-fp8").strip() or "qwen36-27b-fp8"
    model_name = env.get("PAI_MODEL_NAME", DEFAULT_BUILTIN_MODEL_NAME).strip() or DEFAULT_BUILTIN_MODEL_NAME
    service: dict[str, object] = {
        "name": service_name,
        "instance_count": _int_value(env, "PAI_INSTANCE_COUNT", 1),
    }
    for env_key, plan_key in (
        ("PAI_INSTANCE_TYPE", "instance_type"),
        ("PAI_RESOURCE_GROUP_ID", "resource_id"),
        ("PAI_SERVICE_TYPE", "service_type"),
    ):
        value = env.get(env_key, "").strip()
        if value:
            service[plan_key] = value
    plan: dict[str, object] = {
        "deployment_mode": "builtin",
        "platform": "alibaba-pai-model-gallery",
        "model": {
            "name": model_name,
            "provider": env.get("PAI_MODEL_PROVIDER", "pai").strip() or "pai",
        },
        "service": service,
        "apply": {
            "sdk": "alipai",
            "required_env": ["PAI_REGION_ID", "PAI_WORKSPACE_ID"],
            "note": "The model is resolved from PAI Model Gallery; no OSS model mount is used.",
        },
    }
    model_version = env.get("PAI_MODEL_VERSION", "").strip()
    if model_version:
        plan["model"]["version"] = model_version  # type: ignore[index]
    return plan


def build_deployment(env: Mapping[str, str]) -> dict[str, object]:
    """Build either a native Model Gallery plan or a custom EAS request."""

    return build_builtin_plan(env) if _model_source(env) == "builtin" else build_config(env)


def write_config(config: Mapping[str, object], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(config, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def build_eascmd_command(eascmd: str, output_path: Path) -> list[str]:
    return [eascmd, "create", str(output_path)]


def apply_builtin_plan(plan: Mapping[str, object], env: Mapping[str, str]) -> None:
    """Submit a PAI Model Gallery deployment with the optional official SDK."""

    try:
        from pai.model import RegisteredModel
        from pai.session import setup_default_session
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "PAI builtin apply requires the official SDK; use Python 3.12 and install "
            '"setuptools" and "alipai>=0.4.0"'
        ) from exc

    model = plan["model"]
    service = plan["service"]
    if not isinstance(model, Mapping) or not isinstance(service, Mapping):
        raise ValueError("invalid builtin PAI plan")
    region = _required(env, "PAI_REGION_ID")
    workspace = _required(env, "PAI_WORKSPACE_ID")
    access_key_id = env.get("PAI_ACCESS_KEY_ID", "").strip() or env.get("ALIBABA_CLOUD_ACCESS_KEY_ID", "").strip()
    access_key_secret = env.get("PAI_ACCESS_KEY_SECRET", "").strip() or env.get(
        "ALIBABA_CLOUD_ACCESS_KEY_SECRET", ""
    ).strip()
    session = setup_default_session(
        access_key_id=access_key_id or None,
        access_key_secret=access_key_secret or None,
        security_token=env.get("PAI_SECURITY_TOKEN", "").strip() or None,
        region_id=region,
        workspace_id=workspace,
    )
    registered_model = RegisteredModel(
        model_name=str(model["name"]),
        model_version=str(model["version"]) if model.get("version") else None,
        model_provider=str(model.get("provider", "pai")),
        session=session,
    )
    deploy_kwargs: dict[str, object] = {
        "service_name": str(service["name"]),
        "instance_count": int(service.get("instance_count", 1)),
        "wait": _bool_value(env, "PAI_WAIT_FOR_READY", False),
    }
    for plan_key, kwarg in (("instance_type", "instance_type"), ("resource_id", "resource_id"), ("service_type", "service_type")):
        if service.get(plan_key):
            deploy_kwargs[kwarg] = service[plan_key]
    predictor = registered_model.deploy(**deploy_kwargs)
    console_uri = getattr(predictor, "console_uri", "")
    print(f"PAI Model Gallery service submitted: {service['name']}")
    if console_uri:
        print(f"Console: {console_uri}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, default=Path(DEFAULT_ENV_FILE))
    parser.add_argument("--output", type=Path, default=Path(DEFAULT_OUTPUT))
    parser.add_argument("--eascmd", default="eascmd64", help="eascmd executable used by custom --apply")
    parser.add_argument("--apply", action="store_true", help="submit the selected deployment")
    args = parser.parse_args()

    try:
        env = load_env_file(args.env_file)
        config = build_deployment(env)
        write_config(config, args.output)
        if config.get("deployment_mode") == "builtin":
            print(f"Wrote PAI Model Gallery plan: {args.output}")
            if args.apply:
                apply_builtin_plan(config, env)
            else:
                print("Review it, then deploy with the official alipai SDK or the PAI Model Gallery console.")
            return 0
    except (FileNotFoundError, ValueError, RuntimeError) as exc:
        parser.error(str(exc))

    print(f"Wrote PAI EAS configuration: {args.output}")
    command = build_eascmd_command(args.eascmd, args.output)
    if not args.apply:
        print("Review it, then deploy with:")
        print(shlex.join(command))
        return 0

    executable = shutil.which(args.eascmd) or (args.eascmd if Path(args.eascmd).is_file() else None)
    if not executable:
        parser.error(f"{args.eascmd} was not found; install/authenticate EASCMD before using --apply")
    subprocess.run(build_eascmd_command(executable, args.output), check=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
