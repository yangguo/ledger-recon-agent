#!/usr/bin/env python3
"""Plan a TI-ONE deployment for the platform-provided Qwen3.6-27B FP8 model.

The default ``builtin`` source creates a no-storage 大模型广场 plan. Set
``TIONE_MODEL_SOURCE=custom`` for the CFS/CreateModelService fallback. Pass
``--apply`` only for the custom path after reviewing the request and confirming
the selected GPU resource and billing mode.
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
from pathlib import Path
from typing import Mapping


DEFAULT_ENV_FILE = ".env"
DEFAULT_OUTPUT = "tione-qwen36-27b-fp8.create-model-service.json"
DEFAULT_MODEL_NAME = "qwen3.6-27b-fp8"
DEFAULT_BUILTIN_MODEL_NAME = "Qwen3.6-27B"
DEFAULT_MODEL_MOUNT_PATH = "/data/model"
DEFAULT_PORT = 8000
DEFAULT_VLLM_IMAGE = "mirror.ccs.tencentyun.com/vllm/vllm-openai:v0.11.0"


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
    source = env.get("TIONE_MODEL_SOURCE", "builtin").strip().lower() or "builtin"
    if source not in {"builtin", "custom"}:
        raise ValueError("TIONE_MODEL_SOURCE must be builtin or custom")
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


def _bool_value(env: Mapping[str, str], key: str, default: bool) -> bool:
    raw = env.get(key, "true" if default else "false").strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{key} must be true or false")


def _extra_args(env: Mapping[str, str]) -> list[str]:
    value = env.get("TIONE_VLLM_EXTRA_ARGS", "").strip()
    if not value:
        return []
    try:
        return shlex.split(value)
    except ValueError as exc:
        raise ValueError("TIONE_VLLM_EXTRA_ARGS is not valid shell syntax") from exc


def build_vllm_command(
    env: Mapping[str, str],
    model_mount_path: str,
    model_name: str,
    port: int,
    tensor_parallel: int,
    deploy_type: str,
) -> str:
    args = [
        "vllm",
        "serve",
        model_mount_path,
        "--port",
        str(port),
        "--host",
        "0.0.0.0",
        "--served-model-name",
        model_name,
        "--tensor-parallel-size",
        str(tensor_parallel),
        "--max-model-len",
        str(_int_value(env, "TIONE_MAX_MODEL_LEN", 16384)),
    ]
    if _bool_value(env, "TIONE_TRUST_REMOTE_CODE", True):
        args.append("--trust-remote-code")
    load_strategy = env.get("TIONE_SAFETENSORS_LOAD_STRATEGY", "eager").strip()
    if load_strategy:
        args.extend(["--safetensors-load-strategy", load_strategy])
    args.extend(_extra_args(env))
    serve_command = shlex.join(args)
    if deploy_type != "DIST":
        return serve_command
    return (
        'if [ "$RANK" = "0" ]; then '
        "ray start --head --port 6700 --include-dashboard false --disable-usage-stats "
        f"&& {serve_command}; "
        'else ray start --address "$MASTER_ADDR:6700" --block; fi'
    )


def build_request(env: Mapping[str, str]) -> dict[str, object]:
    """Build a TI-ONE ``CreateModelService`` request body."""

    if _model_source(env) != "custom":
        raise ValueError("build_request requires TIONE_MODEL_SOURCE=custom; use build_deployment for builtin")

    service_name = _required(env, "TIONE_SERVICE_GROUP_NAME")
    instance_type = _required(env, "TIONE_INSTANCE_TYPE")
    cfs_id = _required(env, "TIONE_CFS_ID")
    cfs_path = _required(env, "TIONE_CFS_PATH")
    model_mount_path = env.get("TIONE_MODEL_MOUNT_PATH", DEFAULT_MODEL_MOUNT_PATH).strip() or DEFAULT_MODEL_MOUNT_PATH
    if not model_mount_path.startswith("/"):
        raise ValueError("TIONE_MODEL_MOUNT_PATH must be an absolute path")

    deploy_type = env.get("TIONE_DEPLOY_TYPE", "STANDARD").strip().upper() or "STANDARD"
    if deploy_type not in {"STANDARD", "DIST"}:
        raise ValueError("TIONE_DEPLOY_TYPE must be STANDARD or DIST")
    tensor_parallel = _int_value(env, "TIONE_TP", 2)
    port = _int_value(env, "TIONE_SERVICE_PORT", DEFAULT_PORT)
    service_name_for_model = env.get("TIONE_SERVED_MODEL_NAME", DEFAULT_MODEL_NAME).strip() or DEFAULT_MODEL_NAME

    image_type = env.get("TIONE_IMAGE_TYPE", "CUSTOM").strip().upper() or "CUSTOM"
    if image_type not in {"CUSTOM", "CCR", "TCR", "PRESET"}:
        raise ValueError("TIONE_IMAGE_TYPE must be CUSTOM, CCR, TCR, or PRESET")
    if image_type == "PRESET":
        image_type = "PreSet"
    charge_type = env.get("TIONE_CHARGE_TYPE", "POSTPAID_BY_HOUR").strip().upper() or "POSTPAID_BY_HOUR"
    if charge_type not in {"POSTPAID_BY_HOUR", "PREPAID"}:
        raise ValueError("TIONE_CHARGE_TYPE must be POSTPAID_BY_HOUR or PREPAID")

    env_vars: list[dict[str, str]] = [
        {"Name": "SERVED_MODEL_NAME", "Value": service_name_for_model},
        {"Name": "TP", "Value": str(tensor_parallel)},
    ]
    if deploy_type == "DIST":
        env_vars.append({"Name": "VLLM_ALLREDUCE_USE_SYMM_MEM", "Value": "0"})
    if _bool_value(env, "TIONE_HF_HUB_OFFLINE", True):
        env_vars.append({"Name": "HF_HUB_OFFLINE", "Value": "1"})
    extra_env = env.get("TIONE_EXTRA_ENV_JSON", "").strip()
    if extra_env:
        try:
            parsed_extra_env = json.loads(extra_env)
        except json.JSONDecodeError as exc:
            raise ValueError("TIONE_EXTRA_ENV_JSON must be a JSON array") from exc
        if not isinstance(parsed_extra_env, list) or not all(
            isinstance(item, dict) and {"Name", "Value"} <= item.keys() for item in parsed_extra_env
        ):
            raise ValueError("TIONE_EXTRA_ENV_JSON must contain objects with Name and Value")
        env_vars.extend({"Name": str(item["Name"]), "Value": str(item["Value"])} for item in parsed_extra_env)

    request: dict[str, object] = {
        "ServiceGroupName": service_name,
        "ServiceDescription": env.get(
            "TIONE_SERVICE_DESCRIPTION", "Qwen3.6-27B-FP8 served through vLLM"
        ).strip(),
        "ChargeType": charge_type,
        "InstanceType": instance_type,
        "ImageInfo": {
            "ImageType": image_type,
            "ImageUrl": env.get("TIONE_IMAGE_URL", DEFAULT_VLLM_IMAGE).strip() or DEFAULT_VLLM_IMAGE,
            **({"RegistryRegion": env["TIONE_REGISTRY_REGION"].strip()} if env.get("TIONE_REGISTRY_REGION", "").strip() else {}),
            **({"RegistryId": env["TIONE_REGISTRY_ID"].strip()} if env.get("TIONE_REGISTRY_ID", "").strip() else {}),
        },
        "Env": env_vars,
        "Replicas": _int_value(env, "TIONE_REPLICAS", 1),
        "ScaleMode": env.get("TIONE_SCALE_MODE", "MANUAL").strip().upper() or "MANUAL",
        "AuthorizationEnable": _bool_value(env, "TIONE_AUTHORIZATION_ENABLE", True),
        "ModelHotUpdateEnable": False,
        "Command": build_vllm_command(
            env, model_mount_path, service_name_for_model, port, tensor_parallel, deploy_type
        ),
        "ServicePort": port,
        "DeployType": deploy_type,
        "GrpcEnable": _bool_value(env, "TIONE_GRPC_ENABLE", False),
        "VolumeMounts": [
            {
                "VolumeSourceType": "CFS",
                "CFSConfig": {
                    "Id": cfs_id,
                    "Path": cfs_path,
                    "MountType": env.get("TIONE_CFS_MOUNT_TYPE", "STORAGE").strip().upper() or "STORAGE",
                    "Protocol": env.get("TIONE_CFS_PROTOCOL", "NFS").strip().upper() or "NFS",
                },
                "MountPath": model_mount_path,
            }
        ],
    }
    service_group_id = env.get("TIONE_SERVICE_GROUP_ID", "").strip()
    if service_group_id:
        request["ServiceGroupId"] = service_group_id
    resource_group_id = env.get("TIONE_RESOURCE_GROUP_ID", "").strip()
    if resource_group_id:
        request["ResourceGroupId"] = resource_group_id
    ti_project_id = env.get("TIONE_PROJECT_ID", "").strip()
    if ti_project_id:
        request["TiProjectId"] = ti_project_id
    if deploy_type == "DIST":
        request["InstancePerReplicas"] = _int_value(env, "TIONE_INSTANCE_PER_REPLICA", 2, minimum=2)
    model_version_id = env.get("TIONE_MODEL_VERSION_ID", "").strip()
    if model_version_id:
        request["ModelInfo"] = {"ModelVersionId": model_version_id}
    return request


def build_builtin_plan(env: Mapping[str, str]) -> dict[str, object]:
    """Build a no-storage plan for a TI-ONE 大模型广场 model."""

    if _model_source(env) != "builtin":
        raise ValueError("build_builtin_plan requires TIONE_MODEL_SOURCE=builtin")
    service_name = _required(env, "TIONE_SERVICE_GROUP_NAME")
    model_id = _required(env, "TIONE_BUILTIN_MODEL_ID")
    model_name = env.get("TIONE_BUILTIN_MODEL_NAME", DEFAULT_BUILTIN_MODEL_NAME).strip() or DEFAULT_BUILTIN_MODEL_NAME
    service: dict[str, object] = {
        "group_name": service_name,
        "charge_type": env.get("TIONE_CHARGE_TYPE", "POSTPAID_BY_HOUR").strip() or "POSTPAID_BY_HOUR",
        "replicas": _int_value(env, "TIONE_REPLICAS", 1),
    }
    for env_key, plan_key in (("TIONE_REGION", "region"), ("TIONE_INSTANCE_TYPE", "instance_type"), ("TIONE_RESOURCE_GROUP_ID", "resource_group_id")):
        value = env.get(env_key, "").strip()
        if value:
            service[plan_key] = value
    plan: dict[str, object] = {
        "deployment_mode": "builtin",
        "platform": "tencent-tione-model-gallery",
        "model": {
            "id": model_id,
            "name": model_name,
        },
        "service": service,
        "apply": {
            "console_flow": "大模型广场 -> 模型卡片 -> 新建在线服务",
            "api_model_parameter": "Use the ID shown in 在线服务 > 服务管理 after creation.",
            "note": "The platform-provided model is selected by TI-ONE; no CFS mount or user model upload is used.",
        },
    }
    public_version_id = env.get("TIONE_PUBLIC_ALGO_VERSION_ID", "").strip()
    if public_version_id:
        plan["model"]["public_algo_version_id"] = public_version_id  # type: ignore[index]
    return plan


def build_deployment(env: Mapping[str, str]) -> dict[str, object]:
    """Build either a native 大模型广场 plan or a custom CreateModelService request."""

    return build_builtin_plan(env) if _model_source(env) == "builtin" else build_request(env)


def write_request(request: Mapping[str, object], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(request, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def apply_request(request: Mapping[str, object], env: Mapping[str, str]) -> None:
    """Submit a request with the official optional TI-ONE Python SDK."""

    try:
        from tencentcloud.common import credential
        from tencentcloud.common.profile.client_profile import ClientProfile
        from tencentcloud.common.profile.http_profile import HttpProfile
        from tencentcloud.tione.v20211111 import models, tione_client
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "TI-ONE apply requires the official SDK: "
            "python -m pip install tencentcloud-sdk-python-common tencentcloud-sdk-python-tione"
        ) from exc

    secret_id = _required(env, "TC_SECRET_ID")
    secret_key = _required(env, "TC_SECRET_KEY")
    region = _required(env, "TIONE_REGION")
    cred = credential.Credential(secret_id, secret_key)
    http_profile = HttpProfile()
    http_profile.endpoint = "tione.tencentcloudapi.com"
    client_profile = ClientProfile()
    client_profile.httpProfile = http_profile
    client = tione_client.TioneClient(cred, region, client_profile)
    request_obj = models.CreateModelServiceRequest()
    request_obj.from_json_string(json.dumps(request, ensure_ascii=False))
    response = client.CreateModelService(request_obj)
    print(response.to_json_string())


def apply_builtin_plan(plan: Mapping[str, object], env: Mapping[str, str]) -> None:
    """Explain the native TI-ONE apply boundary instead of submitting a guessed payload."""

    del plan, env
    raise RuntimeError(
        "TI-ONE native model-gallery deployment must be created from the built-in model card "
        "or a request captured with API Inspector; the public CreateModelService docs do not "
        "define a stable model-gallery payload. The generated plan contains no CFS/OSS dependency."
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, default=Path(DEFAULT_ENV_FILE))
    parser.add_argument("--output", type=Path, default=Path(DEFAULT_OUTPUT))
    parser.add_argument("--apply", action="store_true", help="submit the custom CreateModelService request")
    args = parser.parse_args()

    try:
        env = load_env_file(args.env_file)
        request = build_deployment(env)
        write_request(request, args.output)
        if args.apply:
            if request.get("deployment_mode") == "builtin":
                apply_builtin_plan(request, env)
            else:
                apply_request(request, env)
    except (FileNotFoundError, ValueError, RuntimeError) as exc:
        parser.error(str(exc))

    if request.get("deployment_mode") == "builtin":
        print(f"Wrote TI-ONE native model-gallery plan: {args.output}")
        if not args.apply:
            print("Use the TI-ONE 大模型广场 card to create the service; no model upload is required.")
        return 0

    print(f"Wrote TI-ONE CreateModelService request: {args.output}")
    if not args.apply:
        print("Review it, then rerun with --apply after installing the TI-ONE SDK and setting TC_SECRET_ID/TC_SECRET_KEY.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
