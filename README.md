# Ledger Recon Agent

用于 JE（分录/序时账）与 TB（科目余额表）对账的本地 Web 应用。后端通过标准
OpenAI-compatible API 调用模型；可使用第三方提供商、Colab/Kaggle 上的自部署 llama.cpp 服务，或阿里云 PAI、腾讯云 TI-ONE 上的 vLLM 服务。

## 配置

```bash
cp .env.example .env
```

编辑 `.env`：

```dotenv
LLM_API_KEY=your-api-key
LLM_BASE_URL=https://provider.example/v1
LLM_MODEL=your-model-name
LLM_TEMPERATURE=0.3
LLM_TIMEOUT_SECONDS=600
LLM_MAX_TOKENS=
LLM_EXTRA_BODY_JSON={}
```

`LLM_EXTRA_BODY_JSON` 仅用于某些服务的非标准请求字段。例如 Qwen 服务可按其支持的
格式配置关闭思考模式。PostgreSQL checkpoint 可选，使用 `DATABASE_URL`；未配置时使用
进程内内存。

也可使用 profile 切换 endpoint，profile 本身不保存 API key：

```bash
export COLAB_LLM_API_KEY='...'
source <(uv run python scripts/llm_profiles.py --profile colab-qwen36)
```

## 本地运行

安装后端依赖：

```bash
uv sync
```

启动后端：

```bash
bash scripts/http_run.sh -p 8001
```

启动前端：

```bash
cd frontend
npm ci
NEXT_PUBLIC_BACKEND_URL=http://127.0.0.1:8001 PORT=3000 npm run dev
```

打开 `http://127.0.0.1:3000`。前端仅连接本地后端，`LLM_API_KEY` 不会发送到浏览器。

## API

- `GET /health`
- `POST /upload`：上传 `.xlsx`、`.xlsm` 或 `.csv`
- `POST /run`：同步执行 agent
- `POST /v1/chat/completions`：OpenAI-compatible chat 接口，支持 `stream: true`

## 自部署 OpenAI-compatible API

所有自部署方式最终都会输出以下三项；将它们写入本机项目根目录的 `.env`，然后重启后端。

```dotenv
LLM_BASE_URL=https://服务地址/v1
LLM_MODEL=服务输出的模型ID
LLM_API_KEY=启动服务时设置的API key
```

不要用 `/v1/models` 判断鉴权：llama.cpp 可以公开该端点。应对推理端点测试：无 key 返回 `401`，带
key 返回 `200`。

```bash
curl -i "$LLM_BASE_URL/chat/completions" \
  -H "Authorization: Bearer $LLM_API_KEY" \
  -H 'Content-Type: application/json' \
  -d "{\"model\":\"${LLM_MODEL}\",\"messages\":[{\"role\":\"user\",\"content\":\"连接成功\"}],\"max_tokens\":32}"
```

### 方式选择

| 运行平台 | 发布方式 | 是否需要域名 | 地址稳定性 | 适合场景 |
| --- | --- | --- | --- | --- |
| Google Colab | Cloudflare Named Tunnel | 是 | 稳定 | 有自有域名、希望长期使用同一地址 |
| Google Colab | ngrok | 否 | 每次重启变化 | 最简单的流式开发环境 |
| Google Colab | Cloudflare Quick Tunnel | 否 | 每次重启变化 | 仅非流式临时测试 |
| Kaggle | ngrok | 否 | 每次运行变化 | Kaggle GPU 临时 API |
| 阿里云 PAI Model Gallery/EAS | PAI gateway | 否（取决于网关） | 由 EAS 网关决定 | 内置模型无需 OSS；自定义模型才需要 OSS |
| 腾讯云 TI-ONE 大模型广场/在线服务 | TI-ONE 网关 | 否（取决于网关） | 由服务组决定 | 内置模型无需 CFS；自定义模型才需要 CFS |

Colab 和 Kaggle 都是临时运行环境；会话、配额或 tunnel 结束后，公网地址会失效。

## Google Colab：启动 Qwen3.6-27B

启动器 [colab/qwen36_27b_api.py](colab/qwen36_27b_api.py) 使用 llama.cpp 启动
`Qwen3.6-27B-Q4_K_M.gguf`，至少需要 20 GiB 空闲显存。建议选择 L4、A100 或更高规格 GPU。

### 1. 创建 Colab GPU session

在本机安装并登录 Colab CLI，然后创建 session、上传启动器并打开远程 console：

```bash
uv tool install google-colab-cli
uv run python scripts/colab_qwen_api_cli.py --gpu L4
```

接下来所有模型和 tunnel 命令都在打开的 **Colab remote console** 中执行，不是在 Mac 本地终端：

```bash
python /content/qwen36_27b_api.py
```

脚本会要求选择发布方式，并要求输入一个新的强 `LLM_API_KEY`。Hugging Face token 对公开模型可留空。
不要把 Cloudflare token、ngrok token 或 API key 写入 Notebook、Git、命令历史或日志。

### 2. Cloudflare Named Tunnel（有自有域名）

适用于希望固定 URL 和保留流式响应的场景。

1. 在 Cloudflare Zero Trust 创建 **Named Tunnel**。
2. 为该 tunnel 增加 public hostname，例如 `llm.example.com`。
3. public hostname 的 service 填写 `http://127.0.0.1:8000`。
4. 复制 tunnel token。
5. 启动器提示选择时输入 `1`，再输入 tunnel token 和 `https://llm.example.com/v1`。

看到 `llama.cpp authentication self-check passed.` 和 `Service is live.` 后，填写：

```dotenv
LLM_BASE_URL=https://llm.example.com/v1
LLM_MODEL=qwen3.6-27b-q4_k_m
LLM_API_KEY=你在Colab启动器中输入的值
```

### 3. ngrok（无需域名，支持流式）

适用于不想配置 Cloudflare public hostname 的开发场景。

1. 在 [ngrok Dashboard](https://dashboard.ngrok.com/get-started/your-authtoken) 创建并复制 authtoken。
2. 运行 Colab 启动器后选择 `2`，输入 authtoken。
3. 脚本会打印随机的 `https://....ngrok-free.app/v1` 地址。

将打印的地址填到 `.env`：

```dotenv
LLM_BASE_URL=https://随机地址.ngrok-free.app/v1
LLM_MODEL=qwen3.6-27b-q4_k_m
LLM_API_KEY=你在Colab启动器中输入的值
```

ngrok 地址在每次 Colab 重启后都会变化。停止 Colab session：

```bash
uv run python scripts/colab_qwen_api_cli.py --stop
```

### 4. Cloudflare Quick Tunnel（无需域名，仅非流式）

运行启动器后选择 `3`。它不需要 Cloudflare 账号或 token，并会打印随机的
`https://....trycloudflare.com/v1` 地址。

Quick Tunnel 不支持 SSE；本项目若通过它调用，必须关闭流式响应：

```json
{"stream": false}
```

因此前端流式聊天应使用 Cloudflare Named Tunnel 或 ngrok，Quick Tunnel 仅用于 API 连通性测试。

## Kaggle：启动 Qwen 14B 或 Qwen3.6-27B

启动器 [kaggle/qwen3_14b_api.py](kaggle/qwen3_14b_api.py) 使用 ngrok 暴露 llama.cpp API。
Kaggle 会话结束或配额耗尽后地址会失效。Kaggle CLI 用于上传 Notebook 源码；Secret 绑定必须在
Kaggle 网页 Notebook 中完成。

### 1. 本机准备和上传 Notebook

安装并登录 Kaggle CLI，在 Kaggle 设置中生成 API token 后配置 CLI：

```bash
uv tool install kaggle
uv run python scripts/kaggle_qwen_api_cli.py --kernel 你的Kaggle用户名/qwen-api
```

该命令不会上传模型 API key 或 ngrok token。它会生成私有 GPU Notebook 并推送源码。CLI 触发的
运行不携带网页中的 Secret 绑定，因此它可能立即因读取 Secret 失败；这是预期现象。

### 2. 在网页绑定 Kaggle Secrets

打开刚推送的 Notebook，进入 **Add-ons > Secrets**，勾选要附加到本 Notebook 的 Secret：

- `LLM_API_KEY`：新建的 llama.cpp API key。
- `NGROK_AUTHTOKEN`：ngrok authtoken。
- `MODEL_SIZE`：`14B` 或 `27B`。
- `HF_TOKEN`：可选；公开 GGUF 下载可留空。

勾选后使用右上角 **Save Version > Save & Run All (Commit)**。仅保存或仅从 CLI 运行不会执行已绑定
Secret 的版本。

### 3. Kaggle 14B 配置（单卡）

默认值为 `MODEL_SIZE=14B`。需要至少一张 GPU，且至少 14 GiB 空闲显存；常见的 P100 16 GB 可用。
运行日志中的模型 ID 是：

```dotenv
LLM_MODEL=qwen3-14b-q4_k_m
```

### 4. Kaggle Qwen3.6-27B 配置（T4 x2）

在 Kaggle Notebook 设置中选择 **GPU T4 x2**，并在 `Add-ons > Secrets` 将 `MODEL_SIZE` 的值设置为：

```text
27B
```

若从 CLI 创建 Notebook，请显式请求双 T4：

```bash
uv run python scripts/kaggle_qwen_api_cli.py \
  --kernel 你的Kaggle用户名/qwen36-27b-api \
  --accelerator NvidiaTeslaT4x2
```

启动器会在启动前检查：至少两张 GPU、每张至少 12 GiB 空闲显存、合计至少 28 GiB；并自动采用：

```text
--split-mode layer --tensor-split 1,1
```

实际运行时，启动器从 `unsloth/Qwen3.6-27B-GGUF` 下载 `Qwen3.6-27B-Q4_K_M.gguf`。若 8K context
发生 OOM，将启动器中的 context 改为 4096 后重启。成功日志会输出：

```dotenv
LLM_MODEL=qwen3.6-27b-q4_k_m
```

### 5. 使用 Kaggle 输出配置本地项目

运行成功后，日志会显示：

```text
Service is live. Set these values in local .env:
LLM_BASE_URL=https://随机地址.ngrok-free.app/v1
LLM_MODEL=...
```

将该地址、日志中的模型 ID，以及同一个 `LLM_API_KEY` 写入本机 `.env`。不要把 Secret 值贴到 Kaggle
日志、终端输出或 Git 中。

## 阿里云 PAI：部署 Qwen3.6-27B-FP8

优先使用 PAI Model Gallery/LLM Deployment 中已经发布的模型。该路径由 PAI 管理模型和推理运行时，
不需要下载权重，也不需要上传 OSS；只有平台没有你需要的版本时才使用下面的 custom fallback。
官方入口和模型名称以当前地域、工作空间的控制台为准：[PAI Model Gallery](https://www.alibabacloud.com/help/en/pai/model-gallery/)。

### 首选：Model Gallery 内置模型

在 PAI 控制台选择 **EAS → Deploy Service → Scenario-based Model Deployment → LLM Deployment**，
模型来源选 **Public Model**，搜索并选择 `Qwen3.6-27B-FP8`（如果控制台显示的精确名称不同，使用控制台名称）。
选择 vLLM 和合适的单机/多卡 GPU 模板后直接部署。

脚本也可以生成无存储计划：

```dotenv
PAI_MODEL_SOURCE=builtin
PAI_MODEL_NAME=Qwen3.6-27B-FP8
PAI_MODEL_PROVIDER=pai
PAI_REGION_ID=你的PAI地域
PAI_WORKSPACE_ID=你的PAI工作空间ID
PAI_SERVICE_NAME=qwen36-27b-fp8
PAI_INSTANCE_TYPE=你的GPU实例规格
```

```bash
uv run python scripts/deploy_pai_qwen36_fp8.py \
  --env-file .env \
  --output /tmp/pai-qwen36-27b-fp8.builtin.json
```

生成的 JSON 只有模型和服务参数，没有 `storage`、OSS 路径或 Hugging Face 下载命令。需要脚本提交时，
使用 Python 3.12 和官方 SDK（`alipai` 当前依赖旧版运行时）：

```bash
uv run --isolated --python 3.12 --with 'alipai>=0.4.0' \
  --with setuptools \
  python scripts/deploy_pai_qwen36_fp8.py \
  --env-file .env \
  --output pai-qwen36-27b-fp8.builtin.json \
  --apply
```

PAI 服务详情页会给出 endpoint 和 token。将 endpoint 的 `/v1` 基地址和模型 ID 写入本项目 `.env`；
Model Gallery 文档中的请求路径形如 `/api/predict/{service_name}/v1/chat/completions`。服务达到
**Running** 后再用本 README 前面的真实 `curl` 请求验证。

### fallback：自定义 OSS + EAS JSON

只有在 PAI Model Gallery 没有目标版本，或你要部署自己的微调/量化模型时，才设置：

```dotenv
PAI_MODEL_SOURCE=custom
PAI_MODEL_PATH=oss://你的bucket/models/Qwen3.6-27B-FP8/
PAI_INSTANCE_TYPE=你的GPU实例规格
PAI_VLLM_IMAGE=vllm/vllm-openai:已验证的固定版本
PAI_GPU_COUNT=1
PAI_TP=1
PAI_MAX_MODEL_LEN=16384
PAI_WORKSPACE_ID=你的PAI工作空间ID
```

此时脚本生成 OSS 只读挂载的 EAS JSON。自定义 EAS 有两种提交方式：

- 使用下面的 EASCMD CLI 直接提交 JSON。
- 让脚本在 `--apply` 时调用本机已经认证的 EASCMD：

```bash
uv run python scripts/deploy_pai_qwen36_fp8.py \
  --env-file .env \
  --output pai-qwen36-27b-fp8.service.json \
  --apply
```

#### CLI 完整流程：EASCMD

1. 从 [EASCMD 下载与认证文档](https://www.alibabacloud.com/help/en/pai/developer-reference/download-the-eascmd-client-and-complete-user-authentication)
   下载与本机匹配的客户端。下面假设文件名是 `eascmd64`：

```bash
chmod +x ./eascmd64
```

2. 用 PAI 所在地域的 EAS endpoint 完成认证。endpoint 以官方地域列表为准，下面只展示格式：

```bash
export ALIBABA_CLOUD_ACCESS_KEY_ID='你的AccessKey ID'
export ALIBABA_CLOUD_ACCESS_KEY_SECRET='你的AccessKey Secret'
export PAI_EAS_ENDPOINT='pai-eas.cn-hangzhou.aliyuncs.com'

./eascmd64 config \
  -i "$ALIBABA_CLOUD_ACCESS_KEY_ID" \
  -k "$ALIBABA_CLOUD_ACCESS_KEY_SECRET" \
  -e "$PAI_EAS_ENDPOINT"
./eascmd64 ls
```

不要把 AccessKey 写入 README、Git 或日志。也可以使用 RAM 用户的最小权限凭证。

3. 先生成并检查 EAS JSON，再直接提交：

```bash
uv run python scripts/deploy_pai_qwen36_fp8.py \
  --env-file .env \
  --output pai-qwen36-27b-fp8.service.json

./eascmd64 create pai-qwen36-27b-fp8.service.json
./eascmd64 ls
./eascmd64 w qwen36-27b-fp8
```

`qwen36-27b-fp8` 必须与 `PAI_SERVICE_NAME`（以及 JSON 中的 `metadata.name`）一致。
看到服务为 **Running** 后，从 PAI 服务详情页复制 endpoint 和 token，再使用本 README 前面的真实
`curl` 请求验证。也可以把 `./eascmd64` 的路径传给脚本的 `--eascmd` 参数，由脚本执行同一个
`create` 命令。

内置 Model Gallery 模型不是这份自定义 EAS JSON 的目标，仍使用 PAI 控制台或 `alipai` SDK；不要
为了走 EASCMD CLI 再把平台已经提供的模型下载到 OSS。

参考 [PAI LLM 一键部署](https://www.alibabacloud.com/help/en/pai/deploy-an-llm/)、
[PAI JSON 部署参数](https://www.alibabacloud.com/help/en/pai/parameters-of-model-services) 和
[EASCMD 命令参考](https://www.alibabacloud.com/help/en/pai/developer-reference/run-commands-to-use-the-eascmd-client)。

## 腾讯云 TI-ONE：部署 Qwen3.6-27B-FP8

优先使用 TI-ONE **大模型广场**已经上架的模型。大模型广场的内置模型由平台提供，
不需要下载权重或上传到自己的 CFS；官方流程是从模型卡片直接新建在线服务。
当前模型清单以控制台为准，产品动态已列出 Qwen3.6-27B：[TI-ONE 大模型广场](https://cloud.tencent.com/document/product/851/109354)。

### 首选：大模型广场内置模型

在 TI-ONE 控制台选择 **大模型广场 → Qwen3.6-27B → 新建在线服务**，选择按量计费和可用 GPU，
确认“模型来源”为 **镜像**、模型和运行环境为 **内置大模型**，同意开源协议后启动。这个流程不需要 CFS。

脚本用于生成一个无存储计划，便于记录配置：

```dotenv
TIONE_MODEL_SOURCE=builtin
TIONE_REGION=你的TI-ONE地域
TIONE_BUILTIN_MODEL_ID=模型卡片或API Inspector中的内置模型ID
TIONE_BUILTIN_MODEL_NAME=Qwen3.6-27B
TIONE_SERVICE_GROUP_NAME=qwen36-27b-fp8
TIONE_INSTANCE_TYPE=你的TI-ONE GPU实例规格
```

```bash
uv run python scripts/deploy_tione_qwen36_fp8.py \
  --env-file .env \
  --output /tmp/tione-qwen36-27b-fp8.builtin.json
```

该 JSON 不含 `VolumeMounts`、CFS 路径、vLLM 自定义命令或模型下载步骤。当前 TI-ONE 文档公开的
`CreateModelService` API 没有稳定描述“模型广场卡片选择”所需的完整请求体，因此脚本不会用猜测的字段
执行 `--apply`；需要自动化时，在控制台打开 **API Inspector**，创建一次服务后导出对应请求，再把该请求
作为平台侧自动化模板。脚本对原生模式执行 `--apply` 会明确报错，不会创建错误或半配置的付费服务。

创建完成后，在服务管理页复制服务调用地址和 token。API Explorer 中的 `Model` 参数按 TI-ONE 文档使用
服务管理页显示的 **ID**，不要把模型卡片标题直接当作请求模型名。若当前内置服务只提供在线体验/API
Explorer 而没有公网服务调用地址，则不能直接作为本项目的 `LLM_BASE_URL`；此时改用下面的 custom fallback。

### fallback：自定义 CFS + TI-ONE `CreateModelService`

只有在要部署自己的微调/量化模型，或者内置模型没有提供可外部调用的服务时，才设置 `custom`：

```dotenv
TIONE_MODEL_SOURCE=custom
TIONE_REGION=ap-guangzhou
TC_SECRET_ID=你的腾讯云SecretId
TC_SECRET_KEY=你的腾讯云SecretKey
TIONE_SERVICE_GROUP_NAME=qwen36-27b-fp8
TIONE_INSTANCE_TYPE=你的TI-ONE GPU实例规格
TIONE_CFS_ID=cfs-xxxxxxxx
TIONE_CFS_PATH=/models/Qwen3.6-27B-FP8
TIONE_IMAGE_URL=mirror.ccs.tencentyun.com/vllm/vllm-openai:v0.11.0
TIONE_IMAGE_TYPE=CUSTOM
TIONE_TP=2
TIONE_MAX_MODEL_LEN=16384
TIONE_SERVICE_PORT=8000
TIONE_DEPLOY_TYPE=STANDARD
TIONE_REPLICAS=1
TIONE_AUTHORIZATION_ENABLE=true
TIONE_SAFETENSORS_LOAD_STRATEGY=eager
```

先 dry-run，再选择官方 Python SDK 或下面的 TCCLI 提交：

```bash
uv run python scripts/deploy_tione_qwen36_fp8.py \
  --env-file .env \
  --output /tmp/tione-qwen36-27b-fp8.json

uv run \
  --with tencentcloud-sdk-python-common \
  --with tencentcloud-sdk-python-tione \
  python scripts/deploy_tione_qwen36_fp8.py \
  --env-file .env \
  --output tione-qwen36-27b-fp8.json \
  --apply
```

#### CLI 完整流程：TCCLI

TCCLI 适用于上面 `TIONE_MODEL_SOURCE=custom` 生成的 `CreateModelService` 请求。它不替代
大模型广场的模型卡片流程，也不会把内置模型转换成 CFS 自定义部署。

1. 安装并检查 TCCLI：

```bash
python -m pip install --upgrade tccli
tccli --version
```

2. 配置凭证。交互方式会把凭证保存到本机的 TCCLI 配置目录：

```bash
tccli configure
# 输入 SecretId、SecretKey、TIONE_REGION 和 output=json
```

在无浏览器或自动化环境，也可以使用环境变量/命令行参数；不要把真实密钥写入仓库：

```bash
export TC_SECRET_ID='你的腾讯云SecretId'
export TC_SECRET_KEY='你的腾讯云SecretKey'
export TIONE_REGION='ap-guangzhou'

tccli configure set secretId "$TC_SECRET_ID"
tccli configure set secretKey "$TC_SECRET_KEY"
tccli configure set region "$TIONE_REGION" output json
```

3. 生成并审阅请求 JSON，然后通过 `tione CreateModelService` 提交。`--cli-input-json` 必须使用
绝对路径：

```bash
uv run python scripts/deploy_tione_qwen36_fp8.py \
  --env-file .env \
  --output /tmp/tione-qwen36-27b-fp8.json

tccli tione CreateModelService \
  --region "$TIONE_REGION" \
  --cli-input-json file:///tmp/tione-qwen36-27b-fp8.json \
  --output json
```

CI 不使用本机配置文件时，可以在命令上显式传递凭证：

```bash
tccli tione CreateModelService \
  --secretId "$TC_SECRET_ID" \
  --secretKey "$TC_SECRET_KEY" \
  --region "$TIONE_REGION" \
  --cli-input-json file:///tmp/tione-qwen36-27b-fp8.json \
  --output json
```

4. 从创建响应中记录 `ServiceGroupId`，查询服务状态和调用地址：

```bash
tccli tione DescribeModelServiceGroup \
  --region "$TIONE_REGION" \
  --ServiceGroupId 'ms-xxxxxxxx'

tccli tione DescribeModelServiceCallInfo \
  --region "$TIONE_REGION" \
  --ServiceGroupId 'ms-xxxxxxxx'
```

服务达到 **Running/Normal** 后，把返回的调用地址、模型 ID 和 token 写入 `.env`，再用本 README
前面的真实 `curl` 请求验证。参考 [TCCLI 安装](https://cloud.tencent.com/document/product/440/34011)、
[TCCLI 配置](https://cloud.tencent.com/document/product/440/34012)、
[TCCLI JSON 调用](https://cloud.tencent.com/document/product/440/34013)、
[TI-ONE CreateModelService](https://cloud.tencent.com/document/api/851/82291) 和
[TI-ONE 调用信息查询](https://cloud.tencent.com/document/api/851/82286)。

标准部署使用一台机器；确实需要多机时设置 `TIONE_DEPLOY_TYPE=DIST`、
`TIONE_INSTANCE_PER_REPLICA=2` 和总 GPU 卡数的 `TIONE_TP`。SDK 和 CLI 都使用同一套
`CreateModelService` 请求字段。参考 [TI-ONE 内置大模型快速部署](https://cloud.tencent.com/document/product/851/96710)、
[内置推理镜像说明](https://cloud.tencent.com/document/product/851/102913)、
[自定义 LLM 部署](https://cloud.tencent.com/document/product/851/107924) 和
[CreateModelService API](https://cloud.tencent.com/document/api/851/82291)。
