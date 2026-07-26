# Ledger Recon Agent

用于 JE（分录/序时账）与 TB（科目余额表）对账的本地 Web 应用。后端通过标准
OpenAI-compatible API 调用模型；可使用第三方提供商或 Colab 上的自部署 llama.cpp 服务。

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
