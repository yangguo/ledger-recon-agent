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

## Colab 自部署

`colab/qwen36_27b_api.py` 可在 GPU Colab runtime 上启动 Qwen3.6-27B Q4_K_M，并通过
Cloudflare Named Tunnel、ngrok 或 Cloudflare Quick Tunnel 发布 HTTPS `/v1` endpoint。将脚本输出
的地址、模型名和 API key 分别填入 `LLM_BASE_URL`、`LLM_MODEL`、`LLM_API_KEY`，与第三方服务的
配置方式完全相同。

Colab 是临时运行环境，不适合持久或公开生产服务。启动器在 tunnel 建立前会检查推理接口的
鉴权是否生效。

### 隧道选择

| 模式 | 是否需要 public hostname | 凭证 | 地址稳定性 | `stream: true` |
| --- | --- | --- | --- | --- |
| Cloudflare Named Tunnel | 是 | Cloudflare tunnel token | 稳定、自有域名 | 支持 |
| ngrok | 否 | ngrok authtoken | 每次启动生成 | 支持 |
| Cloudflare Quick Tunnel | 否 | 无 | 每次启动生成 | **不支持** |

Quick Tunnel 仅用于临时开发测试；Cloudflare 不提供 SLA，并且不支持 SSE。因此此项目的前端流式
聊天必须使用 Named Tunnel 或 ngrok；若选择 Quick Tunnel，调用 API 时必须传入 `"stream": false`。

### 启动步骤

1. 若使用 Cloudflare Named Tunnel，在 Cloudflare Zero Trust 创建 **Named Tunnel**，为它添加
   public hostname，例如 `llm.example.com`，服务地址填写 `http://127.0.0.1:8000`，并复制 tunnel
   token。若使用 ngrok，请在 [ngrok Dashboard](https://dashboard.ngrok.com/get-started/your-authtoken)
   获取 authtoken；不需要配置域名。Quick Tunnel 不需要任何 tunnel 凭证或域名。
2. 本机安装并登录 Google Colab CLI，然后创建 GPU session、上传启动器并进入远程 console：

   ```bash
   uv tool install google-colab-cli
   uv run python scripts/colab_qwen_api_cli.py --gpu G4
   ```

3. 在打开的 **Colab remote console**（不是 Mac 本地终端）中运行：

   ```bash
   python /content/qwen36_27b_api.py
   ```

   首先选择 tunnel 模式：`1` 为 Named Tunnel、`2` 为 ngrok、`3` 为无需 public hostname 的
   Quick Tunnel。Named Tunnel 需要预先配置的 `https://.../v1` 地址与 token；ngrok 只需要
   authtoken，脚本会自动发现 HTTPS 地址；Quick Tunnel 会自动输出随机 `trycloudflare.com` 地址。
   随后输入一个新的强 API key；Hugging Face token 可留空。不要把 token 或 API key 写入
   notebook、命令历史或日志。
4. 只有看到以下两行后才可对外调用：

   ```text
   llama.cpp authentication self-check passed.
   Service is live.
   ```

5. 脚本会打印以下形式的本地 `.env` 配置；将占位符改为你刚输入的真实 API key：

   ```dotenv
   LLM_API_KEY=你在Colab设置的API_key
   LLM_BASE_URL=https://脚本输出的域名/v1
   LLM_MODEL=qwen3.6-27b-q4_k_m
   ```

   ngrok 和 Quick Tunnel 的地址会在每次 Colab 重启后变化，因此必须更新本地 `.env`。停止
   session：

   ```bash
   uv run python scripts/colab_qwen_api_cli.py --stop
   ```

### 无 public hostname 的调用示例

ngrok 与 Quick Tunnel 会显示一个随机 HTTPS 地址。将它填入 `LLM_BASE_URL` 后即可调用；ngrok
支持流式调用。Quick Tunnel 只能做非流式测试：

```bash
curl -i "$LLM_BASE_URL/chat/completions" \
  -H "Authorization: Bearer $LLM_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"model":"qwen3.6-27b-q4_k_m","messages":[{"role":"user","content":"连接成功"}],"stream":false,"max_tokens":64}'
```

### 鉴权验证

`/v1/models` 和健康检查在 llama.cpp 中可以公开，不能用于判断鉴权。请测试推理接口：无
`Authorization` 请求必须返回 `401`；带 key 请求应返回 `200`。

```bash
curl -i "$LLM_BASE_URL/chat/completions" \
  -H "Authorization: Bearer $LLM_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"model":"qwen3.6-27b-q4_k_m","messages":[{"role":"user","content":"连接成功"}],"max_tokens":64}'
```
