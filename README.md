# Ledger Recon Agent

企业财务分录对账智能体，用于读取 JE（Journal Entry，分录/序时账）和 TB（Trial Balance，科目余额表）文件，按账套与科目执行借贷发生额对账，并输出差异结果。

项目基于 FastAPI + LangGraph/LangChain + Coze 运行时构建，核心业务逻辑在 `src/tools/reconciliation_tool.py`。

## 功能

- 加载一个或多个 JE 文件
- 加载 TB 文件
- 自动识别常见中文财务列名
- 支持 Excel (`.xlsx`, `.xlsm`) 和 CSV (`.csv`)
- 按 `(账套, 科目编码)` 汇总 JE/TB 借贷金额
- 识别：
  - JE/TB 金额差异
  - 仅存在于 JE 的科目
  - 仅存在于 TB 的科目
  - 凭证借贷不平衡
- 支持大文件分批处理，避免一次性将大型 Excel 工作表读入内存

## 项目结构

```text
config/agent_llm_config.json      # LLM 参数和 Agent system prompt
src/main.py                       # FastAPI 服务入口和本地运行入口
src/agents/agent.py               # 构建 LangGraph/LangChain Agent
src/tools/reconciliation_tool.py  # JE/TB 加载与对账核心逻辑
src/storage/                      # Postgres checkpoint 和 S3 存储封装
scripts/setup.sh                  # uv 安装依赖
scripts/local_run.sh              # 本地 flow/node/agent 模式启动
scripts/http_run.sh               # HTTP 服务启动
.env.example                      # 本地环境变量示例
```

## 环境准备

要求：

- Python >= 3.12
- `uv`
- 可选：PostgreSQL，用于持久化 LangGraph checkpoint
- 可选：S3 兼容存储

安装依赖：

```bash
bash scripts/setup.sh
```

本地开发建议：

```bash
cp .env.example .env
# 编辑 .env，至少配置 COZE_WORKSPACE_PATH、模型 API key 和 base URL
```

加载环境变量：

```bash
set -a
. ./.env
set +a
```

> `scripts/load_env.sh` 会尝试通过 `coze_workload_identity` 从 Coze 平台加载环境变量；纯本地运行时通常直接使用 `.env` 更简单。

## 关键环境变量

| 变量 | 必需 | 说明 |
|---|---:|---|
| `COZE_WORKSPACE_PATH` | 是 | 工作空间根目录，本地设置为仓库路径 |
| `COZE_WORKLOAD_IDENTITY_API_KEY` | 是 | LLM API key |
| `COZE_INTEGRATION_MODEL_BASE_URL` | 是 | LLM OpenAI-compatible base URL |
| `PGDATABASE_URL` | 否 | Postgres checkpoint 存储；未配置时退化为内存存储 |
| `COZE_BUCKET_ENDPOINT_URL` | 否 | S3 兼容存储 endpoint |
| `COZE_BUCKET_NAME` | 否 | S3 bucket 名称 |

完整示例见 `.env.example`。

## 运行

### 本地 flow 模式

```bash
bash scripts/local_run.sh -m flow -i '{"messages":[{"role":"user","content":"请对账 JE=/path/je.xlsx TB=/path/tb.xlsx"}]}'
```

### 本地 agent 测试模式

```bash
bash scripts/local_run.sh -m agent
```

### HTTP 服务

```bash
bash scripts/http_run.sh -p 5000
# 或
python src/main.py -m http -p 5000
```

健康检查：

```bash
curl http://127.0.0.1:5000/health
```

同步运行：

```bash
curl -X POST http://127.0.0.1:5000/run \
  -H 'Content-Type: application/json' \
  -d '{"messages":[{"role":"user","content":"请用 /data/je.xlsx 和 /data/tb.xlsx 做对账"}]}'
```

流式运行：

```bash
curl -N -X POST http://127.0.0.1:5000/stream_run \
  -H 'Content-Type: application/json' \
  -d '{"messages":[{"role":"user","content":"请做对账"}]}'
```

取消任务：

```bash
curl -X POST http://127.0.0.1:5000/cancel/<run_id>
```

## 直接使用对账工具

Agent 可调用以下工具：

- `load_je_data(je_file_paths)`
- `load_tb_data(tb_file_path)`
- `run_reconciliation(je_file_paths, tb_file_path, target_patterns="", threshold=0.01, check_voucher_balance=True, check_sequence=False, batch_size=10000)`

`run_reconciliation` 是主入口，支持多个 JE 文件，用英文逗号分隔：

```text
/path/je_jan.xlsx,/path/je_feb.xlsx,/path/je_mar.csv
```

可通过 `target_patterns` 限定科目，例如：

```text
1001,1002,1122
```

## 输入文件列名

工具会自动匹配常见中文列名。

JE 常见列：

- 账套：`账套`、`公司`、`工厂`
- 凭证号：`凭证号`、`凭证编号`、`凭证`、`记账凭证号`
- 科目：`科目`、`会计科目`、`科目编码`、`科目代码`
- 借方：`借方本位币`、`借方本位币金额`、`借方金额(本位币)`、`借方金额`、`借贷方本位币`
- 贷方：`贷方本位币`、`贷方本位币金额`、`贷方金额(本位币)`、`贷方金额`
- 红字：`红字`、`红冲`、`冲销`、`反方向`

TB 常见列：

- 账套：`核算账套名称`、`主体账套`、`账套`、`公司`
- 科目编码：`科目编码`、`总账科目`、`科目`
- 科目名称：`科目名称`、`科目全称`、`名称`
- 借方：`本期借方.1`、`本期借方发生.1`、`本期借方`、`借方累计`
- 贷方：`本期贷方.1`、`本期贷方发生.1`、`本期贷方`、`贷方累计`

## 大文件处理说明

`src/tools/reconciliation_tool.py` 对大文件做了分批处理：

- CSV：使用 pandas `chunksize` 分批读取
- Excel (`.xlsx`, `.xlsm`)：使用 openpyxl `read_only=True` + `iter_rows()` 流式遍历行，再按 `batch_size` 生成 DataFrame 批次
- 默认 `batch_size=10000`

这避免了旧实现中“先把整个 Excel 工作表读入 list/DataFrame，再切 chunk”的内存放大问题。

注意：

- TB 通常较小，当前仍会全量加载后汇总与过滤末级科目。
- `load_je_data` 会把规范化后的 JE 数据写入唯一临时 CSV 文件，避免并发请求互相覆盖。
- 临时文件仍可能包含财务数据，生产环境建议挂载到受限目录并定期清理。

## 安全注意事项

当前项目默认面向受控内网/平台运行。若直接暴露 HTTP 服务，请先补充：

1. API 鉴权，例如 API key、OAuth2 或网关鉴权。
2. HTTPS / 内网访问控制。
3. 财务文件临时目录权限控制和清理策略。
4. 请求日志脱敏，避免记录完整财务数据或文件路径。
5. 数据送入 LLM 前的最小化与脱敏策略。

## 开发建议

- 为 `src/tools/reconciliation_tool.py` 增加单元测试和样例数据。
- 将列名映射配置化，便于适配不同 ERP/财务系统。
- 将 `src/main.py` 拆分为 API 路由、GraphService、CLI 入口。
- 生产环境中避免使用 pickle 保存中间财务数据。
- 为 README 中的 HTTP payload 增加与实际 Agent prompt 对齐的更多示例。

## 输出大小与模型上下文

`run_reconciliation` 的返回值会进入 LLM 上下文。为避免大数据集触发 `context_window_exceeded`，工具默认只在 JSON 响应中返回：

- 汇总统计
- 每类问题最多 5 条 preview
- 完整明细 CSV 的临时文件路径（`result_files`）

完整差异明细请读取 `result_files.differences_csv`、`result_files.only_in_je_csv`、`result_files.only_in_tb_csv` 等文件。这样可以让 Agent 先生成摘要，需要时再按文件路径分批读取明细。

### 413 / context_window_exceeded 排查

如果运行平台报：

- `context_window_exceeded`
- `RequestError code: 413`
- `invalid character 'R' looking for beginning of value`

通常表示发送给模型或上游 API 的请求体过大。当前默认 `DEFAULT_RESULT_PREVIEW_LIMIT = 0`，`run_reconciliation` 不会在 tool JSON 中返回明细样例，只返回统计和 CSV 文件路径。若仍报 413，应检查调用方是否把原始 Excel/CSV 内容或完整 CSV 明细再次塞进 prompt；正确做法是只读取 `result_files` 中必要的少量行，或基于 CSV 文件路径做分页/筛选。

## 本地真实附件 smoke test

原始财务附件不提交到 Git。可将本地 JE/TB 文件放到 `tests/fixtures/local/`（已被 `*.xlsx` ignore），然后运行：

```bash
python -m venv .test-venv
.test-venv/bin/python -m pip install pandas openpyxl numpy
.test-venv/bin/python tests/smoke_actual_fixtures.py \
  --je tests/fixtures/local/je.xlsx \
  --tb tests/fixtures/local/tb.xlsx
```

该脚本会验证：

- JE/TB Excel 能被解析
- 大文件 JE 分批读取可用
- 发生额及余额表双层表头可用
- `run_reconciliation` 默认不返回 preview，避免 413 / token 超限
