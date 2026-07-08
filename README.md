# aicoding-chat-viewer

本地 Web 工具，用于浏览与复盘 **Antigravity IDE** 与 **Cursor** 的 AI 结对编程聊天记录。

数据全部留在本机：同步、浏览、统计不依赖云端；可选配置 LLM 做研发 Blocks 提取、模块合并与 Markdown 报告生成。

## 功能

- **会话浏览**：按 workspace 聚合 Antigravity + Cursor 对话，支持搜索与消息展开
- **增量同步**：从 Antigravity 本地目录增量导入会话到 SQLite 索引库
- **研发统计**：活跃天数、消息量、研发日历热力图
- **三级分析管线**（需配置 API Key）：
  1. 细粒度 Blocks — 分批从用户消息提取功能点
  2. 模块总览 — 合并为 5～15 个研发模块
  3. Markdown 报告 — 基于模块与统计生成复盘文档
- **增量提取**：新消息追加新批次，支持断点续跑

## 数据源

| 来源 | 默认路径 | 说明 |
|------|----------|------|
| Antigravity | `~/.gemini/antigravity-ide` | `brain/`、`conversations/` 等本地落盘文件 |
| Antigravity 旧版摘要 | `~/.gemini/antigravity` | `agyhub_summaries_proto.pb` |
| Cursor | `~/Library/Application Support/Cursor/User` | 只读 `state.vscdb`（**当前主要适配 macOS**） |

> Cursor 集成通过读取本地 SQLite 实现，Cursor 升级后 schema 可能变化，需自行验证。

## 环境要求

- Python 3.10+
- macOS（推荐；Cursor 路径默认按 macOS 配置）
- 已安装并使用 [Antigravity IDE](https://antigravity.google/) 和/或 Cursor

## 快速开始

```bash
git clone https://github.com/lepfinder/aicoding-chat-viewer.git
cd aicoding-chat-viewer

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# 编辑 .env，填入 OPENAI_API_KEY（仅分析功能需要）

./bin/start.sh
```

浏览器访问：<http://127.0.0.1:8788>

### 常用命令

```bash
./bin/start.sh    # 启动
./bin/stop.sh     # 停止
./bin/restart.sh  # 重启
```

启动后可在 Web 界面执行 **同步**，将 Antigravity 本地会话导入索引库。

命令行启动并同步：

```bash
python app.py --sync-on-start          # 增量同步后启动
python app.py --full-sync --sync-on-start  # 全量同步后启动
```

## 配置

复制 `.env.example` 为 `.env`：

### LLM（研发分析可选）

| 变量 | 说明 |
|------|------|
| `OPENAI_API_KEY` | OpenAI 兼容 API Key（Blocks / 报告功能必填） |
| `OPENAI_BASE_URL` | API 地址，默认火山方舟 Coding API |
| `OPENAI_MODEL` | 模型名称 |
| `OPENAI_DISABLE_THINKING` | 对支持思考的模型建议设为 `true` |

未配置 API Key 时仍可浏览会话与统计，无法使用研发分析管线。

### 路径与服务（均有默认值，可不配置）

| 变量 | 默认值 |
|------|--------|
| `ANTIGRAVITY_DATA_DIR` | `~/.gemini/antigravity-ide` |
| `ANTIGRAVITY_LEGACY_DATA_DIR` | `~/.gemini/antigravity` |
| `CHAT_VIEWER_DB_PATH` | `data/antigravity_chats.db`（相对项目根） |
| `CURSOR_USER_DIR` | `~/Library/Application Support/Cursor/User` |
| `CURSOR_DB_PATH` | `{CURSOR_USER_DIR}/globalStorage/state.vscdb` |
| `CURSOR_WS_STORAGE_DIR` | `{CURSOR_USER_DIR}/workspaceStorage` |
| `CURSOR_PROJECTS_DIR` | `~/.cursor/projects` |
| `CHAT_VIEWER_HOST` | `127.0.0.1` |
| `CHAT_VIEWER_PORT` | `8788` |

路径支持 `~` 展开；相对路径相对于项目根目录解析。Linux / Windows 用户可通过 `CURSOR_USER_DIR` 等覆盖 Cursor 路径（例如 Linux 常为 `~/.config/Cursor/User`）。

本地索引库位于 `data/`（已在 `.gitignore` 中排除）。

## 项目结构

```
├── app.py              # Flask 入口
├── sync.py             # Antigravity 增量同步
├── parser.py           # 会话解析（transcript / overview / sqlite）
├── cursor_reader.py    # Cursor state.vscdb 只读
├── report_pipeline.py  # Blocks 提取 / 合并 / 报告
├── storage.py          # SQLite 存储
├── templates/          # SSR 页面
└── bin/                # 启停脚本
```

## 隐私说明

- 本工具设计为 **本地运行**，不会上传聊天记录到第三方（除你主动配置的 LLM API 用于分析外）
- `.env` 与 `data/` 不应提交到版本库

## 已知限制

- 数据格式绑定 Antigravity 本地目录结构与 Cursor `state.vscdb` schema
- 加密会话（仅存在 `.pb` 而无可读日志）会标记为 `encrypted_only`

## License

[MIT](LICENSE)
