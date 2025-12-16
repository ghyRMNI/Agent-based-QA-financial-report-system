# 财务数据分析 Agent - 前后端应用

这是一个基于 FastAPI 后端和 React 前端的财务数据分析系统。

## 环境要求

- Python 3.8+
- Node.js 16+
- npm 或 yarn

## 项目结构

```
.
├── main.py                 # 原始的主程序（保持不变）
├── api/                    # 后端 API 代码
│   ├── main.py            # FastAPI 主应用
│   ├── agent_service.py   # Agent 服务封装
│   ├── streaming_handler.py  # Print 输出捕获
│   └── models.py          # API 模型
├── frontend/              # 前端代码
│   ├── src/
│   │   ├── pages/
│   │   │   └── MainPage.tsx   # 主页面（合并了聊天和控制台）
│   │   └── ...
│   └── package.json
├── requirements.txt       # Python 依赖
└── .env                   # 环境变量（需要创建）
```

## 快速开始

### 第一步：配置环境变量

在项目根目录创建 `.env` 文件，并添加以下内容：

```env
OPENAI_API_KEY=your_api_key_here
```

### 第二步：后端

**注意**：前端和后端需要在**两个不同的命令窗口**中运行。

1. 在项目根目录打开第一个命令窗口（终端/命令行），执行：
```bash
pip install -r requirements.txt
```

2. 命令窗口**中（已在项目根目录），执行以下命令
```bash
python -m uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
```

后端启动后，你应该看到类似以下的输出：
```
INFO:     Uvicorn running on http://0.0.0.0:8000 (Press CTRL+C to quit)
INFO:     Started reloader process
INFO:     Started server process
INFO:     Waiting for application startup.
INFO:     Application startup complete.
```

后端将在 `http://localhost:8000` 启动。

### 第三步：前端

**注意**：前端和后端需要在**两个不同的命令窗口**中运行。

在项目根目录打开第二个命令窗口（终端/命令行），执行：

```bash
cd frontend
npm install
```

在**第二个命令窗口**中，确保在 `frontend` 目录下，执行：

```bash
npm run dev
```

前端启动后，你应该看到类似以下的输出：
```
  VITE v5.x.x  ready in xxx ms

  ➜  Local:   http://localhost:3000/
  ➜  Network: use --host to expose
```

前端将在 `http://localhost:3000` 启动。

### 第四步：访问应用

在浏览器中打开 `http://localhost:3000`，你将看到：

- **左侧**：聊天界面 - 输入你的问题，AI 会回复
- **右侧**：命令行输出 - 实时显示所有 print 输出和 Agent 的思考过程

## 使用说明

### 聊天界面

1. 在左侧输入框中输入你的问题
2. 点击"发送"按钮或按 Enter 键
3. AI 会处理你的请求并回复

### 命令行输出

右侧的命令行窗口会实时显示：
- 用户输入
- Agent 的思考过程
- 工具调用信息
- 所有的 print 输出
- AI 的回复

## 停止服务

要停止服务，分别在两个命令窗口中按 `Ctrl+C`（Windows/Linux/Mac）。

## 常见问题

### 1. 端口被占用

如果 8000 或 3000 端口被占用，可以修改：

**后端端口**：修改 `api/main.py` 中的端口号，或使用：
```bash
python -m uvicorn api.main:app --host 0.0.0.0 --port 8001 --reload
```

**前端端口**：修改 `frontend/vite.config.ts` 中的 `port` 配置。

### 2. 无法连接到后端

- 确保后端在 8000 端口运行
- 检查浏览器控制台（F12）是否有错误
- 确认 `frontend/vite.config.ts` 中的代理配置正确

### 3. 前端显示空白

- 检查浏览器控制台是否有错误
- 确认前端依赖已正确安装（`npm install`）
- 尝试清除缓存：`npm run build` 然后 `npm run dev`

### 4. WebSocket 连接失败

- 确保后端正在运行
- 检查防火墙设置
- 查看后端控制台是否有错误信息

### 5. API Key 错误

- 检查 `.env` 文件是否存在
- 确认 `OPENAI_API_KEY` 已正确设置
- 确保 `.env` 文件在项目根目录

## 开发

### 后端开发

后端代码在 `api/` 目录中。修改代码后，如果使用了 `--reload` 参数，后端会自动重新加载。

### 前端开发

前端代码在 `frontend/src/` 目录中。修改代码后，前端会自动热重载。

### 构建生产版本

**前端：**
```bash
cd frontend
npm run build
```

构建后的文件在 `frontend/dist` 目录中。

## API 端点

### REST API

- `GET /api/health` - 健康检查
- `POST /api/chat` - 发送聊天消息

### WebSocket

- `ws://localhost:8000/ws/console` - 实时控制台输出流

## 技术栈

- **后端**：FastAPI, LangChain, OpenAI
- **前端**：React, TypeScript, Vite
- **通信**：REST API, WebSocket

## 许可证

[根据需要添加许可证信息]
