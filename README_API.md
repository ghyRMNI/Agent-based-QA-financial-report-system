# 财务数据分析 Agent - 前后端应用

这是一个基于 FastAPI 后端和 React 前端的财务数据分析系统。

## 项目结构

```
.
├── main.py                 # 原始的主程序（保持不变）
├── api/                    # 后端 API 代码
│   ├── __init__.py
│   ├── main.py            # FastAPI 主应用
│   ├── agent_service.py   # Agent 服务封装
│   ├── streaming_handler.py  # Print 输出捕获
│   └── models.py          # API 模型
├── frontend/              # 前端代码
│   ├── src/
│   │   ├── pages/        # 页面组件
│   │   │   ├── ChatPage.tsx      # 聊天界面
│   │   │   └── ConsolePage.tsx   # 命令行输出界面
│   │   ├── hooks/        # React Hooks
│   │   ├── utils/        # 工具函数
│   │   └── App.tsx       # 主应用组件
│   └── package.json
└── requirements.txt       # Python 依赖
```

## 安装和运行

### 1. 后端设置

```bash
# 安装 Python 依赖
pip install -r requirements.txt

# 确保 .env 文件存在并包含 OPENAI_API_KEY
# .env 文件示例：
# OPENAI_API_KEY=your_api_key_here

# 运行后端服务器
cd api
python main.py

# 或者使用 uvicorn
uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
```

后端将在 `http://localhost:8000` 启动。

### 2. 前端设置

```bash
# 进入前端目录
cd frontend

# 安装依赖
npm install

# 启动开发服务器
npm run dev
```

前端将在 `http://localhost:3000` 启动。

### 3. 构建前端（生产环境）

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
- `ws://localhost:8000/ws/chat` - 实时聊天（支持流式响应）

## 使用说明

### 聊天界面

1. 访问 `http://localhost:3000/` 打开聊天界面
2. 在输入框中输入您的问题
3. AI 会处理您的请求并返回回复

### 命令行输出界面

1. 访问 `http://localhost:3000/console` 打开命令行输出界面
2. 这个界面会实时显示所有的 print 输出
3. 当您在聊天界面发送请求时，相关的输出会在这里显示

## 功能特性

- ✅ 保持原有逻辑不变
- ✅ 双界面设计：简洁聊天界面 + 详细命令行输出
- ✅ 实时输出流式传输
- ✅ 会话管理（支持多用户）
- ✅ 错误处理和重连机制

## 注意事项

1. **环境变量**：确保 `.env` 文件包含正确的 `OPENAI_API_KEY`
2. **端口**：默认后端端口 8000，前端端口 3000
3. **CORS**：生产环境应配置正确的 CORS 设置
4. **会话管理**：当前实现使用内存存储会话，重启服务器会丢失

## 开发

### 后端开发

后端代码在 `api/` 目录中。主要文件：
- `api/main.py` - FastAPI 应用和路由
- `api/agent_service.py` - Agent 逻辑封装
- `api/streaming_handler.py` - 输出捕获机制

### 前端开发

前端代码在 `frontend/src/` 目录中。主要文件：
- `src/pages/ChatPage.tsx` - 聊天界面
- `src/pages/ConsolePage.tsx` - 命令行输出界面
- `src/hooks/useWebSocket.ts` - WebSocket Hook
- `src/hooks/useChat.ts` - 聊天 Hook

## 故障排除

1. **无法连接到后端**：检查后端是否在 8000 端口运行
2. **WebSocket 连接失败**：检查防火墙设置和 CORS 配置
3. **输出不显示**：确保在聊天界面发送请求，输出会在命令行界面显示

