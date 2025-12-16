# 快速启动指南

## 前置要求

1. Python 3.8+
2. Node.js 16+
3. `.env` 文件包含 `OPENAI_API_KEY`

## 启动步骤

### 1. 安装后端依赖

```bash
pip install -r requirements.txt
```

### 2. 启动后端服务器

**Windows:**
```bash
start_backend.bat
```

**Linux/Mac:**
```bash
chmod +x start_backend.sh
./start_backend.sh
```

**或者直接使用 uvicorn:**
```bash
uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
```

后端将在 `http://localhost:8000` 启动

### 3. 安装前端依赖

```bash
cd frontend
npm install
```

### 4. 启动前端开发服务器

```bash
npm run dev
```

前端将在 `http://localhost:3000` 启动

## 使用

1. **聊天界面**: 访问 `http://localhost:3000/`
   - 简单的输入框界面
   - 输入问题，AI 会回复

2. **命令行输出界面**: 访问 `http://localhost:3000/console`
   - 实时显示所有 print 输出
   - 当在聊天界面发送请求时，相关输出会在这里显示

## 测试 API

### 健康检查
```bash
curl http://localhost:8000/api/health
```

### 发送聊天消息
```bash
curl -X POST http://localhost:8000/api/chat \
  -H "Content-Type: application/json" \
  -d '{"message": "你好"}'
```

## 常见问题

1. **端口被占用**: 修改 `api/main.py` 中的端口号或前端 `vite.config.ts` 中的代理端口
2. **无法连接**: 确保后端在 8000 端口运行
3. **WebSocket 错误**: 检查防火墙和 CORS 设置

