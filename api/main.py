"""
FastAPI 主应用
"""
import os
import sys
import json
import asyncio
from typing import Dict, Optional
from contextlib import asynccontextmanager

# 添加项目根目录到路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

# 使用相对导入
try:
    from api.models import ChatRequest, ChatResponse, HealthResponse
    from api.agent_service import AgentService
    from api.streaming_handler import PrintCapture, OutputQueue
except ImportError:
    # 如果相对导入失败，尝试直接导入
    from models import ChatRequest, ChatResponse, HealthResponse
    from agent_service import AgentService
    from streaming_handler import PrintCapture, OutputQueue

# 全局 agent 服务实例（每个会话独立）
# 为了支持多用户，我们使用字典存储不同会话的 agent 服务
_agent_services: Dict[str, AgentService] = {}
_output_queues: Dict[str, OutputQueue] = {}
_active_console_websockets: Dict[str, WebSocket] = {}  # 存储活跃的控制台 WebSocket 连接


def get_or_create_agent_service(session_id: str = "default") -> AgentService:
    """获取或创建指定会话的 agent 服务"""
    if session_id not in _agent_services:
        service = AgentService()
        service.initialize()
        _agent_services[session_id] = service
    return _agent_services[session_id]


def get_or_create_output_queue(session_id: str = "default") -> OutputQueue:
    """获取或创建指定会话的输出队列"""
    if session_id not in _output_queues:
        _output_queues[session_id] = OutputQueue()
    return _output_queues[session_id]


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    # 启动时初始化
    print("FastAPI application starting...")
    yield
    # 关闭时清理
    print("FastAPI application shutting down...")


app = FastAPI(
    title="Financial Agent API",
    description="财务数据分析 Agent API",
    version="1.0.0",
    lifespan=lifespan
)

# 配置 CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 生产环境应该限制具体域名
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/", response_model=HealthResponse)
async def root():
    """根路径，返回健康检查信息"""
    try:
        service = get_or_create_agent_service()
        return HealthResponse(status="ok", initialized=service._initialized)
    except Exception as e:
        return HealthResponse(status=f"error: {str(e)}", initialized=False)


@app.get("/api/health", response_model=HealthResponse)
async def health_check():
    """健康检查端点"""
    try:
        service = get_or_create_agent_service()
        return HealthResponse(status="ok", initialized=service._initialized)
    except Exception as e:
        return HealthResponse(status=f"error: {str(e)}", initialized=False)


@app.post("/api/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    """
    处理聊天请求
    
    这是一个同步端点，返回完整的响应。
    如果需要流式响应，可以使用 WebSocket 端点。
    """
    session_id = request.session_id or "default"
    service = get_or_create_agent_service(session_id)
    output_queue = get_or_create_output_queue(session_id)
    
    # 设置输出捕获，将输出添加到队列
    def output_callback(text: str):
        """同步回调，将输出添加到队列（立即添加，确保实时）"""
        output_queue.put(text)
        # 注意：这里不能使用 await，因为这是同步回调
        # WebSocket 读取循环会实时从队列中取出消息
    
    capture = PrintCapture(callback=output_callback)
    
    # 先输出用户输入
    output_queue.put(f"\n你: {request.message}\n\n")
    
    # 创建 LangChain 回调处理器，用于捕获 Agent 的思考过程
    from api.streaming_handler import LangChainCallbackHandler
    langchain_callback = LangChainCallbackHandler(output_callback=output_callback)
    callbacks = [langchain_callback]
    
    try:
        # 开始捕获输出
        capture.start_capture()
        
        # 将同步的 Agent 执行放到线程池中运行，避免阻塞事件循环
        # 这样 WebSocket 循环可以继续运行，实时读取队列
        import concurrent.futures
        
        def run_agent():
            """在线程池中运行 Agent"""
            return service.process_user_input(request.message, callbacks=callbacks)
        
        # 使用线程池执行同步任务，同时确保事件循环继续运行
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # 提交任务到线程池
            future = executor.submit(run_agent)
            
            # 等待任务完成，但允许事件循环继续运行
            while not future.done():
                # 让事件循环有机会处理其他任务（包括 WebSocket 读取）
                await asyncio.sleep(0.01)
            
            # 获取结果
            result = future.result()
        
        # 停止捕获
        captured_output = capture.stop_capture()
        
        # 输出 AI 回复
        ai_response = result.get("response", "")
        output_queue.put(f"AI: {ai_response}\n")
        output_queue.put("-" * 60 + "\n")
        
        return ChatResponse(
            response=ai_response,
            success=result.get("success", True),
            error=result.get("error")
        )
    except Exception as e:
        capture.stop_capture()
        return ChatResponse(
            response=f"处理请求时出错: {str(e)}",
            success=False,
            error=str(e)
        )


@app.websocket("/ws/console")
async def websocket_console(websocket: WebSocket):
    """
    WebSocket 端点，用于实时传输所有 print 输出
    
    客户端连接后，会实时接收到所有的 print 输出。
    """
    await websocket.accept()
    session_id = "default"
    
    # 注册这个 WebSocket 连接
    _active_console_websockets[session_id] = websocket
    
    try:
        output_queue = get_or_create_output_queue(session_id)
        
        # 不发送欢迎消息，避免重复显示
        # 欢迎消息只会在第一次连接时显示一次（通过前端处理）
        
        # 持续发送队列中的消息（快速轮询以实现流式输出）
        import time
        last_heartbeat_time = time.time()
        
        while True:
            # 快速轮询队列（超时时间很短，确保实时性）
            message = output_queue.get(timeout=0.02)
            if message:
                try:
                    await websocket.send_text(json.dumps({
                        "type": "output",
                        "content": message
                    }))
                    last_heartbeat_time = time.time()  # 有消息时更新心跳时间
                except Exception:
                    # WebSocket 发送失败，退出循环
                    break
            else:
                # 队列为空，但需要保持活跃，短时间等待后继续
                current_time = time.time()
                # 每5秒发送一次心跳
                if current_time - last_heartbeat_time > 5:
                    try:
                        await websocket.send_text(json.dumps({
                            "type": "heartbeat"
                        }))
                        last_heartbeat_time = current_time
                    except Exception:
                        # WebSocket 发送失败，退出循环
                        break
                # 短暂等待，避免 CPU 占用过高，但保持快速响应
                await asyncio.sleep(0.01)
                
    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"WebSocket error: {e}")
        try:
            await websocket.send_text(json.dumps({
                "type": "error",
                "content": str(e)
            }))
        except:
            pass
    finally:
        # 清理：移除 WebSocket 连接
        _active_console_websockets.pop(session_id, None)


@app.websocket("/ws/chat")
async def websocket_chat(websocket: WebSocket):
    """
    WebSocket 端点，用于实时聊天（支持流式响应）
    
    客户端发送消息，服务器实时返回响应和所有 print 输出。
    """
    await websocket.accept()
    session_id = "default"
    
    try:
        service = get_or_create_agent_service(session_id)
        output_queue = get_or_create_output_queue(session_id)
        
        await websocket.send_text(json.dumps({
            "type": "connected",
            "message": "已连接到聊天服务"
        }))
        
        while True:
            # 接收客户端消息
            data = await websocket.receive_text()
            message_data = json.loads(data)
            user_message = message_data.get("message", "")
            session_id = message_data.get("session_id", "default")
            
            if not user_message:
                continue
            
            # 更新服务实例（如果会话ID改变）
            service = get_or_create_agent_service(session_id)
            output_queue = get_or_create_output_queue(session_id)
            output_queue.clear()
            
            # 设置输出捕获
            def output_callback(text: str):
                output_queue.put(text)
            
            capture = PrintCapture(callback=output_callback)
            
            try:
                # 开始捕获
                capture.start_capture()
                
                # 发送开始处理的消息
                await websocket.send_text(json.dumps({
                    "type": "status",
                    "status": "processing",
                    "message": "正在处理您的请求..."
                }))
                
                # 处理用户输入（在后台任务中）
                result = service.process_user_input(user_message)
                
                # 发送 AI 响应
                await websocket.send_text(json.dumps({
                    "type": "response",
                    "content": result.get("response", "")
                }))
                
                # 发送所有捕获的输出
                captured_output = capture.stop_capture()
                if captured_output:
                    await websocket.send_text(json.dumps({
                        "type": "output",
                        "content": captured_output
                    }))
                
                # 发送处理完成消息
                await websocket.send_text(json.dumps({
                    "type": "status",
                    "status": "completed"
                }))
                
            except Exception as e:
                capture.stop_capture()
                await websocket.send_text(json.dumps({
                    "type": "error",
                    "content": str(e)
                }))
                
    except WebSocketDisconnect:
        print(f"WebSocket client disconnected: {session_id}")
    except Exception as e:
        print(f"WebSocket error: {e}")
        try:
            await websocket.send_text(json.dumps({
                "type": "error",
                "content": str(e)
            }))
        except:
            pass


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

