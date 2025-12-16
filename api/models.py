"""
API 请求和响应模型
"""
from pydantic import BaseModel, Field
from typing import Optional


class ChatRequest(BaseModel):
    """聊天请求模型"""
    message: str = Field(..., description="用户输入的消息")
    session_id: Optional[str] = Field(None, description="会话ID，用于保持对话上下文")


class ChatResponse(BaseModel):
    """聊天响应模型"""
    response: str = Field(..., description="AI的回复")
    success: bool = Field(..., description="请求是否成功")
    error: Optional[str] = Field(None, description="错误信息（如果失败）")


class HealthResponse(BaseModel):
    """健康检查响应模型"""
    status: str = Field(..., description="服务状态")
    initialized: bool = Field(..., description="Agent是否已初始化")

