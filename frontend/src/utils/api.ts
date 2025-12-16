/**
 * API 工具函数
 */

const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

export interface ChatRequest {
  message: string;
  session_id?: string;
}

export interface ChatResponse {
  response: string;
  success: boolean;
  error?: string;
}

/**
 * 发送聊天消息（同步）
 */
export async function sendChatMessage(message: string, sessionId?: string): Promise<ChatResponse> {
  const response = await fetch(`${API_BASE_URL}/api/chat`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      message,
      session_id: sessionId,
    }),
  });

  if (!response.ok) {
    throw new Error(`HTTP error! status: ${response.status}`);
  }

  return response.json();
}

/**
 * WebSocket 消息类型
 */
export interface WSMessage {
  type: 'connected' | 'status' | 'response' | 'output' | 'error' | 'heartbeat' | 'message';
  content?: string;
  status?: string;
  message?: string;
}

