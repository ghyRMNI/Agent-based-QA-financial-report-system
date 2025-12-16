import { useState, useRef, useEffect } from 'react';
import { useChat } from '../hooks/useChat';
import { useWebSocket, WSMessage } from '../hooks/useWebSocket';
import './MainPage.css';

function MainPage() {
  const { messages, isLoading, sendMessage } = useChat();
  const [input, setInput] = useState('');
  const [consoleOutput, setConsoleOutput] = useState<string[]>([]);
  const [isAutoScroll, setIsAutoScroll] = useState(true);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const consoleEndRef = useRef<HTMLDivElement>(null);
  const consoleContainerRef = useRef<HTMLDivElement>(null);

  // WebSocket 连接用于控制台输出
  const wsUrl = `ws://localhost:8000/ws/console`;
  const handleConsoleMessage = (message: WSMessage) => {
    console.log('Console message received:', message);
    if (message.type === 'output' && message.content) {
      setConsoleOutput(prev => [...prev, message.content!]);
    } else if (message.type === 'message' && message.content) {
      setConsoleOutput(prev => [...prev, message.content!]);
    } else if (message.type === 'error' && message.content) {
      setConsoleOutput(prev => [...prev, `[ERROR] ${message.content}`]);
    }
    // 忽略 heartbeat 消息
  };
  const { isConnected: wsConnected } = useWebSocket(wsUrl, handleConsoleMessage);

  // 聊天消息滚动
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  // 控制台输出滚动
  useEffect(() => {
    if (isAutoScroll && consoleEndRef.current) {
      consoleEndRef.current.scrollIntoView({ behavior: 'smooth' });
    }
  }, [consoleOutput, isAutoScroll]);

  const handleScroll = () => {
    if (consoleContainerRef.current) {
      const { scrollTop, scrollHeight, clientHeight } = consoleContainerRef.current;
      const isAtBottom = scrollHeight - scrollTop - clientHeight < 50;
      setIsAutoScroll(isAtBottom);
    }
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!input.trim() || isLoading) return;

    const message = input;
    setInput('');
    await sendMessage(message);
  };

  const clearConsole = () => {
    setConsoleOutput([]);
  };

  return (
    <div className="main-page">
      <div className="main-container">
        {/* 左侧：聊天区域 */}
        <div className="chat-section">
          <div className="chat-header">
            <h2>财务数据分析 Agent</h2>
            <p>输入您的问题，我将帮助您分析财务数据</p>
          </div>

          <div className="chat-messages">
            {messages.length === 0 && (
              <div className="chat-empty">
                <p>👋 您好！我是财务数据分析助手</p>
                <p>您可以问我关于股票数据收集或财务数据分析的问题</p>
              </div>
            )}
            
            {messages.map((message) => (
              <div
                key={message.id}
                className={`chat-message ${message.role === 'user' ? 'user-message' : 'assistant-message'}`}
              >
                <div className="message-content">
                  {message.content.split('\n').map((line, idx) => (
                    <div key={idx}>{line || <br />}</div>
                  ))}
                </div>
                <div className="message-time">
                  {message.timestamp.toLocaleTimeString()}
                </div>
              </div>
            ))}
            
            {isLoading && (
              <div className="chat-message assistant-message">
                <div className="message-content">
                  <div className="loading-dots">
                    <span></span>
                    <span></span>
                    <span></span>
                  </div>
                </div>
              </div>
            )}
            
            <div ref={messagesEndRef} />
          </div>

          <form className="chat-input-form" onSubmit={handleSubmit}>
            <input
              type="text"
              className="chat-input"
              value={input}
              onChange={(e) => setInput(e.target.value)}
              placeholder="输入您的问题..."
              disabled={isLoading}
            />
            <button
              type="submit"
              className="chat-send-button"
              disabled={!input.trim() || isLoading}
            >
              发送
            </button>
          </form>
        </div>

        {/* 右侧：控制台输出区域 */}
        <div className="console-section">
          <div className="console-header">
            <div className="console-title">
              <h2>控制台输出</h2>
              <div className={`connection-status ${wsConnected ? 'connected' : 'disconnected'}`}>
                <span className="status-dot"></span>
                <span>{wsConnected ? '已连接' : '未连接'}</span>
              </div>
            </div>
            <div className="console-controls">
              <button
                className="console-button"
                onClick={clearConsole}
                title="清空输出"
              >
                清空
              </button>
              <label className="auto-scroll-toggle">
                <input
                  type="checkbox"
                  checked={isAutoScroll}
                  onChange={(e) => setIsAutoScroll(e.target.checked)}
                />
                <span>自动滚动</span>
              </label>
            </div>
          </div>

          <div
            className="console-output"
            ref={consoleContainerRef}
            onScroll={handleScroll}
          >
            {consoleOutput.length === 0 && (
              <div className="console-empty">
                <p>✅ 已连接到控制台输出流</p>
                <p>等待输出...</p>
              </div>
            )}

            {consoleOutput.map((line, index) => (
              <div key={index} className="console-line">
                <span className="line-number">{index + 1}</span>
                <span className="line-content">{line}</span>
              </div>
            ))}

            <div ref={consoleEndRef} />
          </div>

          <div className="console-footer">
            <div className="console-info">
              <span>总行数: {consoleOutput.length}</span>
              {isAutoScroll && <span className="auto-scroll-indicator">自动滚动中</span>}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

export default MainPage;

