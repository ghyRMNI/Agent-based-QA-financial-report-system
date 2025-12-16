import { useState, useEffect, useRef } from 'react';
import { useWebSocket, WSMessage } from '../hooks/useWebSocket';
import './ConsolePage.css';

function ConsolePage() {
  const [output, setOutput] = useState<string[]>([]);
  const [isAutoScroll, setIsAutoScroll] = useState(true);
  const outputEndRef = useRef<HTMLDivElement>(null);
  const outputContainerRef = useRef<HTMLDivElement>(null);

  const handleMessage = (message: WSMessage) => {
    if (message.type === 'output' && message.content) {
      setOutput(prev => [...prev, message.content!]);
    } else if (message.type === 'message' && message.content) {
      setOutput(prev => [...prev, message.content!]);
    } else if (message.type === 'error' && message.content) {
      setOutput(prev => [...prev, `[ERROR] ${message.content}`]);
    }
  };

  const wsUrl = `ws://localhost:8000/ws/console`;
  const { isConnected, error: wsError } = useWebSocket(wsUrl, handleMessage);

  useEffect(() => {
    if (isAutoScroll && outputEndRef.current) {
      outputEndRef.current.scrollIntoView({ behavior: 'smooth' });
    }
  }, [output, isAutoScroll]);

  const handleScroll = () => {
    if (outputContainerRef.current) {
      const { scrollTop, scrollHeight, clientHeight } = outputContainerRef.current;
      const isAtBottom = scrollHeight - scrollTop - clientHeight < 50;
      setIsAutoScroll(isAtBottom);
    }
  };

  const clearOutput = () => {
    setOutput([]);
  };

  return (
    <div className="console-page">
      <div className="console-container">
        <div className="console-header">
          <div className="console-title">
            <h1>命令行输出</h1>
            <div className={`connection-status ${isConnected ? 'connected' : 'disconnected'}`}>
              <span className="status-dot"></span>
              <span>{isConnected ? '已连接' : '未连接'}</span>
            </div>
          </div>
          <div className="console-controls">
            <button
              className="console-button"
              onClick={clearOutput}
              title="清空输出"
            >
              🗑️ 清空
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
          ref={outputContainerRef}
          onScroll={handleScroll}
        >
          {output.length === 0 && (
            <div className="console-empty">
              {wsError ? (
                <div className="error-message">
                  <p>❌ 连接错误: {wsError}</p>
                  <p>请确保后端服务正在运行 (http://localhost:8000)</p>
                </div>
              ) : !isConnected ? (
                <div className="loading-message">
                  <p>⏳ 正在连接到服务器...</p>
                </div>
              ) : (
                <div className="info-message">
                  <p>✅ 已连接到控制台输出流</p>
                  <p>等待输出...</p>
                </div>
              )}
            </div>
          )}

          {output.map((line, index) => (
            <div key={index} className="console-line">
              <span className="line-number">{index + 1}</span>
              <span className="line-content">{line}</span>
            </div>
          ))}

          <div ref={outputEndRef} />
        </div>

        <div className="console-footer">
          <div className="console-info">
            <span>总行数: {output.length}</span>
            {isAutoScroll && <span className="auto-scroll-indicator">自动滚动中</span>}
          </div>
        </div>
      </div>
    </div>
  );
}

export default ConsolePage;

