import { useState, useRef, useEffect } from 'react';
import { useChat } from '../hooks/useChat';
import './ChatPage.css';

function ChatPage() {
  const { messages, isLoading, sendMessage } = useChat();
  const [input, setInput] = useState('');
  const messagesEndRef = useRef<HTMLDivElement>(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!input.trim() || isLoading) return;

    const message = input;
    setInput('');
    await sendMessage(message);
  };

  return (
    <div className="chat-page">
      <div className="chat-container">
        <div className="chat-header">
          <h1>财务数据分析 Agent</h1>
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
    </div>
  );
}

export default ChatPage;

