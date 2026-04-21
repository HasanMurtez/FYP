import React, { useState, useRef, useEffect } from 'react';
import './ChatBot.css';

const API_KEY_STORAGE = 'fcm_chat_api_key';

// Suggested questions for quick access
const SUGGESTIONS = [
  "Which players should I rest this week?",
  "Who are the highest risk players across the league?",
  "Compare Salah and Saka",
  "Which team has the most injuries?",
  "Should I play Bruno Fernandes this week?",
];

function ChatBot({ apiBaseUrl }) {
  const [isOpen, setIsOpen] = useState(false);
  const [messages, setMessages] = useState([
    {
      role: 'assistant',
      content: "Hey boss! I'm your AI Manager Assistant. I've got the full squad data for all 20 Premier League teams. Ask me anything — should you play someone, who's high risk, who to scout as a replacement, you name it."
    }
  ]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const [apiKey, setApiKey] = useState(() => {
    try { return localStorage.getItem(API_KEY_STORAGE) || ''; } catch { return ''; }
  });
  const [showKeyInput, setShowKeyInput] = useState(false);
  const messagesEndRef = useRef(null);
  const inputRef = useRef(null);

  useEffect(() => {
    if (messagesEndRef.current) {
      messagesEndRef.current.scrollIntoView({ behavior: 'smooth' });
    }
  }, [messages]);

  useEffect(() => {
    if (isOpen && inputRef.current) {
      inputRef.current.focus();
    }
  }, [isOpen]);

  const saveApiKey = (key) => {
    setApiKey(key);
    try { localStorage.setItem(API_KEY_STORAGE, key); } catch {}
    setShowKeyInput(false);
  };

  const sendMessage = async (text) => {
    const messageText = text || input.trim();
    if (!messageText || loading) return;

    if (!apiKey) {
      setShowKeyInput(true);
      return;
    }

    const userMessage = { role: 'user', content: messageText };
    const newMessages = [...messages, userMessage];
    setMessages(newMessages);
    setInput('');
    setLoading(true);

    try {
      const response = await fetch(`${apiBaseUrl}/api/chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          message: messageText,
          history: newMessages.slice(1), // exclude initial greeting
          api_key: apiKey
        })
      });

      const data = await response.json();

      if (data.success) {
        setMessages([...newMessages, {
          role: 'assistant',
          content: data.data.reply
        }]);
      } else {
        setMessages([...newMessages, {
          role: 'assistant',
          content: `Sorry boss, something went wrong: ${data.error}. Check your API key maybe?`
        }]);
      }
    } catch (err) {
      setMessages([...newMessages, {
        role: 'assistant',
        content: "Can't reach the server right now. Make sure the backend is running."
      }]);
    } finally {
      setLoading(false);
    }
  };

  const handleKeyDown = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      sendMessage();
    }
  };

  const handleSuggestionClick = (suggestion) => {
    sendMessage(suggestion);
  };

  const showSuggestions = messages.length <= 1 && !loading;

  return (
    <>
      {/* Chat Toggle Button */}
      <button
        className={`chat-toggle ${isOpen ? 'chat-toggle-open' : ''}`}
        onClick={() => setIsOpen(!isOpen)}
        title="AI Manager Assistant"
      >
        {isOpen ? '✕' : '⚽'}
      </button>

      {/* Chat Panel */}
      {isOpen && (
        <div className="chat-panel">
          {/* Header */}
          <div className="chat-header">
            <div className="chat-header-info">
              <div className="chat-header-dot" />
              <div>
                <div className="chat-header-title">AI Manager</div>
                <div className="chat-header-subtitle">Premier League Assistant</div>
              </div>
            </div>
            <button
              className="chat-settings-btn"
              onClick={() => setShowKeyInput(!showKeyInput)}
              title="API Key Settings"
            >
              ⚙
            </button>
          </div>

          {/* API Key Input */}
          {showKeyInput && (
            <div className="chat-key-input">
              <input
                type="password"
                placeholder="Enter Anthropic API key..."
                value={apiKey}
                onChange={(e) => setApiKey(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter') saveApiKey(apiKey);
                }}
              />
              <button onClick={() => saveApiKey(apiKey)}>Save</button>
            </div>
          )}

          {/* Messages */}
          <div className="chat-messages">
            {messages.map((msg, i) => (
              <div
                key={i}
                className={`chat-msg ${msg.role === 'user' ? 'chat-msg-user' : 'chat-msg-ai'}`}
              >
                {msg.role === 'assistant' && (
                  <div className="chat-msg-avatar">⚽</div>
                )}
                <div className="chat-msg-bubble">
                  {msg.content}
                </div>
              </div>
            ))}

            {loading && (
              <div className="chat-msg chat-msg-ai">
                <div className="chat-msg-avatar">⚽</div>
                <div className="chat-msg-bubble chat-typing">
                  <span className="typing-dot" />
                  <span className="typing-dot" />
                  <span className="typing-dot" />
                </div>
              </div>
            )}

            {/* Suggestions */}
            {showSuggestions && (
              <div className="chat-suggestions">
                {SUGGESTIONS.map((s, i) => (
                  <button
                    key={i}
                    className="chat-suggestion-btn"
                    onClick={() => handleSuggestionClick(s)}
                  >
                    {s}
                  </button>
                ))}
              </div>
            )}

            <div ref={messagesEndRef} />
          </div>

          {/* Input */}
          <div className="chat-input-area">
            <input
              ref={inputRef}
              type="text"
              placeholder="Ask about your squad..."
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={handleKeyDown}
              disabled={loading}
              className="chat-input"
            />
            <button
              onClick={() => sendMessage()}
              disabled={loading || !input.trim()}
              className="chat-send-btn"
            >
              ↑
            </button>
          </div>
        </div>
      )}
    </>
  );
}

export default ChatBot;
