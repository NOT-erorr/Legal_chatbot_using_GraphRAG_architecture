import React from 'react';

// Format time utility
const formatTime = (isoString) => {
  if (!isoString) return '';
  const d = new Date(isoString);
  if (isNaN(d.getTime())) return '';
  return d.toLocaleTimeString('vi-VN', { hour: '2-digit', minute: '2-digit' });
};

// Markdown formatting utility
const formatContent = (text) => {
  if (!text) return { __html: '' };
  
  // Safe HTML rendering for basic markdown elements
  let formatted = text
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;') // escape HTML
    .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
    .replace(/\*(.+?)\*/g, '<em>$1</em>')
    .replace(/📌\s*(.+)/g, '<span class="citation">📌 $1</span>')
    .replace(/\n/g, '<br/>');
    
  return { __html: formatted };
};

export const Message = ({ msg, userInitial }) => {
  const isAssistant = msg.role === 'assistant';
  const displayTime = formatTime(msg.time);
  
  return (
    <div className={`message message-${msg.role}`}>
      <div className="message-avatar">
        {isAssistant ? (
          <svg viewBox="0 0 24 24" width="16" height="16" fill="white"><rect x="4" y="4" width="6" height="6" rx="1"/><rect x="14" y="4" width="6" height="6" rx="1"/><rect x="4" y="14" width="6" height="6" rx="1"/><rect x="14" y="14" width="6" height="6" rx="1"/></svg>
        ) : (
          userInitial
        )}
      </div>
      <div className="message-content">
        <div 
          className="message-bubble" 
          dangerouslySetInnerHTML={formatContent(msg.content)} 
        />
        <span className="message-time">
          {displayTime}
          {msg.trace?.wall_clock_ms ? ` · ${(msg.trace.wall_clock_ms / 1000).toFixed(1)}s` : ''}
        </span>
      </div>
    </div>
  );
};

export const TypingIndicator = () => (
  <div className="typing-indicator">
    <div className="message-avatar" style={{background: 'var(--brand-blue)', display: 'flex', alignItems: 'center', justifyContent: 'center', width: '32px', height: '32px', borderRadius: '8px', flexShrink: 0}}>
      <svg viewBox="0 0 24 24" width="16" height="16" fill="white"><rect x="4" y="4" width="6" height="6" rx="1"/><rect x="14" y="4" width="6" height="6" rx="1"/><rect x="4" y="14" width="6" height="6" rx="1"/><rect x="14" y="14" width="6" height="6" rx="1"/></svg>
    </div>
    <div className="typing-dots">
      <span className="typing-dot"></span>
      <span className="typing-dot"></span>
      <span className="typing-dot"></span>
    </div>
  </div>
);
