import React, { useState, useEffect, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import LegalAPI from '../services/api';
import { Sidebar } from '../components/Sidebar';
import { Message, TypingIndicator } from '../components/Message';
import './Chat.css';

const SUGGESTION_MAP = {
  'doanh nghiệp': ['Thủ tục đăng ký', 'Vốn điều lệ', 'Nghĩa vụ thuế', 'So sánh loại hình'],
  'lao động': ['Hợp đồng thử việc', 'Sa thải trái luật', 'Bảo hiểm thất nghiệp', 'Nghỉ phép năm'],
  'đất đai': ['Mức tiền thuê đất', 'Gia hạn hợp đồng', 'Bồi thường khi hết hạn', 'So sánh luật cũ'],
  'ly hôn': ['Chia tài sản', 'Quyền nuôi con', 'Cấp dưỡng', 'Thủ tục tòa án'],
  'thuế': ['Thuế TNCN', 'Thuế VAT', 'Thuế doanh nghiệp', 'Miễn giảm thuế'],
  'bảo hiểm': ['BHXH bắt buộc', 'BHYT gia đình', 'Bảo hiểm thất nghiệp', 'Mức đóng 2024'],
};

const detectTags = (text) => {
  const t = text.toLowerCase();
  const tags = [];
  if (t.includes('đất đai') || t.includes('đất') || t.includes('nhà')) tags.push('Đất đai');
  if (t.includes('lao động') || t.includes('hợp đồng') || t.includes('sa thải')) tags.push('Lao động');
  if (t.includes('doanh nghiệp') || t.includes('công ty')) tags.push('Doanh nghiệp');
  if (t.includes('thuế') || t.includes('tncn')) tags.push('Thuế');
  if (t.includes('ly hôn') || t.includes('hôn nhân')) tags.push('Hôn nhân');
  if (t.includes('bảo hiểm') || t.includes('bhxh')) tags.push('Bảo hiểm');
  if (t.includes('hình sự') || t.includes('tội')) tags.push('Hình sự');
  return tags.slice(0, 2);
};

export default function Chat() {
  const navigate = useNavigate();
  const messagesEndRef = useRef(null);
  const textareaRef = useRef(null);

  const [user, setUser] = useState({ name: 'Nguyễn Văn A', initials: 'NA' });
  const [conversations, setConversations] = useState([]);
  const [activeConversationId, setActiveConversationId] = useState(null);
  const [questionCount, setQuestionCount] = useState(0);
  
  const [inputValue, setInputValue] = useState('');
  const [isTyping, setIsTyping] = useState(false);
  const [suggestions, setSuggestions] = useState([]);

  // Load state on mount
  useEffect(() => {
    const sessionUser = sessionStorage.getItem('legalai_user');
    if (!sessionUser) {
      navigate('/');
      return;
    }
    setUser(JSON.parse(sessionUser));

    try {
      const savedConvs = JSON.parse(localStorage.getItem('legalai_conversations') || '[]');
      const savedCount = parseInt(localStorage.getItem('legalai_question_count') || '0');
      setConversations(savedConvs);
      setQuestionCount(savedCount);
    } catch {
      // Ignore
    }
  }, [navigate]);

  // Save conversations when updated
  useEffect(() => {
    localStorage.setItem('legalai_conversations', JSON.stringify(conversations));
    localStorage.setItem('legalai_question_count', questionCount.toString());
  }, [conversations, questionCount]);

  // Scroll to bottom when messages change
  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };
  
  useEffect(() => {
    scrollToBottom();
  }, [conversations, activeConversationId, isTyping]);

  // Handle textarea resize
  const handleInput = (e) => {
    setInputValue(e.target.value);
    e.target.style.height = 'auto';
    e.target.style.height = Math.min(e.target.scrollHeight, 120) + 'px';
  };

  const handleKeyDown = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  const activeConversation = conversations.find(c => c.id === activeConversationId);
  const messages = activeConversation?.messages || [];

  const handleSend = async (overrideText = null) => {
    const text = overrideText || inputValue.trim();
    if (!text) return;

    if (questionCount >= 50) {
      alert('Bạn đã dùng hết 50 câu hỏi miễn phí. Nâng cấp gói để tiếp tục.');
      return;
    }

    let currentConversationId = activeConversationId;

    if (!currentConversationId) {
      currentConversationId = 'conv_' + Date.now();
      const title = text.length > 50 ? text.substring(0, 50) + '…' : text;
      
      setConversations(prev => [{
        id: currentConversationId,
        title,
        messages: [],
        createdAt: new Date().toISOString(),
        tags: detectTags(text),
      }, ...prev]);
      
      setActiveConversationId(currentConversationId);
    }

    const startIso = new Date().toISOString();
    const userMsg = { role: 'user', content: text, time: startIso };
    
    setConversations(prev => prev.map(c => 
      c.id === currentConversationId 
        ? { ...c, messages: [...c.messages, userMsg], updatedAt: startIso }
        : c
    ));

    setInputValue('');
    if (textareaRef.current) {
      textareaRef.current.style.height = 'auto';
    }
    setSuggestions([]);
    setIsTyping(true);

    try {
      const result = await LegalAPI.chat(text, currentConversationId);
      
      const assistantMsg = {
        role: 'assistant',
        content: result.answer,
        time: new Date().toISOString(),
        trace: result.trace,
        citations: result.citations,
      };

      setConversations(prev => prev.map(c => 
        c.id === currentConversationId 
          ? { ...c, messages: [...c.messages, assistantMsg] }
          : c
      ));

      // Show suggestions
      const q = text.toLowerCase();
      let chips = [];
      for (const [key, vals] of Object.entries(SUGGESTION_MAP)) {
        if (q.includes(key)) { chips = vals; break; }
      }
      setSuggestions(chips);
      setQuestionCount(prev => prev + 1);

    } catch (err) {
      const errMsg = {
        role: 'assistant',
        content: `❌ Lỗi: ${err.message}\n\nVui lòng thử lại sau.`,
        time: new Date().toISOString(),
      };
      setConversations(prev => prev.map(c => 
        c.id === currentConversationId 
          ? { ...c, messages: [...c.messages, errMsg] }
          : c
      ));
    } finally {
      setIsTyping(false);
    }
  };

  const handleTogglePin = (id, e) => {
    e.stopPropagation();
    setConversations(prev => prev.map(c => 
      c.id === id ? { ...c, pinned: !c.pinned } : c
    ));
  };

  const handleDeleteConversation = (id, e) => {
    e.stopPropagation();
    if (window.confirm('Bạn có chắc chắn muốn xóa hội thoại này?')) {
      setConversations(prev => prev.filter(c => c.id !== id));
      if (activeConversationId === id) {
        setActiveConversationId(null);
      }
    }
  };

  const handleLogout = () => {
    sessionStorage.removeItem('legalai_user');
    navigate('/');
  };

  const WelcomeScreen = () => (
    <div className="welcome-screen">
      <div className="welcome-icon">
        <svg viewBox="0 0 24 24"><rect x="3" y="3" width="7" height="7" rx="1.5"/><rect x="14" y="3" width="7" height="7" rx="1.5"/><rect x="3" y="14" width="7" height="7" rx="1.5"/><rect x="14" y="14" width="7" height="7" rx="1.5"/></svg>
      </div>
      <h2>Xin chào! Tôi là LegalAI Assistant</h2>
      <p>Tôi có thể giúp bạn tra cứu và giải thích các quy định pháp luật Việt Nam dựa trên 518,255 văn bản pháp lý chính thức.</p>

      <div className="welcome-topics">
        <div className="topic-card" onClick={() => handleSend("Điều kiện thành lập doanh nghiệp tư nhân theo luật hiện hành?")}>
          <div className="topic-card-title">🏢 Doanh nghiệp</div>
          <div className="topic-card-desc">Thành lập, đăng ký, quản trị</div>
        </div>
        <div className="topic-card" onClick={() => handleSend("Quyền lợi bảo hiểm xã hội cho người lao động 2024?")}>
          <div className="topic-card-title">👷 Lao động</div>
          <div className="topic-card-desc">Hợp đồng, BHXH, sa thải</div>
        </div>
        <div className="topic-card" onClick={() => handleSend("Quyền sử dụng đất và chuyển nhượng theo Luật Đất Đai 2024?")}>
          <div className="topic-card-title">🏡 Đất đai</div>
          <div className="topic-card-desc">Quyền sử dụng, chuyển nhượng</div>
        </div>
        <div className="topic-card" onClick={() => handleSend("Thủ tục ly hôn đơn phương và quyền nuôi con?")}>
          <div className="topic-card-title">👨‍👩‍👧 Hôn nhân</div>
          <div className="topic-card-desc">Ly hôn, quyền nuôi con</div>
        </div>
      </div>
    </div>
  );

  return (
    <div className="chat-page">
      <Sidebar 
        conversations={conversations}
        activeConversationId={activeConversationId}
        onNewChat={() => setActiveConversationId(null)}
        onSelectConversation={setActiveConversationId}
        onTogglePin={handleTogglePin}
        onDeleteConversation={handleDeleteConversation}
        user={user}
        questionCount={questionCount}
        onLogout={handleLogout}
      />

      <main className="chat-main">
        {activeConversation ? (
          <header className="chat-header">
            <div className="chat-header-left">
              <h1 className="chat-title">{activeConversation.title}</h1>
              <div className="chat-tags">
                {(activeConversation.tags || []).map(t => (
                  <span key={t} className="tag">{t}</span>
                ))}
              </div>
            </div>
            <div className="chat-header-actions">
              <button className="btn-icon" title="Xuất PDF">
                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/></svg>
              </button>
            </div>
          </header>
        ) : null}

        {!activeConversation ? (
          <WelcomeScreen />
        ) : (
          <div className="chat-messages">
            {messages.map((msg, idx) => (
              <Message key={idx} msg={msg} userInitial={user.initials} />
            ))}
            {isTyping && <TypingIndicator />}
            <div ref={messagesEndRef} />
          </div>
        )}

        {suggestions.length > 0 && (
          <div className="suggestions">
            {suggestions.map(s => (
              <button key={s} className="suggestion-chip" onClick={() => handleSend(s)}>
                {s}
              </button>
            ))}
          </div>
        )}

        <div className="chat-input-area">
          <div className="chat-input-wrapper">
            <textarea
              ref={textareaRef}
              className="chat-input"
              placeholder="Hỏi về quy định pháp luật Việt Nam…"
              rows="1"
              maxLength="2000"
              value={inputValue}
              onChange={handleInput}
              onKeyDown={handleKeyDown}
            ></textarea>
            <span className="char-counter">{inputValue.length} / 2000</span>
            <div className="input-actions">
              <button 
                className={`btn-send ${inputValue.trim().length > 0 ? 'active' : ''}`} 
                onClick={() => handleSend()}
                disabled={inputValue.trim().length === 0 || isTyping}
              >
                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><line x1="22" y1="2" x2="11" y2="13"/><polygon points="22 2 15 22 11 13 2 9 22 2"/></svg>
              </button>
            </div>
          </div>
          <div className="disclaimer">
            ⚖️ Nội dung chỉ mang tính tham khảo, không thay thế tư vấn pháp lý chính thức.
          </div>
        </div>
      </main>
    </div>
  );
}
