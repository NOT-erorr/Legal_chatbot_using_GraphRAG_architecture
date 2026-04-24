import React from 'react';

// Format time for conversation list
const formatTime = (isoString) => {
  if (!isoString) return '';
  const d = new Date(isoString);
  if (isNaN(d.getTime())) return '';
  return d.toLocaleTimeString('vi-VN', { hour: '2-digit', minute: '2-digit' });
};

const groupConversations = (convs) => {
  const now = new Date();
  const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  const yesterday = new Date(today - 86400000);
  const weekAgo = new Date(today - 7 * 86400000);

  const groups = { 'Đã ghim': [], 'Hôm nay': [], 'Hôm qua': [], 'Tuần trước': [], 'Trước đó': [] };

  for (const conv of convs) {
    if (conv.pinned) {
      groups['Đã ghim'].push(conv);
      continue;
    }
    const d = new Date(conv.updatedAt || conv.createdAt);
    if (d >= today) groups['Hôm nay'].push(conv);
    else if (d >= yesterday) groups['Hôm qua'].push(conv);
    else if (d >= weekAgo) groups['Tuần trước'].push(conv);
    else groups['Trước đó'].push(conv);
  }

  // Sort each group by mostly recent first
  for (const key in groups) {
    groups[key].sort((a, b) => new Date(b.updatedAt || b.createdAt) - new Date(a.updatedAt || a.createdAt));
  }

  return groups;
};

export const Sidebar = ({ 
  conversations, 
  activeConversationId, 
  onNewChat, 
  onSelectConversation, 
  onTogglePin,
  onDeleteConversation,
  user, 
  questionCount, 
  onLogout 
}) => {
  const groupedConvs = groupConversations(conversations);

  return (
    <aside className="sidebar">
      <div className="sidebar-header">
        <div className="logo">
          <div className="logo-icon">
            <svg viewBox="0 0 24 24"><rect x="3" y="3" width="7" height="7" rx="1.5"/><rect x="14" y="3" width="7" height="7" rx="1.5"/><rect x="3" y="14" width="7" height="7" rx="1.5"/><rect x="14" y="14" width="7" height="7" rx="1.5"/></svg>
          </div>
          <span className="logo-text">LegalAI</span>
        </div>
        <button className="btn-new-chat" onClick={onNewChat}>
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>
          Hội thoại mới
        </button>
      </div>

      <div className="conversations-list">
        {conversations.length === 0 && (
          <p style={{padding: '16px', color: 'var(--text-muted)', fontSize: '0.85rem'}}>Chưa có hội thoại nào</p>
        )}
        
        {Object.entries(groupedConvs).map(([label, convs]) => (
          convs.length > 0 && (
            <React.Fragment key={label}>
              <div className="conv-group-label">{label}</div>
              {convs.map(conv => {
                const isActive = conv.id === activeConversationId;
                const msgCount = conv.messages?.length || 0;
                const lastTime = conv.messages?.length
                  ? conv.messages[conv.messages.length - 1].time
                  : conv.createdAt;
                const timeStr = formatTime(lastTime);
                
                return (
                  <div 
                    key={conv.id}
                    className={`conv-item ${isActive ? 'active' : ''}`}
                    onClick={() => onSelectConversation(conv.id)}
                  >
                    <div className="conv-item-content">
                      <span className="conv-item-title">{conv.title}</span>
                      <span className="conv-item-meta">{timeStr} · {msgCount} tin nhắn</span>
                    </div>
                    
                    <div className="conv-item-actions">
                      <button 
                        className={`conv-action-btn ${conv.pinned ? 'pinned' : ''}`}
                        title={conv.pinned ? "Bỏ ghim" : "Ghim"}
                        onClick={(e) => onTogglePin(conv.id, e)}
                      >
                        <svg width="14" height="14" viewBox="0 0 24 24" fill={conv.pinned ? "currentColor" : "none"} stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M21.44 11.05l-9.19 9.19a6 6 0 0 1-8.49-8.49l9.19-9.19a4 4 0 0 1 5.66 5.66l-9.2 9.19a2 2 0 0 1-2.83-2.83l8.49-8.48"/></svg>
                      </button>
                      <button 
                        className="conv-action-btn delete"
                        title="Xóa cuộc trò chuyện"
                        onClick={(e) => onDeleteConversation(conv.id, e)}
                      >
                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/></svg>
                      </button>
                    </div>
                  </div>
                );
              })}
            </React.Fragment>
          )
        ))}
      </div>

      <div className="sidebar-footer">
        <div className="user-avatar">{user.initials}</div>
        <div className="user-info">
          <div className="user-name">{user.name}</div>
          <div className="user-plan">Gói miễn phí · <span>{questionCount}</span>/50 câu hỏi</div>
        </div>
        <button className="btn-icon" title="Đăng xuất" onClick={onLogout}>
          <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round"><path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4"/><polyline points="16 17 21 12 16 7"/><line x1="21" y1="12" x2="9" y2="12"/></svg>
        </button>
      </div>
    </aside>
  );
};
