// src/services/session.js
// Lưu / quản lý phiên đăng nhập trong sessionStorage (shape mà Chat.jsx dùng).

const SESSION_KEY = 'legalai_user';

/**
 * Lưu phiên từ AuthResponse của backend (/auth/login | /auth/register | /auth/google).
 */
export function persistSession(auth) {
  const name = auth.full_name || auth.email.split('@')[0].replace(/[._]/g, ' ');
  sessionStorage.setItem(SESSION_KEY, JSON.stringify({
    email: auth.email,
    name,
    initials: (auth.full_name || auth.email).substring(0, 2).toUpperCase(),
    user_id: auth.user_id,
    role: auth.role,
    question_limit: auth.question_limit,
    access_token: auth.access_token,
  }));
}

export function getSession() {
  try {
    return JSON.parse(sessionStorage.getItem(SESSION_KEY) || 'null');
  } catch {
    return null;
  }
}

/** JWT bearer token của phiên hiện tại (null nếu chưa đăng nhập). */
export function getToken() {
  return getSession()?.access_token || null;
}

export function clearSession() {
  sessionStorage.removeItem(SESSION_KEY);
}
