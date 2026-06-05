import { useState, useEffect, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import LegalAPI from '../services/api';
import { persistSession } from '../services/session';
import './Login.css';

// Strip dấu nháy bao quanh nếu secret lỡ lưu kèm '...' / "..."
const GOOGLE_CLIENT_ID = (import.meta.env.VITE_GOOGLE_CLIENT_ID ?? '').trim().replace(/^['"]|['"]$/g, '');

function Register() {
  const navigate = useNavigate();
  const googleBtnRef = useRef(null);

  const [fullName, setFullName] = useState('');
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [confirm, setConfirm] = useState('');
  const [showPassword, setShowPassword] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  // ── Google Identity Services ──────────────────────────────────────────
  useEffect(() => {
    if (!GOOGLE_CLIENT_ID) return;

    const handleCredential = async (response) => {
      setError('');
      setLoading(true);
      try {
        const auth = await LegalAPI.googleAuth(response.credential);
        persistSession(auth);
        navigate('/chat');
      } catch (err) {
        setError(err.message || 'Đăng ký bằng Google thất bại');
        setLoading(false);
      }
    };

    const init = () => {
      if (!window.google || !googleBtnRef.current) return;
      window.google.accounts.id.initialize({
        client_id: GOOGLE_CLIENT_ID,
        callback: handleCredential,
      });
      window.google.accounts.id.renderButton(googleBtnRef.current, {
        theme: 'outline',
        size: 'large',
        width: 360,
        text: 'signup_with',
        locale: 'vi',
      });
    };

    if (window.google) {
      init();
      return;
    }
    const script = document.createElement('script');
    script.src = 'https://accounts.google.com/gsi/client';
    script.async = true;
    script.defer = true;
    script.onload = init;
    document.body.appendChild(script);
  }, [navigate]);

  const handleRegister = async (e) => {
    e.preventDefault();
    setError('');

    if (!email || !password) return;
    if (password.length < 6) {
      setError('Mật khẩu phải có ít nhất 6 ký tự');
      return;
    }
    if (password !== confirm) {
      setError('Mật khẩu nhập lại không khớp');
      return;
    }

    setLoading(true);
    try {
      const auth = await LegalAPI.register(email, password, fullName || null);
      persistSession(auth);
      navigate('/chat');
    } catch (err) {
      setError(err.message || 'Đăng ký thất bại');
      setLoading(false);
    }
  };

  return (
    <div className="login-page">
      <section className="login-hero">
        <div className="logo">
          <div className="logo-icon">
            <svg viewBox="0 0 24 24"><rect x="3" y="3" width="7" height="7" rx="1.5"/><rect x="14" y="3" width="7" height="7" rx="1.5"/><rect x="3" y="14" width="7" height="7" rx="1.5"/><rect x="14" y="14" width="7" height="7" rx="1.5"/></svg>
          </div>
          <span className="logo-text">LegalAI Assistant</span>
        </div>

        <div className="hero-content">
          <h1>Tạo tài khoản<br />miễn phí</h1>
          <p>Đăng ký để tra cứu, hỏi đáp về luật Thuế Việt Nam với AI được huấn luyện trên hơn 20,000 văn bản pháp lý về thuế chính thức.</p>

          <div className="hero-stats">
            <div className="stat-item">
              <div className="stat-value">20K+</div>
              <div className="stat-label">văn bản<br />pháp lý</div>
            </div>
            <div className="stat-item">
              <div className="stat-value">&gt;95%</div>
              <div className="stat-label">độ chính<br />xác</div>
            </div>
            <div className="stat-item">
              <div className="stat-value">50</div>
              <div className="stat-label">câu hỏi<br />miễn phí</div>
            </div>
          </div>
        </div>
      </section>

      <section className="login-form-panel">
        <h2>Đăng ký</h2>
        <p className="subtitle">Tạo tài khoản để bắt đầu tra cứu pháp lý thông minh</p>

        {error && <div className="form-error">{error}</div>}

        <form onSubmit={handleRegister}>
          <div className="form-group">
            <label className="form-label" htmlFor="fullName">Họ và tên</label>
            <input
              id="fullName"
              className="form-input"
              type="text"
              placeholder="Nguyễn Văn A"
              value={fullName}
              onChange={e => setFullName(e.target.value)}
            />
          </div>

          <div className="form-group">
            <label className="form-label" htmlFor="email">Email *</label>
            <input
              id="email"
              className="form-input"
              type="email"
              placeholder="nguyen.van.a@example.com"
              value={email}
              onChange={e => setEmail(e.target.value)}
              required
            />
          </div>

          <div className="form-group">
            <label className="form-label" htmlFor="password">Mật khẩu *</label>
            <div className="password-wrapper">
              <input
                id="password"
                className="form-input"
                type={showPassword ? 'text' : 'password'}
                placeholder="Tối thiểu 6 ký tự"
                value={password}
                onChange={e => setPassword(e.target.value)}
                required
              />
              <button
                type="button"
                className="password-toggle"
                onClick={() => setShowPassword(!showPassword)}
              >
                {showPassword ? (
                  <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M17.94 17.94A10.07 10.07 0 0 1 12 20c-7 0-11-8-11-8a18.45 18.45 0 0 1 5.06-5.94M9.9 4.24A9.12 9.12 0 0 1 12 4c7 0 11 8 11 8a18.5 18.5 0 0 1-2.16 3.19m-6.72-1.07a3 3 0 1 1-4.24-4.24"/><line x1="1" y1="1" x2="23" y2="23"/></svg>
                ) : (
                  <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg>
                )}
              </button>
            </div>
          </div>

          <div className="form-group">
            <label className="form-label" htmlFor="confirm">Nhập lại mật khẩu *</label>
            <input
              id="confirm"
              className="form-input"
              type={showPassword ? 'text' : 'password'}
              placeholder="••••••••"
              value={confirm}
              onChange={e => setConfirm(e.target.value)}
              required
            />
          </div>

          <button type="submit" className="btn btn-primary btn-block" disabled={loading} style={{ opacity: loading ? 0.7 : 1 }}>
            {loading ? 'Đang tạo tài khoản…' : 'Đăng ký'}
          </button>
        </form>

        <div className="divider">hoặc</div>

        {GOOGLE_CLIENT_ID ? (
          <div className="google-btn-container" ref={googleBtnRef}></div>
        ) : (
          <button className="btn-google" disabled title="Chưa cấu hình VITE_GOOGLE_CLIENT_ID">
            <svg width="20" height="20" viewBox="0 0 48 48"><path fill="#EA4335" d="M24 9.5c3.54 0 6.71 1.22 9.21 3.6l6.85-6.85C35.9 2.38 30.47 0 24 0 14.62 0 6.51 5.38 2.56 13.22l7.98 6.19C12.43 13.72 17.74 9.5 24 9.5z"/><path fill="#4285F4" d="M46.98 24.55c0-1.57-.15-3.09-.38-4.55H24v9.02h12.94c-.58 2.96-2.26 5.48-4.78 7.18l7.73 6c4.51-4.18 7.09-10.36 7.09-17.65z"/><path fill="#FBBC05" d="M10.53 28.59c-.48-1.45-.76-2.99-.76-4.59s.27-3.14.76-4.59l-7.98-6.19C.92 16.46 0 20.12 0 24c0 3.88.92 7.54 2.56 10.78l7.97-6.19z"/><path fill="#34A853" d="M24 48c6.48 0 11.93-2.13 15.89-5.81l-7.73-6c-2.15 1.45-4.92 2.3-8.16 2.3-6.26 0-11.57-4.22-13.47-9.91l-7.98 6.19C6.51 42.62 14.62 48 24 48z"/></svg>
            Đăng ký với Google
          </button>
        )}

        <p className="footer-text">
          Đã có tài khoản? <a href="/" onClick={e => { e.preventDefault(); navigate('/'); }}><strong>Đăng nhập</strong></a>
        </p>
      </section>
    </div>
  );
}

export default Register;
