import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import './Login.css';

function Login() {
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [showPassword, setShowPassword] = useState(false);
  const [loading, setLoading] = useState(false);
  const navigate = useNavigate();

  const handleLogin = (e) => {
    e.preventDefault();
    if (!email || !password) return;

    setLoading(true);
    setTimeout(() => {
      sessionStorage.setItem('legalai_user', JSON.stringify({
        email,
        name: email.split('@')[0].replace(/[._]/g, ' '),
        initials: email.substring(0, 2).toUpperCase(),
      }));
      navigate('/chat');
    }, 800);
  };

  const handleGoogleLogin = () => {
    sessionStorage.setItem('legalai_user', JSON.stringify({
      email: 'user@gmail.com',
      name: 'Nguyễn Văn A',
      initials: 'NA',
    }));
    navigate('/chat');
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
          <h1>Tư vấn pháp luật<br />Việt Nam thông minh</h1>
          <p>Tra cứu nhanh hơn, chính xác hơn với AI được huấn luyện trên 518,255 văn bản pháp lý chính thức.</p>

          <div className="hero-stats">
            <div className="stat-item">
              <div className="stat-value">518K+</div>
              <div className="stat-label">văn bản<br />pháp lý</div>
            </div>
            <div className="stat-item">
              <div className="stat-value">&gt;95%</div>
              <div className="stat-label">độ chính<br />xác</div>
            </div>
            <div className="stat-item">
              <div className="stat-value">&lt;2s</div>
              <div className="stat-label">thời gian<br />phản hồi</div>
            </div>
          </div>
        </div>


      </section>

      <section className="login-form-panel">
        <h2>Đăng nhập</h2>
        <p className="subtitle">Chào mừng trở lại — hãy tiếp tục tra cứu pháp lý của bạn</p>

        <form onSubmit={handleLogin}>
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
            <label className="form-label" htmlFor="password">
              Mật khẩu *
              <a href="#" onClick={e => { e.preventDefault(); alert('Liên hệ support@legalai.vn'); }}>Quên mật khẩu?</a>
            </label>
            <div className="password-wrapper">
              <input
                id="password"
                className="form-input"
                type={showPassword ? 'text' : 'password'}
                placeholder="••••••••"
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

          <button type="submit" className="btn btn-primary btn-block" disabled={loading} style={{ opacity: loading ? 0.7 : 1 }}>
            {loading ? 'Đang đăng nhập…' : 'Đăng nhập'}
          </button>
        </form>

        <div className="divider">hoặc</div>

        <button className="btn-google" onClick={handleGoogleLogin}>
          <svg width="20" height="20" viewBox="0 0 48 48"><path fill="#EA4335" d="M24 9.5c3.54 0 6.71 1.22 9.21 3.6l6.85-6.85C35.9 2.38 30.47 0 24 0 14.62 0 6.51 5.38 2.56 13.22l7.98 6.19C12.43 13.72 17.74 9.5 24 9.5z"/><path fill="#4285F4" d="M46.98 24.55c0-1.57-.15-3.09-.38-4.55H24v9.02h12.94c-.58 2.96-2.26 5.48-4.78 7.18l7.73 6c4.51-4.18 7.09-10.36 7.09-17.65z"/><path fill="#FBBC05" d="M10.53 28.59c-.48-1.45-.76-2.99-.76-4.59s.27-3.14.76-4.59l-7.98-6.19C.92 16.46 0 20.12 0 24c0 3.88.92 7.54 2.56 10.78l7.97-6.19z"/><path fill="#34A853" d="M24 48c6.48 0 11.93-2.13 15.89-5.81l-7.73-6c-2.15 1.45-4.92 2.3-8.16 2.3-6.26 0-11.57-4.22-13.47-9.91l-7.98 6.19C6.51 42.62 14.62 48 24 48z"/></svg>
          Tiếp tục với Google
        </button>

        <p className="footer-text">
          Chưa có tài khoản? <a href="#" onClick={e => e.preventDefault()}><strong>Đăng ký miễn phí</strong></a>
        </p>
      </section>
    </div>
  );
}

export default Login;
