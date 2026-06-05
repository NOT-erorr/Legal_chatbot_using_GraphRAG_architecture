import { Navigate } from 'react-router-dom';
import { getToken } from '../services/session';

// Guard cấp router: chưa có token (chưa đăng nhập) → đẩy về trang Login.
export default function ProtectedRoute({ children }) {
  if (!getToken()) {
    return <Navigate to="/" replace />;
  }
  return children;
}
