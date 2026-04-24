# Frontend Service (React + Vite + Nginx)

`frontend` là giao diện người dùng cho Legal Chatbot. Ở local Docker Compose, service này đồng thời đóng vai trò API gateway nhẹ nhờ Nginx reverse proxy tới `backend`.

## 1. Features

- Chat UI cho legal Q&A.
- Trang login và navigation sidebar.
- Client gọi API qua `src/services/api.js`.
- Fallback mock response khi backend tạm offline.

## 2. Local Development

### Prerequisites

- Node.js 18+

### Setup

```bash
cd frontend
npm install
```

### Run dev server

```bash
npm run dev
```

Mặc định app chạy trên Vite dev server (`http://localhost:5173`).

## 3. API Configuration

Client sử dụng:

- `VITE_API_URL` nếu được cung cấp.
- Nếu không có, dùng relative path (`/api/...`) để đi qua proxy.

Ví dụ:

```bash
VITE_API_URL=http://localhost:8001 npm run dev
```

## 4. Docker Build and Run

```bash
docker build -t legal-frontend ./frontend
docker run --rm -p 8080:8080 legal-frontend
```

## 5. Nginx Routing

Trong `frontend/nginx.conf`:

- `location /` phục vụ SPA và fallback `index.html`.
- `location /api/` proxy về `http://backend:8080/api/` (phù hợp Docker Compose local).

Khi deploy cloud tách service frontend/backend, cần dùng domain backend public hoặc API gateway thay cho host `backend`.

## 6. Troubleshooting

- UI không gọi được API: kiểm tra `VITE_API_URL` hoặc cấu hình proxy.
- Build lỗi: xóa `node_modules` và chạy lại `npm install`.
- Route refresh bị 404: xác nhận Nginx có `try_files $uri $uri/ /index.html;`.
