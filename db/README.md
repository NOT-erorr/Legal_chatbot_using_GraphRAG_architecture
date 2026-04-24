# Database Schema (PostgreSQL)

Thư mục `db` chứa schema SQL cho persistence layer của Legal Chatbot.

## 1. Main File

- `chatbot_schema.sql`: tạo extensions, enums, tables, indexes và trigger cần thiết.

## 2. Domain Tables

Các nhóm bảng chính:

- Identity and access: `users`
- Chat session: `conversations`, `messages`
- RAG observability: `retrieval_logs`, `feedback`
- ETL tracking: `pipeline_runs`

## 3. Apply Schema

### Via Docker Compose (auto init)

`docker-compose.yml` đã mount:

- `./db/chatbot_schema.sql -> /docker-entrypoint-initdb.d/01_chatbot_schema.sql`

Schema sẽ chạy tự động khi volume Postgres khởi tạo lần đầu.

### Manual apply

```bash
psql -h localhost -U postgres -d legal_chatbot -f db/chatbot_schema.sql
```

## 4. Migration Strategy

Hiện tại đang dùng SQL file bootstrap.

Đề xuất cho production:

- Áp dụng migration tool (`alembic` hoặc `dbmate`).
- Mỗi thay đổi schema thành migration riêng có version.
- Không sửa trực tiếp migration cũ đã apply trên production.

## 5. Notes

- Khi đổi schema lớn, cần backup database trước.
- Nếu cần reset local hoàn toàn: xóa volume `postgres_data` rồi khởi tạo lại.
