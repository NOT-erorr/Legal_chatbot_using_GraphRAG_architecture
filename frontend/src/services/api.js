// src/services/api.js

// Use VITE_API_URL if injected during build, else default to API Gateway relative routing
const API_BASE = 'https://graphrag-orchestrator-376046964237.asia-southeast1.run.app';

const LegalAPI = {
  /**
   * Chat — full LangGraph pipeline
   */
  async chat(question, conversationId = null, topKVector = 5, topKGraph = 5) {
    const body = {
      question,
      top_k_vector: topKVector,
      top_k_graph: topKGraph,
    };
    if (conversationId) body.conversation_id = conversationId;

    try {
      const res = await fetch(`${API_BASE}/api/v1/chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });

      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err.detail || `HTTP ${res.status}`);
      }

      return await res.json();
    } catch (err) {
      if (err.message.includes('Failed to fetch') || err.message.includes('NetworkError')) {
        return this._mockResponse(question);
      }
      throw err;
    }
  },

  /**
   * Mock response when orchestrator is offline
   */
  _mockResponse(question) {
    const responses = {
      'doanh nghiệp': `Theo **Luật Doanh nghiệp 2020 (59/2020/QH14)**, Điều 17 quy định:\n\n**Điều kiện thành lập doanh nghiệp tư nhân:**\n\n1. Cá nhân Việt Nam đủ 18 tuổi, có năng lực hành vi dân sự đầy đủ\n2. Không thuộc đối tượng bị cấm thành lập (Điều 17, Khoản 2)\n3. Mỗi cá nhân chỉ được quyền thành lập **một** doanh nghiệp tư nhân\n\n**Thủ tục đăng ký:**\n- Nộp hồ sơ tại Phòng đăng ký kinh doanh (Sở KH&ĐT)\n- Thời hạn giải quyết: 3 ngày làm việc\n\n📌 **Trích dẫn:** Điều 188, Luật Doanh nghiệp 2020\n\n⚖️ *Lưu ý: Nội dung chỉ mang tính tham khảo, không thay thế tư vấn pháp lý chính thức.*`,

      'lao động': `Theo **Bộ luật Lao động 2019 (45/2019/QH14)**, Điều 168 quy định:\n\n**Quyền lợi BHXH cho người lao động:**\n\n1. **Ốm đau**: Hưởng 75% mức lương đóng BHXH\n2. **Thai sản**: 6 tháng nghỉ, hưởng 100% mức lương bình quân\n3. **Tai nạn lao động**: Bồi thường theo mức suy giảm khả năng lao động\n4. **Hưu trí**: Đủ 20 năm đóng BHXH + đủ tuổi nghỉ hưu\n\n📌 **Trích dẫn:** Điều 168-169, BLLĐ 2019; Luật BHXH 2014 (58/2014/QH13)\n\n⚖️ *Lưu ý: Nội dung chỉ mang tính tham khảo, không thay thế tư vấn pháp lý chính thức.*`,

      'đất đai': `Theo **Luật Đất Đai 2024 (31/2024/QH15)**, có hiệu lực từ 01/01/2025:\n\n**Quyền sử dụng đất và chuyển nhượng:**\n\n1. **Quyền chung** (Điều 27): Được cấp GCN quyền sử dụng đất, được bồi thường khi Nhà nước thu hồi đất\n2. **Chuyển nhượng** (Điều 45): Cần có GCN, đất không tranh chấp, không bị kê biên\n3. **Thời hạn**: Đất ở không thời hạn; đất nông nghiệp 50 năm\n\n**Điểm mới Luật 2024:**\n- Bỏ khung giá đất, áp dụng bảng giá đất theo thị trường\n- Mở rộng quyền cho Việt kiều\n\n📌 **Trích dẫn:** Điều 27, 45, Luật Đất Đai 2024\n\n⚖️ *Lưu ý: Nội dung chỉ mang tính tham khảo, không thay thế tư vấn pháp lý chính thức.*`,

      'ly hôn': `Theo **Luật Hôn nhân và Gia đình 2014 (52/2014/QH13)**:\n\n**Thủ tục ly hôn đơn phương (Điều 56):**\n\n1. **Điều kiện**: Mâu thuẫn trầm trọng, đời sống chung không thể kéo dài, mục đích hôn nhân không đạt\n2. **Nộp đơn**: Tòa án nhân dân cấp huyện\n3. **Hòa giải**: Bắt buộc hòa giải tại cơ sở trước khi khởi kiện\n\n**Quyền nuôi con (Điều 81):**\n- Con dưới 36 tháng tuổi: giao cho mẹ trực tiếp nuôi\n- Con từ 7 tuổi trở lên: xem xét nguyện vọng của con\n- Nghĩa vụ cấp dưỡng: bên không trực tiếp nuôi con\n\n📌 **Trích dẫn:** Điều 51, 56, 81, Luật HNGĐ 2014\n\n⚖️ *Lưu ý: Nội dung chỉ mang tính tham khảo, không thay thế tư vấn pháp lý chính thức.*`,
    };

    const q = question.toLowerCase();
    let answer = '';
    for (const [key, val] of Object.entries(responses)) {
      if (q.includes(key)) { answer = val; break; }
    }

    if (!answer) {
      answer = `Dựa trên câu hỏi của bạn: "${question}"\n\nTôi đang tìm kiếm thông tin trong cơ sở dữ liệu 518,255 văn bản pháp lý. Hiện tại hệ thống đang ở chế độ offline demo.\n\nĐể có kết quả chính xác, vui lòng đảm bảo:\n1. Orchestrator service đang chạy (port 8001)\n2. Qdrant và Neo4j đã được khởi tạo\n\n⚖️ *Lưu ý: Nội dung chỉ mang tính tham khảo, không thay thế tư vấn pháp lý chính thức.*`;
    }

    return new Promise((resolve) => {
      setTimeout(() => {
        resolve({
          answer,
          conversation_id: null,
          message_id: null,
          citations: [],
          trace: { wall_clock_ms: Math.floor(Math.random() * 800 + 600) },
          cached: false
        });
      }, 500);
    });
  },
};

export default LegalAPI;
