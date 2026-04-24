import os
import sys
from dotenv import load_dotenv

# Tự động nạp biến môi trường mới nhất từ file .env.production đã được vá lỗi
load_dotenv(".env.production")

# Import các components cốt lõi từ Orchestrator
from orchestrator.config import OrchestratorConfig
from orchestrator.graph import build_graph, invoke_graph

def main():
    print("🚀 Đang nạp cấu hình và khởi tạo hệ thống (Kết nối Qdrant, Neo4j)...")
    try:
        cfg = OrchestratorConfig.from_env()
        # Khởi tạo đạn dược: Retriever (Vector + Graph) và Pipeline tổng hợp
        compiled_graph = build_graph(cfg)
        print("✅ GraphRAG Pipeline đã sẵn sàng!\n")
    except Exception as e:
        print(f"❌ Lỗi khởi tạo hệ thống: {e}")
        sys.exit(1)

    print("=" * 60)
    print("📌 HƯỚNG DẪN: Gõ câu hỏi của bạn và nhấn Enter. Gõ 'exit', 'quit' hoặc Ctrl+C để thoát.")
    print("=" * 60)

    while True:
        try:
            query = input("\n💡 Câu hỏi pháp lý: ").strip()
            
            if not query:
                continue
            if query.lower() in ["exit", "quit", "q"]:
                print("Tạm biệt!")
                break
                
            print(f"\n⏳ Đang nhúng (embedding), tìm kiếm và tổng hợp cho ngữ cảnh: '{query}'...")
            
            # Vận hành GraphRAG: 
            # 1. Embed nội dung qua Gemini Embedding
            # 2. Search song song Qdrant & Neo4j -> RRF Fusion
            # 3. Context Builder -> Prompt
            # 4. Gọi Gemini AI phản hồi
            result = invoke_graph(
                compiled_graph=compiled_graph,
                question=query,
                top_k_vector=5,
                top_k_graph=5
            )
            
            # --- In màn hình Kết Quả ---
            print("=" * 60)
            print("🤖 CÂU TRẢ LỜI CỦA TRỢ LÝ AI:\n")
            print(result.get("answer", ""))
            print("\n" + "=" * 60)
            
            # --- In màn hình Log Debug Chuyên Sâu ---
            trace = result.get("trace", {})
            print("📊 METRICS VÀ DEBUG:")
            print(f"- Chunks trúng đích (Vector/Keyword/Graph): {trace.get('vector_hits', 0)} / {trace.get('keyword_hits', 0)} / {trace.get('graph_hits', 0)}")
            print(f"- Thời gian Tối ưu hoá ngữ cảnh (Retrieval): {trace.get('retrieve_ms', 0)} ms")
            print(f"- Thời gian Suy Luận LLM (Synthesis):     {trace.get('synthesize_ms', 0)} ms")
            print(f"- Tổng thời gian phản hồi:                {trace.get('wall_clock_ms', 0)} ms")
            print("-" * 60)
            
        except KeyboardInterrupt:
            print("\nĐã ép buộc thoát (Ctrl+C).")
            break
        except Exception as e:
            print(f"\n🚨 [LỖI HỆ THỐNG]: {e}\n")

if __name__ == "__main__":
    main()
