import logging
from core.agent_state import AgentState
from rag.qdrant_manager import QdrantSchemaManager

logger = logging.getLogger(__name__)

# Khởi tạo instance. (Ghi chú: Nếu dùng FastAPI/Workers, cân nhắc quản lý via Dependency Injection)
schema_manager = QdrantSchemaManager()

def retrieve_node(state: AgentState) -> dict:
    question = state.get("user_question", "")
    if not question:
        return {"retrieved_schema": "", "is_successful": False, "error_message": "Câu hỏi trống."}
        
    logger.info(f"[NODE: RETRIEVE] Tìm kiếm schema cho câu hỏi: '{question}'")
    
    try:
        # Gọi API nhúng để lấy vector truy vấn
        query_vector = schema_manager.embeddings.embed_query(question)
        
        # [CẢI TIẾN CỐT LÕI] Sử dụng `query_points` thay vì `search` - Chuẩn API v1.18.0+
        hits = schema_manager.client.query_points(
            collection_name=schema_manager.collection_name,
            query=query_vector,
            limit=3
        ).points # Trích xuất list kết quả từ QueryResponse object (tuỳ thuộc vào response, thường trả thẳng list)
        
        # Note: Ở bản python client mới nhất, query_points trả về list[ScoredPoint]
        # Nếu thư viện của bạn map thẳng nó ra list thì dùng trực tiếp 'hits' mà không cần '.points'
        # hits = schema_manager.client.query_points(..., query=query_vector, limit=3)

        if not hits:
            logger.warning("[NODE: RETRIEVE] Qdrant trả về rỗng.")
            return {
                "retrieved_schema": "Hệ thống không tìm thấy bảng dữ liệu phù hợp.",
                "is_successful": True 
            }
            
        schemas = []
        for hit in hits:
            raw_text = hit.payload.get("raw_context", "") if hit.payload else ""
            schemas.append(f"--- BẢNG DỮ LIỆU ---\n{raw_text}")
            
        final_schema_context = "\n\n".join(schemas)
        return {
            "retrieved_schema": final_schema_context,
            "is_successful": True,
            "error_message": None 
        }
        
    except Exception as e:
        error_msg = f"Lỗi truy xuất cơ sở dữ liệu vector: {str(e)}"
        logger.error(f"[NODE: RETRIEVE] {error_msg}")
        return {
            "retrieved_schema": "Lỗi hệ thống.",
            "is_successful": False, 
            "error_message": error_msg 
        }