import logging
from core.agent_state import AgentState
from core.config import MAX_RETRY_COUNT

logger = logging.getLogger(__name__)

def route_after_retrieve(state: AgentState) -> str:
    """Kiểm soát ngã rẽ sau khi lấy Schema, chống ngập lụt lỗi sang LLM."""
    # Lấy state an toàn, mặc định là True để tương thích ngược
    is_successful = state.get("is_successful", True) 
    
    if not is_successful:
        logger.error("[ROUTER] Phát hiện lỗi ở Retrieve Node. Cắt luồng đi tới 'insight_node' để báo lỗi.")
        return "insight_node"
        
    return "sql_gen_node"

def route_after_execute(state: AgentState) -> str:
    """
    Hàm quyết định Node tiếp theo dựa trên trạng thái thực thi hiện tại.
    Được gọi tự động bởi Conditional Edge của LangGraph sau khi 'execute_node' hoàn tất.
    """
    is_successful = state.get("is_successful", False)
    retry_count = state.get("retry_count", 0)
    
    if is_successful:
        logger.info("[ROUTER] Trạng thái: THÀNH CÔNG. Chuyển tiếp tới: 'insight_node'")
        return "insight_node"
        
    if not is_successful and retry_count < MAX_RETRY_COUNT:
        error_snippet = str(state.get("error_message", "Unknown"))[:100]
        logger.warning(
            f"[ROUTER] Trạng thái: LỖI ({error_snippet}...). "
            f"Đang sử dụng quyền trợ giúp tự sửa (Retry: {retry_count}/{MAX_RETRY_COUNT}). "
            f"Quay ngược về: 'sql_gen_node'"
        )
        return "sql_gen_node"
        
    if not is_successful and retry_count >= MAX_RETRY_COUNT:
        logger.error(
            f"[ROUTER] Trạng thái: KIỆT SỨC. "
            f"Đã đạt giới hạn tối đa {MAX_RETRY_COUNT} lần thử lại. "
            f"Chuyển tiếp tới: 'insight_node' (để xuất báo cáo lỗi cho User)"
        )
        return "insight_node"
    
    logger.critical("[ROUTER] Lỗi logic không xác định! Ép buộc đi tới: 'insight_node'")
    return "insight_node"

def route_after_classification(state: AgentState) -> str:
    """Đọc nhãn intent và quyết định ngã rẽ."""
    intent = state.get("intent", "data_query")
    
    if intent == "general_chat":
        return "chitchat_node"
        
    return "retrieve_node"