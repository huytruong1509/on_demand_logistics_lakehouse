import logging
from core.agent_state import AgentState
from core.gemini_client import GeminiClient

logger = logging.getLogger(__name__)

def chitchat_node(state: AgentState) -> dict:
    """Node xử lý giao tiếp, bỏ qua việc query Database."""
    logger.info("--- [NODE] KÍCH HOẠT CHITCHAT (GENERAL KNOWLEDGE) ---")
    user_question = state["user_question"]
    
    client = GeminiClient()
    response = client.chat_general(user_question)
    
    # Cập nhật thẳng vào final_insight và báo thành công
    return {
        "final_insight": response,
        "is_successful": True
    }