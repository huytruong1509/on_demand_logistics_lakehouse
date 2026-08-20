import logging
from core.agent_state import AgentState
from core.gemini_client import GeminiClient

logger = logging.getLogger(__name__)

def classify_intent_node(state: AgentState) -> dict:
    """Node đầu tiên: Đọc câu hỏi và dán nhãn định hướng."""
    logger.info("--- [NODE] ĐANG PHÂN LOẠI MỤC ĐÍCH (INTENT) ---")
    user_question = state["user_question"]
    
    client = GeminiClient()
    intent = client.classify_intent(user_question)
    
    logger.info(f"Kết quả phân loại: {intent}")
    return {"intent": intent}