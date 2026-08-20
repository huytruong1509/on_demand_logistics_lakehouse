import os
import logging
from pathlib import Path
from typing import List, Union
from pydantic import SecretStr
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import SystemMessage, HumanMessage
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log
)
from google.api_core.exceptions import ResourceExhausted, InternalServerError, ServiceUnavailable

logger = logging.getLogger(__name__)

# Định nghĩa đường dẫn gốc động bằng pathlib
BASE_DIR = Path(__file__).resolve().parent.parent
PROMPTS_DIR = BASE_DIR / "prompts"

class GeminiClient:
    """
    LLM Wrapper chuẩn Production giao tiếp với Google Gemini.
    Áp dụng Singleton pattern và Dependency Injection.
    """
    
    def __init__(self, model_name: str = "gemini-3.5-flash", temperature: float = 0.0):
        self.api_key = os.getenv("GEMINI_API_KEY")
        if not self.api_key:
            logger.error("[FATAL] Thiếu biến môi trường GEMINI_API_KEY.")
            raise ValueError("GEMINI_API_KEY is not set.")
            
        self.model_name = os.getenv("GEMINI_CHAT_MODEL", model_name)
        
        self.llm = ChatGoogleGenerativeAI(
            model=self.model_name,
            temperature=temperature,
            api_key=SecretStr(self.api_key),
            max_output_tokens=2048,
            max_retries=0 
        )
        logger.info(f"Đã khởi tạo GeminiClient | Model: {self.model_name} | Temp: {temperature}")

    @retry(
        retry=retry_if_exception_type((ResourceExhausted, InternalServerError, ServiceUnavailable)),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=60),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True
    )
    def _call_llm_safely(self, messages: List[Union[SystemMessage, HumanMessage, tuple]]) -> str:
        """Thực thi gọi API với cơ chế Retry chuẩn xác."""
        response = self.llm.invoke(messages)
        content = response.content
        
        # Xử lý format trả về của Gemini 1.5
        if isinstance(content, list):
            return str(content[0].get("text", ""))
        return str(content)

    def generate_sql(self, system_prompt: str, user_question: str, schema_context: str) -> str:
        """Sinh SQL với cấu trúc XML Tags."""
        full_system_instruction = (
            f"{system_prompt}\n\n"
            f"<schema_context>\n{schema_context}\n</schema_context>"
        )
        
        messages = [
            SystemMessage(content=full_system_instruction),
            HumanMessage(content=f"<user_query>{user_question}</user_query>\nHãy chỉ trả về câu lệnh SQL hợp lệ, không bọc markdown.")
        ]
        
        logger.info("[GEMINI] Đang sinh SQL...")
        raw_sql = self._call_llm_safely(messages)
        # Loại bỏ markdown an toàn
        return raw_sql.replace("```sql", "").replace("```", "").strip()

    def analyze_data(self, analyst_prompt: str, user_question: str, query_result: str) -> str:
        """Tóm tắt Insight từ dữ liệu thô."""
        prompt = (
            f"<user_query>{user_question}</user_query>\n\n"
            f"<query_result>\n{query_result}\n</query_result>"
        )
        
        messages = [
            SystemMessage(content=analyst_prompt), 
            HumanMessage(content=prompt)
        ]
        
        logger.info("[GEMINI] Đang phân tích Insight...")
        return self._call_llm_safely(messages)

    def classify_intent(self, user_question: str) -> str:
        """Phân loại mục đích sử dụng Markdown Prompt."""
        try:
            prompt_path = PROMPTS_DIR / "sys_prompt_semantic_router.md"
            system_prompt = prompt_path.read_text(encoding="utf-8")

            messages = [
                ("system", system_prompt),
                ("human", user_question)
            ]
            
            intent = self._call_llm_safely(messages).strip().lower()
            return "general_chat" if "general_chat" in intent else "data_query"
        except Exception as e:
            logger.error(f"Lỗi đọc prompt phân loại intent: {e}")
            return "data_query"

    def chat_general(self, user_question: str) -> str:
        """Tái sử dụng Persona của Data Agent cho Chitchat."""
        try:
            prompt_path = PROMPTS_DIR / "sys_prompt_sql_agent.md"
            system_prompt = prompt_path.read_text(encoding="utf-8")
        except FileNotFoundError:
            system_prompt = "Bạn là trợ lý AI phân tích dữ liệu."

        messages = [
            ("system", system_prompt),
            ("human", f"<chitchat_query>{user_question}</chitchat_query>")
        ]
        return self._call_llm_safely(messages)
    
gemini_engine = GeminiClient()