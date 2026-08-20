import logging
import json
from pathlib import Path
from core.agent_state import AgentState
from core.gemini_client import gemini_engine

logger = logging.getLogger(__name__)

PROMPTS_DIR = Path(__file__).resolve().parent.parent.parent / "prompts"

def _get_system_prompt() -> str:
    try:
        sys_prompt_path = PROMPTS_DIR / "sys_prompt_sql_agent.md"
        few_shot_path = PROMPTS_DIR / "few_shot_sql_generation.json"
        
        base_prompt = sys_prompt_path.read_text(encoding="utf-8")
        
        if few_shot_path.exists():
            few_shots = json.loads(few_shot_path.read_text(encoding="utf-8"))
            examples_str = "<few_shot_examples>\n"
            for ex in few_shots:
                examples_str += (
                    f"  <example>\n"
                    f"    <intent>{ex.get('intent', '')}</intent>\n"
                    f"    <user_query>{ex.get('user_query', '')}</user_query>\n"
                    f"    <schema_context>{ex.get('schema_context', '')}</schema_context>\n"
                    f"    <expected_sql>{ex.get('expected_sql', '')}</expected_sql>\n"
                    f"  </example>\n"
                )
            examples_str += "</few_shot_examples>"
            return f"{base_prompt}\n\n{examples_str}"
            
        return base_prompt
    except Exception as e:
        logger.error(f"[FATAL] Lỗi cấu trúc file SQL Prompts: {e}")
        return "Bạn là Trino SQL Expert."

def sql_gen_node(state: AgentState) -> dict:
    retry_count = state.get("retry_count", 0)
    logger.info(f"[NODE: SQL_GEN] Bắt đầu sinh SQL. Lượt thử: {retry_count + 1}")
    
    system_prompt = _get_system_prompt()
    user_question = state["user_question"]
    schema_context = state["retrieved_schema"]
    
    if retry_count > 0:
        logger.warning("[NODE: SQL_GEN] Kích hoạt Self-Correction Mode.")
        prev_sql = state.get("generated_sql", "")
        error_msg = state.get("error_message", "")
        
        #Guardrail khi sửa SQL
        user_question += (
            f"\n\n<system_feedback>"
            f"\n<previous_sql>\n{prev_sql}\n</previous_sql>"
            f"\n<execution_error>\n{error_msg}\n</execution_error>"
            f"\n[RÀNG BUỘC NGHIÊM NGẶT]: Phân tích lỗi và viết lại SQL. "
            f"TUYỆT ĐỐI KHÔNG tự ý thay đổi tên catalog (ví dụ: nessie, default, hive). "
            f"CHỈ SỬ DỤNG cấu trúc bảng FQN chính xác như được cung cấp trong Context."
            f"\n</system_feedback>"
        )
        
    sql = gemini_engine.generate_sql(
        system_prompt=system_prompt,
        user_question=user_question,
        schema_context=schema_context
    )
    
    return {"generated_sql": sql}