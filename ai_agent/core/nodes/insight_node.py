import logging
import json
from pathlib import Path
from core.agent_state import AgentState
from core.gemini_client import gemini_engine
from core.config import MAX_RAW_DATA_ROWS

logger = logging.getLogger(__name__)

PROMPTS_DIR = Path(__file__).resolve().parent.parent.parent / "prompts"

def _get_analyst_prompt() -> str:
    """Nạp Markdown Analyst Prompt và Few-shot JSON."""
    try:
        sys_prompt_path = PROMPTS_DIR / "sys_prompt_data_analyst.md"
        few_shot_path = PROMPTS_DIR / "few_shot_analyst_report.json"
        
        base_prompt = sys_prompt_path.read_text(encoding="utf-8")
        
        if few_shot_path.exists():
            few_shots = json.loads(few_shot_path.read_text(encoding="utf-8"))
            examples_str = "<few_shot_examples>\n"
            for ex in few_shots:
                examples_str += (
                    f"  <example scenario='{ex.get('scenario', '')}'>\n"
                    f"    <user_query>{ex.get('user_query', '')}</user_query>\n"
                    f"    <raw_data>{json.dumps(ex.get('raw_data', []), ensure_ascii=False)}</raw_data>\n"
                    f"    <expected_output>{ex.get('expected_output', '')}</expected_output>\n"
                    f"  </example>\n"
                )
            examples_str += "</few_shot_examples>"
            return f"{base_prompt}\n\n{examples_str}"
            
        return base_prompt
    except Exception as e:
        logger.error(f"[FATAL] Lỗi đọc Analyst Prompts: {e}")
        return "Bạn là Executive Data Analyst. Hãy tóm tắt số liệu."

def insight_node(state: AgentState) -> dict:
    logger.info("[NODE: INSIGHT] Bắt đầu tổng hợp Insight kinh doanh.")
    
    # 1. Fallback an toàn
    if not state.get("is_successful"):
        err_msg = state.get("error_message", "Lỗi không xác định")
        fallback_msg = (
            f"**Hệ thống không thể lấy dữ liệu cho câu hỏi này.**\n"
            f"> *Chi tiết kỹ thuật (dành cho Admin): {err_msg}*\n\n"
            f"Vui lòng thử diễn đạt lại câu hỏi một cách đơn giản hơn."
        )
        return {"final_insight": fallback_msg}

    # 2. Xử lý Truncation bảo vệ Token API
    raw_data = state.get("execution_result", [])
    
    if len(raw_data) > MAX_RAW_DATA_ROWS:
        truncated_data = raw_data[:MAX_RAW_DATA_ROWS]
        data_str = (
            f"{truncated_data}\n\n"
            f"<system_warning>"
            f"Bảng kết quả gốc có {len(raw_data)} dòng. "
            f"Để tối ưu token, chỉ top {MAX_RAW_DATA_ROWS} dòng được truyền cho LLM."
            f"</system_warning>"
        )
        logger.warning(f"[NODE: INSIGHT] Đã cắt xén bớt dữ liệu từ {len(raw_data)} xuống {MAX_RAW_DATA_ROWS} dòng.")
    else:
        data_str = str(raw_data)

    # 3. Gọi LLM
    analyst_prompt = _get_analyst_prompt()
    final_insight = gemini_engine.analyze_data(
        analyst_prompt=analyst_prompt,
        user_question=state["user_question"],
        query_result=data_str
    )
    
    return {"final_insight": final_insight}