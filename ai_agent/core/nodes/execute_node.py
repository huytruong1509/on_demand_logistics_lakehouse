import logging
import re
from core.agent_state import AgentState
from core.trino_executor import trino_engine, SecurityGuardrailException
from core.config import FORBIDDEN_SQL_KEYWORDS

logger = logging.getLogger(__name__)

def execute_node(state: AgentState) -> dict:
    """
    Kiểm định hai lớp (Regex + AST) và đẩy xuống Trino Cluster.
    """
    sql = state["generated_sql"]
    current_retry = state.get("retry_count", 0)
    
    logger.info(f"[NODE: EXECUTE] Validating and executing SQL:\n{sql}")
    
    # --- LỚP 1: TĨNH (REGEX STATIC CHECK) ---
    upper_sql = sql.upper()
    for keyword in FORBIDDEN_SQL_KEYWORDS:
        if re.search(rf"\b{keyword}\b", upper_sql):
            error_msg = f"Guardrail Layer 1 Violation: Tự động chặn từ khóa cấm '{keyword}'."
            logger.error(error_msg)
            return {
                "is_successful": False,
                "error_message": error_msg,
                "retry_count": current_retry + 1
            }

    # --- LỚP 2: ĐỘNG (AST TRINO EXECUTOR) ---
    try:
        # TrinoExecutor đã bao gồm sqlglot AST Guardrail và thực thi trả về pd.DataFrame
        df = trino_engine.execute_query(sql)
        
        # Chuẩn hóa Pandas DataFrame thành dạng List[Dict] để tương thích với AgentState
        json_ready_data = df.to_dict(orient="records")
        
        logger.info(f"[NODE: EXECUTE] Truy vấn thành công, trả về {len(json_ready_data)} dòng.")
        return {
            "is_successful": True,
            "execution_result": json_ready_data,
            "error_message": None # Xóa bỏ mọi lỗi trước đó
        }
        
    except SecurityGuardrailException as se:
        # Bắt riêng lỗi AST của SQLGlot
        logger.error(f"[NODE: EXECUTE] Guardrail Layer 2 AST: {str(se)}")
        return {
            "is_successful": False,
            "error_message": str(se),
            "retry_count": current_retry + 1
        }
    except Exception as e:
        # Bắt lỗi cú pháp, Timeout, Missing Column từ Trino
        logger.error(f"[NODE: EXECUTE] Trino Engine Error: {str(e)}")
        return {
            "is_successful": False,
            "error_message": str(e),
            "retry_count": current_retry + 1
        }