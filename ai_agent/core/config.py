import os

# ==========================================
# CẤU HÌNH HỆ THỐNG AGENT (AGENT SETTINGS)
# ==========================================
MAX_RETRY_COUNT: int = int(os.getenv("AGENT_MAX_RETRY_COUNT", "3"))
MAX_RAW_DATA_ROWS: int = int(os.getenv("AGENT_MAX_RAW_DATA_ROWS", "50"))

# ==========================================
# BẢO MẬT LAYER 1 (STATIC STRING GUARDRAILS)
# ==========================================
FORBIDDEN_SQL_KEYWORDS: tuple[str, ...] = (
    "DROP", "DELETE", "UPDATE", "INSERT", 
    "ALTER", "CREATE", "GRANT", "TRUNCATE", 
    "REPLACE", "MERGE", "EXECUTE", "CALL"
)