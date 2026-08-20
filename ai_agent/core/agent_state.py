from typing import TypedDict, Optional, List, Dict, Any

class AgentState(TypedDict):
    """
    Hợp đồng dữ liệu (Data Contract) luân chuyển qua các Node trong LangGraph.
    Được thiết kế tương thích hoàn toàn với đầu ra của gemini_client và trino_executor.
    """
    # --- 1. Dữ liệu đầu vào ---
    user_question: str

    # Trạng thái điều hướng
    intent: str
    
    # --- 2. Trạng thái sinh SQL ---
    retrieved_schema: str
    generated_sql: Optional[str]
    
    # --- 3. Trạng thái thực thi Data Warehouse ---
    # trino_executor trả về pd.DataFrame. Node Execute sẽ dùng df.to_dict(orient='records') 
    # để chuyển thành List[Dict] trước khi nạp vào state nhằm đảm bảo khả năng serialize của LangGraph.
    execution_result: Optional[List[Dict[str, Any]]] 
    
    # Bắt trực tiếp exception từ TrinoQueryError hoặc SecurityGuardrailException
    error_message: Optional[str] 
    is_successful: bool 
    
    # --- 4. Trạng thái điều phối (Orchestration) ---
    retry_count: int 
    
    # --- 5. Dữ liệu đầu ra ---
    # Kết quả chuỗi Markdown sinh ra từ hàm analyze_data của GeminiClient
    final_insight: Optional[str]