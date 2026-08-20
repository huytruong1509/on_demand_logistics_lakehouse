import os
import logging
import uuid
import pandas as pd
from typing import Any, Dict, List
from pydantic import BaseModel, Field

import chainlit as cl
import chainlit.data as cl_data
from chainlit.server import app as fastapi_app
from chainlit.data.sql_alchemy import SQLAlchemyDataLayer
from fastapi import HTTPException, BackgroundTasks, status

# Import nghiệp vụ
from rag.indexer import run_indexing_job
from core.agent_graph import agent_app

# ==========================================
# CẤU HÌNH HỆ THỐNG (PRODUCTION CONFIG)
# ==========================================
MAX_DISPLAY_ROWS = 100
MAX_FALLBACK_ROWS = 20

# Lấy cấu hình DB từ biến môi trường, mặc định là SQLite local
DB_URL = os.getenv("CHAINLIT_DB_URL", "sqlite+aiosqlite:///chat_history.db")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Khởi tạo Data Layer lưu lịch sử chat
cl_data._data_layer = SQLAlchemyDataLayer(conninfo=DB_URL)


# ==========================================
# PHẦN 1: TÍCH HỢP DATA PIPELINE API (AIRFLOW)
# ==========================================

class SyncRequest(BaseModel):
    source: str = Field(default="airflow", description="Hệ thống trigger")
    schemas_to_sync: List[str] = Field(default_factory=lambda: ["marts", "gold"])


def execute_sync_job_safely(schemas: List[str]) -> None:
    """Task vụ chạy nền để không chặn Event Loop của hệ thống Chat."""
    try:
        logger.info(f"[INDEXING JOB] Bắt đầu đồng bộ schemas: {schemas} vào Qdrant...")
        run_indexing_job()
        logger.info("[INDEXING JOB] Hoàn tất đồng bộ Qdrant.")
    except Exception as e:
        logger.error(f"[INDEXING JOB] Lỗi khi chạy Indexing: {e}", exc_info=True)


@fastapi_app.post("/api/v1/index-metadata", status_code=status.HTTP_202_ACCEPTED)
async def trigger_indexing_api(request: SyncRequest, background_tasks: BackgroundTasks):
    """
    Endpoint Fire-and-Forget chuẩn cho Data Pipeline (Airflow).
    Trả về 202 Accepted ngay lập tức, tiến trình chạy ngầm.
    """
    try:
        logger.info(f"Nhận trigger API từ {request.source}")
        background_tasks.add_task(execute_sync_job_safely, request.schemas_to_sync)
        return {
            "status": "accepted",
            "message": "Đã tiếp nhận yêu cầu đồng bộ. Đang xử lý ngầm (Background Task).",
            "schemas_queued": request.schemas_to_sync
        }
    except Exception as e:
        logger.error(f"[API ERROR] Lỗi API: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal Server Error")


# ==========================================
# PHẦN 2: CHATBOT UI (CHAINLIT) VÀ AGENT INTEGRATION
# ==========================================

@cl.on_chat_start
async def on_chat_start():
    """Khởi tạo phiên làm việc mới."""
    session_id = str(uuid.uuid4())
    cl.user_session.set("thread_id", session_id)
    
    welcome_msg = (
        "👋 **Xin chào! Hệ thống AI Data Agent đã sẵn sàng.**\n\n"
        "Tôi đã kết nối với LangGraph và Trino Data Warehouse. Bạn muốn phân tích dữ liệu gì hôm nay?"
    )
    await cl.Message(content=welcome_msg).send()


@cl.on_chat_resume
async def on_chat_resume(thread: Dict[str, Any]):
    """Khôi phục phiên làm việc CŨ từ Database."""
    cl.user_session.set("thread_id", thread["id"])
    logger.info(f"[RESUME] Người dùng quay lại phiên chat: {thread['id']}")


@cl.on_message
async def main(message: cl.Message):
    """Xử lý luồng chat chính."""
    thread_id = cl.user_session.get("thread_id")
    config = {"configurable": {"thread_id": thread_id}}
    
    current_state: Dict[str, Any] = {
        "user_question": message.content,
        "retry_count": 0,
        "final_insight": "",
        "generated_sql": "",
        "execution_result": []
    }
    
    ui_msg = cl.Message(content="*Đang phân tích yêu cầu...*")
    await ui_msg.send()
    
    try:
        # Stream events từ LangGraph
        async for event in agent_app.astream(current_state, config=config, stream_mode="updates"):
            for node_name, state_update in event.items():
                
                current_state.update(state_update)
                
                async with cl.Step(name=node_name) as step:
                    if node_name == "retrieve_node":
                        step.output = "✅ Đã trích xuất siêu dữ liệu (Schema) từ Qdrant."
                    elif node_name == "sql_gen_node":
                        sql = state_update.get("generated_sql", "")
                        step.output = f"✅ Đã sinh câu lệnh Trino SQL:\n```sql\n{sql}\n```"
                    elif node_name == "execute_node":
                        if state_update.get("is_successful"):
                            results = state_update.get("execution_result", [])
                            row_count = len(results) if isinstance(results, list) else 0
                            step.output = f"✅ Truy vấn thành công! (Lấy về {row_count} dòng dữ liệu)"
                        else:
                            error = state_update.get("error_message", "Lỗi không xác định")
                            retry = state_update.get("retry_count", 0)
                            step.output = f"⚠️ Bị lỗi (Lượt thử {retry}): {error}"
                            step.is_error = True
                    elif node_name == "insight_node":
                        step.output = "✅ Đã tổng hợp xong Insight."

        # Xử lý hiển thị UI (Trích xuất an toàn từ State)
        final_insight = current_state.get("final_insight", "Hệ thống không thể tạo báo cáo do gián đoạn luồng.")
        sql_code = current_state.get("generated_sql")
        raw_data = current_state.get("execution_result")
        
        output_elements = []
        
        # Widget Mã SQL Trino
        if sql_code:
            output_elements.append(
                cl.Text(name="🔍 Trino SQL Query", content=f"```sql\n{sql_code}\n```", display="inline")
            )
            
        # Widget Bảng Dữ Liệu
        if isinstance(raw_data, list) and raw_data:
            try:
                display_data = raw_data[:MAX_DISPLAY_ROWS]
                df = pd.DataFrame(display_data)
                
                # Làm sạch dữ liệu object cho giao diện
                for col in df.select_dtypes(include=['object']).columns:
                    df[col] = df[col].apply(lambda x: str(x) if pd.notnull(x) else None)
                
                output_elements.append(
                    cl.Dataframe(
                        name=f"📊 Dữ liệu (Top {len(display_data)}/{len(raw_data)} dòng)", 
                        data=df, 
                        display="inline"
                    )
                )
            except Exception as e:
                logger.warning(f"[UI FALLBACK] Lỗi render DataFrame: {e}. Thử Markdown...", exc_info=True)
                try:
                    df_fb = pd.DataFrame(raw_data[:MAX_FALLBACK_ROWS])
                    md_table = df_fb.to_markdown(index=False)
                    output_elements.append(
                        cl.Text(
                            name=f"📊 Dữ liệu gốc (Top {MAX_FALLBACK_ROWS} dòng)",
                            content=f"\n\n{md_table}\n\n",
                            display="inline"
                        )
                    )
                except Exception as fb_err:
                    logger.error(f"[UI ERROR] Fallback thất bại: {fb_err}", exc_info=True)
                
        # Cập nhật kết quả cuối
        ui_msg.content = final_insight
        ui_msg.elements = output_elements
        await ui_msg.update()
        
    except Exception as e:
        logger.error(f"[CHAINLIT MAIN] Lỗi hệ thống: {e}", exc_info=True)
        ui_msg.content = "❌ Hệ thống gặp sự cố, xin vui lòng thử lại sau."
        await ui_msg.update()