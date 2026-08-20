"""
File: dbt_presentation.py
Description: DAGs orchestration for dbt Presentation (OBT/Serving) layer using Astronomer Cosmos.
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
from cosmos import (
    DbtTaskGroup,
    ProjectConfig,
    ProfileConfig,
    ExecutionConfig,
    RenderConfig
)

# ---------------------------------------------------------------------------
# CẤU HÌNH ĐƯỜNG DẪN 
# Giữ nguyên cấu hình env vars theo chuẩn của container
# ---------------------------------------------------------------------------
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/opt/airflow/dbt_transform")
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR", "/opt/airflow/dbt_transform")
DBT_EXECUTABLE_PATH = os.getenv("DBT_EXECUTABLE_PATH", "/usr/local/airflow/.local/bin/dbt")

# ---------------------------------------------------------------------------
# THIẾT LẬP COSMOS CHO DBT
# Sử dụng chung profile lakehouse_dbt[cite: 9]
# ---------------------------------------------------------------------------
profile_config = ProfileConfig(
    profile_name="lakehouse_dbt",
    target_name="dev",
    profiles_yml_filepath=f"{DBT_PROFILES_DIR}/profiles.yml"
)

execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH
)

default_args = {
    "owner": "data_engineer",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

# =======================================================================
# [SENIOR PATTERN] DYNAMIC DAG GENERATION
# Khai báo cấu hình luồng chạy chuyên biệt cho tầng PRESENTATION.
# =======================================================================
dag_configs = [
    {
        "dag_id": "dbt_presentation_incremental",
        "is_full_refresh": False,
        "description": "Incremental pipeline cho tầng Presentation (OBT - Chạy hàng ngày)"
    },
    {
        "dag_id": "dbt_presentation_full_load",
        "is_full_refresh": True,
        "description": "Full Refresh pipeline cho tầng Presentation (Drop và Rebuild toàn bộ OBT)"
    }
]

# Vòng lặp sinh DAG tự động[cite: 9]
for config in dag_configs:
    with DAG(
        dag_id=config["dag_id"],
        default_args=default_args,
        schedule_interval=None,  # Để None, đợi DAG dbt_gold trigger
        start_date=datetime(2023, 1, 1),
        catchup=False,
        is_paused_upon_creation=False,
        tags=["lakehouse", "dbt", "trino", "presentation", "obt", "superset"], # Đã update tags
        description=config["description"]
    ) as dag:

        # =======================================================================
        # TASK GROUP: TẦNG PRESENTATION (OBT)
        # =======================================================================
        transform_presentation = DbtTaskGroup(
            group_id="transform_presentation",
            project_config=ProjectConfig(DBT_PROJECT_DIR),
            profile_config=profile_config,
            execution_config=execution_config,
            
            # [QUAN TRỌNG NHẤT]: Trỏ chính xác vào thư mục con presentation
            render_config=RenderConfig(
                select=["path:models/presentation"]
            ),
            
            # Truyền cấu hình Full Refresh động dựa theo DAG hiện tại[cite: 9]
            operator_args={
                "full_refresh": config["is_full_refresh"]
            }
        )
        
        # Đăng ký DAG vừa tạo vào không gian bộ nhớ toàn cục của Airflow[cite: 9]
        globals()[config["dag_id"]] = dag