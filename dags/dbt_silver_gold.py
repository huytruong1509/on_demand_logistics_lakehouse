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
# (Tùy chỉnh theo cấu trúc thư mục của Airflow Container)
# ---------------------------------------------------------------------------
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/opt/airflow/dbt_transform")
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR", "/opt/airflow/dbt_transform")
DBT_EXECUTABLE_PATH = os.getenv("DBT_EXECUTABLE_PATH", "/usr/local/airflow/.local/bin/dbt")

# ---------------------------------------------------------------------------
# THIẾT LẬP COSMOS CHO DBT
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
# Khai báo cấu hình cho 2 luồng chạy. Airflow sẽ quét list này và tạo 2 DAG.
# =======================================================================
dag_configs = [
    {
        "dag_id": "dbt_silver_gold_incremental",
        "is_full_refresh": False,
        "description": "Incremental pipeline cho Silver và Gold (Chạy hàng ngày)"
    },
    {
        "dag_id": "dbt_silver_gold_full_load",
        "is_full_refresh": True,
        "description": "Full Refresh pipeline cho Silver và Gold (Drop và Rebuild)"
    }
]

# Vòng lặp sinh DAG tự động
for config in dag_configs:
    with DAG(
        dag_id=config["dag_id"],
        default_args=default_args,
        schedule_interval=None,  # Để None để các DAG Ingestion (Bronze) gọi tới
        start_date=datetime(2023, 1, 1),
        catchup=False,
        is_paused_upon_creation=False,
        tags=["lakehouse", "dbt", "trino", "silver", "gold"],
        description=config["description"]
    ) as dag:

        # =======================================================================
        # TASK GROUP 1: TẦNG SILVER (Staging)
        # =======================================================================
        transform_silver = DbtTaskGroup(
            group_id="transform_silver",
            project_config=ProjectConfig(DBT_PROJECT_DIR),
            profile_config=profile_config,
            execution_config=execution_config,
            render_config=RenderConfig(
                select=["path:models/staging"]
            ),
            # Truyền cấu hình Full Refresh động dựa theo DAG hiện tại
            operator_args={
                "full_refresh": config["is_full_refresh"]
            }
        )

        # =======================================================================
        # TASK GROUP 2: TẦNG GOLD (Marts)
        # =======================================================================
        transform_gold = DbtTaskGroup(
            group_id="transform_gold",
            project_config=ProjectConfig(DBT_PROJECT_DIR),
            profile_config=profile_config,
            execution_config=execution_config,
            render_config=RenderConfig(
                select=["path:models/marts"]
            ),
            # Truyền cấu hình Full Refresh động
            operator_args={
                "full_refresh": config["is_full_refresh"]
            }
        )

        # =======================================================================
        # THIẾT LẬP DEPENDENCY
        # =======================================================================
        transform_silver >> transform_gold

        # Đăng ký DAG vừa tạo vào không gian bộ nhớ toàn cục của Airflow
        globals()[config["dag_id"]] = dag