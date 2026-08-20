"""
Logistics Setup Lakehouse Infrastructure DAG

DAG này chỉ chạy 1 lần (hoặc có thể trigger tay khi cần) để đảm bảo các 
Schema và Table Iceberg đã được tạo trên hạ tầng Trino/Nessie.
Hỗ trợ tự động Schema Evolution (thêm cột) dựa trên file cấu hình JSON.
"""

import json
import logging
from pathlib import Path

from airflow.decorators import dag, task
from airflow.providers.trino.hooks.trino import TrinoHook
from airflow.providers.http.sensors.http import HttpSensor
from pendulum import datetime

# Import connection ID dùng chung của hệ thống Logistics
from config import TRINO_CONN_ID

# Trỏ đường dẫn đến file JSON "từ điển" của hệ thống Logistics[cite: 8]
SCHEMA_FILE_PATH = Path(__file__).parent / "schemas" / "logistics_schema.json"

@dag(
    dag_id="logistics_setup_lakehouse_infrastructure",
    start_date=datetime(2026, 1, 1, tz="UTC"),
    schedule_interval="@once", # Chỉ chạy 1 lần khi Airflow nhận diện DAG[cite: 8]
    catchup=False,
    tags=["logistics", "setup", "infrastructure", "iceberg"]
)
def setup_logistics_infrastructure():
    
    # --- TASK 1: SENSOR ĐỢI TRINO SẴN SÀNG ---
    wait_for_trino_ready = HttpSensor(
        task_id="wait_for_trino_ready",
        http_conn_id="trino_api_conn", 
        endpoint="v1/info",
        method="GET",
        response_check=lambda response: response.json().get("starting") is False, # Đợi cho tới khi Trino khởi động xong[cite: 8]
        poke_interval=10, 
        timeout=150,      
        mode="poke",
    )

    # --- TASK 2: THỰC THI DDL TẠO BẢNG LAKEHOUSE ---
    @task
    def deploy_schemas_and_tables():
        """Tạo các Schema và Table, đồng thời tự động đồng bộ Schema Evolution (Thêm cột)[cite: 8]"""
        
        with open(SCHEMA_FILE_PATH, 'r') as f:
            schema_config = json.load(f)

        trino_hook = TrinoHook(trino_conn_id=TRINO_CONN_ID)

        for layer_name, details in schema_config.items():
            # Không cần tạo bảng Trino cho raw JSON ở tầng Landing[cite: 8]
            if layer_name == "landing_layer":
                logging.info("⏭️ BỎ QUA TẦNG LANDING: Không cần tạo bảng Trino cho file raw JSON.")
                continue

            catalog = details['catalog']
            schema = details['schema']

            logging.info(f"=== ĐANG DEPLOY TẦNG: {layer_name.upper()} ===")

            # 1. TẠO SCHEMA[cite: 8]
            trino_hook.run(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
            
            # 2. TẠO BẢNG (Day 1)[cite: 8]
            if 'table' in details and 'columns' in details:
                table = details['table']
                expected_columns = details['columns']
                
                col_definitions = ",\n".join([f"    {col_name} {data_type}" for col_name, data_type in expected_columns.items()])
                
                create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{table} (
                {col_definitions}
                )
                """
                
                # Cấu hình properties (format, partitioning)[cite: 8]
                table_properties = []
                fmt = details.get('format', 'PARQUET')
                table_properties.append(f"format = '{fmt}'")
                
                if 'partitioning' in details:
                    partitions = details['partitioning']
                    formatted_parts = ", ".join([f"'{p}'" for p in partitions])
                    table_properties.append(f"partitioning = ARRAY[{formatted_parts}]")
                
                if table_properties:
                    properties_str = ",\n    ".join(table_properties)
                    create_table_sql += f"\nWITH (\n    {properties_str}\n)"
                
                trino_hook.run(create_table_sql)
                
                # =====================================================================
                # 3. SCHEMA EVOLUTION: TỰ ĐỘNG THÊM CỘT MỚI (Day 2 Operations)[cite: 8]
                # =====================================================================
                logging.info(f"Đang kiểm tra Schema Drift cho bảng {catalog}.{schema}.{table}...")
                
                # Lấy danh sách cột thực tế từ Trino (chuyển về chữ thường để so sánh chuẩn xác)[cite: 8]
                check_cols_sql = f"""
                    SELECT column_name 
                    FROM {catalog}.information_schema.columns 
                    WHERE table_schema = '{schema}' AND table_name = '{table}'
                """
                actual_cols_records = trino_hook.get_records(check_cols_sql)
                actual_cols = {row[0].lower() for row in actual_cols_records}
                
                # Duyệt qua các cột mong muốn từ JSON file[cite: 8]
                for expected_col_name, expected_data_type in expected_columns.items():
                    if expected_col_name.lower() not in actual_cols:
                        logging.warning(f"🚨 Phát hiện cột mới: '{expected_col_name}'. Đang tiến hành ALTER TABLE...")
                        
                        alter_sql = f"""
                            ALTER TABLE {catalog}.{schema}.{table} 
                            ADD COLUMN {expected_col_name} {expected_data_type}
                        """
                        try:
                            trino_hook.run(alter_sql)
                            logging.info(f"✅ Đã thêm cột {expected_col_name} thành công!")
                        except Exception as e:
                            logging.error(f"❌ Lỗi khi thêm cột {expected_col_name}: {e}")
                            raise e

    # =========================================================================
    # THIẾT LẬP LUỒNG PHỤ THUỘC (DEPENDENCY)
    # =========================================================================
    wait_for_trino_ready >> deploy_schemas_and_tables()

logistics_setup_instance = setup_logistics_infrastructure()